package grpcapi

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	schedulerv1 "github.com/muthuku37/distributed-scheduler/gen/scheduler/v1"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/coordinator"
	"github.com/muthuku37/distributed-scheduler/internal/scheduler/domain"
	"github.com/muthuku37/distributed-scheduler/internal/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const workerCleanupTimeout = 5 * time.Second

type workerCoordinator interface {
	RegisterWorker(worker coordinator.RegisteredWorker, assignments chan<- domain.JobAssignment) error
	UnregisterWorker(ctx context.Context, workerID string) error
	HandleJobUpdate(ctx context.Context, workerID string, update domain.JobUpdate) error
}

// Server accepts long-lived connections initiated by workers.
type Server struct {
	schedulerv1.UnimplementedWorkerGatewayServer
	coordinator workerCoordinator
}

func NewServer(coordinator workerCoordinator) *Server {
	return &Server{coordinator: coordinator}
}

func (s *Server) Connect(stream grpc.BidiStreamingServer[schedulerv1.WorkerEvent, schedulerv1.CoordinatorCommand]) error {
	firstEvent, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "receive worker registration: %v", err)
	}

	registration := firstEvent.GetRegister()
	if registration == nil {
		return status.Error(codes.FailedPrecondition, "the first worker event must be a registration")
	}

	worker, err := registeredWorker(registration)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	assignments := make(chan domain.JobAssignment, worker.Capacity)
	if err := s.coordinator.RegisterWorker(worker, assignments); err != nil {
		if errors.Is(err, coordinator.ErrWorkerAlreadyRegistered) {
			return status.Error(codes.AlreadyExists, err.Error())
		}
		return status.Error(codes.InvalidArgument, err.Error())
	}
	defer s.unregisterWorker(worker.ID)

	log.Printf("worker %q connected with capacity %d and tasks %v", worker.ID, worker.Capacity, worker.SupportedTasks)
	defer log.Printf("worker %q disconnected", worker.ID)

	received := make(chan receivedWorkerEvent, 1)
	go receiveWorkerEvents(stream, received)

	for {
		select {
		case assignment := <-assignments:
			command, err := assignmentCommand(assignment)
			if err != nil {
				return status.Errorf(codes.Internal, "encode job assignment: %v", err)
			}
			if err := stream.Send(command); err != nil {
				return err
			}
		case incoming := <-received:
			if errors.Is(incoming.err, io.EOF) {
				return nil
			}
			if incoming.err != nil {
				return incoming.err
			}
			if err := s.handleWorkerEvent(stream.Context(), worker.ID, incoming.event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

type receivedWorkerEvent struct {
	event *schedulerv1.WorkerEvent
	err   error
}

func receiveWorkerEvents(
	stream grpc.BidiStreamingServer[schedulerv1.WorkerEvent, schedulerv1.CoordinatorCommand],
	received chan<- receivedWorkerEvent,
) {
	for {
		event, err := stream.Recv()
		select {
		case received <- receivedWorkerEvent{event: event, err: err}:
		case <-stream.Context().Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *Server) handleWorkerEvent(ctx context.Context, workerID string, event *schedulerv1.WorkerEvent) error {
	switch {
	case event.GetHeartbeat() != nil:
		return nil
	case event.GetJobUpdate() != nil:
		update, err := jobUpdate(event.GetJobUpdate())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if err := s.coordinator.HandleJobUpdate(ctx, workerID, update); err != nil {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		log.Printf("worker %q moved job %q to %s", workerID, update.JobID, update.Status)
		return nil
	case event.GetRegister() != nil:
		return status.Error(codes.FailedPrecondition, "worker is already registered on this stream")
	default:
		return status.Error(codes.InvalidArgument, "worker event is empty")
	}
}

func (s *Server) unregisterWorker(workerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), workerCleanupTimeout)
	defer cancel()

	if err := s.coordinator.UnregisterWorker(ctx, workerID); err != nil {
		log.Printf("unregister worker %q: %v", workerID, err)
	}
}

func registeredWorker(registration *schedulerv1.RegisterWorker) (coordinator.RegisteredWorker, error) {
	workerID := strings.TrimSpace(registration.GetWorkerId())
	if workerID == "" {
		return coordinator.RegisteredWorker{}, coordinator.ErrWorkerIDRequired
	}
	if registration.GetCapacity() == 0 {
		return coordinator.RegisteredWorker{}, coordinator.ErrWorkerCapacityRequired
	}

	supportedTasks := make([]task.Name, 0, len(registration.GetSupportedTasks()))
	seenTasks := make(map[task.Name]struct{}, len(registration.GetSupportedTasks()))
	for _, taskType := range registration.GetSupportedTasks() {
		taskName, ok := taskNameFromProto(taskType)
		if !ok {
			return coordinator.RegisteredWorker{}, errors.New("worker contains an unsupported task type")
		}
		if _, exists := seenTasks[taskName]; exists {
			continue
		}

		seenTasks[taskName] = struct{}{}
		supportedTasks = append(supportedTasks, taskName)
	}
	if len(supportedTasks) == 0 {
		return coordinator.RegisteredWorker{}, errors.New("worker must support at least one task")
	}

	return coordinator.RegisteredWorker{
		ID:             workerID,
		Capacity:       registration.GetCapacity(),
		SupportedTasks: supportedTasks,
	}, nil
}

func assignmentCommand(assignment domain.JobAssignment) (*schedulerv1.CoordinatorCommand, error) {
	payload, err := structpb.NewStruct(assignment.Payload)
	if err != nil {
		return nil, err
	}
	taskType, ok := taskNameToProto(assignment.Task)
	if !ok {
		return nil, errors.New("unsupported task in job assignment")
	}

	return &schedulerv1.CoordinatorCommand{
		Command: &schedulerv1.CoordinatorCommand_Assignment{
			Assignment: &schedulerv1.JobAssignment{
				JobId:   string(assignment.JobID),
				Task:    taskType,
				Payload: payload,
			},
		},
	}, nil
}

func jobUpdate(update *schedulerv1.JobUpdate) (domain.JobUpdate, error) {
	if strings.TrimSpace(update.GetJobId()) == "" {
		return domain.JobUpdate{}, errors.New("job update requires a job ID")
	}

	var statusValue domain.JobStatus
	switch update.GetState() {
	case schedulerv1.JobState_JOB_STATE_RUNNING:
		statusValue = domain.JobStatusRunning
	case schedulerv1.JobState_JOB_STATE_SUCCEEDED:
		statusValue = domain.JobStatusSucceeded
	case schedulerv1.JobState_JOB_STATE_FAILED:
		statusValue = domain.JobStatusFailed
	default:
		return domain.JobUpdate{}, errors.New("unsupported job update state")
	}

	return domain.JobUpdate{
		JobID:        domain.JobID(update.GetJobId()),
		Status:       statusValue,
		Result:       update.GetOutput(),
		ErrorMessage: update.GetErrorMessage(),
	}, nil
}

func taskNameFromProto(taskType schedulerv1.TaskType) (task.Name, bool) {
	switch taskType {
	case schedulerv1.TaskType_TASK_TYPE_GENERATE_REPORT:
		return task.GenerateReport, true
	case schedulerv1.TaskType_TASK_TYPE_SEND_EMAIL:
		return task.SendEmail, true
	case schedulerv1.TaskType_TASK_TYPE_SEND_NOTIFICATION:
		return task.SendNotification, true
	default:
		return "", false
	}
}

func taskNameToProto(taskName task.Name) (schedulerv1.TaskType, bool) {
	switch taskName {
	case task.GenerateReport:
		return schedulerv1.TaskType_TASK_TYPE_GENERATE_REPORT, true
	case task.SendEmail:
		return schedulerv1.TaskType_TASK_TYPE_SEND_EMAIL, true
	case task.SendNotification:
		return schedulerv1.TaskType_TASK_TYPE_SEND_NOTIFICATION, true
	default:
		return schedulerv1.TaskType_TASK_TYPE_UNSPECIFIED, false
	}
}
