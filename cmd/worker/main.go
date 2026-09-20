package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	schedulerv1 "github.com/muthuku37/distributed-scheduler/gen/scheduler/v1"
	"github.com/muthuku37/distributed-scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultSchedulerAddress = "localhost:9090"
	defaultWorkerCapacity   = uint32(1)
	heartbeatInterval       = 10 * time.Second
)

type jobAssignment struct {
	jobID   string
	task    worker.Task
	payload worker.Payload
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	workerID, err := workerIDFromEnvironment()
	if err != nil {
		return err
	}

	capacity, err := workerCapacityFromEnvironment()
	if err != nil {
		return err
	}

	schedulerAddress := os.Getenv("WORKER_SCHEDULER_ADDRESS")
	if schedulerAddress == "" {
		schedulerAddress = defaultSchedulerAddress
	}

	workerContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	connection, err := grpc.NewClient(
		schedulerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		stop()
		return fmt.Errorf("create scheduler connection: %w", err)
	}
	defer connection.Close()

	stream, err := schedulerv1.NewWorkerGatewayClient(connection).Connect(workerContext)
	if err != nil {
		stop()
		return fmt.Errorf("connect to scheduler: %w", err)
	}

	if err := stream.Send(registrationEvent(workerID, capacity)); err != nil {
		stop()
		return fmt.Errorf("register worker: %w", err)
	}
	log.Printf("worker %q registered with scheduler at %s", workerID, schedulerAddress)

	assignments := make(chan jobAssignment, capacity)
	outboundEvents := make(chan *schedulerv1.WorkerEvent, capacity*3)
	receiveError := make(chan error, 1)
	go receiveCommands(workerContext, stream, assignments, receiveError)

	executor := worker.NewExecutor()
	var executionWorkers sync.WaitGroup
	for range capacity {
		executionWorkers.Add(1)
		go executeJobs(workerContext, &executionWorkers, workerID, executor, assignments, outboundEvents)
	}
	defer func() {
		stop()
		executionWorkers.Wait()
	}()

	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-workerContext.Done():
			return nil
		case err := <-receiveError:
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("receive scheduler command: %w", err)
		case event := <-outboundEvents:
			if err := stream.Send(event); err != nil {
				return fmt.Errorf("send job update: %w", err)
			}
		case <-heartbeat.C:
			if err := stream.Send(heartbeatEvent()); err != nil {
				return fmt.Errorf("send worker heartbeat: %w", err)
			}
		}
	}
}

func registrationEvent(workerID string, capacity uint32) *schedulerv1.WorkerEvent {
	return &schedulerv1.WorkerEvent{
		Event: &schedulerv1.WorkerEvent_Register{
			Register: &schedulerv1.RegisterWorker{
				WorkerId: workerID,
				Capacity: capacity,
				SupportedTasks: []schedulerv1.TaskType{
					schedulerv1.TaskType_TASK_TYPE_GENERATE_REPORT,
					schedulerv1.TaskType_TASK_TYPE_SEND_EMAIL,
					schedulerv1.TaskType_TASK_TYPE_SEND_NOTIFICATION,
				},
			},
		},
	}
}

func heartbeatEvent() *schedulerv1.WorkerEvent {
	return &schedulerv1.WorkerEvent{
		Event: &schedulerv1.WorkerEvent_Heartbeat{
			Heartbeat: &schedulerv1.Heartbeat{},
		},
	}
}

func receiveCommands(
	ctx context.Context,
	stream grpc.BidiStreamingClient[schedulerv1.WorkerEvent, schedulerv1.CoordinatorCommand],
	assignments chan<- jobAssignment,
	receiveError chan<- error,
) {
	for {
		command, err := stream.Recv()
		if err != nil {
			receiveError <- err
			return
		}

		assignment := command.GetAssignment()
		if assignment == nil {
			continue
		}
		job, err := assignmentFromProto(assignment)
		if err != nil {
			receiveError <- err
			return
		}

		select {
		case assignments <- job:
		case <-ctx.Done():
			return
		}
	}
}

func assignmentFromProto(assignment *schedulerv1.JobAssignment) (jobAssignment, error) {
	if assignment.GetJobId() == "" {
		return jobAssignment{}, errors.New("received assignment without a job ID")
	}

	var taskName worker.Task
	switch assignment.GetTask() {
	case schedulerv1.TaskType_TASK_TYPE_GENERATE_REPORT:
		taskName = worker.TaskGenerateReport
	case schedulerv1.TaskType_TASK_TYPE_SEND_EMAIL:
		taskName = worker.TaskSendEmail
	case schedulerv1.TaskType_TASK_TYPE_SEND_NOTIFICATION:
		taskName = worker.TaskSendNotification
	default:
		return jobAssignment{}, fmt.Errorf("received unsupported task type %s", assignment.GetTask())
	}

	payload := make(worker.Payload)
	if assignment.GetPayload() != nil {
		payload = assignment.GetPayload().AsMap()
	}
	return jobAssignment{
		jobID:   assignment.GetJobId(),
		task:    taskName,
		payload: payload,
	}, nil
}

func executeJobs(
	ctx context.Context,
	waitGroup *sync.WaitGroup,
	workerID string,
	executor *worker.Executor,
	assignments <-chan jobAssignment,
	outboundEvents chan<- *schedulerv1.WorkerEvent,
) {
	defer waitGroup.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case assignment := <-assignments:
			log.Printf("worker %q started job %q (%s)", workerID, assignment.jobID, assignment.task)
			if !sendEvent(ctx, outboundEvents, jobUpdateEvent(assignment.jobID, schedulerv1.JobState_JOB_STATE_RUNNING, "", "")) {
				return
			}

			result, err := executor.Execute(ctx, assignment.task, assignment.payload)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("worker %q failed job %q: %v", workerID, assignment.jobID, err)
				if !sendEvent(ctx, outboundEvents, jobUpdateEvent(assignment.jobID, schedulerv1.JobState_JOB_STATE_FAILED, "", err.Error())) {
					return
				}
				continue
			}

			log.Printf("worker %q completed job %q", workerID, assignment.jobID)
			if !sendEvent(ctx, outboundEvents, jobUpdateEvent(assignment.jobID, schedulerv1.JobState_JOB_STATE_SUCCEEDED, result, "")) {
				return
			}
		}
	}
}

func sendEvent(ctx context.Context, events chan<- *schedulerv1.WorkerEvent, event *schedulerv1.WorkerEvent) bool {
	select {
	case events <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func jobUpdateEvent(jobID string, state schedulerv1.JobState, output, errorMessage string) *schedulerv1.WorkerEvent {
	return &schedulerv1.WorkerEvent{
		Event: &schedulerv1.WorkerEvent_JobUpdate{
			JobUpdate: &schedulerv1.JobUpdate{
				JobId:        jobID,
				State:        state,
				Output:       output,
				ErrorMessage: errorMessage,
			},
		},
	}
}

func workerIDFromEnvironment() (string, error) {
	if workerID := os.Getenv("WORKER_ID"); workerID != "" {
		return workerID, nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("determine worker ID from hostname: %w", err)
	}
	return hostname, nil
}

func workerCapacityFromEnvironment() (uint32, error) {
	value := os.Getenv("WORKER_CAPACITY")
	if value == "" {
		return defaultWorkerCapacity, nil
	}

	capacity, err := strconv.ParseUint(value, 10, 32)
	if err != nil || capacity == 0 {
		return 0, errors.New("WORKER_CAPACITY must be a positive integer")
	}
	return uint32(capacity), nil
}
