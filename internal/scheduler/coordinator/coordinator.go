package coordinator

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/muthuku37/distributed-scheduler/internal/scheduler/domain"
	"github.com/muthuku37/distributed-scheduler/internal/task"
)

var (
	ErrTaskRequired    = errors.New("task is required")
	ErrUnsupportedTask = errors.New("unsupported task")
)

type JobStore interface {
	CreateJob(ctx context.Context, job domain.Job) error
	ClaimNextQueuedJob(ctx context.Context, workerID string, supportedTasks []task.Name) (domain.Job, bool, error)
	RequeueJob(ctx context.Context, jobID domain.JobID, workerID string) error
	RequeueWorkerJobs(ctx context.Context, workerID string) error
	UpdateJobStatus(ctx context.Context, workerID string, update domain.JobUpdate) error
}

// Coordinator accepts jobs and dispatches them to connected workers.
type Coordinator struct {
	jobStore        JobStore
	workers         *workerRegistry
	dispatchRequest chan struct{}
}

func New(jobStore JobStore) *Coordinator {
	return &Coordinator{
		jobStore:        jobStore,
		workers:         newWorkerRegistry(),
		dispatchRequest: make(chan struct{}, 1),
	}
}

// SubmitJob creates an immediate job and persists it for future dispatch.
func (c *Coordinator) SubmitJob(ctx context.Context, taskValue string, payload map[string]any, priority int32) (domain.Job, error) {
	if err := ctx.Err(); err != nil {
		return domain.Job{}, err
	}

	taskValue = strings.TrimSpace(taskValue)
	if taskValue == "" {
		return domain.Job{}, ErrTaskRequired
	}
	taskName, ok := task.Parse(taskValue)
	if !ok {
		return domain.Job{}, fmt.Errorf("%w: %q", ErrUnsupportedTask, taskValue)
	}
	if payload == nil {
		payload = make(map[string]any)
	}

	now := time.Now().UTC()
	job := domain.Job{
		ID:        domain.JobID(rand.Text()),
		Task:      taskName,
		Payload:   payload,
		Priority:  priority,
		Status:    domain.JobStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := c.jobStore.CreateJob(ctx, job); err != nil {
		return domain.Job{}, fmt.Errorf("create job: %w", err)
	}

	c.requestDispatch()
	return job, nil
}

// Run dispatches work whenever jobs or worker capacity become available.
func (c *Coordinator) Run(ctx context.Context) {
	c.requestDispatch()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.dispatchRequest:
			if err := c.dispatchAvailable(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("dispatch queued jobs: %v", err)
			}
		}
	}
}

func (c *Coordinator) requestDispatch() {
	select {
	case c.dispatchRequest <- struct{}{}:
	default:
	}
}

func (c *Coordinator) dispatchAvailable(ctx context.Context) error {
	for {
		candidates := c.workers.availableWorkers()
		if len(candidates) == 0 {
			return nil
		}

		assigned := false
		for _, worker := range candidates {
			if !c.workers.reserveSlot(worker.ID) {
				continue
			}

			job, found, err := c.jobStore.ClaimNextQueuedJob(ctx, worker.ID, worker.SupportedTasks)
			if err != nil {
				c.workers.releaseSlot(worker.ID)
				return fmt.Errorf("claim job for worker %s: %w", worker.ID, err)
			}
			if !found {
				c.workers.releaseSlot(worker.ID)
				continue
			}

			assignment := domain.JobAssignment{
				JobID:   job.ID,
				Task:    job.Task,
				Payload: job.Payload,
			}
			if err := c.workers.queueAssignment(worker.ID, assignment); err != nil {
				c.workers.releaseSlot(worker.ID)
				if requeueErr := c.jobStore.RequeueJob(ctx, job.ID, worker.ID); requeueErr != nil {
					return errors.Join(err, fmt.Errorf("requeue job %s: %w", job.ID, requeueErr))
				}
				continue
			}

			log.Printf("assigned job %q (%s) to worker %q", job.ID, job.Task, worker.ID)
			assigned = true
			break
		}

		if !assigned {
			return nil
		}
	}
}
