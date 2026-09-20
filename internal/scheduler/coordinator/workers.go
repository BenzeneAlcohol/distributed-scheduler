package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/muthuku37/distributed-scheduler/internal/scheduler/domain"
	"github.com/muthuku37/distributed-scheduler/internal/task"
)

var (
	ErrWorkerIDRequired        = errors.New("worker ID is required")
	ErrWorkerCapacityRequired  = errors.New("worker capacity must be greater than zero")
	ErrWorkerAlreadyRegistered = errors.New("worker is already registered")
	ErrWorkerNotRegistered     = errors.New("worker is not registered")
	ErrWorkerHasNoReservedSlot = errors.New("worker has no reserved slot")
	ErrJobNotAssignedToWorker  = errors.New("job is not assigned to worker")
)

// RegisteredWorker describes a worker with an active gRPC connection.
type RegisteredWorker struct {
	ID             string
	Capacity       uint32
	ActiveJobs     uint32
	SupportedTasks []task.Name
}

type workerSession struct {
	RegisteredWorker
	assignments chan<- domain.JobAssignment
	jobIDs      map[domain.JobID]struct{}
}

type workerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*workerSession
}

func newWorkerRegistry() *workerRegistry {
	return &workerRegistry{workers: make(map[string]*workerSession)}
}

// RegisterWorker records a worker for as long as its gRPC stream is active.
func (c *Coordinator) RegisterWorker(worker RegisteredWorker, assignments chan<- domain.JobAssignment) error {
	worker.ID = strings.TrimSpace(worker.ID)
	if worker.ID == "" {
		return ErrWorkerIDRequired
	}
	if worker.Capacity == 0 {
		return ErrWorkerCapacityRequired
	}
	if assignments == nil {
		return errors.New("worker assignment channel is required")
	}

	worker.SupportedTasks = append([]task.Name(nil), worker.SupportedTasks...)
	worker.ActiveJobs = 0

	c.workers.mu.Lock()
	if _, exists := c.workers.workers[worker.ID]; exists {
		c.workers.mu.Unlock()
		return ErrWorkerAlreadyRegistered
	}
	c.workers.workers[worker.ID] = &workerSession{
		RegisteredWorker: worker,
		assignments:      assignments,
		jobIDs:           make(map[domain.JobID]struct{}),
	}
	c.workers.mu.Unlock()

	c.requestDispatch()
	return nil
}

// UnregisterWorker removes a worker and makes its unfinished jobs dispatchable again.
func (c *Coordinator) UnregisterWorker(ctx context.Context, workerID string) error {
	c.workers.mu.Lock()
	session, exists := c.workers.workers[workerID]
	if exists {
		delete(c.workers.workers, workerID)
	}
	c.workers.mu.Unlock()

	if !exists {
		return nil
	}
	if len(session.jobIDs) > 0 {
		if err := c.jobStore.RequeueWorkerJobs(ctx, workerID); err != nil {
			return fmt.Errorf("requeue jobs from worker %s: %w", workerID, err)
		}
	}

	c.requestDispatch()
	return nil
}

// HandleJobUpdate persists a worker state transition and releases terminal jobs.
func (c *Coordinator) HandleJobUpdate(ctx context.Context, workerID string, update domain.JobUpdate) error {
	if update.JobID == "" {
		return errors.New("job ID is required")
	}

	switch update.Status {
	case domain.JobStatusRunning, domain.JobStatusSucceeded, domain.JobStatusFailed:
	default:
		return fmt.Errorf("unsupported worker job status %q", update.Status)
	}

	if !c.workers.hasAssignment(workerID, update.JobID) {
		return fmt.Errorf("%w: job %s", ErrJobNotAssignedToWorker, update.JobID)
	}
	if err := c.jobStore.UpdateJobStatus(ctx, workerID, update); err != nil {
		return fmt.Errorf("update job %s to %s: %w", update.JobID, update.Status, err)
	}

	if update.Status == domain.JobStatusSucceeded || update.Status == domain.JobStatusFailed {
		if !c.workers.completeAssignment(workerID, update.JobID) {
			return fmt.Errorf("%w: job %s", ErrJobNotAssignedToWorker, update.JobID)
		}
		c.requestDispatch()
	}

	return nil
}

// RegisteredWorkers returns a snapshot of the currently connected workers.
func (c *Coordinator) RegisteredWorkers() []RegisteredWorker {
	c.workers.mu.RLock()
	defer c.workers.mu.RUnlock()

	workers := make([]RegisteredWorker, 0, len(c.workers.workers))
	for _, session := range c.workers.workers {
		worker := session.RegisteredWorker
		worker.SupportedTasks = append([]task.Name(nil), worker.SupportedTasks...)
		workers = append(workers, worker)
	}

	return workers
}

func (r *workerRegistry) availableWorkers() []RegisteredWorker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workers := make([]RegisteredWorker, 0, len(r.workers))
	for _, session := range r.workers {
		if session.ActiveJobs >= session.Capacity {
			continue
		}

		worker := session.RegisteredWorker
		worker.SupportedTasks = append([]task.Name(nil), worker.SupportedTasks...)
		workers = append(workers, worker)
	}
	return workers
}

func (r *workerRegistry) reserveSlot(workerID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.workers[workerID]
	if !exists || session.ActiveJobs >= session.Capacity {
		return false
	}
	session.ActiveJobs++
	return true
}

func (r *workerRegistry) releaseSlot(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.workers[workerID]
	if exists && session.ActiveJobs > 0 {
		session.ActiveJobs--
	}
}

func (r *workerRegistry) queueAssignment(workerID string, assignment domain.JobAssignment) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.workers[workerID]
	if !exists {
		return ErrWorkerNotRegistered
	}
	if session.ActiveJobs == 0 {
		return ErrWorkerHasNoReservedSlot
	}

	select {
	case session.assignments <- assignment:
		session.jobIDs[assignment.JobID] = struct{}{}
		return nil
	default:
		return errors.New("worker assignment channel is full")
	}
}

func (r *workerRegistry) hasAssignment(workerID string, jobID domain.JobID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	session, exists := r.workers[workerID]
	if !exists {
		return false
	}
	_, exists = session.jobIDs[jobID]
	return exists
}

func (r *workerRegistry) completeAssignment(workerID string, jobID domain.JobID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, exists := r.workers[workerID]
	if !exists {
		return false
	}
	if _, exists := session.jobIDs[jobID]; !exists {
		return false
	}

	delete(session.jobIDs, jobID)
	if session.ActiveJobs > 0 {
		session.ActiveJobs--
	}
	return true
}
