package domain

import (
	"time"

	"github.com/muthuku37/distributed-scheduler/internal/task"
)

// JobID identifies a job throughout its lifetime.
type JobID string

// JobStatus describes the lifecycle state of a job.
type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusAssigned  JobStatus = "assigned"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
)

// Job is an immediate piece of work submitted to the scheduler.
type Job struct {
	ID               JobID
	Task             task.Name
	Payload          map[string]any
	Priority         int32
	Status           JobStatus
	AssignedWorkerID string
	Result           string
	ErrorMessage     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// JobAssignment is delivered to a connected worker after a job is claimed.
type JobAssignment struct {
	JobID   JobID
	Task    task.Name
	Payload map[string]any
}

// JobUpdate reports a state transition made by a worker.
type JobUpdate struct {
	JobID        JobID
	Status       JobStatus
	Result       string
	ErrorMessage string
}
