package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/muthuku37/distributed-scheduler/internal/task"
)

// Task identifies a kind of work this worker knows how to execute.
type Task = task.Name

const (
	TaskGenerateReport   = task.GenerateReport
	TaskSendEmail        = task.SendEmail
	TaskSendNotification = task.SendNotification
	jobCompletedMessage  = "Job done successfully"
)

// Payload is the input object supplied with a job.
type Payload map[string]any

var ErrUnsupportedTask = errors.New("unsupported task")

// Executor dispatches an assigned job to its task implementation.
type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

// Execute selects the implementation for the assigned task.
func (e *Executor) Execute(ctx context.Context, task Task, payload Payload) (string, error) {
	switch task {
	case TaskGenerateReport:
		return e.GenerateReport(ctx, payload)
	case TaskSendEmail:
		return e.SendEmail(ctx, payload)
	case TaskSendNotification:
		return e.SendNotification(ctx, payload)
	default:
		return "", fmt.Errorf("%w: %q", ErrUnsupportedTask, task)
	}
}

// GenerateReport is a placeholder for report generation.
func (e *Executor) GenerateReport(ctx context.Context, payload Payload) (string, error) {
	return completeDummyTask(ctx, payload)
}

// SendEmail is a placeholder for sending an email.
func (e *Executor) SendEmail(ctx context.Context, payload Payload) (string, error) {
	return completeDummyTask(ctx, payload)
}

// SendNotification is a placeholder for sending a notification.
func (e *Executor) SendNotification(ctx context.Context, payload Payload) (string, error) {
	return completeDummyTask(ctx, payload)
}

func completeDummyTask(ctx context.Context, payload Payload) (string, error) {
	// The dummy implementations accept a payload but do not inspect it yet.
	_ = payload

	delay := time.Duration(rand.IntN(9)+2) * time.Second
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return jobCompletedMessage, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
