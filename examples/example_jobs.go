package examples

import (
	"context"
	"errors"
	"fmt"
	"time"

	"job-queue-go/queue"
)

const (
	JobTypePrint      = "print"
	JobTypeOccasional = "occasionally_fail"
)

// Handlers returns demonstration job handlers.
func Handlers() map[string]queue.Handler {
	return map[string]queue.Handler{
		JobTypePrint:      printMessage,
		JobTypeOccasional: occasionalFailure,
	}
}

func printMessage(ctx context.Context, job queue.Job) error {
	message := string(job.Payload)
	if message == "" {
		message = "<empty>"
	}
	fmt.Printf("[print] job %s payload=%q\n", job.ID, message)

	// Simulate some work while remaining cancellable.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
		return nil
	}
}

func occasionalFailure(ctx context.Context, job queue.Job) error {
	fmt.Printf("[flaky] job %s attempt %d\n", job.ID, job.Attempts)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(150 * time.Millisecond):
	}

	if job.Attempts < 2 {
		return errors.New("simulated transient error")
	}
	fmt.Printf("[flaky] job %s recovered\n", job.ID)
	return nil
}
