package examples

import (
	"context"
	"encoding/json"
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

// printMessage logs textual or JSON payloads to the console to illustrate handler behaviour.
func printMessage(ctx context.Context, job queue.Job) error {
	if job.IsJSONPayload() {
		var payload any
		if err := job.UnmarshalPayload(&payload); err != nil {
			fmt.Printf("[print] job %s json_error=%v\n", job.ID, err)
		} else {
			pretty, _ := json.MarshalIndent(payload, "", "  ")
			fmt.Printf("[print] job %s json_payload=\n%s\n", job.ID, string(pretty))
		}
	} else {
		message := string(job.Payload)
		if message == "" {
			message = "<empty>"
		}
		fmt.Printf("[print] job %s payload=%q\n", job.ID, message)
	}

	// Simulate some work while remaining cancellable.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
		return nil
	}
}

// occasionalFailure simulates a flaky handler to exercise the retry and backoff flow.
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
