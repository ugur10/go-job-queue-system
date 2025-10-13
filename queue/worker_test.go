package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// TestWorkerPoolProcessesJob ensures jobs are executed and marked complete.
func TestWorkerPoolProcessesJob(t *testing.T) {
	q := NewQueue(Config{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	processed := make(chan Job, 1)
	pool := NewWorkerPool(q, map[string]Handler{
		"test": func(ctx context.Context, job Job) error {
			processed <- job
			return nil
		},
	}, WorkerConfig{NumWorkers: 2, BaseBackoff: 10 * time.Millisecond, Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	defer pool.Stop()

	job, err := q.Submit(context.Background(), "test", []byte("data"))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatalf("handler was not invoked")
	}

	assertStateEventually(t, q, job.ID, JobCompleted, time.Second)
}

// TestWorkerPoolRetriesUntilSuccess exercises the retry logic until success.
func TestWorkerPoolRetriesUntilSuccess(t *testing.T) {
	q := NewQueue(Config{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var mu sync.Mutex
	attemptLog := make([]int, 0, 3)

	pool := NewWorkerPool(q, map[string]Handler{
		"retry": func(ctx context.Context, job Job) error {
			mu.Lock()
			attemptLog = append(attemptLog, job.Attempts)
			mu.Unlock()

			if job.Attempts < 3 {
				return errors.New("retry")
			}
			return nil
		},
	}, WorkerConfig{NumWorkers: 1, BaseBackoff: 20 * time.Millisecond, Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	defer pool.Stop()

	job, err := q.Submit(context.Background(), "retry", nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	assertStateEventually(t, q, job.ID, JobCompleted, time.Second)

	if len(attemptLog) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(attemptLog))
	}
	for idx, attempt := range attemptLog {
		expected := idx + 1
		if attempt != expected {
			t.Fatalf("expected attempt %d, got %d", expected, attempt)
		}
	}

	snapshot, ok := q.Get(job.ID)
	if !ok {
		t.Fatalf("expected job to exist")
	}
	if snapshot.Attempts != 3 {
		t.Fatalf("expected Attempts to be 3, got %d", snapshot.Attempts)
	}
}

// TestWorkerPoolMarksFailedAfterExhaustingRetries ensures we raise a failed state after max attempts.
func TestWorkerPoolMarksFailedAfterExhaustingRetries(t *testing.T) {
	q := NewQueue(Config{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool := NewWorkerPool(q, map[string]Handler{
		"fail": func(ctx context.Context, job Job) error {
			return errors.New("boom")
		},
	}, WorkerConfig{NumWorkers: 1, BaseBackoff: 10 * time.Millisecond, Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	defer pool.Stop()

	job, err := q.Submit(context.Background(), "fail", nil)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	assertStateEventually(t, q, job.ID, JobFailed, time.Second)

	snapshot, ok := q.Get(job.ID)
	if !ok {
		t.Fatalf("expected job to exist")
	}
	if snapshot.Attempts != snapshot.MaxAttempts {
		t.Fatalf("expected Attempts to equal %d, got %d", snapshot.MaxAttempts, snapshot.Attempts)
	}
	if snapshot.LastError == "" {
		t.Fatalf("expected LastError to be recorded")
	}
}

// assertStateEventually polls the queue until the job reaches the requested state or times out.
func assertStateEventually(t *testing.T, q *Queue, jobID string, state JobState, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, ok := q.Get(jobID)
		if ok && snapshot.State == state {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	snapshot, _ := q.Get(jobID)
	t.Fatalf("job %s did not reach state %s, last snapshot: %+v", jobID, state, snapshot)
}
