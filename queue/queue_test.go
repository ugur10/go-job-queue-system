package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSubmitStoresJobAndStats ensures submitted jobs are persisted and counted.
func TestSubmitStoresJobAndStats(t *testing.T) {
	q := NewQueue(Config{})

	job, err := q.Submit(context.Background(), "email", []byte("hello"))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if job.ID == "" {
		t.Fatalf("expected job ID to be set")
	}
	if job.State != JobPending {
		t.Fatalf("expected pending state, got %s", job.State)
	}

	stats := q.Stats()
	if stats.Pending != 1 || stats.Processing != 0 || stats.Completed != 0 || stats.Failed != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

// TestSubmitCopiesPayload verifies the queue protects internal payload buffers.
func TestSubmitCopiesPayload(t *testing.T) {
	q := NewQueue(Config{})

	payload := []byte("payload")
	job, err := q.Submit(context.Background(), "copy-test", payload)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	payload[0] = 'X'

	snapshot, ok := q.Get(job.ID)
	if !ok {
		t.Fatalf("expected job to exist")
	}
	if string(snapshot.Payload) != "payload" {
		t.Fatalf("expected payload to remain unchanged, got %q", string(snapshot.Payload))
	}
}

// TestReserveFifoOrdering confirms FIFO semantics and attempt increments.
func TestReserveFifoOrdering(t *testing.T) {
	q := NewQueue(Config{})

	job1, _ := q.Submit(context.Background(), "type", []byte("one"))
	job2, _ := q.Submit(context.Background(), "type", []byte("two"))

	got1, err := q.Reserve(context.Background())
	if err != nil {
		t.Fatalf("Reserve 1 failed: %v", err)
	}
	if got1.Attempts != 1 {
		t.Fatalf("expected attempts to increment, got %d", got1.Attempts)
	}
	if got1.ID != job1.ID {
		t.Fatalf("expected first job %s, got %s", job1.ID, got1.ID)
	}

	got2, err := q.Reserve(context.Background())
	if err != nil {
		t.Fatalf("Reserve 2 failed: %v", err)
	}
	if got2.ID != job2.ID {
		t.Fatalf("expected second job %s, got %s", job2.ID, got2.ID)
	}
}

// TestReserveBlocksUntilJobArrives covers the wait-notify path for idle workers.
func TestReserveBlocksUntilJobArrives(t *testing.T) {
	q := NewQueue(Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type result struct {
		job *Job
		err error
	}
	done := make(chan result)

	go func() {
		job, err := q.Reserve(ctx)
		done <- result{job: job, err: err}
	}()

	select {
	case <-time.After(50 * time.Millisecond):
	case <-done:
		t.Fatalf("reserve returned before job enqueued")
	}

	if _, err := q.Submit(context.Background(), "async", []byte("data")); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("Reserve returned error: %v", res.err)
		}
		if res.job == nil || res.job.Type != "async" {
			t.Fatalf("unexpected job: %+v", res.job)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Reserve did not unblock after submit")
	}
}

// TestReserveHonoursContextCancellation ensures contexts abort cleanly.
func TestReserveHonoursContextCancellation(t *testing.T) {
	q := NewQueue(Config{})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	job, err := q.Reserve(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded error, got %v", err)
	}
	if job != nil {
		t.Fatalf("expected no job, got %+v", job)
	}
}

// TestMarkStateUpdatesStats checks state transitions feed into Stats.
func TestMarkStateUpdatesStats(t *testing.T) {
	q := NewQueue(Config{})

	job, err := q.Submit(context.Background(), "type", []byte("payload"))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if _, err := q.Reserve(context.Background()); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	if _, err := q.MarkCompleted(job.ID); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}

	stats := q.Stats()
	if stats.Completed != 1 || stats.Pending != 0 || stats.Processing != 0 || stats.Failed != 0 {
		t.Fatalf("unexpected stats after completion: %+v", stats)
	}

	job2, _ := q.Submit(context.Background(), "type", []byte("payload"))
	if _, err := q.Reserve(context.Background()); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	if _, err := q.MarkFailed(job2.ID, errors.New("boom")); err != nil {
		t.Fatalf("MarkFailed failed: %v", err)
	}

	stats = q.Stats()
	if stats.Completed != 1 || stats.Failed != 1 {
		t.Fatalf("unexpected stats after failure: %+v", stats)
	}
}

// TestSubmitRequiresJobType guards against anonymous job submissions.
func TestSubmitRequiresJobType(t *testing.T) {
	q := NewQueue(Config{})
	_, err := q.Submit(context.Background(), "", nil)
	if err == nil {
		t.Fatalf("expected error for missing job type")
	}
}

// TestRequeueDelay validates delayed retries and attempt counters.
func TestRequeueDelay(t *testing.T) {
	q := NewQueue(Config{})

	job, err := q.Submit(context.Background(), "retry", []byte("payload"))
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	reserved, err := q.Reserve(context.Background())
	if err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if reserved.ID != job.ID {
		t.Fatalf("expected job %s, got %s", job.ID, reserved.ID)
	}

	delay := 100 * time.Millisecond
	if _, err := q.Requeue(job.ID, delay, errors.New("retry")); err != nil {
		t.Fatalf("Requeue failed: %v", err)
	}

	stats := q.Stats()
	if stats.Pending != 1 || stats.Processing != 0 || stats.Completed != 0 || stats.Failed != 0 {
		t.Fatalf("unexpected stats after requeue: %+v", stats)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type reserveResult struct {
		job *Job
		err error
	}

	results := make(chan reserveResult, 1)
	go func() {
		job, err := q.Reserve(ctx)
		results <- reserveResult{job: job, err: err}
	}()

	select {
	case res := <-results:
		t.Fatalf("Reserve returned before delay: job=%+v err=%v", res.job, res.err)
	case <-time.After(delay / 2):
	}

	select {
	case res := <-results:
		if res.err != nil {
			t.Fatalf("Reserve returned error: %v", res.err)
		}
		if res.job.ID != job.ID {
			t.Fatalf("expected job %s, got %s", job.ID, res.job.ID)
		}
		if res.job.Attempts != 2 {
			t.Fatalf("expected second attempt, got %d", res.job.Attempts)
		}
	case <-time.After(delay + 200*time.Millisecond):
		t.Fatalf("reserve did not return after delay")
	}
}

// TestJobsSnapshot verifies job listings are in order and defensive copied.
func TestJobsSnapshot(t *testing.T) {
	q := NewQueue(Config{})

	job1, _ := q.Submit(context.Background(), "one", []byte("payload-1"))
	time.Sleep(time.Millisecond) // ensure ordering difference
	job2, _ := q.Submit(context.Background(), "two", []byte("payload-2"))

	jobs := q.Jobs()
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if jobs[0].ID != job1.ID || jobs[1].ID != job2.ID {
		t.Fatalf("unexpected job order: %+v", jobs)
	}

	jobs[0].Payload[0] = 'X'
	snapshot, _ := q.Get(job1.ID)
	if string(snapshot.Payload) != "payload-1" {
		t.Fatalf("expected payload copy, got %q", string(snapshot.Payload))
	}
}

// TestJobJSONHelpers ensures Job exposes convenient JSON helpers.
func TestJobJSONHelpers(t *testing.T) {
	job := Job{Payload: []byte(`{"name":"gopher"}`)}
	if !job.IsJSONPayload() {
		t.Fatalf("expected payload to be recognized as JSON")
	}

	var decoded map[string]string
	if err := job.UnmarshalPayload(&decoded); err != nil {
		t.Fatalf("UnmarshalPayload failed: %v", err)
	}
	if decoded["name"] != "gopher" {
		t.Fatalf("unexpected payload value: %+v", decoded)
	}

	job.Payload = []byte("plain text")
	if job.IsJSONPayload() {
		t.Fatalf("expected IsJSONPayload to be false")
	}
	if err := job.UnmarshalPayload(&decoded); !errors.Is(err, ErrPayloadNotJSON) {
		t.Fatalf("expected ErrPayloadNotJSON, got %v", err)
	}
}
