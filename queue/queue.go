package queue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrJobNotFound indicates the requested job is not tracked by the queue.
	ErrJobNotFound = errors.New("queue: job not found")
)

// Config configures queue behaviour.
type Config struct {
	// MaxAttempts overrides the default maximum attempts per job.
	MaxAttempts int
}

// Queue maintains in-memory jobs and dispatches them to workers.
type Queue struct {
	mu        sync.RWMutex
	notifyCh  chan struct{}
	jobs      map[string]*Job
	pending   []*Job
	nextID    uint64
	maxTrials int
}

// NewQueue creates a queue using the provided configuration.
func NewQueue(cfg Config) *Queue {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}

	return &Queue{
		notifyCh:  make(chan struct{}),
		jobs:      make(map[string]*Job),
		maxTrials: maxAttempts,
	}
}

// Submit enqueues a new job instance.
func (q *Queue) Submit(ctx context.Context, jobType string, payload []byte) (*Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if jobType == "" {
		return nil, errors.New("queue: job type required")
	}

	now := time.Now()
	payloadCopy := append([]byte(nil), payload...)
	id := strconv.FormatUint(atomic.AddUint64(&q.nextID, 1), 10)

	job := &Job{
		ID:          id,
		Type:        jobType,
		Payload:     payloadCopy,
		State:       JobPending,
		MaxAttempts: q.maxTrials,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	q.mu.Lock()
	q.jobs[id] = job
	q.pending = append(q.pending, job)
	q.signalLocked()
	q.mu.Unlock()

	clone := job.clone()
	return &clone, nil
}

// Reserve retrieves the next pending job, blocking until one is available or ctx ends.
func (q *Queue) Reserve(ctx context.Context) (*Job, error) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			job := q.pending[0]
			q.pending[0] = nil
			q.pending = q.pending[1:]

			job.State = JobProcessing
			job.Attempts++
			job.UpdatedAt = time.Now()

			q.mu.Unlock()

			clone := job.clone()
			return &clone, nil
		}

		wait := q.notifyCh
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
			continue
		}
	}
}

// MarkCompleted records a successful job completion.
func (q *Queue) MarkCompleted(jobID string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}

	job.State = JobCompleted
	job.LastError = ""
	job.UpdatedAt = time.Now()

	clone := job.clone()
	return &clone, nil
}

// MarkFailed records a job failure with its most recent error.
func (q *Queue) MarkFailed(jobID string, err error) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}

	if err != nil {
		job.LastError = err.Error()
	} else {
		job.LastError = ""
	}
	job.State = JobFailed
	job.UpdatedAt = time.Now()

	clone := job.clone()
	return &clone, nil
}

// Get returns a snapshot of the job if present.
func (q *Queue) Get(jobID string) (*Job, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return nil, false
	}

	clone := job.clone()
	return &clone, true
}

// Stats reports the counts of jobs in each state.
func (q *Queue) Stats() Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	stats := Stats{}
	for _, job := range q.jobs {
		switch job.State {
		case JobPending:
			stats.Pending++
		case JobProcessing:
			stats.Processing++
		case JobCompleted:
			stats.Completed++
		case JobFailed:
			stats.Failed++
		}
	}

	return stats
}

// Stats summarises current queue counts.
type Stats struct {
	Pending    int
	Processing int
	Completed  int
	Failed     int
}

func (q *Queue) signalLocked() {
	close(q.notifyCh)
	q.notifyCh = make(chan struct{})
}
