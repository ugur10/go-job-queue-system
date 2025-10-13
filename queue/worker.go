package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Handler processes a job instance.
type Handler func(context.Context, Job) error

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	NumWorkers  int
	BaseBackoff time.Duration
}

// WorkerPool coordinates a set of workers consuming jobs from the queue.
type WorkerPool struct {
	queue    *Queue
	handlers map[string]Handler
	cfg      WorkerConfig

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// NewWorkerPool constructs a worker pool with the supplied handler registry.
func NewWorkerPool(q *Queue, handlers map[string]Handler, cfg WorkerConfig) *WorkerPool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 1
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}

	handlerCopy := make(map[string]Handler, len(handlers))
	for k, v := range handlers {
		handlerCopy[k] = v
	}

	return &WorkerPool{
		queue:    q,
		handlers: handlerCopy,
		cfg:      cfg,
	}
}

// Start launches the worker goroutines bound to the provided context.
func (p *WorkerPool) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		p.ctx, p.cancel = context.WithCancel(ctx)

		for i := 0; i < p.cfg.NumWorkers; i++ {
			p.wg.Add(1)
			go p.worker()
		}
	})
}

// Stop gracefully stops workers and waits for inflight jobs to finish.
func (p *WorkerPool) Stop() {
	p.stopOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		p.wg.Wait()
	})
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		job, err := p.queue.Reserve(p.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			continue
		}

		handler := p.handlers[job.Type]
		if handler == nil {
			recordErr := fmt.Errorf("queue: handler missing for type %q", job.Type)
			if _, markErr := p.queue.MarkFailed(job.ID, recordErr); markErr != nil {
				// Queue no longer tracks the job; nothing else we can do.
			}
			continue
		}

		jobCtx, jobCancel := context.WithCancel(p.ctx)
		handleErr := handler(jobCtx, *job)
		jobCancel()

		if handleErr != nil {
			if job.Attempts >= job.MaxAttempts {
				if _, markErr := p.queue.MarkFailed(job.ID, handleErr); markErr != nil {
					// ignore best-effort error
				}
				continue
			}

			delay := p.backoff(job.Attempts)
			if _, requeueErr := p.queue.Requeue(job.ID, delay, handleErr); requeueErr != nil {
				// if we cannot requeue, mark as failed so it doesn't disappear silently
				if _, markErr := p.queue.MarkFailed(job.ID, handleErr); markErr != nil {
					// ignore best-effort error
				}
			}
			continue
		}

		if _, markErr := p.queue.MarkCompleted(job.ID); markErr != nil {
			// best effort; queue might have been cleared.
		}
	}
}

func (p *WorkerPool) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	multiplier := 1 << (attempt - 1)
	return time.Duration(multiplier) * p.cfg.BaseBackoff
}
