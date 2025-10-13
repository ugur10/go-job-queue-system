package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Handler processes a job instance.
type Handler func(context.Context, Job) error

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	NumWorkers  int
	BaseBackoff time.Duration
	Logger      *slog.Logger
}

// WorkerPool coordinates a set of workers consuming jobs from the queue.
type WorkerPool struct {
	queue    *Queue
	handlers map[string]Handler
	cfg      WorkerConfig
	logger   *slog.Logger

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
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	handlerCopy := make(map[string]Handler, len(handlers))
	for k, v := range handlers {
		handlerCopy[k] = v
	}

	return &WorkerPool{
		queue:    q,
		handlers: handlerCopy,
		cfg:      cfg,
		logger:   logger,
	}
}

// Start launches the worker goroutines bound to the provided context.
func (p *WorkerPool) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		p.ctx, p.cancel = context.WithCancel(ctx)

		for i := 0; i < p.cfg.NumWorkers; i++ {
			p.wg.Add(1)
			go p.worker(i)
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

// worker loops, reserving jobs from the queue until the shared context is canceled.
func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()

	logger := p.logger.With(slog.Int("worker_id", id))
	logger.Info("worker started")
	defer logger.Info("worker stopped")

	for {
		select {
		case <-p.ctx.Done():
			logger.Debug("worker context done", slog.Any("error", p.ctx.Err()))
			return
		default:
		}

		job, err := p.queue.Reserve(p.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logger.Debug("reserve terminated by context", slog.Any("error", err))
				return
			}
			logger.Warn("reserve failed", slog.Any("error", err))
			continue
		}

		handler := p.handlers[job.Type]
		if handler == nil {
			recordErr := fmt.Errorf("queue: handler missing for type %q", job.Type)
			if _, markErr := p.queue.MarkFailed(job.ID, recordErr); markErr != nil {
				// Queue no longer tracks the job; nothing else we can do.
			}
			logger.Error("missing handler for job type", slog.String("job_type", job.Type), slog.String("job_id", job.ID))
			continue
		}

		jobCtx, jobCancel := context.WithCancel(p.ctx)
		logger.Info("processing job", slog.String("job_id", job.ID), slog.String("job_type", job.Type), slog.Int("attempt", job.Attempts), slog.Int("max_attempts", job.MaxAttempts))
		handleErr := handler(jobCtx, *job)
		jobCancel()

		if handleErr != nil {
			if job.Attempts >= job.MaxAttempts {
				if _, markErr := p.queue.MarkFailed(job.ID, handleErr); markErr != nil {
					// ignore best-effort error
				}
				logger.Error("job failed permanently", slog.String("job_id", job.ID), slog.String("job_type", job.Type), slog.Int("attempt", job.Attempts), slog.Int("max_attempts", job.MaxAttempts), slog.Any("error", handleErr))
				continue
			}

			delay := p.backoff(job.Attempts)
			if _, requeueErr := p.queue.Requeue(job.ID, delay, handleErr); requeueErr != nil {
				// if we cannot requeue, mark as failed so it doesn't disappear silently
				if _, markErr := p.queue.MarkFailed(job.ID, handleErr); markErr != nil {
					// ignore best-effort error
				}
				logger.Error("job failed and could not be requeued", slog.String("job_id", job.ID), slog.String("job_type", job.Type), slog.Int("attempt", job.Attempts), slog.Any("error", requeueErr))
				continue
			}
			logger.Warn("job failed; retry scheduled", slog.String("job_id", job.ID), slog.String("job_type", job.Type), slog.Int("attempt", job.Attempts), slog.Int("max_attempts", job.MaxAttempts), slog.Duration("backoff", delay), slog.Any("error", handleErr))
			continue
		}

		if _, markErr := p.queue.MarkCompleted(job.ID); markErr != nil {
			// best effort; queue might have been cleared.
			logger.Warn("job completed but state update failed", slog.String("job_id", job.ID), slog.Any("error", markErr))
			continue
		}
		logger.Info("job completed", slog.String("job_id", job.ID), slog.String("job_type", job.Type), slog.Int("attempt", job.Attempts))
	}
}

func (p *WorkerPool) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Exponential backoff: double the delay for each successive attempt.
	multiplier := 1 << (attempt - 1)
	return time.Duration(multiplier) * p.cfg.BaseBackoff
}
