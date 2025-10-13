# Job Queue Go

A compact job queue written in Go to showcase core backend patterns: safe concurrency, worker orchestration, and graceful shutdowns. It relies only on the standard library and keeps everything in-memory so the behaviour is easy to inspect.

## What It Demonstrates
- Concurrency primitives (`context`, goroutines, channels, `sync`) for coordinating work.
- Worker pool with exponential backoff retries and capped attempts.
- Structured logging with `log/slog` around worker lifecycle events.
- Clean separation between queue mechanics, worker coordination, and CLI wiring.
- Unit tests covering queue behaviours, scheduling delays, and worker retry logic.

## Quick Start
1. Run the tests: `go test ./...`
2. Start the CLI (interactive mode): `go run .`
3. Submit a one-off job without the REPL: `go run . submit print "hello world"`
4. Send structured data: `go run . submit print '{"subject":"welcome","user":"ugur"}'`
5. Switch logging modes: `go run . -log-format=json -log-level=debug submit print "hello"`

## Architecture Overview
- `queue/` – job model, in-memory queue, and worker pool implementation.
- `examples/` – sample handlers demonstrating logging and transient failures.
- `main.go` – CLI harness that wires the queue, workers, and example handlers together.

## Example Usage
```
$ go run .
time=2024-10-01T12:00:00Z level=INFO msg="worker started" component=worker_pool worker_id=0
time=2024-10-01T12:00:00Z level=INFO msg="worker started" component=worker_pool worker_id=1
Job queue CLI ready. Commands: submit <type> <payload>, status, jobs, help, exit
> submit print hello queue
enqueued job 1 (type=print)
time=2024-10-01T12:00:01Z level=INFO msg="processing job" component=worker_pool job_id=1 job_type=print attempt=1 max_attempts=3 worker_id=0
[print] job 1 payload="hello queue"
time=2024-10-01T12:00:01Z level=INFO msg="job completed" component=worker_pool job_id=1 job_type=print attempt=1 worker_id=0
job 1 finished with state=completed attempts=1
> submit print '{"subject":"hello","user":"ugur"}'
payload parsed as JSON
enqueued job 2 (type=print)
time=2024-10-01T12:00:02Z level=INFO msg="processing job" component=worker_pool job_id=2 job_type=print attempt=1 max_attempts=3 worker_id=1
[print] job 2 json_payload=
{
  "subject": "hello",
  "user": "ugur"
}
time=2024-10-01T12:00:02Z level=INFO msg="job completed" component=worker_pool job_id=2 job_type=print attempt=1 worker_id=1
job 2 finished with state=completed attempts=1
> status
Queue stats - pending:0 processing:0 completed:2 failed:0
> exit
Exiting...
```

Available job types are defined under `examples/` and can be expanded with your own handlers.

Pass `-log-format=json` or `-log-level=debug` in front of commands when you need different verbosity or machine-readable logs.
