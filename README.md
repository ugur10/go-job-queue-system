# Job Queue Go

A compact job queue written in Go to showcase core backend patterns: safe concurrency, worker orchestration, and graceful shutdowns. It relies only on the standard library and keeps everything in-memory so the behaviour is easy to inspect.

## What It Demonstrates
- Concurrency primitives (`context`, goroutines, channels, `sync`) for coordinating work.
- Worker pool with exponential backoff retries and capped attempts.
- Clean separation between queue mechanics, worker coordination, and CLI wiring.
- Unit tests covering queue behaviours, scheduling delays, and worker retry logic.

## Quick Start
1. Run the tests: `go test ./...`
2. Start the CLI (interactive mode): `go run .`
3. Submit a one-off job without the REPL: `go run . submit print "hello world"`

## Architecture Overview
- `queue/` – job model, in-memory queue, and worker pool implementation.
- `examples/` – sample handlers demonstrating logging and transient failures.
- `main.go` – CLI harness that wires the queue, workers, and example handlers together.

## Example Usage
```
$ go run .
Job queue CLI ready. Commands: submit <type> <payload>, status, jobs, help, exit
> submit print hello queue
enqueued job 1 (type=print)
[print] job 1 payload="hello queue"
job 1 finished with state=completed attempts=1
> submit occasionally_fail demo
[flaky] job 2 attempt 1
[flaky] job 2 attempt 2
[flaky] job 2 recovered
job 2 finished with state=completed attempts=2
> status
Queue stats - pending:0 processing:0 completed:2 failed:0
> exit
Exiting...
```

Available job types are defined under `examples/` and can be expanded with your own handlers.
