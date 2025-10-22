# go-job-queue-system

An in-memory job queue implemented with Go's standard library. The project focuses on concurrency primitives, worker orchestration, retry policies, and graceful shutdown—all without external dependencies.

---

## Quick Links
- [Highlights](#highlights)
- [Architecture Overview](#architecture-overview)
- [Getting Started](#getting-started)
- [CLI Usage](#cli-usage)
- [Commands](#commands)
- [Logging Options](#logging-options)
- [Examples](#examples)
- [Testing](#testing)
- [Project Layout](#project-layout)
- [License](#license)

---

## Highlights
- Worker pool with configurable concurrency, retry attempts, and exponential backoff.
- Thread-safe queue storing jobs, metadata, and state transitions in memory.
- Context-driven cancellation and graceful shutdown using `signal.NotifyContext`.
- Structured logging with `log/slog` for lifecycle visibility.
- Extensible handler registry showcasing print and flaky demo jobs.

## Architecture Overview
- **Workers** pull jobs from the queue, mark state transitions, and execute registered handlers.
- **Scheduler** applies retry policies (max attempts + exponential delay) for transient failures.
- **Queue** maintains pending, processing, completed, and failed lists with `sync.RWMutex`.
- **CLI** wires everything together, allowing interactive commands or single-shot invocations.

---

## Getting Started
```bash
git clone https://github.com/ugur10/go-job-queue-system.git
cd go-job-queue-system
go run .
```

The CLI starts an interactive REPL by default. Type `help` to see available commands.

Run a one-off command without entering the REPL:
```bash
go run . submit print "hello world"
```

---

## CLI Usage
```
$ go run .
worker started (id=0)
worker started (id=1)
Job queue CLI ready. Commands: submit <type> <payload>, status, jobs, help, exit
> submit print hello queue
enqueued job 1 (type=print)
[print] job 1 payload="hello queue"
job 1 finished with state=completed attempts=1
> status
Queue stats - pending:0 processing:0 completed:1 failed:0
> exit
Exiting...
```

### Commands
| Command | Description |
| ------- | ----------- |
| `submit <type> <payload>` | Enqueue a job with the specified handler type and payload (plain text or JSON). |
| `status` | Display queue statistics (pending, processing, completed, failed). |
| `jobs` | List recent jobs with state, attempts, and timestamps. |
| `help` | Show command usage. |
| `exit` | Stop the CLI and trigger graceful shutdown. |

Handlers live in [`examples/`](examples) and can be extended with additional job types.

---

## Logging Options
Use flags in front of commands to control output:
```bash
# JSON logs with debug verbosity
go run . -log-format=json -log-level=debug submit print "hello"

# Default text logs at info level
go run . submit print '{"subject":"welcome","user":"ugur"}'
```

Log lines include worker IDs, job metadata, attempts, and retry delays to simplify debugging.

---

## Examples
The [`examples/`](examples) package includes:
- `print` — echoes payloads (text or JSON) to stdout.
- `flaky` — simulates transient failures to exercise backoff and retries.

Register your own handlers by extending the map in `main.go`.

---

## Testing
Run the full suite:
```bash
go test ./...
```

Tests cover queue operations, scheduling behaviour, and worker retry logic.

---

## Project Layout
```
examples/      # Sample job handlers
queue/         # Queue model, scheduler, worker pool
main.go        # CLI wiring, signal handling, logging setup
```

---

## License
Distributed under the [MIT License](LICENSE).
