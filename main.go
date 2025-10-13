package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"job-queue-go/examples"
	"job-queue-go/queue"
)

// main wires the CLI entry point, configures logging, and starts the worker pool.
func main() {
	logFormat := flag.String("log-format", "text", "log output format (text or json)")
	logLevel := flag.String("log-level", "info", "log verbosity (debug, info, warn, error)")
	flag.Parse()

	// Build the root logger once so every component shares the same configuration.
	logger, err := buildLogger(*logLevel, *logFormat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid logging configuration: %v\n", err)
		os.Exit(1)
	}
	// Make the configured logger globally available so queue workers share the same output sink.
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q := queue.NewQueue(queue.Config{})
	workerLogger := logger.With(slog.String("component", "worker_pool"))
	pool := queue.NewWorkerPool(q, examples.Handlers(), queue.WorkerConfig{NumWorkers: 2, BaseBackoff: 200 * time.Millisecond, Logger: workerLogger})
	pool.Start(ctx)
	defer pool.Stop()

	// Treat any remaining arguments as CLI subcommands (submit/status/jobs).
	args := flag.Args()
	if len(args) > 0 {
		if err := runCommand(ctx, q, args); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	runREPL(ctx, q)
}

// runCommand handles single-shot invocations such as "submit" or "status".
func runCommand(ctx context.Context, q *queue.Queue, args []string) error {
	switch args[0] {
	case "submit":
		if len(args) < 3 {
			return errors.New("usage: submit <job_type> <payload>")
		}
		jobType := args[1]
		payload := strings.Join(args[2:], " ")
		return submitCommand(ctx, q, jobType, payload)
	case "status":
		printStatus(q)
		return nil
	case "jobs":
		printJobs(q)
		return nil
	case "help":
		printHelp()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// runREPL provides an interactive shell for exploring the queue.
func runREPL(ctx context.Context, q *queue.Queue) {
	fmt.Println("Job queue CLI ready. Commands: submit <type> <payload>, status, jobs, help, exit")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "input error: %v\n", err)
			}
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch {
		case line == "status":
			printStatus(q)
		case line == "jobs":
			printJobs(q)
		case line == "help":
			printHelp()
		case line == "exit" || line == "quit":
			fmt.Println("Exiting...")
			return
		case strings.HasPrefix(line, "submit"):
			if err := handleSubmitLine(ctx, q, line); err != nil {
				fmt.Fprintf(os.Stderr, "submit error: %v\n", err)
			}
		default:
			fmt.Fprintf(os.Stderr, "unknown command %q\n", line)
		}
	}
}

// handleSubmitLine parses a free-form submit command from the REPL.
func handleSubmitLine(ctx context.Context, q *queue.Queue, line string) error {
	trimmed := strings.TrimSpace(line[len("submit"):])
	if trimmed == "" {
		return errors.New("usage: submit <job_type> <payload>")
	}

	parts := strings.SplitN(strings.TrimSpace(trimmed), " ", 2)
	if len(parts) < 2 {
		return errors.New("usage: submit <job_type> <payload>")
	}

	jobType := strings.TrimSpace(parts[0])
	payload := strings.TrimSpace(parts[1])
	return submitCommand(ctx, q, jobType, payload)
}

// submitCommand normalises payloads, enqueues the job, and waits for its result.
func submitCommand(ctx context.Context, q *queue.Queue, jobType, payload string) error {
	payloadBytes, isJSON, err := preparePayload(payload)
	if err != nil {
		return err
	}

	job, err := q.Submit(ctx, jobType, payloadBytes)
	if err != nil {
		return err
	}

	fmt.Printf("enqueued job %s (type=%s)\n", job.ID, job.Type)
	if isJSON {
		fmt.Println("payload parsed as JSON")
	}
	slog.Default().Info("job submitted", slog.String("job_id", job.ID), slog.String("job_type", job.Type), slog.Bool("payload_json", isJSON))

	final, err := waitForTerminalState(ctx, q, job.ID, 5*time.Second)
	if err != nil {
		return err
	}

	fmt.Printf("job %s finished with state=%s attempts=%d\n", final.ID, final.State, final.Attempts)
	if final.LastError != "" {
		fmt.Printf("last error: %s\n", final.LastError)
	}
	if final.IsJSONPayload() {
		// Mirror the worker-side insight so the CLI caller knows the payload shape we detected.
		fmt.Println("job payload is JSON")
	}
	return nil
}

// buildLogger prepares a slog.Logger using the caller's desired verbosity and encoding.
func buildLogger(levelStr, formatStr string) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", levelStr)
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch strings.ToLower(formatStr) {
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	default:
		return nil, fmt.Errorf("unsupported log format %q", formatStr)
	}

	return slog.New(handler), nil
}

// preparePayload normalises raw user input, validating JSON when applicable.
func preparePayload(raw string) ([]byte, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return []byte{}, false, nil
	}

	var js any
	if err := json.Unmarshal([]byte(trimmed), &js); err == nil {
		normalized, err := json.Marshal(js)
		if err != nil {
			return nil, false, err
		}
		return normalized, true, nil
	} else if looksLikeJSON(trimmed) {
		return nil, false, fmt.Errorf("invalid JSON payload: %w", err)
	}

	return []byte(raw), false, nil
}

// looksLikeJSON performs a light heuristic so we can surface JSON parsing errors early.
func looksLikeJSON(s string) bool {
	if s == "" {
		return false
	}
	switch s[0] {
	case '{', '[', '"':
		return true
	default:
		return false
	}
}

// waitForTerminalState polls the queue until the job completes, fails, or times out.
func waitForTerminalState(ctx context.Context, q *queue.Queue, jobID string, timeout time.Duration) (queue.Job, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return queue.Job{}, ctx.Err()
		case <-ticker.C:
			job, ok := q.Get(jobID)
			if ok && (job.State == queue.JobCompleted || job.State == queue.JobFailed) {
				return *job, nil
			}
			if time.Now().After(deadline) {
				return queue.Job{}, fmt.Errorf("job %s did not finish within %s", jobID, timeout)
			}
		}
	}
}

// printStatus reports high-level job counts.
func printStatus(q *queue.Queue) {
	stats := q.Stats()
	fmt.Printf("Queue stats - pending:%d processing:%d completed:%d failed:%d\n", stats.Pending, stats.Processing, stats.Completed, stats.Failed)
}

// printJobs lists known jobs with helpful markers for JSON payloads and errors.
func printJobs(q *queue.Queue) {
	jobs := q.Jobs()
	if len(jobs) == 0 {
		fmt.Println("No jobs tracked yet.")
		return
	}

	for _, job := range jobs {
		line := fmt.Sprintf("[%s] id=%s type=%s attempts=%d", job.State, job.ID, job.Type, job.Attempts)
		if job.IsJSONPayload() {
			line += " payload=json"
		}
		if job.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", job.LastError)
		}
		fmt.Println(line)
	}
}

// printHelp summarises supported commands and runtime flags.
func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  submit <type> <payload>  - Enqueue a job and wait for completion")
	fmt.Println("  status                   - Show high-level queue stats")
	fmt.Println("  jobs                     - List all tracked jobs")
	fmt.Println("  help                     - Display this help message")
	fmt.Println("  exit                     - Quit the CLI")
	fmt.Println("Flags (pass before commands): -log-format=text|json, -log-level=debug|info|warn|error")
	fmt.Println("Payloads: provide plain strings or JSON (objects/arrays must be quoted in the shell)")
}
