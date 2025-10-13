package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"job-queue-go/examples"
	"job-queue-go/queue"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q := queue.NewQueue(queue.Config{})
	pool := queue.NewWorkerPool(q, examples.Handlers(), queue.WorkerConfig{NumWorkers: 2, BaseBackoff: 200 * time.Millisecond})
	pool.Start(ctx)
	defer pool.Stop()

	args := os.Args[1:]
	if len(args) > 0 {
		if err := runCommand(ctx, q, args); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	runREPL(ctx, q)
}

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

func submitCommand(ctx context.Context, q *queue.Queue, jobType, payload string) error {
	job, err := q.Submit(ctx, jobType, []byte(payload))
	if err != nil {
		return err
	}

	fmt.Printf("enqueued job %s (type=%s)\n", job.ID, job.Type)

	final, err := waitForTerminalState(ctx, q, job.ID, 5*time.Second)
	if err != nil {
		return err
	}

	fmt.Printf("job %s finished with state=%s attempts=%d\n", final.ID, final.State, final.Attempts)
	if final.LastError != "" {
		fmt.Printf("last error: %s\n", final.LastError)
	}
	return nil
}

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

func printStatus(q *queue.Queue) {
	stats := q.Stats()
	fmt.Printf("Queue stats - pending:%d processing:%d completed:%d failed:%d\n", stats.Pending, stats.Processing, stats.Completed, stats.Failed)
}

func printJobs(q *queue.Queue) {
	jobs := q.Jobs()
	if len(jobs) == 0 {
		fmt.Println("No jobs tracked yet.")
		return
	}

	for _, job := range jobs {
		line := fmt.Sprintf("[%s] id=%s type=%s attempts=%d", job.State, job.ID, job.Type, job.Attempts)
		if job.LastError != "" {
			line += fmt.Sprintf(" last_error=%q", job.LastError)
		}
		fmt.Println(line)
	}
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  submit <type> <payload>  - Enqueue a job and wait for completion")
	fmt.Println("  status                   - Show high-level queue stats")
	fmt.Println("  jobs                     - List all tracked jobs")
	fmt.Println("  help                     - Display this help message")
	fmt.Println("  exit                     - Quit the CLI")
}
