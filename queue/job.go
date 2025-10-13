package queue

import "time"

// JobState captures the execution lifecycle for a job.
type JobState string

const (
	JobPending    JobState = "pending"
	JobProcessing JobState = "processing"
	JobCompleted  JobState = "completed"
	JobFailed     JobState = "failed"
)

const (
	// DefaultMaxAttempts controls how many times a job is retried.
	DefaultMaxAttempts = 3
)

// Job represents a unit of work tracked by the queue.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	State       JobState
	Attempts    int
	MaxAttempts int
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// clone creates an immutable copy to hand out to callers.
func (j *Job) clone() Job {
	payloadCopy := make([]byte, len(j.Payload))
	copy(payloadCopy, j.Payload)

	return Job{
		ID:          j.ID,
		Type:        j.Type,
		Payload:     payloadCopy,
		State:       j.State,
		Attempts:    j.Attempts,
		MaxAttempts: j.MaxAttempts,
		LastError:   j.LastError,
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
	}
}
