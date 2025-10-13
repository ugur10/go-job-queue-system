package queue

import (
	"encoding/json"
	"errors"
	"time"
)

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

// ErrPayloadNotJSON indicates a payload is not valid JSON when JSON operations are requested.
var ErrPayloadNotJSON = errors.New("queue: payload is not valid JSON")

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
	ReadyAt     time.Time
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
		ReadyAt:     j.ReadyAt,
	}
}

// IsJSONPayload reports whether the payload is valid JSON.
func (j Job) IsJSONPayload() bool {
	return json.Valid(j.Payload)
}

// UnmarshalPayload decodes the payload into v if it contains valid JSON.
func (j Job) UnmarshalPayload(v any) error {
	if !j.IsJSONPayload() {
		return ErrPayloadNotJSON
	}
	return json.Unmarshal(j.Payload, v)
}
