package job

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Job struct {
	ID          string          `json:"id"`
	Payload     json.RawMessage `json:"payload"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

func New(payload json.RawMessage, maxAttempts int) *Job {
	return &Job{
		ID:          newID(),
		Payload:     payload,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
	}
}

func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {

		panic(err)
	}
	return hex.EncodeToString(b)
}

func (j *Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func Unmarshal(data []byte) (*Job, error) {
	var j Job

	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	return &j, nil
}
