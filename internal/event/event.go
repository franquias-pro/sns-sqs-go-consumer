package event

import (
	"encoding/json"
	"time"
)

// Event is the SNS message body delivered through SQS.
type Event struct {
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}
