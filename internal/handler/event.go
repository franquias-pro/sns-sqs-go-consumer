package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
)

type Event struct {
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

type Handler struct { logger *slog.Logger }

func New(logger *slog.Logger) *Handler { return &Handler{logger: logger} }

func (h *Handler) Handle(ctx context.Context, body string) error {
	var event Event
	if err := json.Unmarshal([]byte(body), &event); err != nil { return err }
	if event.EventID == "" || event.Type == "" { return errors.New("event_id and type are required") }
	if err := ctx.Err(); err != nil { return err }

	// Replace this branch with an idempotent write or business action. A standard
	// SQS queue can deliver the same event more than once.
	switch event.Type {
	case "order.created":
		h.logger.Debug("order event handled", "event_id", event.EventID)
	default:
		return errors.New("unsupported event type: " + event.Type)
	}
	return nil
}
