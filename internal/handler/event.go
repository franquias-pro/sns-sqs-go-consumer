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

type Repository interface {
	Save(context.Context, Event, string) (inserted bool, err error)
}

type Handler struct {
	logger *slog.Logger
	repository Repository
}

func New(logger *slog.Logger, repository Repository) *Handler {
	return &Handler{logger: logger, repository: repository}
}

func (h *Handler) Handle(ctx context.Context, body string) error {
	var event Event
	if err := json.Unmarshal([]byte(body), &event); err != nil { return err }
	if event.EventID == "" || event.Type == "" { return errors.New("event_id and type are required") }
	if err := ctx.Err(); err != nil { return err }

	switch event.Type {
	case "order.created":
	default:
		return errors.New("unsupported event type: " + event.Type)
	}
	inserted, err := h.repository.Save(ctx, event, body)
	if err != nil { return err }
	if inserted {
		h.logger.Debug("event persisted", "event_id", event.EventID)
	} else {
		h.logger.Debug("duplicate event ignored", "event_id", event.EventID)
	}
	return nil
}
