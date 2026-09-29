package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Event struct {
	EventID    string          `json:"event_id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

type orderData struct {
	OrderID string `json:"order_id"`
}

// Repository exposes the persistence operations used by the event commands.
type Repository interface {
	Create(context.Context, Event, string, string) (inserted bool, err error)
	DeleteByOrderID(context.Context, string) (deleted int64, err error)
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
	if err := json.Unmarshal([]byte(body), &event); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	if event.EventID == "" || event.Type == "" {
		return errors.New("event_id and type are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	switch event.Type {
	case "order.created":
		data, err := decodeOrderData(event.Data)
		if err != nil { return err }
		inserted, err := h.repository.Create(ctx, event, data.OrderID, body)
		if err != nil { return err }
		if inserted {
			h.logger.Debug("order event persisted", "event_id", event.EventID, "order_id", data.OrderID)
		} else {
			h.logger.Debug("order event ignored (duplicate or deleted order)", "event_id", event.EventID, "order_id", data.OrderID)
		}
	case "order.deleted":
		data, err := decodeOrderData(event.Data)
		if err != nil { return err }
		deleted, err := h.repository.DeleteByOrderID(ctx, data.OrderID)
		if err != nil { return err }
		h.logger.Info("order deleted", "event_id", event.EventID, "order_id", data.OrderID, "deleted_events", deleted)
	default:
		return errors.New("unsupported event type: " + event.Type)
	}
	return nil
}

func decodeOrderData(raw json.RawMessage) (orderData, error) {
	var data orderData
	if err := json.Unmarshal(raw, &data); err != nil {
		return data, fmt.Errorf("decode order data: %w", err)
	}
	if strings.TrimSpace(data.OrderID) == "" {
		return data, errors.New("data.order_id is required")
	}
	return data, nil
}
