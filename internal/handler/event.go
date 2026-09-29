package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mamartins1997/sns-sqs-go-consumer/internal/event"
)

type orderData struct {
	OrderID string `json:"order_id"`
}

// Orders is the application service used by the event dispatcher.
type Orders interface {
	CreateOrder(context.Context, event.Event, string, string) error
	DeleteOrder(context.Context, event.Event, string) error
}

type Handler struct {
	orders Orders
}

func New(orders Orders) *Handler {
	return &Handler{orders: orders}
}

func (h *Handler) Handle(ctx context.Context, body string) error {
	var evt event.Event
	if err := json.Unmarshal([]byte(body), &evt); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	if evt.EventID == "" || evt.Type == "" {
		return errors.New("event_id and type are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	switch evt.Type {
	case "order.created":
		data, err := decodeOrderData(evt.Data)
		if err != nil { return err }
		return h.orders.CreateOrder(ctx, evt, data.OrderID, body)
	case "order.deleted":
		data, err := decodeOrderData(evt.Data)
		if err != nil { return err }
		return h.orders.DeleteOrder(ctx, evt, data.OrderID)
	default:
		return errors.New("unsupported event type: " + evt.Type)
	}
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
