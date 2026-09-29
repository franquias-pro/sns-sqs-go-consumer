package service

import (
	"context"
	"log/slog"

	"github.com/mamartins1997/sns-sqs-go-consumer/internal/event"
)

// OrderRepository is implemented by the MongoDB adapter. Other persistence
// operations can be added here when new event commands need them.
type OrderRepository interface {
	Create(context.Context, event.Event, string, string) (inserted bool, err error)
	DeleteByOrderID(context.Context, string) (deleted int64, err error)
}

type Orders struct {
	logger     *slog.Logger
	repository OrderRepository
}

func NewOrders(logger *slog.Logger, repository OrderRepository) *Orders {
	return &Orders{logger: logger, repository: repository}
}

func (s *Orders) CreateOrder(ctx context.Context, evt event.Event, orderID, body string) error {
	inserted, err := s.repository.Create(ctx, evt, orderID, body)
	if err != nil {
		return err
	}
	if inserted {
		s.logger.Info("order event persisted", "event_id", evt.EventID, "order_id", orderID)
	} else {
		s.logger.Info("order event ignored (duplicate or deleted order)", "event_id", evt.EventID, "order_id", orderID)
	}
	return nil
}

func (s *Orders) DeleteOrder(ctx context.Context, evt event.Event, orderID string) error {
	deleted, err := s.repository.DeleteByOrderID(ctx, orderID)
	if err != nil {
		return err
	}
	s.logger.Info("order deleted", "event_id", evt.EventID, "order_id", orderID, "deleted_events", deleted)
	return nil
}
