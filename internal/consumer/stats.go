package consumer

import (
	"context"
	"sync/atomic"

	"go.elastic.co/apm/v2"
)

type Stats struct {
	Received atomic.Uint64
	Processed atomic.Uint64
	Failed atomic.Uint64
	Deleted atomic.Uint64
	DeleteFailed atomic.Uint64
	InFlight atomic.Int64
}

func (s *Stats) GatherMetrics(_ context.Context, m *apm.Metrics) error {
	m.Add("consumer.received.total", nil, float64(s.Received.Load()))
	m.Add("consumer.processed.total", nil, float64(s.Processed.Load()))
	m.Add("consumer.failed.total", nil, float64(s.Failed.Load()))
	m.Add("consumer.deleted.total", nil, float64(s.Deleted.Load()))
	m.Add("consumer.delete_failed.total", nil, float64(s.DeleteFailed.Load()))
	m.Add("consumer.in_flight", nil, float64(s.InFlight.Load()))
	return nil
}
