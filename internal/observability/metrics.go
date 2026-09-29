package observability

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"go.elastic.co/apm/v2"
)

// Counters keeps per-process execution data. It also implements the Elastic
// APM metrics gatherer, keeping the agent-specific API outside the consumer.
type Counters struct {
	received     atomic.Uint64
	processed    atomic.Uint64
	failed       atomic.Uint64
	deleted      atomic.Uint64
	deleteFailed atomic.Uint64
	inFlight     atomic.Int64
}

type Snapshot struct {
	Received     uint64
	Processed    uint64
	Failed       uint64
	Deleted      uint64
	DeleteFailed uint64
	InFlight     int64
}

func (c *Counters) Received()         { c.received.Add(1); c.inFlight.Add(1) }
func (c *Counters) Processed()        { c.processed.Add(1) }
func (c *Counters) ProcessingFailed() { c.failed.Add(1) }
func (c *Counters) Deleted()          { c.deleted.Add(1) }
func (c *Counters) DeleteFailed()     { c.deleteFailed.Add(1) }
func (c *Counters) Done()             { c.inFlight.Add(-1) }

func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		Received: c.received.Load(), Processed: c.processed.Load(),
		Failed: c.failed.Load(), Deleted: c.deleted.Load(),
		DeleteFailed: c.deleteFailed.Load(), InFlight: c.inFlight.Load(),
	}
}

func (c *Counters) GatherMetrics(_ context.Context, m *apm.Metrics) error {
	s := c.Snapshot()
	m.Add("consumer.received.total", nil, float64(s.Received))
	m.Add("consumer.processed.total", nil, float64(s.Processed))
	m.Add("consumer.failed.total", nil, float64(s.Failed))
	m.Add("consumer.deleted.total", nil, float64(s.Deleted))
	m.Add("consumer.delete_failed.total", nil, float64(s.DeleteFailed))
	m.Add("consumer.in_flight", nil, float64(s.InFlight))
	return nil
}

func (c *Counters) LogPeriodic(done <-chan struct{}, logger *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var previous Snapshot
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			s := c.Snapshot()
			logger.Info("execution summary",
				"received_1m", s.Received-previous.Received,
				"processed_1m", s.Processed-previous.Processed,
				"failed_1m", s.Failed-previous.Failed,
				"deleted_1m", s.Deleted-previous.Deleted,
				"deleted_per_second_1m", float64(s.Deleted-previous.Deleted)/60,
				"in_flight", s.InFlight,
				"delete_failed_total", s.DeleteFailed)
			previous = s
		}
	}
}

func (c *Counters) LogTotals(logger *slog.Logger) {
	s := c.Snapshot()
	logger.Info("execution totals", "received", s.Received, "processed", s.Processed,
		"failed", s.Failed, "deleted", s.Deleted,
		"delete_failed", s.DeleteFailed, "in_flight", s.InFlight)
}
