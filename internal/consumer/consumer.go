package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.elastic.co/apm/v2"

	"example.com/sns-sqs-go-consumer/internal/config"
)

type Queue interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

type Handler interface { Handle(context.Context, string) error }

type Consumer struct {
	queue Queue
	cfg config.Config
	handler Handler
	log *slog.Logger
	tracer *apm.Tracer
	stats *Stats
	slots chan struct{}
	jobs chan types.Message
	acks chan acknowledgement
}

type acknowledgement struct {
	message types.Message
	tx *apm.Transaction
	traceID string
}

func New(queue Queue, cfg config.Config, handler Handler, log *slog.Logger, tracer *apm.Tracer, stats *Stats) *Consumer {
	return &Consumer{
		queue: queue, cfg: cfg, handler: handler, log: log, tracer: tracer, stats: stats,
		slots: make(chan struct{}, cfg.Workers), jobs: make(chan types.Message), acks: make(chan acknowledgement, cfg.Workers),
	}
}

// Run stops receiving on cancellation, drains received work, then confirms
// successful messages. On deadline, unfinished work returns to SQS visibility.
func (c *Consumer) Run(ctx context.Context) {
	processCtx, cancelProcess := context.WithCancel(context.Background())
	defer cancelProcess()
	statsDone := make(chan struct{})
	go c.reportStats(statsDone)
	defer close(statsDone)
	var pollWG, workerWG, ackWG sync.WaitGroup
	for range c.cfg.Ackers {
		ackWG.Add(1)
		go func() { defer ackWG.Done(); c.ackLoop(processCtx) }()
	}
	for range c.cfg.Workers {
		workerWG.Add(1)
		go func() { defer workerWG.Done(); c.worker(processCtx) }()
	}
	for range c.cfg.Pollers {
		pollWG.Add(1)
		go func() { defer pollWG.Done(); c.poll(ctx) }()
	}
	<-ctx.Done()
	c.log.Info("shutdown requested")
	stopTimer := time.AfterFunc(c.cfg.ShutdownTimeout, cancelProcess)
	defer stopTimer.Stop()
	pollWG.Wait()
	close(c.jobs)
	workerWG.Wait()
	close(c.acks)
	ackWG.Wait()
	if processCtx.Err() != nil { c.log.Warn("shutdown deadline reached; unfinished messages will be retried") }
	c.log.Info("execution totals", "received", c.stats.Received.Load(), "processed", c.stats.Processed.Load(),
		"failed", c.stats.Failed.Load(), "deleted", c.stats.Deleted.Load(),
		"delete_failed", c.stats.DeleteFailed.Load(), "in_flight", c.stats.InFlight.Load())
}

func (c *Consumer) reportStats(done <-chan struct{}) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var received, processed, failed, deleted uint64
	for {
		select {
		case <-done: return
		case <-ticker.C:
			r, p, f, d := c.stats.Received.Load(), c.stats.Processed.Load(), c.stats.Failed.Load(), c.stats.Deleted.Load()
			c.log.Info("execution summary", "received_1m", r-received, "processed_1m", p-processed,
				"failed_1m", f-failed, "deleted_1m", d-deleted,
				"deleted_per_second_1m", float64(d-deleted)/60,
				"in_flight", c.stats.InFlight.Load(), "delete_failed_total", c.stats.DeleteFailed.Load())
			received, processed, failed, deleted = r, p, f, d
		}
	}
}

func (c *Consumer) poll(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		reserved := 0
		// Reserve capacity *before* ReceiveMessage. A returned message never
		// sits in a large local backlog while its visibility clock runs.
		for reserved < 10 {
			select {
			case c.slots <- struct{}{}: reserved++
			case <-ctx.Done():
				c.release(reserved)
				return
			}
		}
		result, err := c.queue.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.cfg.QueueURL), MaxNumberOfMessages: 10,
			WaitTimeSeconds: 20, VisibilityTimeout: c.cfg.VisibilityTimeout,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			c.release(reserved)
			if ctx.Err() != nil { return }
			c.log.Error("sqs receive failed", "error", err)
			select { case <-time.After(backoff): case <-ctx.Done(): return }
			if backoff < 16*time.Second { backoff *= 2 }
			continue
		}
		backoff = time.Second
		c.release(reserved - len(result.Messages))
		for _, msg := range result.Messages {
			c.stats.Received.Add(1)
			c.stats.InFlight.Add(1)
			// Work already received must be handed to workers, even if the
			// receive context is cancelled during shutdown.
			c.jobs <- msg
		}
	}
}

func (c *Consumer) worker(ctx context.Context) {
	for msg := range c.jobs { c.process(ctx, msg) }
}

func (c *Consumer) process(parent context.Context, msg types.Message) {
	tx := c.tracer.StartTransaction("SQS process event", "messaging")
	traceID := tx.TraceContext().Trace.String()
	ctx, cancel := context.WithTimeout(parent, c.cfg.ProcessTimeout)
	defer cancel()
	var err error
	func() {
		defer func() { if p := recover(); p != nil { err = fmt.Errorf("handler panic: %v", p) } }()
		err = c.handler.Handle(ctx, aws.ToString(msg.Body))
	}()
	if err == nil { err = ctx.Err() }
	if err != nil {
		c.stats.Failed.Add(1)
		tx.Result = "failure"
		apmErr := c.tracer.NewError(err)
		apmErr.SetTransaction(tx)
		apmErr.Send()
		tx.End()
		c.log.Error("message processing failed", "message_id", aws.ToString(msg.MessageId), "trace.id", traceID, "error", err)
		c.done()
		return
	}
	c.stats.Processed.Add(1)
	c.acks <- acknowledgement{message: msg, tx: tx, traceID: traceID}
}

func (c *Consumer) release(n int) { for range n { <-c.slots } }
func (c *Consumer) done() { c.stats.InFlight.Add(-1); c.release(1) }
