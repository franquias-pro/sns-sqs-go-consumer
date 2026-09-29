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

	"example.com/sns-sqs-go-consumer/internal/config"
)

type Queue interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

type Handler interface { Handle(context.Context, string) error }

// Consumer owns only message flow. Instrumentation can be replaced without
// changing the polling, processing, or acknowledgement logic.
type Metrics interface {
	Received()
	Processed()
	ProcessingFailed()
	Deleted()
	DeleteFailed()
	Done()
}

type Tracer interface { StartMessage() MessageTrace }

type MessageTrace interface {
	TraceID() string
	End(error)
}

type Consumer struct {
	queue Queue
	cfg config.Config
	handler Handler
	log *slog.Logger
	tracer Tracer
	metrics Metrics
	slots chan struct{}
	jobs chan types.Message
	acks chan acknowledgement
}

type acknowledgement struct {
	message types.Message
	tx MessageTrace
	traceID string
}

func New(queue Queue, cfg config.Config, handler Handler, log *slog.Logger, tracer Tracer, metrics Metrics) *Consumer {
	return &Consumer{
		queue: queue, cfg: cfg, handler: handler, log: log, tracer: tracer, metrics: metrics,
		slots: make(chan struct{}, cfg.Workers), jobs: make(chan types.Message), acks: make(chan acknowledgement, cfg.Workers),
	}
}

// Run stops receiving on cancellation, drains received work, then confirms
// successful messages. On deadline, unfinished work returns to SQS visibility.
func (c *Consumer) Run(ctx context.Context) {
	processCtx, cancelProcess := context.WithCancel(context.Background())
	defer cancelProcess()
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
}

func (c *Consumer) poll(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		reserved := 0
		// Receive a single FIFO message at a time. SQS will not expose the
		// next message from its group until this one is acknowledged, so
		// workers can process different groups concurrently in order.
		for reserved < 1 {
			select {
			case c.slots <- struct{}{}: reserved++
			case <-ctx.Done():
				c.release(reserved)
				return
			}
		}
		result, err := c.queue.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.cfg.QueueURL), MaxNumberOfMessages: 1,
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
			c.metrics.Received()
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
	tx := c.tracer.StartMessage()
	traceID := tx.TraceID()
	ctx, cancel := context.WithTimeout(parent, c.cfg.ProcessTimeout)
	defer cancel()
	var err error
	func() {
		defer func() { if p := recover(); p != nil { err = fmt.Errorf("handler panic: %v", p) } }()
		err = c.handler.Handle(ctx, aws.ToString(msg.Body))
	}()
	if err == nil { err = ctx.Err() }
	if err != nil {
		c.metrics.ProcessingFailed()
		tx.End(err)
		c.log.Error("message processing failed", "message_id", aws.ToString(msg.MessageId), "trace.id", traceID, "error", err)
		c.done()
		return
	}
	c.metrics.Processed()
	c.acks <- acknowledgement{message: msg, tx: tx, traceID: traceID}
}

func (c *Consumer) release(n int) { for range n { <-c.slots } }
func (c *Consumer) done() { c.metrics.Done(); c.release(1) }
