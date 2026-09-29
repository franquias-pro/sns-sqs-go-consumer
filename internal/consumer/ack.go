package consumer

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func (c *Consumer) ackLoop(ctx context.Context) {
	batch := make([]acknowledgement, 0, 10)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() { <-timer.C }
	defer timer.Stop()
	flush := func() {
		if len(batch) == 0 { return }
		c.deleteBatch(ctx, batch)
		batch = batch[:0]
	}
	for {
		select {
		case ack, ok := <-c.acks:
			if !ok { flush(); return }
			batch = append(batch, ack)
			if len(batch) == 1 { timer.Reset(50 * time.Millisecond) }
			if len(batch) == 10 {
				if !timer.Stop() { select { case <-timer.C: default: } }
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

func (c *Consumer) deleteBatch(ctx context.Context, batch []acknowledgement) {
	entries := make([]types.DeleteMessageBatchRequestEntry, len(batch))
	for i, ack := range batch {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id: aws.String(strconv.Itoa(i)), ReceiptHandle: ack.message.ReceiptHandle,
		}
	}
	result, err := c.queue.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.cfg.QueueURL), Entries: entries,
	})
	failed := make(map[string]string)
	if err == nil {
		for _, f := range result.Failed { failed[aws.ToString(f.Id)] = aws.ToString(f.Code) }
	}
	for i, ack := range batch {
		id := aws.ToString(entries[i].Id)
		fields := []any{"message_id", aws.ToString(ack.message.MessageId), "trace.id", ack.traceID}
		if err != nil || failed[id] != "" {
			c.stats.DeleteFailed.Add(1)
			ack.tx.Result = "failure"
			c.log.Log(ctx, slog.LevelError, "message delete failed; SQS will retry after visibility timeout", append(fields, "error", err, "sqs_code", failed[id])...)
		} else {
			c.stats.Deleted.Add(1)
			ack.tx.Result = "success"
			c.log.Log(ctx, slog.LevelDebug, "message acknowledged", fields...)
		}
		ack.tx.End()
		c.done()
	}
}
