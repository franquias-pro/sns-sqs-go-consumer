package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/mamartins1997/sns-sqs-go-consumer/internal/observability"
)

// The load generator publishes through SNS so the observed throughput includes
// SNS delivery, SQS polling, processing, and acknowledgement.
func main() {
	logger := observability.NewLogger("info")
	topic := os.Getenv("SNS_TOPIC_ARN")
	if topic == "" { logger.Error("SNS_TOPIC_ARN is required"); os.Exit(1) }
	rate, err := envInt("RATE_PER_SECOND", 1000)
	if err != nil || rate < 10 || rate > 10000 || rate%10 != 0 {
		logger.Error("RATE_PER_SECOND must be a multiple of 10 between 10 and 10000")
		os.Exit(1)
	}
	publishers, err := envInt("PUBLISHERS", 20)
	if err != nil || publishers < 1 || publishers > 200 {
		logger.Error("PUBLISHERS must be between 1 and 200")
		os.Exit(1)
	}
	duration := 60 * time.Second
	if v := os.Getenv("DURATION"); v != "" {
		duration, err = time.ParseDuration(v)
		if err != nil || duration <= 0 { logger.Error("DURATION must be a positive Go duration"); os.Exit(1) }
	}
	region := os.Getenv("AWS_REGION")
	if region == "" { region = "us-east-1" }
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil { logger.Error("aws configuration failed", "error", err); os.Exit(1) }
	var snsOptions []func(*sns.Options)
	if endpoint := os.Getenv("SNS_ENDPOINT_URL"); endpoint != "" {
		snsOptions = append(snsOptions, func(o *sns.Options) { o.BaseEndpoint = &endpoint })
	}
	client := sns.NewFromConfig(awsCfg, snsOptions...)

	jobs := make(chan []types.PublishBatchRequestEntry, publishers*4)
	var submitted, published, failed atomic.Uint64
	var wg sync.WaitGroup
	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				result, err := client.PublishBatch(requestCtx, &sns.PublishBatchInput{
					TopicArn: aws.String(topic), PublishBatchRequestEntries: batch,
				})
				cancel()
				if err != nil {
					failed.Add(uint64(len(batch)))
					logger.Error("sns publish batch failed", "error", err)
					continue
				}
				published.Add(uint64(len(result.Successful)))
				failed.Add(uint64(len(result.Failed)))
				if len(result.Failed) > 0 { logger.Error("sns publish batch partially failed", "failed", len(result.Failed)) }
			}
		}()
	}
	start := time.Now()
	period := time.Second / time.Duration(rate/10)
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	prefix := strconv.FormatInt(start.UnixNano(), 10)
	sequence := uint64(0)
	loop:
	for {
		select {
		case <-ctx.Done(): break loop
		case <-deadline.C: break loop
		case <-ticker.C:
			batch := make([]types.PublishBatchRequestEntry, 10)
			for i := range batch {
				sequence++
				body := fmt.Sprintf(`{"event_id":"%s-%d","type":"order.created","occurred_at":"%s","data":{"order_id":"load-%d"}}`,
					prefix, sequence, time.Now().UTC().Format(time.RFC3339Nano), sequence)
				batch[i] = types.PublishBatchRequestEntry{
					Id: aws.String(strconv.Itoa(i)), Message: aws.String(body),
					MessageGroupId: aws.String(fmt.Sprintf("load-%d", sequence)),
					MessageDeduplicationId: aws.String(fmt.Sprintf("%s-%d", prefix, sequence)),
				}
			}
			select {
			case jobs <- batch: submitted.Add(10)
			case <-ctx.Done(): break loop
			}
		}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	logger.Info("load generation finished", "submitted", submitted.Load(), "published", published.Load(),
		"failed", failed.Load(), "elapsed_seconds", elapsed, "published_per_second", float64(published.Load())/elapsed)
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" { return fallback, nil }
	return strconv.Atoi(value)
}
