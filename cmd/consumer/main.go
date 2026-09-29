package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.elastic.co/apm/v2"

	appconfig "example.com/sns-sqs-go-consumer/internal/config"
	"example.com/sns-sqs-go-consumer/internal/consumer"
	"example.com/sns-sqs-go-consumer/internal/handler"
)

func main() {
	cfg, err := appconfig.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	level := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// The HTTP timeout must exceed the SQS 20-second long poll.
	httpClient := awshttp.NewBuildableClient().WithTimeout(30 * time.Second).
		WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConns = cfg.Pollers + cfg.Ackers + 32
			t.MaxIdleConnsPerHost = cfg.Pollers + cfg.Ackers + 32
		})
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.AWSRegion), config.WithHTTPClient(httpClient))
	if err != nil {
		logger.Error("aws configuration failed", "error", err)
		os.Exit(1)
	}

	tracer := apm.DefaultTracer()
	metrics := &consumer.Stats{}
	deregister := tracer.RegisterMetricsGatherer(metrics)
	defer deregister()
	defer tracer.Flush(nil)

	client := sqs.NewFromConfig(awsCfg)
	runner := consumer.New(client, cfg, handler.New(logger), logger, tracer, metrics)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("consumer started", "pollers", cfg.Pollers, "workers", cfg.Workers, "ackers", cfg.Ackers)
	runner.Run(ctx)
	logger.Info("consumer stopped")
}
