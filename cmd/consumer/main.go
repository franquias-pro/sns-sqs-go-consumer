package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.elastic.co/apm/v2"

	appconfig "github.com/mamartins1997/sns-sqs-go-consumer/internal/config"
	"github.com/mamartins1997/sns-sqs-go-consumer/internal/consumer"
	"github.com/mamartins1997/sns-sqs-go-consumer/internal/handler"
	"github.com/mamartins1997/sns-sqs-go-consumer/internal/health"
	"github.com/mamartins1997/sns-sqs-go-consumer/internal/observability"
	"github.com/mamartins1997/sns-sqs-go-consumer/internal/repository/mongodb"
	"github.com/mamartins1997/sns-sqs-go-consumer/internal/service"
)

func main() {
	cfg, err := appconfig.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger := observability.NewLogger(cfg.LogLevel)
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

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 10*time.Second)
	store, err := mongodb.Connect(connectCtx, cfg.MongoURI, cfg.MongoDatabase, cfg.MongoCollection, uint64(cfg.MongoMaxPoolSize))
	cancelConnect()
	if err != nil { logger.Error("mongodb initialization failed", "error", err); os.Exit(1) }
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Close(ctx); err != nil { logger.Error("mongodb disconnect failed", "error", err) }
	}()

	tracer := apm.DefaultTracer()
	metrics := &observability.Counters{}
	deregister := tracer.RegisterMetricsGatherer(metrics)
	defer deregister()

	var sqsOptions []func(*sqs.Options)
	if cfg.SQSEndpointURL != "" {
		sqsOptions = append(sqsOptions, func(o *sqs.Options) { o.BaseEndpoint = &cfg.SQSEndpointURL })
	}
	client := sqs.NewFromConfig(awsCfg, sqsOptions...)
	orders := service.NewOrders(logger, store)
	runner := consumer.New(client, cfg, handler.New(orders), logger, observability.NewElasticTracer(tracer), metrics)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	healthHandler := health.New(store)
	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil { logger.Error("health listener failed", "error", err); os.Exit(1) }
	server := &http.Server{Handler: healthHandler.Routes(), ReadHeaderTimeout: 2*time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("health server stopped unexpectedly", "error", err)
			stop()
		}
	}()
	healthHandler.SetReady(true)
	go func() { <-ctx.Done(); healthHandler.SetReady(false) }()
	statsDone := make(chan struct{})
	go metrics.LogPeriodic(statsDone, logger)
	logger.Info("consumer started", "pollers", cfg.Pollers, "workers", cfg.Workers, "ackers", cfg.Ackers)
	runner.Run(ctx)
	close(statsDone)
	metrics.LogTotals(logger)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	if err := server.Shutdown(shutdownCtx); err != nil { logger.Error("health server shutdown failed", "error", err) }
	cancelShutdown()
	abort := make(chan struct{})
	timer := time.AfterFunc(5*time.Second, func() { close(abort) })
	tracer.Flush(abort)
	timer.Stop()
	logger.Info("consumer stopped")
}
