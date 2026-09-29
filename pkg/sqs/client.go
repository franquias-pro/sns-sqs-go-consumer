package sqsclient

import (
	"context"
	"fmt"
	"net/http"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Config contains only the settings needed to create an SQS client.
type Config struct {
	Region             string
	EndpointURL        string
	MaxIdleConnections int
}

func New(ctx context.Context, cfg Config) (*sqs.Client, error) {
	// The HTTP timeout must exceed the 20-second SQS long poll.
	httpClient := awshttp.NewBuildableClient().WithTimeout(30 * time.Second).
		WithTransportOptions(func(t *http.Transport) {
			t.MaxIdleConns = cfg.MaxIdleConnections
			t.MaxIdleConnsPerHost = cfg.MaxIdleConnections
		})
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region), config.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	var options []func(*sqs.Options)
	if cfg.EndpointURL != "" {
		options = append(options, func(o *sqs.Options) { o.BaseEndpoint = &cfg.EndpointURL })
	}
	return sqs.NewFromConfig(awsCfg, options...), nil
}
