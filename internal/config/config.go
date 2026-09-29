package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AWSRegion         string
	QueueURL          string
	Pollers           int
	Workers           int
	Ackers            int
	ProcessTimeout    time.Duration
	VisibilityTimeout int32 // seconds
	ShutdownTimeout   time.Duration
	LogLevel          string
}

func Load() (Config, error) {
	c := Config{AWSRegion: env("AWS_REGION", "us-east-1"), QueueURL: os.Getenv("SQS_QUEUE_URL"), LogLevel: env("LOG_LEVEL", "info")}
	var err error
	if c.Pollers, err = integer("POLLERS", 16, 1, 128); err != nil { return c, err }
	if c.Workers, err = integer("WORKERS", 200, 10, 10000); err != nil { return c, err }
	if c.Ackers, err = integer("ACKERS", 8, 1, 128); err != nil { return c, err }
	visibility, err := integer("VISIBILITY_TIMEOUT", 120, 1, 43200)
	if err != nil { return c, err }
	c.VisibilityTimeout = int32(visibility)
	if c.ProcessTimeout, err = duration("PROCESS_TIMEOUT", 15*time.Second); err != nil { return c, err }
	if c.ShutdownTimeout, err = duration("SHUTDOWN_TIMEOUT", 45*time.Second); err != nil { return c, err }
	if c.QueueURL == "" { return c, fmt.Errorf("SQS_QUEUE_URL is required") }
	if c.ProcessTimeout <= 0 || c.ShutdownTimeout <= 0 { return c, fmt.Errorf("timeouts must be positive") }
	if c.ProcessTimeout+30*time.Second >= time.Duration(c.VisibilityTimeout)*time.Second {
		return c, fmt.Errorf("VISIBILITY_TIMEOUT must exceed PROCESS_TIMEOUT by more than 30 seconds")
	}
	if c.LogLevel != "info" && c.LogLevel != "debug" { return c, fmt.Errorf("LOG_LEVEL must be info or debug") }
	return c, nil
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" { return v }
	return fallback
}

func integer(key string, fallback, min, max int) (int, error) {
	v := env(key, strconv.Itoa(fallback))
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max { return 0, fmt.Errorf("%s must be an integer between %d and %d", key, min, max) }
	return n, nil
}

func duration(key string, fallback time.Duration) (time.Duration, error) {
	v := env(key, fallback.String())
	d, err := time.ParseDuration(v)
	if err != nil { return 0, fmt.Errorf("%s: %w", key, err) }
	return d, nil
}
