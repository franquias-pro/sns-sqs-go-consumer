package observability

import (
	"log/slog"
	"os"
)

func NewLogger(levelName string) *slog.Logger {
	level := slog.LevelInfo
	if levelName == "debug" { level = slog.LevelDebug }
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
