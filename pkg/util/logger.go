package util

import (
	"log/slog"
	"os"
	"strings"
)

func CreateLogger() *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(GetEnvOrDefault("LOG_LEVEL", "info")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
