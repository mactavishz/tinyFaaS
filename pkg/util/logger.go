package util

import (
	"strings"

	"go.uber.org/zap"
)

func CreateLogger() *zap.Logger {
	var logger *zap.Logger
	if strings.ToLower(GetEnvOrDefault("TF_ENV", "development")) == "development" {
		logger = zap.Must(zap.NewDevelopment())
	} else {
		logger = zap.Must(zap.NewProduction())
	}
	return logger
}
