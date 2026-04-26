package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/gateway"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"go.uber.org/zap"
)

const (
	DEFAULT_IP   = "0.0.0.0"
	DEFAULT_PROT = "8080"
	DEFAULT_MODE = "development"
)

func main() {
	logger := util.CreateLogger()
	defer logger.Sync() // flushes buffer, if any

	port := util.GetEnvOrDefault("GATEWAY_PORT", DEFAULT_PROT)
	ip := util.GetEnvOrDefault("GATEWAY_IP", DEFAULT_IP)
	// Create gateway instance with options
	var opts []gateway.Option

	if rproxyPort := os.Getenv("RPROXY_PORT"); rproxyPort != "" {
		opts = append(opts, gateway.WithRProxyPort(rproxyPort))
	}

	if managerPort := os.Getenv("MANAGER_PORT"); managerPort != "" {
		opts = append(opts, gateway.WithManagerPort(managerPort))
	}

	// Get mode from environment variable
	env := util.GetEnvOrDefault("ENV", DEFAULT_MODE)
	logger.Info("Gateway ENV", zap.String("env", env))
	opts = append(opts, gateway.WithMode(env))

	g := gateway.New(logger, opts...)

	mux := http.NewServeMux()
	g.RegisterHandlers(mux)

	addr := fmt.Sprintf("%s:%s", ip, port)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start server in goroutine
	go func() {
		logger.Info("tinyFaaS Gateway starting",
			zap.String("address", addr),
			zap.String("rproxyPort", g.GetRProxyPort()),
			zap.String("managerPort", g.GetManagerPort()))

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("gateway failed to start", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("received shutdown signal, initiating graceful shutdown...")

	// Create context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Gracefully shutdown server
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("shutdown complete, exiting")
}
