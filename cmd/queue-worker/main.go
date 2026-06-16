package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/queue"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
)

func main() {
	logger := util.CreateLogger()
	gatewayPort := util.GetEnvOrDefault("GATEWAY_PORT", "8080")
	defaultGateway := "http://127.0.0.1:" + gatewayPort
	ackWait := queue.DefaultAckWait
	if raw := os.Getenv("TINYFAAS_QUEUE_ACK_WAIT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil {
			ackWait = parsed
		} else {
			logger.Warn("invalid TINYFAAS_QUEUE_ACK_WAIT", "value", raw, "err", err)
		}
	}
	maxInflight := queue.DefaultMaxFlight
	if raw := os.Getenv("TINYFAAS_QUEUE_MAX_INFLIGHT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			maxInflight = parsed
		} else {
			logger.Warn("invalid TINYFAAS_QUEUE_MAX_INFLIGHT", "value", raw, "err", err)
		}
	}

	worker := queue.NewWorker(queue.WorkerConfig{
		NATSURL:    util.GetEnvOrDefault("TINYFAAS_NATS_URL", "nats://127.0.0.1:4222"),
		ClusterID:  util.GetEnvOrDefault("TINYFAAS_NATS_CLUSTER", queue.DefaultClusterID),
		ClientID:   util.GetEnvOrDefault("TINYFAAS_QUEUE_CLIENT_ID", "tinyfaas-queue-worker"),
		Subject:    util.GetEnvOrDefault("TINYFAAS_NATS_SUBJECT", queue.DefaultSubject),
		QueueGroup: util.GetEnvOrDefault("TINYFAAS_NATS_QUEUE_GROUP", queue.DefaultQueue),
		// TINYFAAS_INTERNAL_URL points at the gateway by default so dequeued
		// async invocations go through the scaling middleware and singleflight
		// scale-up.
		TargetBaseURL: util.GetEnvOrDefault("TINYFAAS_INTERNAL_URL", defaultGateway),
		DispatchPath:  util.GetEnvOrDefault("TINYFAAS_QUEUE_DISPATCH_PATH", "/queue-fn/"),
		AckWait:       ackWait,
		MaxInflight:   maxInflight,
	}, logger)

	for {
		if err := worker.Start(); err != nil {
			logger.Warn("failed to start queue worker, retrying", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	if err := worker.Stop(); err != nil {
		logger.Error("queue worker shutdown error", "err", err)
	}
}
