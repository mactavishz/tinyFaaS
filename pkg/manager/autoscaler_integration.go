package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"log/slog"
)

// TinyFaaSScaleOp implements the autoscaler.ScaleOperation interface for tinyFaaS
type TinyFaaSScaleOp struct {
	ms     *ManagementService
	logger *slog.Logger
}

// NewTinyFaaSScaleOp creates a new TinyFaaSScaleOp
func NewTinyFaaSScaleOp(ms *ManagementService, logger *slog.Logger) *TinyFaaSScaleOp {
	return &TinyFaaSScaleOp{ms: ms, logger: logger}
}

// ScaleDown stops the function containers
func (op *TinyFaaSScaleOp) ScaleDown(functionName string) error {
	op.ms.mux.Lock()
	handler, exists := op.ms.functionHandlers[functionName]
	op.ms.mux.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	if err := op.notifyRProxyClearIPs(functionName); err != nil {
		op.logger.Error("failed to notify rproxy about scale down", "function", functionName, "err", err)
		return fmt.Errorf("failed to notify rproxy about scale down: %w", err)
	}

	// Measure scale-down time
	startTime := time.Now()

	// Stop the containers
	if err := handler.Stop(); err != nil {
		return fmt.Errorf("failed to stop function %s: %w", functionName, err)
	}

	scaleDownDuration := time.Since(startTime)

	// Record scale-down time to callgraph tracker via rproxy
	op.ms.notifyScaleDown(functionName, startTime, scaleDownDuration)

	op.logger.Info("function scaled down",
		"function", functionName,
		"duration", scaleDownDuration)
	return nil
}

// ScaleUp starts the function containers
func (op *TinyFaaSScaleOp) ScaleUp(functionName string) error {
	op.ms.mux.Lock()
	handler, exists := op.ms.functionHandlers[functionName]
	op.ms.mux.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	// Restart the containers
	if err := handler.Restart(); err != nil {
		return fmt.Errorf("failed to restart function %s: %w", functionName, err)
	}

	// Notify rproxy to add function back to routing table
	if err := op.notifyRProxyAdd(functionName, handler.IPs()); err != nil {
		op.logger.Error("failed to notify rproxy", "function", functionName, "err", err)
		return fmt.Errorf("function restarted but rproxy notification failed: %w", err)
	}

	op.logger.Info("function scaled up", "function", functionName)
	return nil
}

// notifyRProxyAdd notifies rproxy to add a function to the routing table
func (op *TinyFaaSScaleOp) notifyRProxyAdd(functionName string, ips []string) error {
	d := struct {
		FunctionName string   `json:"name"`
		FunctionIPs  []string `json:"ips"`
	}{
		FunctionName: functionName,
		FunctionIPs:  ips,
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://127.0.0.1:%s/config", op.ms.rproxyPort), bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rproxy returned status code %d", resp.StatusCode)
	}

	return nil
}

// notifyRProxyClearIPs notifies rproxy to remove a function's IPs from the routing table
func (op *TinyFaaSScaleOp) notifyRProxyClearIPs(functionName string) error {
	d := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: functionName,
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPatch, fmt.Sprintf("http://127.0.0.1:%s/config", op.ms.rproxyPort), bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rproxy returned status code %d", resp.StatusCode)
	}

	return nil
}

// SetAutoScaler sets the autoscaler for the management service
func (ms *ManagementService) SetAutoScaler(as *autoscaler.AutoScaler) {
	ms.autoscaler = as
}

// GetAutoScaler returns the autoscaler instance
func (ms *ManagementService) GetAutoScaler() *autoscaler.AutoScaler {
	return ms.autoscaler
}

// ScaleUp scales up a function and records the scale-up time to callgraph tracker via rproxy
// cold=true means this is a user-facing cold start
// cold=false means this is a proactive prewarm
func (ms *ManagementService) ScaleUp(functionName string, cold bool) error {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return fmt.Errorf("autoscaler not enabled")
	}

	// Measure scale-up time
	startTime := time.Now()

	if err := ms.autoscaler.ScaleUp(functionName); err != nil {
		return err
	}

	scaleUpDuration := time.Since(startTime)

	// Record scale-up time to callgraph tracker via rproxy
	ms.notifyScaleUp(functionName, startTime, scaleUpDuration, cold)

	ms.logger.Info("scale-up completed",
		"function", functionName,
		"cold", cold,
		"duration", scaleUpDuration)

	return nil
}

// StartRequest marks a function as blocked while serving a request.
func (ms *ManagementService) StartRequest(functionName string) error {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return nil
	}
	return ms.autoscaler.StartInvocation(functionName)
}

// EndRequest marks a function request completion.
func (ms *ManagementService) EndRequest(functionName string) {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return
	}
	ms.autoscaler.EndInvocation(functionName)
}

// Heartbeat records activity for a function to prevent it from being scaled down
func (ms *ManagementService) Heartbeat(functionName string) error {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return fmt.Errorf("autoscaler not enabled")
	}

	// Record activity in autoscaler
	ms.autoscaler.RecordActivity(functionName)

	return nil
}

// HeartbeatBatch records activity for multiple functions to prevent them from being scaled down.
// This is more efficient than individual heartbeats for batch processing.
func (ms *ManagementService) HeartbeatBatch(functionNames []string) error {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return fmt.Errorf("autoscaler not enabled")
	}

	// Record batch activity in autoscaler
	ms.autoscaler.RecordActivityBatch(functionNames)

	return nil
}
