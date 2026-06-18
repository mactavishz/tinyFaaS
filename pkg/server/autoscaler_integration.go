package server

import (
	"errors"
	"fmt"
	"time"

	"log/slog"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
)

// TinyFaaSScaleOp implements the autoscaler.ScaleOperation interface for tinyFaaS
type TinyFaaSScaleOp struct {
	ms     *ManagementService
	logger *slog.Logger
}

// NewTinyFaaSScaleOp creates a new TinyFaaSScaleOp
func NewTinyFaaSScaleOp(ms *ManagementService, logger *slog.Logger) *TinyFaaSScaleOp {
	return &TinyFaaSScaleOp{
		ms:     ms,
		logger: logger,
	}
}

// ScaleDown stops the function containers
func (op *TinyFaaSScaleOp) ScaleDown(functionName string) error {
	op.ms.mux.Lock()
	handler, exists := op.ms.functionHandlers[functionName]
	config := op.ms.functionConfigs[functionName]
	op.ms.mux.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	ips := handler.IPs()
	if err := op.notifyRouteClearIPs(functionName); err != nil {
		op.logger.Error("failed to clear route before scale down", "function", functionName, "err", err)
		return fmt.Errorf("failed to clear route before scale down: %w", err)
	}

	// Measure scale-down time
	startTime := time.Now()

	// Stop the containers
	if err := handler.Stop(); err != nil {
		if restoreErr := op.notifyRouteAdd(functionName, ips, config.Labels); restoreErr != nil {
			op.logger.Error("failed to restore route after scale down failure",
				"function", functionName,
				"err", restoreErr)
			return errors.Join(
				fmt.Errorf("failed to stop function %s: %w", functionName, err),
				fmt.Errorf("failed to restore route: %w", restoreErr),
			)
		}
		return fmt.Errorf("failed to stop function %s: %w", functionName, err)
	}

	scaleDownDuration := time.Since(startTime)

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
	config := op.ms.functionConfigs[functionName]
	op.ms.mux.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	// Restart the containers
	if err := handler.Restart(); err != nil {
		return fmt.Errorf("failed to restart function %s: %w", functionName, err)
	}

	if err := op.notifyRouteAdd(functionName, handler.IPs(), config.Labels); err != nil {
		op.logger.Error("failed to restore route", "function", functionName, "err", err)
		return fmt.Errorf("function restarted but route restore failed: %w", err)
	}

	op.logger.Info("function scaled up", "function", functionName)
	return nil
}

func (op *TinyFaaSScaleOp) notifyRouteAdd(functionName string, ips []string, labels map[string]string) error {
	return op.ms.notifyRouteAdd(functionName, ips, labels)
}

func (op *TinyFaaSScaleOp) notifyRouteClearIPs(functionName string) error {
	return op.ms.notifyRouteUpdate(functionName)
}

// SetAutoScaler sets the autoscaler for the management service
func (ms *ManagementService) SetAutoScaler(as *autoscaler.AutoScaler) {
	ms.autoscaler = as
}

// GetAutoScaler returns the autoscaler instance
func (ms *ManagementService) GetAutoScaler() *autoscaler.AutoScaler {
	return ms.autoscaler
}

// ScaleUp scales up a function and records the scale-up time to the local callgraph tracker.
// cold=true means this is a user-facing cold start
// cold=false means this is a proactive prewarm
func (ms *ManagementService) ScaleUp(functionName string, cold bool) error {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return fmt.Errorf("autoscaler not enabled")
	}

	recordScaleUp, stateBefore, claimStatus := ms.claimScaleUpRecord(functionName)
	startTime := time.Now()
	if recordScaleUp {
		defer ms.releaseScaleUpRecord(functionName)
	}

	if err := ms.autoscaler.ScaleUpWhenReady(functionName); err != nil {
		return err
	}

	scaleUpDuration := time.Since(startTime)

	if recordScaleUp {
		ms.notifyScaleUp(functionName, startTime, scaleUpDuration, cold)
	}

	stateAfter, _ := ms.autoscaler.GetState(functionName)
	source := "prewarm"
	if cold {
		source = "demand"
	}
	ms.logger.Info("scale-up completed",
		"function", functionName,
		"cold", cold,
		"source", source,
		"duration", scaleUpDuration,
		"record_owner", recordScaleUp,
		"claim_status", claimStatus,
		"state_before", stateBefore,
		"state_after", stateAfter)

	return nil
}

func (ms *ManagementService) claimScaleUpRecord(functionName string) (bool, autoscaler.LifecycleState, string) {
	state, ok := ms.autoscaler.GetState(functionName)
	if !ok {
		return false, "", "state_not_found"
	}
	if state != autoscaler.StateScaledDown {
		return false, state, "state_not_scaled_down"
	}

	ms.mux.Lock()
	defer ms.mux.Unlock()
	if _, exists := ms.scaleUpClaims[functionName]; exists {
		return false, state, "already_claimed"
	}
	ms.scaleUpClaims[functionName] = struct{}{}
	return true, state, "claimed"
}

func (ms *ManagementService) releaseScaleUpRecord(functionName string) {
	ms.mux.Lock()
	delete(ms.scaleUpClaims, functionName)
	ms.mux.Unlock()
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
