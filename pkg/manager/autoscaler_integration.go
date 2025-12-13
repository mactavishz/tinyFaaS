package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
)

// TinyFaaSScaleOp implements the autoscaler.ScaleOperation interface for tinyFaaS
type TinyFaaSScaleOp struct {
	ms *ManagementService
}

// NewTinyFaaSScaleOp creates a new TinyFaaSScaleOp
func NewTinyFaaSScaleOp(ms *ManagementService) *TinyFaaSScaleOp {
	return &TinyFaaSScaleOp{ms: ms}
}

// ScaleDown stops the function containers
func (op *TinyFaaSScaleOp) ScaleDown(functionName string) error {
	op.ms.functionHandlersMutex.Lock()
	handler, exists := op.ms.functionHandlers[functionName]
	op.ms.functionHandlersMutex.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	if err := op.notifyRProxyClearIPs(functionName); err != nil {
		log.Printf("failed to notify rproxy about scale down of %s: %v", functionName, err)
		return fmt.Errorf("failed to notify rproxy about scale down: %w", err)
	}

	op.ms.autoscaler.MarkScalingDown(functionName, true)
	defer op.ms.autoscaler.MarkScalingDown(functionName, false)

	// Stop the containers
	if err := handler.Stop(); err != nil {
		return fmt.Errorf("failed to stop function %s: %w", functionName, err)
	}

	log.Printf("scaled down function %s", functionName)
	return nil
}

// ScaleUp starts the function containers
func (op *TinyFaaSScaleOp) ScaleUp(functionName string) error {
	op.ms.functionHandlersMutex.Lock()
	handler, exists := op.ms.functionHandlers[functionName]
	op.ms.functionHandlersMutex.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	// Restart the containers
	if err := handler.Restart(); err != nil {
		return fmt.Errorf("failed to restart function %s: %w", functionName, err)
	}

	// Notify rproxy to add function back to routing table
	if err := op.notifyRProxyAdd(functionName, handler.IPs()); err != nil {
		log.Printf("error: failed to notify rproxy about scale up of %s: %v", functionName, err)
		return fmt.Errorf("function restarted but rproxy notification failed: %w", err)
	}

	log.Printf("scaled up function %s", functionName)
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

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s:%d/config", op.ms.rproxyAddr, op.ms.rproxyPort), bytes.NewBuffer(b))
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

	req, err := http.NewRequest(http.MethodPatch, fmt.Sprintf("http://%s:%d/config", op.ms.rproxyAddr, op.ms.rproxyPort), bytes.NewBuffer(b))
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

// ScaleUp scales up a function (for cold start)
func (ms *ManagementService) ScaleUp(functionName string) error {
	if ms.autoscaler == nil || !ms.autoscaler.IsEnabled() {
		return fmt.Errorf("autoscaler not enabled")
	}

	if ms.autoscaler.IsScalingDown(functionName) {
		return fmt.Errorf("function %s is currently scaling down", functionName)
	}

	if !ms.autoscaler.IsScaledDown(functionName) {
		return fmt.Errorf("function %s is not scaled down", functionName)
	}

	// Scale up the function
	if err := ms.autoscaler.ScaleUp(functionName); err != nil {
		return err
	}

	// Mark as scaled up in autoscaler
	ms.autoscaler.MarkScaledDown(functionName, false)

	return nil
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
