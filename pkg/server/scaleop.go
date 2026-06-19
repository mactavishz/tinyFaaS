package server

import (
	"fmt"
	"time"

	"log/slog"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
)

// TinyFaaSScaleOp implements the autoscaler.ScaleOperation interface for the
// merged tinyFaaS server. Scaling operations manipulate the routing table and
// callgraph tracker directly instead of going through HTTP.
type TinyFaaSScaleOp struct {
	srv    *Server
	logger *slog.Logger
}

// NewTinyFaaSScaleOp creates a new TinyFaaSScaleOp bound to a server.
func NewTinyFaaSScaleOp(srv *Server, logger *slog.Logger) *TinyFaaSScaleOp {
	return &TinyFaaSScaleOp{
		srv:    srv,
		logger: logger,
	}
}

// ScaleDown stops the function containers and removes them from routing.
func (op *TinyFaaSScaleOp) ScaleDown(functionName string) error {
	op.srv.mux.Lock()
	handler, exists := op.srv.functionHandlers[functionName]
	op.srv.mux.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	ips := handler.IPs()
	labels := handler.GetLabels()

	// Remove the function's IPs from the routing table before stopping containers.
	if err := op.srv.Update(functionName); err != nil {
		op.logger.Error("failed to deactivate route for scale down", "function", functionName, "err", err)
		return fmt.Errorf("failed to deactivate route for scale down: %w", err)
	}

	// Measure scale-down time
	startTime := time.Now()

	if err := handler.Stop(); err != nil {
		// Restore the route so the function keeps serving on failure.
		if restoreErr := op.srv.Add(functionName, ips, labels); restoreErr != nil {
			op.logger.Error("failed to restore route after scale down failure",
				"function", functionName,
				"err", restoreErr)
		}
		return fmt.Errorf("failed to stop function %s: %w", functionName, err)
	}

	scaleDownDuration := time.Since(startTime)

	// Record scale-down time to callgraph tracker.
	op.srv.recordScaleDown(functionName, startTime, scaleDownDuration)

	op.logger.Info("function scaled down",
		"function", functionName,
		"duration", scaleDownDuration)
	return nil
}

// ScaleUp starts the function containers and adds them back to routing.
func (op *TinyFaaSScaleOp) ScaleUp(functionName string) error {
	op.srv.mux.Lock()
	handler, exists := op.srv.functionHandlers[functionName]
	op.srv.mux.Unlock()

	if !exists {
		return fmt.Errorf("function %s not found", functionName)
	}

	if err := handler.Restart(); err != nil {
		return fmt.Errorf("failed to restart function %s: %w", functionName, err)
	}

	// Re-register the function route directly.
	if err := op.srv.Add(functionName, handler.IPs(), handler.GetLabels()); err != nil {
		op.logger.Error("failed to register route after scale up", "function", functionName, "err", err)
		return fmt.Errorf("function restarted but route registration failed: %w", err)
	}

	op.logger.Info("function scaled up", "function", functionName)
	return nil
}

// ScaleUp scales up a function and records the scale-up time to the callgraph
// tracker. cold=true means this is a user-facing cold start; cold=false means a
// proactive prewarm.
func (s *Server) ScaleUp(functionName string, cold bool) error {
	if !s.autoscalerEnabled() {
		return fmt.Errorf("autoscaler not enabled")
	}

	recordScaleUp := s.claimScaleUpRecord(functionName)
	startTime := time.Now()
	if recordScaleUp {
		defer s.releaseScaleUpRecord(functionName)
	}

	if err := s.autoscaler.ScaleUpWhenReady(functionName); err != nil {
		return err
	}

	scaleUpDuration := time.Since(startTime)

	if recordScaleUp {
		s.recordScaleUp(functionName, startTime, scaleUpDuration, cold)
	}

	s.logger.Info("scale-up completed",
		"function", functionName,
		"cold", cold,
		"duration", scaleUpDuration)

	return nil
}

func (s *Server) claimScaleUpRecord(functionName string) bool {
	state, ok := s.autoscaler.GetState(functionName)
	if !ok || state != autoscaler.StateScaledDown {
		return false
	}

	s.mux.Lock()
	defer s.mux.Unlock()
	if _, exists := s.scaleUpClaims[functionName]; exists {
		return false
	}
	s.scaleUpClaims[functionName] = struct{}{}
	return true
}

func (s *Server) releaseScaleUpRecord(functionName string) {
	s.mux.Lock()
	delete(s.scaleUpClaims, functionName)
	s.mux.Unlock()
}

// StartRequest marks a function as blocked while serving a request.
func (s *Server) StartRequest(functionName string) error {
	if !s.autoscalerEnabled() {
		return nil
	}
	return s.autoscaler.StartInvocation(functionName)
}

// EndRequest marks a function request completion.
func (s *Server) EndRequest(functionName string) {
	if !s.autoscalerEnabled() {
		return
	}
	s.autoscaler.EndInvocation(functionName)
}
