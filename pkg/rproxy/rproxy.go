package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"log/slog"

	retry "github.com/avast/retry-go/v5"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
)

type Route struct {
	ips              []string
	isActive         bool
	callgraphEnabled bool
}

type RProxy struct {
	routingTable        map[string]*Route
	reverseRoutingTable map[string]string
	mode                string
	routingTableMux     sync.RWMutex
	autoscalerEnabled   bool
	tracker             callgraph.FullTracker
	logger              *slog.Logger
	// gatewayAddr is the host:port of the gateway used for heartbeat and scale-up requests
	gatewayAddr  string
	heatbeatAddr string
	scaleUpAddr  string
	// Shared HTTP client for internal requests (heartbeat, scale-up)
	// Configured with connection pooling for efficient local communication
	httpClient        *http.Client
	requestStartAddr  string
	requestFinishAddr string
	// Heartbeat worker: single goroutine sends batch heartbeats periodically
	// Functions are added to pendingHeartbeats when invoked, and the worker
	// flushes them to the manager at heartbeatInterval
	heartbeatInterval time.Duration
	pendingHeartbeats map[string]struct{}
	heartbeatMux      sync.Mutex
	heartbeatStopChan chan struct{}
	heartbeatDoneChan chan struct{}

	scaleUpHook        func(name string, cold bool) error
	requestStartHook   func(name string) error
	requestFinishHook  func(name string)
	heartbeatHook      func(name string) error
	heartbeatBatchHook func(names []string) error
}

func (r *Route) PickIP() (string, error) {
	if len(r.ips) == 0 {
		return "", fmt.Errorf("no IP available")
	}
	return r.ips[rand.Intn(len(r.ips))], nil
}

// defaultHeartbeatInterval is the interval at which the heartbeat worker sends batch heartbeats.
// This should be significantly smaller than the autoscaler's check interval (default 10s)
// to ensure at least 2-3 heartbeats are sent per check cycle under sustained load.
// Using 2 seconds provides ~5 heartbeats per check cycle as a safety margin.
const (
	defaultHeartbeatInterval = 2 * time.Second

	// After manager accepts a scale-up request, rproxy may still need a brief window
	// for route/IP state to be visible locally before invocation can proceed.
	scaleUpRouteReadyTimeout  = 500 * time.Millisecond
	scaleUpRouteReadyInterval = 25 * time.Millisecond
)

// newHTTPClient creates an HTTP client optimized for internal communication.
// Uses connection pooling to reduce connection overhead for frequent requests.
func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 20, // High since we communicate with few hosts (localhost)
			MaxConnsPerHost:     50,
			IdleConnTimeout:     120 * time.Second,
			DisableCompression:  true,  // Local communication, no need for compression
			DisableKeepAlives:   false, // Keep connections alive for reuse
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
		},
	}
}

func New(logger *slog.Logger, mode string) *RProxy {
	return &RProxy{
		routingTable:        make(map[string]*Route),
		reverseRoutingTable: make(map[string]string),
		mode:                mode,
		logger:              logger,
		httpClient:          newHTTPClient(),
		heartbeatInterval:   defaultHeartbeatInterval,
		pendingHeartbeats:   make(map[string]struct{}),
		heartbeatStopChan:   make(chan struct{}),
		heartbeatDoneChan:   make(chan struct{}),
	}
}

func (r *RProxy) IsDev() bool {
	return strings.ToLower(r.mode) == "development"
}

func (r *RProxy) IsProd() bool {
	return strings.ToLower(r.mode) == "production"
}

func (r *RProxy) SetTracker(tracker callgraph.FullTracker) {
	r.tracker = tracker
}

func (r *RProxy) SetAutoScalerEnabled(enabled bool) {
	r.autoscalerEnabled = enabled
}

func (r *RProxy) SetGatewayAddr(addr string) {
	r.gatewayAddr = addr
	r.heatbeatAddr = fmt.Sprintf("http://%s/system/heartbeat", addr)
	r.scaleUpAddr = fmt.Sprintf("http://%s/system/scale-up", addr)
	r.requestStartAddr = fmt.Sprintf("http://%s/system/request-start", addr)
	r.requestFinishAddr = fmt.Sprintf("http://%s/system/request-finish", addr)
	r.logger.Debug("gateway url set", "url", r.gatewayAddr)
	r.logger.Debug("heartbeat url set", "url", r.heatbeatAddr)
	r.logger.Debug("scale-up url set", "url", r.scaleUpAddr)
}

func (r *RProxy) SetScaleUpHook(hook func(name string, cold bool) error) {
	r.scaleUpHook = hook
}

func (r *RProxy) SetRequestHooks(start func(name string) error, finish func(name string)) {
	r.requestStartHook = start
	r.requestFinishHook = finish
}

func (r *RProxy) SetHeartbeatHooks(single func(name string) error, batch func(names []string) error) {
	r.heartbeatHook = single
	r.heartbeatBatchHook = batch
}

// CallgraphEnabled returns whether callgraph tracking is enabled for a given function,
func (r *RProxy) CallgraphEnabled(functionName string) bool {
	if r.tracker == nil || !r.tracker.Enabled() {
		return false
	}
	if strings.TrimSpace(functionName) == "" {
		return false
	}

	r.routingTableMux.RLock()
	route, ok := r.routingTable[functionName]
	r.routingTableMux.RUnlock()
	if !ok {
		// If we don't have routing state (e.g., early startup), fall back to global enablement.
		return true
	}
	return route.callgraphEnabled
}

func (r *RProxy) Add(name string, ips []string, labels map[string]string) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}

	r.logger.Debug("adding function route", "name", name, "ips", ips)
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	callgraphEnabled := callgraph.ParseCallgraphConfig(labels, "tinyfaas", r.tracker)

	r.routingTable[name] = &Route{
		ips:              ips,
		isActive:         true,
		callgraphEnabled: callgraphEnabled,
	}
	for _, ip := range ips {
		r.reverseRoutingTable[ip] = name
	}
	return nil
}

func (r *RProxy) Del(name string) error {
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	route, ok := r.routingTable[name]
	if !ok {
		return fmt.Errorf("function not found")
	}

	r.logger.Debug("deleting function route", "name", name)

	// Clean up reverse routing table first (before deleting from routing table)
	for _, ip := range route.ips {
		delete(r.reverseRoutingTable, ip)
	}

	// Now delete from routing table
	delete(r.routingTable, name)

	// Clear callgraph data for deleted function
	if r.tracker != nil {
		r.tracker.ClearFunctionData(name)
	}

	return nil
}

func (r *RProxy) Update(name string) error {
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	route, ok := r.routingTable[name]
	if !ok {
		return nil
	}

	r.logger.Debug("deactivating function route", "name", name)
	for _, ip := range route.ips {
		delete(r.reverseRoutingTable, ip)
	}
	route.ips = nil
	route.isActive = false

	return nil
}

func copyRoute(route *Route) *Route {
	if route == nil {
		return nil
	}

	ips := make([]string, len(route.ips))
	copy(ips, route.ips)

	return &Route{
		ips:              ips,
		isActive:         route.isActive,
		callgraphEnabled: route.callgraphEnabled,
	}
}

func (r *RProxy) getRouteSnapshot(name string) (*Route, bool) {
	r.routingTableMux.RLock()
	defer r.routingTableMux.RUnlock()

	route, ok := r.routingTable[name]
	if !ok {
		return nil, false
	}

	return copyRoute(route), true
}

func (r *RProxy) waitForRouteReady(name string, timeout time.Duration) (*Route, bool) {
	deadline := time.Now().Add(timeout)
	for {
		route, exists := r.getRouteSnapshot(name)
		if !exists {
			return nil, false
		}

		if route.isActive && len(route.ips) > 0 {
			return route, true
		}

		if time.Now().After(deadline) {
			return route, true
		}

		time.Sleep(scaleUpRouteReadyInterval)
	}
}

func (r *RProxy) Call(name string, payload []byte, async bool, header http.Header) (int, []byte) {
	startTime := time.Now()
	if async {
		r.logger.Warn("legacy rproxy async flag ignored; use /async-fn endpoint", "name", name)
	}

	return r.invoke(name, payload, header, startTime)
}

func (r *RProxy) invoke(name string, payload []byte, header http.Header, startTime time.Time) (int, []byte) {
	if header == nil {
		header = http.Header{}
	}

	callID := header.Get("X-Call-Id")
	if callID == "" {
		r.logger.Warn("missing X-Call-Id header", "function", name)
		// Generate fallback callID
		callID = fmt.Sprintf("unknown-%s", uuid.New().String())
	}

	callerExecID := header.Get("X-Exec-Id")
	calleeExecID := uuid.New().String()
	header.Set("X-Exec-Id", calleeExecID)

	// Extract source IP to detect if this is an internal call
	sourceIP := header.Get("X-Source-Ip")
	caller := ""
	if sourceIP != "" {
		// Check if this is an internal call (from another function)
		if callerFunc, ok := r.GetFunctionNameByIP(sourceIP); ok {
			caller = callerFunc
			r.logger.Debug("detected internal call",
				"caller", caller,
				"callee", name,
				"callID", callID)
		}
	}

	calleeRoute, ok := r.getRouteSnapshot(name)
	callerRoute, callerRouteOK := r.getRouteSnapshot(caller)

	if !ok {
		r.logger.Error("function not found", "name", name)
		return http.StatusNotFound, nil
	}

	r.logger.Debug("found function route", "ips", calleeRoute.ips, "active", calleeRoute.isActive)

	// Check if function is scaled down and trigger cold start if needed
	if r.autoscalerEnabled && !calleeRoute.isActive {
		r.logger.Info("function is scaled down, triggering cold start", "name", name)
		// cold=true since this is triggered by a user request
		// Manager will measure and record the cold start time
		err := r.scaleUpFunction(name, true)
		if err != nil {
			r.logger.Error("failed to trigger cold start", "name", name, "err", err)
			return http.StatusServiceUnavailable, nil
		}

		updatedRoute, exists := r.waitForRouteReady(name, scaleUpRouteReadyTimeout)
		if !exists {
			r.logger.Error("function route disappeared after scale-up", "name", name)
			return http.StatusServiceUnavailable, nil
		}
		calleeRoute = updatedRoute
	}

	if !calleeRoute.isActive || len(calleeRoute.ips) == 0 {
		r.logger.Warn("function route not ready for invocation",
			"name", name,
			"active", calleeRoute.isActive,
			"ipCount", len(calleeRoute.ips))
		return http.StatusServiceUnavailable, nil
	}

	// Record the edge only after the route is confirmed ready for invocation.
	// This avoids polluting callgraph edges with failed cold-start attempts.
	if calleeRoute.callgraphEnabled && r.tracker != nil {
		effectiveCaller := ""
		// If caller's callgraph is disabled but callee is enabled, record as external (caller="")
		if caller != "" && callerRouteOK && callerRoute.callgraphEnabled {
			effectiveCaller = caller
		}
		r.tracker.RecordEdge(effectiveCaller, name, callID, callerExecID, startTime)
	}

	r.queueHeartbeat(name)
	finishRequest := func() {
		if err := r.notifyRequestFinish(name); err != nil {
			r.logger.Warn("failed to mark request finish", "name", name, "err", err)
		}
	}
	if r.autoscalerEnabled {
		if err := r.notifyRequestStart(name); err != nil {
			r.logger.Error("failed to mark request start", "name", name, "err", err)
			return http.StatusServiceUnavailable, nil
		}
	}

	// choose random handler
	ip, err := calleeRoute.PickIP()

	if err != nil {
		r.logger.Error("failed to pick function ip", "err", err)
		if r.autoscalerEnabled {
			finishRequest()
		}
		return http.StatusInternalServerError, nil
	}

	r.logger.Debug("chosen function ip", "ip", ip)

	// Trigger prewarming for downstream functions (fire-and-forget, non-blocking)
	// Prewarming requires both callgraph and autoscaler to be enabled
	if calleeRoute.callgraphEnabled && r.autoscalerEnabled && r.tracker != nil && r.tracker.PrewarmEnabled() {
		go r.prewarmDownstream(name)
	}

	// Mark that this function is starting execution
	startedExecution := false
	functionStartTime := time.Now()
	if calleeRoute.callgraphEnabled && r.tracker != nil {
		r.tracker.StartExecution(name, callID, calleeExecID, functionStartTime)
		startedExecution = true
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		r.logger.Error("failed to create request", "err", err)
		if startedExecution {
			r.tracker.EndExecution(name, callID, calleeExecID, time.Now())
		}
		if r.autoscalerEnabled {
			finishRequest()
		}
		return http.StatusInternalServerError, nil
	}

	req.Header = header

	// call function and return results
	if r.autoscalerEnabled {
		defer finishRequest()
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.logger.Error("failed to invoke function", "err", err)
		return http.StatusInternalServerError, nil
	}

	defer resp.Body.Close()
	res_body, err := io.ReadAll(resp.Body)

	if err != nil {
		r.logger.Error("failed to read response body", "err", err)
		return http.StatusInternalServerError, nil
	}

	// End execution - records function stats and cleans up context
	if startedExecution {
		r.tracker.EndExecution(name, callID, calleeExecID, time.Now())
	}

	return resp.StatusCode, res_body
}

func (r *RProxy) GetFunctionNameByIP(ip string) (string, bool) {
	r.routingTableMux.RLock()
	defer r.routingTableMux.RUnlock()
	name, ok := r.reverseRoutingTable[ip]
	return name, ok
}

// GetTracker returns the callgraph tracker
func (r *RProxy) GetTracker() callgraph.Tracker {
	return r.tracker
}

// prewarmDownstream triggers prewarming for downstream functions based on call graph analysis.
// This function should be called asynchronously (fire-and-forget) to avoid adding latency to the request.
func (r *RProxy) prewarmDownstream(functionName string) {
	targets := r.tracker.GetPrewarmTargets(functionName)
	if len(targets) == 0 {
		return
	}

	r.logger.Debug("prewarming downstream functions",
		"caller", functionName,
		"targetCount", len(targets))

	for _, target := range targets {
		r.schedulePrewarm(functionName, target)
	}
}

// schedulePrewarm schedules a prewarm operation for a target function.
// It calculates the delay based on lead time and cold start estimates,
// ensuring the function is ready just in time for when it's needed.
func (r *RProxy) schedulePrewarm(caller string, target callgraph.PrewarmTarget) {
	// Respect per-function callgraph enablement: if target is disabled, skip prewarming it.
	if !r.CallgraphEnabled(target.FunctionName) {
		return
	}

	// Check if the function is currently scaled down
	r.routingTableMux.RLock()
	route, exists := r.routingTable[target.FunctionName]
	isScaledDown := exists && !route.isActive
	r.routingTableMux.RUnlock()

	if !isScaledDown {
		r.logger.Debug("skipping prewarming - function already active",
			"function", target.FunctionName)
		return
	}

	// Get the estimated cold start time from function stats
	var coldStartTime time.Duration
	if stats, ok := r.tracker.GetFunctionStats(target.FunctionName); ok {
		coldStartTime = stats.AvgColdStartDuration
	}

	// Calculate when to trigger prewarm:
	// delay = leadTime - coldStartTime - margin
	// We want the function to be ready *before* it's needed
	const safetyMargin = 50 * time.Millisecond
	delay := target.LeadTime - coldStartTime - safetyMargin

	if delay <= 0 {
		r.logger.Info("prewarming immediately",
			"caller", caller,
			"target", target.FunctionName,
			"leadTime", target.LeadTime,
			"coldStartTime", coldStartTime,
			"delay", delay)
		go r.executePrewarm(target.FunctionName)
		return
	}

	r.logger.Info("scheduling prewarm",
		"caller", caller,
		"target", target.FunctionName,
		"leadTime", target.LeadTime,
		"coldStartTime", coldStartTime,
		"delay", delay)

	// Schedule the prewarm after the calculated delay
	time.AfterFunc(delay, func() {
		// Re-check if function is still scaled down at trigger time
		r.routingTableMux.RLock()
		route, exists := r.routingTable[target.FunctionName]
		stillScaledDown := exists && !route.isActive
		r.routingTableMux.RUnlock()

		if !stillScaledDown {
			r.logger.Debug("skipping scheduled prewarm - function became active",
				"function", target.FunctionName)
			return
		}

		r.executePrewarm(target.FunctionName)
	})
}

// executePrewarm performs the actual prewarm operation for a function.
func (r *RProxy) executePrewarm(funcName string) {
	if err := r.scaleUpFunction(funcName, false); err != nil {
		r.logger.Debug("prewarm downstream function failed",
			"function", funcName,
			"err", err)
	} else {
		r.logger.Info("prewarm downstream function completed",
			"function", funcName)
	}
}

// queueHeartbeat adds a function to the pending heartbeat set.
// The heartbeat worker will batch-send heartbeats for all pending functions.
// This is a non-blocking operation that avoids spawning goroutines per-call.
func (r *RProxy) queueHeartbeat(name string) {
	if name == "" {
		return
	}
	r.heartbeatMux.Lock()
	r.pendingHeartbeats[name] = struct{}{}
	r.heartbeatMux.Unlock()
}

// StartHeartbeatWorker starts the background goroutine that sends batch heartbeats.
// This should be called once when the rproxy starts.
func (r *RProxy) StartHeartbeatWorker() {
	go r.heartbeatWorker()
}

// StopHeartbeatWorker stops the heartbeat worker gracefully.
// This should be called during shutdown.
func (r *RProxy) StopHeartbeatWorker() {
	close(r.heartbeatStopChan)
	<-r.heartbeatDoneChan
}

// heartbeatWorker is the background goroutine that periodically sends batch heartbeats.
func (r *RProxy) heartbeatWorker() {
	defer close(r.heartbeatDoneChan)

	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.flushHeartbeats()
		case <-r.heartbeatStopChan:
			// Final flush before stopping
			r.flushHeartbeats()
			return
		}
	}
}

// flushHeartbeats collects all pending heartbeats and sends them in a single batch request.
func (r *RProxy) flushHeartbeats() {
	// Collect and clear pending heartbeats atomically
	r.heartbeatMux.Lock()
	if len(r.pendingHeartbeats) == 0 {
		r.heartbeatMux.Unlock()
		return
	}
	functions := make([]string, 0, len(r.pendingHeartbeats))
	for name := range r.pendingHeartbeats {
		functions = append(functions, name)
	}
	// Clear the map by creating a new one (more efficient for large maps)
	r.pendingHeartbeats = make(map[string]struct{})
	r.heartbeatMux.Unlock()

	// Send batch heartbeat
	if err := r.sendBatchHeartbeat(functions); err != nil {
		r.logger.Debug("failed to send batch heartbeat", "functions", functions, "err", err)
		// Re-queue failed functions for next cycle
		r.heartbeatMux.Lock()
		for _, name := range functions {
			r.pendingHeartbeats[name] = struct{}{}
		}
		r.heartbeatMux.Unlock()
	}
}

// sendBatchHeartbeat sends a batch heartbeat request to the manager.
func (r *RProxy) sendBatchHeartbeat(functions []string) error {
	if r.heartbeatBatchHook != nil {
		return r.heartbeatBatchHook(functions)
	}

	reqData := struct {
		Functions []string `json:"functions"`
	}{
		Functions: functions,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	err = retry.New(
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.OnRetry(func(attempt uint, err error) {
			r.logger.Debug("batch heartbeat attempt failed", "attempt", attempt, "functions", functions, "err", err)
		}),
	).Do(
		func() error {
			req, err := http.NewRequest(http.MethodPost, r.heatbeatAddr, bytes.NewBuffer(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := r.httpClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("batch heartbeat returned status %d", resp.StatusCode)
			}

			r.logger.Debug("successfully sent batch heartbeat", "count", len(functions))
			return nil
		},
	)

	return err
}

// scaleUpFunction calls the manager to scale up a function
// cold=true means this is a user-facing cold start
// cold=false means this is a proactive prewarm
func (r *RProxy) scaleUpFunction(name string, cold bool) error {
	if r.scaleUpHook != nil {
		return r.scaleUpHook(name, cold)
	}

	reqData := struct {
		FunctionName string `json:"name"`
		Cold         bool   `json:"cold"`
	}{
		FunctionName: name,
		Cold:         cold,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	err = retry.New(
		retry.Attempts(5),
		retry.Delay(100*time.Millisecond),
		retry.OnRetry(func(attempt uint, err error) {
			r.logger.Debug("scale-up attempt failed", "attempt", attempt, "name", name, "cold", cold, "err", err)
		}),
	).Do(
		func() error {
			req, err := http.NewRequest(http.MethodPost, r.scaleUpAddr, bytes.NewBuffer(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := r.httpClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("scale-up returned status %d", resp.StatusCode)
			}

			r.logger.Info("successfully scaled up the function", "name", name, "cold", cold)
			return nil
		},
	)
	if err != nil {
		r.logger.Error("failed to scale up the function", "name", name, "cold", cold, "err", err)
		return err
	}

	return nil
}

func (r *RProxy) notifyRequestStart(name string) error {
	if r.requestStartHook != nil {
		return r.requestStartHook(name)
	}

	reqData := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, r.requestStartAddr, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request-start returned status %d", resp.StatusCode)
	}
	return nil
}

func (r *RProxy) notifyRequestFinish(name string) error {
	if r.requestFinishHook != nil {
		r.requestFinishHook(name)
		return nil
	}

	reqData := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, r.requestFinishAddr, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request-finish returned status %d", resp.StatusCode)
	}
	return nil
}
