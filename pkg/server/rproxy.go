package server

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
)

type Route struct {
	ips              []string
	isActive         bool
	callgraphEnabled bool
}

type InvocationRouter struct {
	routingTable        map[string]*Route
	reverseRoutingTable map[string]string
	mode                string
	routingTableMux     sync.RWMutex
	autoscalerEnabled   bool
	tracker             callgraph.FullTracker
	logger              *slog.Logger
	// Shared HTTP client for function invocation requests
	// Configured with connection pooling for efficient local communication
	httpClient *http.Client
	// Heartbeat worker: single goroutine sends batch heartbeats periodically
	// Functions are added to pendingHeartbeats when invoked, and the worker
	// flushes them through the configured heartbeat hook at heartbeatInterval
	heartbeatInterval time.Duration
	pendingHeartbeats map[string]struct{}
	heartbeatMux      sync.Mutex
	heartbeatStopChan chan struct{}
	heartbeatDoneChan chan struct{}
	prewarmMux        sync.Mutex
	prewarmInFlight   map[string]struct{}

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

	// After the scale-up hook returns, the invocation router may still need a brief window
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

func NewInvocationRouter(logger *slog.Logger, mode string) *InvocationRouter {
	return &InvocationRouter{
		routingTable:        make(map[string]*Route),
		reverseRoutingTable: make(map[string]string),
		mode:                mode,
		logger:              logger,
		httpClient:          newHTTPClient(),
		heartbeatInterval:   defaultHeartbeatInterval,
		pendingHeartbeats:   make(map[string]struct{}),
		heartbeatStopChan:   make(chan struct{}),
		heartbeatDoneChan:   make(chan struct{}),
		prewarmInFlight:     make(map[string]struct{}),
	}
}

func (r *InvocationRouter) IsDev() bool {
	return strings.ToLower(r.mode) == "development"
}

func (r *InvocationRouter) IsProd() bool {
	return strings.ToLower(r.mode) == "production"
}

func (r *InvocationRouter) SetTracker(tracker callgraph.FullTracker) {
	r.tracker = tracker
}

func (r *InvocationRouter) SetAutoScalerEnabled(enabled bool) {
	r.autoscalerEnabled = enabled
}

func (r *InvocationRouter) SetScaleUpHook(hook func(name string, cold bool) error) {
	r.scaleUpHook = hook
}

func (r *InvocationRouter) SetRequestHooks(start func(name string) error, finish func(name string)) {
	r.requestStartHook = start
	r.requestFinishHook = finish
}

func (r *InvocationRouter) SetHeartbeatHooks(single func(name string) error, batch func(names []string) error) {
	r.heartbeatHook = single
	r.heartbeatBatchHook = batch
}

// CallgraphEnabled returns whether callgraph tracking is enabled for a given function,
func (r *InvocationRouter) CallgraphEnabled(functionName string) bool {
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

func (r *InvocationRouter) Add(name string, ips []string, labels map[string]string) error {
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
	r.clearPrewarmReservation(name)
	return nil
}

func (r *InvocationRouter) Del(name string) error {
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
	r.clearPrewarmReservation(name)

	// Clear callgraph data for deleted function
	if r.tracker != nil {
		r.tracker.ClearFunctionData(name)
	}

	return nil
}

func (r *InvocationRouter) Update(name string) error {
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
	r.clearPrewarmReservation(name)

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

func (r *InvocationRouter) getRouteSnapshot(name string) (*Route, bool) {
	r.routingTableMux.RLock()
	defer r.routingTableMux.RUnlock()

	route, ok := r.routingTable[name]
	if !ok {
		return nil, false
	}

	return copyRoute(route), true
}

func (r *InvocationRouter) waitForRouteReady(name string, timeout time.Duration) (*Route, bool) {
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

func (r *InvocationRouter) Call(name string, payload []byte, async bool, header http.Header) (int, []byte) {
	startTime := time.Now()
	if async {
		r.logger.Warn("legacy async flag ignored; use /async-fn endpoint", "name", name)
	}

	return r.invoke(name, payload, header, startTime)
}

func (r *InvocationRouter) invoke(name string, payload []byte, header http.Header, startTime time.Time) (int, []byte) {
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

	// Record the edge once the callee route is known. This matches the provider
	// lifecycle more closely: accepted invocations contribute to callgraph
	// timing before runtime dispatch and before any cold-start wait.
	if calleeRoute.callgraphEnabled && r.tracker != nil {
		effectiveCaller := ""
		// If caller's callgraph is disabled but callee is enabled, record as external (caller="").
		if caller != "" && callerRouteOK && callerRoute.callgraphEnabled {
			effectiveCaller = caller
		}
		r.tracker.RecordEdge(effectiveCaller, name, callID, callerExecID, startTime)
	}

	startedExecution := r.startCallgraphExecution(name, callID, calleeExecID, calleeRoute)
	if startedExecution {
		defer r.finishCallgraphExecution(name, callID, calleeExecID)
	}

	callerColdStartOffset := time.Duration(0)
	if r.autoscalerEnabled && !calleeRoute.isActive && r.tracker != nil {
		if stats, ok := r.tracker.GetFunctionStats(name); ok {
			callerColdStartOffset = stats.AvgColdStartDuration
		}
	}

	// Trigger prewarming for downstream functions before a cold demanded caller
	// is scaled up, so first-hop targets can warm while the caller is starting.
	if calleeRoute.callgraphEnabled && r.autoscalerEnabled && r.tracker != nil && r.tracker.PrewarmEnabled() {
		r.prewarmDownstreamWithLeadOffset(name, callerColdStartOffset)
	}

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

	requestStarted := false
	finishRequest := func() {
		if !requestStarted {
			return
		}
		requestStarted = false
		if err := r.notifyRequestFinish(name); err != nil {
			r.logger.Warn("failed to mark request finish", "name", name, "err", err)
		}
	}
	if r.autoscalerEnabled {
		if err := r.notifyRequestStart(name); err != nil {
			r.logger.Error("failed to mark request start", "name", name, "err", err)
			return http.StatusServiceUnavailable, nil
		}
		requestStarted = true
		defer finishRequest()
	}

	r.queueHeartbeat(name)

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

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		r.logger.Error("failed to create request", "err", err)
		return http.StatusInternalServerError, nil
	}

	req.Header = header

	// call function and return results
	resp, err := r.httpClient.Do(req)
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

	return resp.StatusCode, res_body
}

func (r *InvocationRouter) GetFunctionNameByIP(ip string) (string, bool) {
	r.routingTableMux.RLock()
	defer r.routingTableMux.RUnlock()
	name, ok := r.reverseRoutingTable[ip]
	return name, ok
}

// GetTracker returns the callgraph tracker
func (r *InvocationRouter) GetTracker() callgraph.Tracker {
	return r.tracker
}

func (r *InvocationRouter) startCallgraphExecution(name string, callID string, executionID string, route *Route) bool {
	if route == nil || !route.callgraphEnabled || r.tracker == nil {
		return false
	}

	r.tracker.StartExecution(name, callID, executionID, time.Now())
	return true
}

func (r *InvocationRouter) finishCallgraphExecution(name string, callID string, executionID string) {
	if r.tracker == nil {
		return
	}

	r.tracker.EndExecution(name, callID, executionID, time.Now())
}

// prewarmDownstream triggers prewarming for downstream functions based on call graph analysis.
// This function should be called asynchronously (fire-and-forget) to avoid adding latency to the request.
func (r *InvocationRouter) prewarmDownstream(functionName string) {
	r.prewarmDownstreamWithLeadOffset(functionName, 0)
}

func (r *InvocationRouter) prewarmDownstreamWithLeadOffset(functionName string, leadOffset time.Duration) {
	targets := r.tracker.GetPrewarmTargets(functionName)
	if len(targets) == 0 {
		return
	}

	r.logger.Debug("prewarming downstream functions",
		"caller", functionName,
		"targetCount", len(targets),
		"leadOffset", leadOffset)

	for _, target := range targets {
		r.schedulePrewarmWithLeadOffset(functionName, target, leadOffset)
	}
}

// schedulePrewarm schedules a prewarm operation for a target function.
// It calculates the delay based on lead time and cold start estimates,
// ensuring the function is ready just in time for when it's needed.
func (r *InvocationRouter) schedulePrewarm(caller string, target callgraph.PrewarmTarget) {
	r.schedulePrewarmWithLeadOffset(caller, target, 0)
}

func (r *InvocationRouter) schedulePrewarmWithLeadOffset(caller string, target callgraph.PrewarmTarget, leadOffset time.Duration) {
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
	if r.tracker != nil {
		stats, ok := r.tracker.GetFunctionStats(target.FunctionName)
		if ok {
			coldStartTime = stats.AvgColdStartDuration
		}
	}

	// Calculate when to trigger prewarm:
	// delay = leadTime - coldStartTime - margin - leadOffset
	// We want the function to be ready *before* it's needed
	const safetyMargin = 50 * time.Millisecond
	delay := target.LeadTime - coldStartTime - safetyMargin - leadOffset

	if delay <= 0 {
		if !r.reservePrewarm(target.FunctionName) {
			r.logger.Debug("skipping prewarming - target already reserved",
				"function", target.FunctionName)
			return
		}
		r.logger.Info("prewarming immediately",
			"caller", caller,
			"target", target.FunctionName,
			"leadTime", target.LeadTime,
			"coldStartTime", coldStartTime,
			"leadOffset", leadOffset,
			"delay", delay)
		go r.executePrewarm(target.FunctionName)
		return
	}

	if !r.reservePrewarm(target.FunctionName) {
		r.logger.Debug("skipping scheduled prewarm - target already reserved",
			"function", target.FunctionName)
		return
	}

	r.logger.Info("scheduling prewarm",
		"caller", caller,
		"target", target.FunctionName,
		"leadTime", target.LeadTime,
		"coldStartTime", coldStartTime,
		"leadOffset", leadOffset,
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
			r.clearPrewarmReservation(target.FunctionName)
			return
		}

		r.executePrewarm(target.FunctionName)
	})
}

func (r *InvocationRouter) reservePrewarm(funcName string) bool {
	r.prewarmMux.Lock()
	defer r.prewarmMux.Unlock()

	if _, exists := r.prewarmInFlight[funcName]; exists {
		return false
	}
	r.prewarmInFlight[funcName] = struct{}{}
	return true
}

func (r *InvocationRouter) clearPrewarmReservation(funcName string) {
	r.prewarmMux.Lock()
	delete(r.prewarmInFlight, funcName)
	r.prewarmMux.Unlock()
}

// executePrewarm performs the actual prewarm operation for a function.
func (r *InvocationRouter) executePrewarm(funcName string) {
	if err := r.scaleUpFunction(funcName, false); err != nil {
		r.clearPrewarmReservation(funcName)
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
func (r *InvocationRouter) queueHeartbeat(name string) {
	if name == "" {
		return
	}
	r.heartbeatMux.Lock()
	r.pendingHeartbeats[name] = struct{}{}
	r.heartbeatMux.Unlock()
}

// StartHeartbeatWorker starts the background goroutine that sends batch heartbeats.
// This should be called once when the invocation router starts.
func (r *InvocationRouter) StartHeartbeatWorker() {
	go r.heartbeatWorker()
}

// StopHeartbeatWorker stops the heartbeat worker gracefully.
// This should be called during shutdown.
func (r *InvocationRouter) StopHeartbeatWorker() {
	close(r.heartbeatStopChan)
	<-r.heartbeatDoneChan
}

// heartbeatWorker is the background goroutine that periodically sends batch heartbeats.
func (r *InvocationRouter) heartbeatWorker() {
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
func (r *InvocationRouter) flushHeartbeats() {
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

func (r *InvocationRouter) sendBatchHeartbeat(functions []string) error {
	if r.heartbeatBatchHook != nil {
		return r.heartbeatBatchHook(functions)
	}
	return fmt.Errorf("heartbeat batch hook not configured")
}

// cold=true means this is a user-facing cold start
// cold=false means this is a proactive prewarm
func (r *InvocationRouter) scaleUpFunction(name string, cold bool) error {
	if r.scaleUpHook != nil {
		return r.scaleUpHook(name, cold)
	}
	return fmt.Errorf("scale-up hook not configured")
}

func (r *InvocationRouter) notifyRequestStart(name string) error {
	if r.requestStartHook != nil {
		return r.requestStartHook(name)
	}
	return fmt.Errorf("request-start hook not configured")
}

func (r *InvocationRouter) notifyRequestFinish(name string) error {
	if r.requestFinishHook != nil {
		r.requestFinishHook(name)
		return nil
	}
	return nil
}
