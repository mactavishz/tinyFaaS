package server

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
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

// EdgeKindHeader is propagated by the gateway/queue worker to tell the router
// whether the caller invoked the callee synchronously (/fn/) or asynchronously
// (/async-fn/). It feeds the prewarm scheduler so async edges are deprioritised.
const EdgeKindHeader = "X-Tinyfaas-Edge-Kind"

// PrewarmTuning controls how the invocation router schedules prewarms.
type PrewarmTuning struct {
	// Concurrency caps how many prewarm scale-ups may run at the same time.
	// Demand cold starts always bypass this cap.
	Concurrency int
	// PerCallLimit caps how many prewarms a single caller invocation may schedule.
	// Targets are sorted by priority (sync first, then expected savings) and the
	// first PerCallLimit are scheduled; the rest are dropped.
	PerCallLimit int
	// MinSavings is the minimum expected savings a prewarm target must offer
	// to be considered. Targets with savings below this are skipped.
	MinSavings time.Duration
	// SafetyMargin is subtracted from the lead time when computing the prewarm
	// firing delay so the callee is ready slightly before it's needed.
	SafetyMargin time.Duration
}

type prewarmAttempt struct {
	id          string
	callID      string
	executionID string
	caller      string
	target      string
	kind        callgraph.EdgeKind
	createdAt   time.Time
	fireAt      time.Time
	leadTime    time.Duration
	coldStart   time.Duration
}

type prewarmAdmission struct {
	semaphore chan struct{}
	used      int
	capacity  int
	rejection string
}

const (
	prewarmRejectedConcurrency = "concurrency_budget_exhausted"
	prewarmRejectedSyncReserve = "sync_capacity_reserved"
)

func (a prewarmAttempt) logAttrs(extra ...any) []any {
	attrs := []any{
		"prewarm_id", a.id,
		"call_id", a.callID,
		"execution_id", a.executionID,
		"caller", a.caller,
		"target", a.target,
		"kind", a.kind,
		"leadTime", a.leadTime,
		"coldStartTime", a.coldStart,
	}
	return append(attrs, extra...)
}

// DefaultPrewarmTuning returns reasonable defaults
func DefaultPrewarmTuning() PrewarmTuning {
	return PrewarmTuning{
		Concurrency:  2,
		PerCallLimit: 2,
		MinSavings:   100 * time.Millisecond,
		SafetyMargin: 50 * time.Millisecond,
	}
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
	httpClient      *http.Client
	prewarmMux      sync.Mutex
	prewarmInFlight map[string]struct{}
	// prewarmSemaphore caps concurrent prewarm scale-ups (demand bypasses).
	// nil disables the cap. Non-sync prewarms may use at most N-1 slots so
	// synchronous critical-path work always has reserved capacity.
	prewarmSemaphore chan struct{}
	prewarmTuning    PrewarmTuning

	scaleUpHook       func(name string, cold bool) error
	requestStartHook  func(name string) error
	requestFinishHook func(name string)
}

func (r *Route) PickIP() (string, error) {
	if len(r.ips) == 0 {
		return "", fmt.Errorf("no IP available")
	}
	return r.ips[rand.Intn(len(r.ips))], nil
}

const (
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
	tuning := DefaultPrewarmTuning()
	r := &InvocationRouter{
		routingTable:        make(map[string]*Route),
		reverseRoutingTable: make(map[string]string),
		mode:                mode,
		logger:              logger,
		httpClient:          newHTTPClient(),
		prewarmInFlight:     make(map[string]struct{}),
		prewarmTuning:       tuning,
	}
	if tuning.Concurrency > 0 {
		r.prewarmSemaphore = make(chan struct{}, tuning.Concurrency)
	}
	return r
}

// SetPrewarmTuning replaces the prewarm scheduling parameters. The semaphore
// is rebuilt to match the new concurrency cap. Calling this while prewarms are
// in-flight is safe but in-flight goroutines will continue to use the old
// semaphore until they release it; subsequent prewarms see the new cap.
func (r *InvocationRouter) SetPrewarmTuning(tuning PrewarmTuning) {
	r.prewarmMux.Lock()
	defer r.prewarmMux.Unlock()
	r.prewarmTuning = tuning
	if tuning.Concurrency > 0 {
		r.prewarmSemaphore = make(chan struct{}, tuning.Concurrency)
	} else {
		r.prewarmSemaphore = nil
	}
}

// PrewarmTuning returns the current scheduling parameters.
func (r *InvocationRouter) PrewarmTuning() PrewarmTuning {
	r.prewarmMux.Lock()
	defer r.prewarmMux.Unlock()
	return r.prewarmTuning
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
	header.Set("X-Call-Id", callID)

	callerExecID := header.Get("X-Exec-Id")
	calleeExecID := uuid.New().String()
	header.Set("X-Exec-Id", calleeExecID)

	caller, callerFound, callerCallgraphEnabled := r.extractCaller(callID, callerExecID, header)

	calleeRoute, ok := r.getRouteSnapshot(name)

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

	// Record the edge only after the route is ready and the autoscaler accepts the invocation.
	if calleeRoute.callgraphEnabled && r.tracker != nil {
		effectiveCaller := ""
		// If caller's callgraph is disabled but callee is enabled, record as external (caller="")
		if caller != "" && callerFound && callerCallgraphEnabled {
			effectiveCaller = caller
		}
		// Edge kind is propagated by the gateway / queue worker via header.
		// Sync edges (/fn/) appear on the caller's critical path; async edges
		// (/async-fn/) do not. The prewarm scheduler uses this to deprioritise
		// async fan-out that does not contribute to user-visible latency.
		edgeKind := callgraph.ParseEdgeKind(strings.TrimSpace(header.Get(EdgeKindHeader)))
		r.tracker.RecordEdgeWithKind(effectiveCaller, name, callID, callerExecID, startTime, edgeKind)
	}

	// choose random handler
	ip, err := calleeRoute.PickIP()

	if err != nil {
		r.logger.Error("failed to pick function ip", "err", err)
		return http.StatusInternalServerError, nil
	}

	r.logger.Debug("chosen function ip", "ip", ip)

	if calleeRoute.callgraphEnabled && r.tracker != nil {
		r.tracker.StartExecution(name, callID, calleeExecID, time.Now())
		defer func() {
			r.tracker.EndExecution(name, callID, calleeExecID, time.Now())
		}()

		// Trigger prewarming after the demanded function is ready and execution is tracked.
		if r.autoscalerEnabled && r.tracker.PrewarmEnabled() {
			go r.prewarmDownstream(name, callID, calleeExecID)
		}
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		r.logger.Error("failed to create request", "err", err)
		return http.StatusInternalServerError, nil
	}

	req.Header = header

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

func (r *InvocationRouter) extractCaller(callID string, callerExecID string, header http.Header) (string, bool, bool) {
	if r.tracker != nil && callID != "" && callerExecID != "" {
		if caller, ok := r.tracker.GetExecutionContextFunction(callID, callerExecID); ok {
			return r.lookupCallerRoute(caller)
		}
	}

	if header != nil {
		for _, ip := range forwardedIPs(header.Get("X-Forwarded-For")) {
			if caller, ok := r.GetFunctionNameByIP(ip); ok {
				r.logger.Debug("detected internal call by forwarded ip", "caller", caller, "callID", callID)
				return r.lookupCallerRoute(caller)
			}
		}

		if caller, ok := r.GetFunctionNameByIP(header.Get("X-Source-Ip")); ok {
			r.logger.Debug("detected internal call by source ip", "caller", caller, "callID", callID)
			return r.lookupCallerRoute(caller)
		}
	}

	return "", false, false
}

func (r *InvocationRouter) lookupCallerRoute(name string) (string, bool, bool) {
	if strings.TrimSpace(name) == "" {
		return "", false, false
	}

	if route, ok := r.getRouteSnapshot(name); ok {
		return name, true, route.callgraphEnabled
	}

	return name, true, true
}

func forwardedIPs(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	ips := make([]string, 0, len(parts))
	for i := len(parts) - 1; i >= 0; i-- {
		ip := normalizeForwardedIP(parts[i])
		if ip != "" {
			ips = append(ips, ip)
		}
	}
	return ips
}

func normalizeForwardedIP(value string) string {
	ip := strings.TrimSpace(value)
	if ip == "" {
		return ""
	}

	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}

	if strings.HasPrefix(ip, "[") && strings.HasSuffix(ip, "]") {
		ip = strings.TrimPrefix(strings.TrimSuffix(ip, "]"), "[")
	}

	return strings.TrimSpace(ip)
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

// prewarmDownstream triggers prewarming for downstream functions based on call graph analysis.
// This function should be called asynchronously (fire-and-forget) to avoid adding latency to the request.
//
// The scheduler runs in three stages despite Docker's higher per-cold-start cost:
//
//  1. Filter out targets with insufficient expected savings or no useful data.
//  2. Sort by priority: sync edges first (they sit on user-visible latency),
//     then by expected savings descending.
//  3. Take the top PerCallLimit targets and dispatch them. Concurrent prewarms
//     are further capped by a semaphore inside executePrewarm.
func (r *InvocationRouter) prewarmDownstream(functionName string, callID string, executionID string) {
	if r.tracker == nil {
		return
	}

	tuning := r.PrewarmTuning()
	rawTargets := r.tracker.GetPrewarmTargets(functionName)
	selection := r.selectPrewarmTargets(rawTargets, tuning)
	if len(selection.targets) == 0 {
		if len(rawTargets) > 0 {
			r.logger.Debug("no prewarm targets selected",
				"caller", functionName,
				"call_id", callID,
				"execution_id", executionID,
				"raw", len(rawTargets),
				"rejected_no_cold_data", selection.noColdData,
				"rejected_invalid_lead", selection.invalidLead,
				"rejected_low_savings", selection.lowSavings)
		}
		return
	}

	r.logger.Info("prewarm targets selected",
		"caller", functionName,
		"call_id", callID,
		"execution_id", executionID,
		"raw", len(rawTargets),
		"eligible", len(selection.targets)+selection.limited,
		"selected", len(selection.targets),
		"rejected_no_cold_data", selection.noColdData,
		"rejected_invalid_lead", selection.invalidLead,
		"rejected_low_savings", selection.lowSavings,
		"rejected_per_call_limit", selection.limited)

	for _, target := range selection.targets {
		r.schedulePrewarmWithContext(functionName, target, callID, executionID)
	}
}

// rankedPrewarmTarget pairs a callgraph target with its computed scheduling
// metadata so we don't have to recompute it during sorting.
type rankedPrewarmTarget struct {
	target   callgraph.PrewarmTarget
	savings  time.Duration
	syncEdge bool
}

type prewarmSelection struct {
	targets     []callgraph.PrewarmTarget
	noColdData  int
	invalidLead int
	lowSavings  int
	limited     int
}

// selectPrewarmTargets applies the savings filter, sorts targets so that sync
// edges with the highest expected savings come first, and trims to the
// per-call budget. It also returns compact rejection counts for tracing.
func (r *InvocationRouter) selectPrewarmTargets(targets []callgraph.PrewarmTarget, tuning PrewarmTuning) prewarmSelection {
	selection := prewarmSelection{}
	if len(targets) == 0 {
		return selection
	}

	ranked := make([]rankedPrewarmTarget, 0, len(targets))
	for _, t := range targets {
		// Drop targets without cold-start data (we cannot estimate savings).
		if t.AvgColdStartDuration <= 0 {
			selection.noColdData++
			continue
		}
		if t.LeadTime <= 0 {
			selection.invalidLead++
			continue
		}

		// Expected savings = how much the caller could save vs a fresh cold
		// start, assuming prewarm fires when the caller starts executing and
		// runs in parallel with the caller's prior work.
		//
		//   savings = min(coldStart, leadTime - margin)
		//
		// If lead time is shorter than cold start, prewarm cannot complete in
		// time, so savings = leadTime. Subtract a safety margin so we don't
		// schedule prewarms that only buy a few milliseconds.
		usableLead := t.LeadTime - tuning.SafetyMargin
		if usableLead <= 0 {
			selection.invalidLead++
			continue
		}
		savings := t.AvgColdStartDuration
		if usableLead < savings {
			savings = usableLead
		}
		if savings < tuning.MinSavings {
			selection.lowSavings++
			continue
		}

		ranked = append(ranked, rankedPrewarmTarget{
			target:   t,
			savings:  savings,
			syncEdge: t.Kind == callgraph.EdgeKindSync,
		})
	}

	// Stable sort: sync first, then by expected savings descending, then by
	// function name for determinism.
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].syncEdge != ranked[j].syncEdge {
			return ranked[i].syncEdge
		}
		if ranked[i].savings != ranked[j].savings {
			return ranked[i].savings > ranked[j].savings
		}
		return ranked[i].target.FunctionName < ranked[j].target.FunctionName
	})

	limit := tuning.PerCallLimit
	if limit > 0 && len(ranked) > limit {
		selection.limited = len(ranked) - limit
		ranked = ranked[:limit]
	}

	selection.targets = make([]callgraph.PrewarmTarget, 0, len(ranked))
	for _, rt := range ranked {
		selection.targets = append(selection.targets, rt.target)
	}
	return selection
}

// schedulePrewarm schedules a prewarm operation for a target function.
// It calculates the delay based on lead time and cold start estimates,
// ensuring the function is ready just in time for when it's needed.
func (r *InvocationRouter) schedulePrewarm(caller string, target callgraph.PrewarmTarget) {
	r.schedulePrewarmWithContext(caller, target, "", "")
}

func (r *InvocationRouter) schedulePrewarmWithContext(caller string, target callgraph.PrewarmTarget, callID string, executionID string) {
	now := time.Now()
	attempt := prewarmAttempt{
		id:          uuid.NewString(),
		callID:      callID,
		executionID: executionID,
		caller:      caller,
		target:      target.FunctionName,
		kind:        target.Kind,
		createdAt:   now,
		leadTime:    target.LeadTime,
		coldStart:   target.AvgColdStartDuration,
	}

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
	coldStartTime := target.AvgColdStartDuration
	if coldStartTime == 0 && r.tracker != nil {
		stats, ok := r.tracker.GetFunctionStats(target.FunctionName)
		if ok {
			coldStartTime = stats.AvgColdStartDuration
		}
	}
	attempt.coldStart = coldStartTime

	// Calculate when to trigger prewarm:
	// delay = leadTime - coldStartTime - margin
	// We want the function to be ready *before* it's needed
	tuning := r.PrewarmTuning()
	delay := target.LeadTime - coldStartTime - tuning.SafetyMargin

	if delay <= 0 {
		if !r.reservePrewarm(target.FunctionName) {
			r.logger.Debug("skipping prewarming - target already reserved", "function", target.FunctionName)
			return
		}
		attempt.fireAt = now
		r.logger.Info("prewarming immediately", attempt.logAttrs(
			"delay", delay)...)
		go r.executePrewarmAttempt(attempt)
		return
	}

	if !r.reservePrewarm(target.FunctionName) {
		r.logger.Debug("skipping scheduled prewarm - target already reserved", "function", target.FunctionName)
		return
	}

	attempt.fireAt = now.Add(delay)
	r.logger.Info("scheduling prewarm", attempt.logAttrs(
		"delay", delay)...)

	// Schedule the prewarm after the calculated delay
	time.AfterFunc(delay, func() {
		// Re-check if function is still scaled down at trigger time
		r.routingTableMux.RLock()
		route, exists := r.routingTable[target.FunctionName]
		stillScaledDown := exists && !route.isActive
		r.routingTableMux.RUnlock()

		if !stillScaledDown {
			// The request beat the scheduled prewarm: the timer fired after the
			// function had already been cold-started by an actual invocation.
			// Indicates the prewarm was scheduled too late (delay too large).
			r.logger.Info("prewarm missed - function became active before scheduled fire", attempt.logAttrs(
				"delay", delay,
				"timer_lateness", nonNegativeDuration(time.Since(attempt.fireAt)))...)
			r.clearPrewarmReservation(target.FunctionName)
			return
		}

		r.executePrewarmAttempt(attempt)
	})
}

func nonNegativeDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
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
// Acquires the global prewarm semaphore non-blockingly so a backlog of
// prewarms cannot stall iterations or pile up Docker container creates.
// If the budget is exhausted, the prewarm is dropped and the reservation
// cleared so a future iteration can retry.
func (r *InvocationRouter) executePrewarm(funcName string) {
	now := time.Now()
	r.executePrewarmAttempt(prewarmAttempt{
		id:        uuid.NewString(),
		target:    funcName,
		kind:      callgraph.EdgeKindSync,
		createdAt: now,
		fireAt:    now,
	})
}

func (r *InvocationRouter) executePrewarmAttempt(attempt prewarmAttempt) {
	admission := r.acquirePrewarmSlot(attempt.kind)
	if admission.rejection != "" {
		message := "prewarm dropped - concurrency budget exhausted"
		if admission.rejection == prewarmRejectedSyncReserve {
			message = "prewarm dropped - sync capacity reserved"
		}
		r.logger.Info(message, attempt.logAttrs(
			"reason", admission.rejection,
			"semaphore_used", admission.used,
			"semaphore_capacity", admission.capacity,
			"timer_lateness", nonNegativeDuration(time.Since(attempt.fireAt)))...)
		r.clearPrewarmReservation(attempt.target)
		return
	}
	if admission.semaphore != nil {
		defer func() { <-admission.semaphore }()
	}

	scaleUpStarted := time.Now()
	if err := r.scaleUpFunction(attempt.target, false); err != nil {
		r.clearPrewarmReservation(attempt.target)
		r.logger.Warn("prewarm downstream function failed", attempt.logAttrs(
			"err", err,
			"scaleup_duration", time.Since(scaleUpStarted),
			"total_duration", time.Since(attempt.createdAt),
			"timer_lateness", nonNegativeDuration(scaleUpStarted.Sub(attempt.fireAt)))...)
	} else {
		r.logger.Info("prewarm downstream function completed", attempt.logAttrs(
			"scaleup_duration", time.Since(scaleUpStarted),
			"total_duration", time.Since(attempt.createdAt),
			"timer_lateness", nonNegativeDuration(scaleUpStarted.Sub(attempt.fireAt)),
			"semaphore_capacity", admission.capacity)...)
	}
}

// acquirePrewarmSlot atomically applies global admission policy. Unknown edges
// are treated like async edges: they may use spare capacity but cannot consume
// the final slot reserved for known synchronous work.
func (r *InvocationRouter) acquirePrewarmSlot(kind callgraph.EdgeKind) prewarmAdmission {
	r.prewarmMux.Lock()
	defer r.prewarmMux.Unlock()

	sem := r.prewarmSemaphore
	if sem == nil {
		return prewarmAdmission{}
	}

	admission := prewarmAdmission{
		semaphore: sem,
		used:      len(sem),
		capacity:  cap(sem),
	}
	if kind != callgraph.EdgeKindSync && admission.used >= admission.capacity-1 {
		admission.rejection = prewarmRejectedSyncReserve
		return admission
	}

	select {
	case sem <- struct{}{}:
		return admission
	default:
		admission.rejection = prewarmRejectedConcurrency
		return admission
	}
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
