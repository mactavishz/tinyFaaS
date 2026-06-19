package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	tflogs "github.com/OpenFogStack/tinyFaaS/pkg/logs"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
)

var (
	// TmpDir can be overridden via TMP_DIR environment variable
	TmpDir = util.GetEnvOrDefault("TMP_DIR", "/var/lib/tinyfaas/tmp")
)

const (
	// After a function is scaled up, the route/IP state may need a brief window
	// to become visible locally before invocation can proceed.
	scaleUpRouteReadyTimeout  = 500 * time.Millisecond
	scaleUpRouteReadyInterval = 25 * time.Millisecond

	// defaultMaxConcurrentPrewarms bounds how many speculative prewarm
	// scale-ups may run at once. Prewarming is best-effort: when the budget is
	// exhausted additional targets are skipped rather than queued, so
	// speculative work never piles up against request-path cold starts.
	// Overridable via TINYFAAS_MAX_CONCURRENT_PREWARMS.
	defaultMaxConcurrentPrewarms = 3
)

// maxConcurrentPrewarms reads the prewarm concurrency budget from the
// environment, falling back to defaultMaxConcurrentPrewarms.
func maxConcurrentPrewarms() int {
	if v := util.GetEnvOrDefault("TINYFAAS_MAX_CONCURRENT_PREWARMS", ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultMaxConcurrentPrewarms
}

// Backend is the runtime backend used to create and manage function containers.
type Backend interface {
	Create(name string, env string, threads int, filedir string, envs map[string]string, labels map[string]string, limits ResourceLimits) (Handler, error)
	Stop() error
}

// Handler represents the running containers for a single function.
type Handler interface {
	IPs() []string
	Start() error
	Stop() error
	Restart() error
	Destroy() error
	Logs() (io.Reader, error)
	IsRunning() bool
	GetLabels() map[string]string
}

// Route holds the routing state for a function.
type Route struct {
	ips              []string
	isActive         bool
	callgraphEnabled bool
}

func (r *Route) PickIP() (string, error) {
	if len(r.ips) == 0 {
		return "", fmt.Errorf("no IP available")
	}
	return r.ips[rand.Intn(len(r.ips))], nil
}

// Server is the merged tinyFaaS server. It combines the responsibilities that
// were previously split between the reverse proxy (request routing and call
// graph tracking) and the manager (function lifecycle and autoscaling). Because
// everything now lives in a single process, the components communicate via
// direct method calls instead of HTTP round trips.
type Server struct {
	id      string
	mode    string
	backend Backend
	logger  *slog.Logger

	// Management state (function lifecycle)
	mux              sync.Mutex
	functionHandlers map[string]Handler
	functionConfigs  map[string]FunctionConfig
	scaleUpClaims    map[string]struct{}

	// Routing state (request dispatch)
	routingTableMux     sync.RWMutex
	routingTable        map[string]*Route
	reverseRoutingTable map[string]string

	// Integrations
	autoscaler *autoscaler.AutoScaler
	tracker    callgraph.FullTracker

	// prewarmSem bounds concurrent speculative prewarm scale-ups so they
	// cannot starve request-path cold starts (best-effort, non-blocking).
	prewarmSem chan struct{}
}

// New creates a new merged server.
func New(id string, mode string, tfBackend Backend, logger *slog.Logger) *Server {
	return &Server{
		id:                  id,
		mode:                mode,
		backend:             tfBackend,
		logger:              logger,
		functionHandlers:    make(map[string]Handler),
		functionConfigs:     make(map[string]FunctionConfig),
		scaleUpClaims:       make(map[string]struct{}),
		routingTable:        make(map[string]*Route),
		reverseRoutingTable: make(map[string]string),
		prewarmSem:          make(chan struct{}, maxConcurrentPrewarms()),
	}
}

func (s *Server) IsDev() bool {
	return strings.ToLower(s.mode) == "development"
}

func (s *Server) IsProd() bool {
	return strings.ToLower(s.mode) == "production"
}

// SetTracker sets the callgraph tracker.
func (s *Server) SetTracker(tracker callgraph.FullTracker) {
	s.tracker = tracker
}

// GetTracker returns the callgraph tracker.
func (s *Server) GetTracker() callgraph.Tracker {
	return s.tracker
}

// SetAutoScaler sets the autoscaler instance.
func (s *Server) SetAutoScaler(as *autoscaler.AutoScaler) {
	s.autoscaler = as
}

// GetAutoScaler returns the autoscaler instance.
func (s *Server) GetAutoScaler() *autoscaler.AutoScaler {
	return s.autoscaler
}

// autoscalerEnabled reports whether the autoscaler is configured and enabled.
func (s *Server) autoscalerEnabled() bool {
	return s.autoscaler != nil && s.autoscaler.IsEnabled()
}

// CallgraphEnabled returns whether callgraph tracking is enabled for a function.
func (s *Server) CallgraphEnabled(functionName string) bool {
	if s.tracker == nil || !s.tracker.Enabled() {
		return false
	}
	if strings.TrimSpace(functionName) == "" {
		return false
	}

	s.routingTableMux.RLock()
	route, ok := s.routingTable[functionName]
	s.routingTableMux.RUnlock()
	if !ok {
		// If we don't have routing state (e.g., early startup), fall back to global enablement.
		return true
	}
	return route.callgraphEnabled
}

// ---------------------------------------------------------------------------
// Routing table management
// ---------------------------------------------------------------------------

// Add registers (or replaces) the routing entry for a function.
func (s *Server) Add(name string, ips []string, labels map[string]string) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}

	s.logger.Debug("adding function route", "name", name, "ips", ips)
	s.routingTableMux.Lock()
	defer s.routingTableMux.Unlock()

	callgraphEnabled := callgraph.ParseCallgraphConfig(labels, "tinyfaas", s.tracker)

	s.routingTable[name] = &Route{
		ips:              ips,
		isActive:         true,
		callgraphEnabled: callgraphEnabled,
	}
	for _, ip := range ips {
		s.reverseRoutingTable[ip] = name
	}
	return nil
}

// Del removes the routing entry for a function and clears its callgraph data.
func (s *Server) Del(name string) error {
	s.routingTableMux.Lock()
	defer s.routingTableMux.Unlock()

	route, ok := s.routingTable[name]
	if !ok {
		return fmt.Errorf("function not found")
	}

	s.logger.Debug("deleting function route", "name", name)

	for _, ip := range route.ips {
		delete(s.reverseRoutingTable, ip)
	}
	delete(s.routingTable, name)

	if s.tracker != nil {
		s.tracker.ClearFunctionData(name)
	}

	return nil
}

// Update deactivates a function's route (used when scaling down).
func (s *Server) Update(name string) error {
	s.routingTableMux.Lock()
	defer s.routingTableMux.Unlock()

	route, ok := s.routingTable[name]
	if !ok {
		return nil
	}

	s.logger.Debug("deactivating function route", "name", name)
	for _, ip := range route.ips {
		delete(s.reverseRoutingTable, ip)
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

func (s *Server) getRouteSnapshot(name string) (*Route, bool) {
	s.routingTableMux.RLock()
	defer s.routingTableMux.RUnlock()

	route, ok := s.routingTable[name]
	if !ok {
		return nil, false
	}

	return copyRoute(route), true
}

func (s *Server) waitForRouteReady(name string, timeout time.Duration) (*Route, bool) {
	deadline := time.Now().Add(timeout)
	for {
		route, exists := s.getRouteSnapshot(name)
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

// GetFunctionNameByIP resolves a function name from one of its container IPs.
func (s *Server) GetFunctionNameByIP(ip string) (string, bool) {
	s.routingTableMux.RLock()
	defer s.routingTableMux.RUnlock()
	name, ok := s.reverseRoutingTable[ip]
	return name, ok
}

// ---------------------------------------------------------------------------
// Request dispatch
// ---------------------------------------------------------------------------

// Call dispatches a request to a function. When async is true, the invocation
// runs in a background goroutine and the caller receives StatusAccepted
// immediately (the async dispatch behaviour is unchanged from the standalone
// reverse proxy).
func (s *Server) Call(name string, payload []byte, async bool, header http.Header) (int, []byte) {
	startTime := time.Now()

	kind := callgraph.EdgeKindSync
	if async {
		kind = callgraph.EdgeKindAsync
	}

	if async {
		if _, ok := s.getRouteSnapshot(name); !ok {
			s.logger.Error("function not found", "name", name)
			return http.StatusNotFound, nil
		}

		asyncPayload := append([]byte(nil), payload...)
		asyncHeader := header.Clone()

		go func() {
			status, _ := s.invoke(name, asyncPayload, asyncHeader, startTime, kind)
			if status >= http.StatusBadRequest {
				s.logger.Warn("async invocation failed", "name", name, "status", status)
			}
		}()

		return http.StatusAccepted, nil
	}

	return s.invoke(name, payload, header, startTime, kind)
}

func (s *Server) invoke(name string, payload []byte, header http.Header, startTime time.Time, kind callgraph.EdgeKind) (int, []byte) {
	if header == nil {
		header = http.Header{}
	}

	callID := header.Get("X-Call-Id")
	if callID == "" {
		s.logger.Warn("missing X-Call-Id header", "function", name)
		callID = fmt.Sprintf("unknown-%s", uuid.New().String())
	}

	callerExecID := header.Get("X-Exec-Id")
	calleeExecID := uuid.New().String()
	header.Set("X-Exec-Id", calleeExecID)

	// Extract source IP to detect if this is an internal call
	sourceIP := header.Get("X-Source-Ip")
	caller := ""
	if sourceIP != "" {
		if callerFunc, ok := s.GetFunctionNameByIP(sourceIP); ok {
			caller = callerFunc
			s.logger.Debug("detected internal call",
				"caller", caller,
				"callee", name,
				"callID", callID)
		}
	}

	calleeRoute, ok := s.getRouteSnapshot(name)
	callerRoute, callerRouteOK := s.getRouteSnapshot(caller)

	if !ok {
		s.logger.Error("function not found", "name", name)
		return http.StatusNotFound, nil
	}

	s.logger.Debug("found function route", "ips", calleeRoute.ips, "active", calleeRoute.isActive)

	// prewarmTriggered records whether downstream prewarming has already been
	// kicked off for this invocation, so we do not trigger it twice.
	prewarmTriggered := false

	// Check if function is scaled down and trigger cold start if needed
	if s.autoscalerEnabled() && !calleeRoute.isActive {
		s.logger.Info("function is scaled down, triggering cold start", "name", name)

		// The caller's own cold start is otherwise-idle time during which we can
		// warm its predicted downstream functions. Kick this off *before*
		// blocking on the caller's scale-up so the downstream containers start in
		// parallel and are ready (or nearly so) by the time the caller begins
		// executing and dispatches to them. This is what makes prewarming pay off
		// on the synchronous critical path, where the caller->callee lead time is
		// otherwise too small to schedule a prewarm against.
		if calleeRoute.callgraphEnabled && s.tracker != nil && s.tracker.PrewarmEnabled() {
			go s.prewarmDownstreamEager(name)
			prewarmTriggered = true
		}

		// cold=true since this is triggered by a user request
		if err := s.ScaleUp(name, true); err != nil {
			s.logger.Error("failed to trigger cold start", "name", name, "err", err)
			return http.StatusServiceUnavailable, nil
		}

		updatedRoute, exists := s.waitForRouteReady(name, scaleUpRouteReadyTimeout)
		if !exists {
			s.logger.Error("function route disappeared after scale-up", "name", name)
			return http.StatusServiceUnavailable, nil
		}
		calleeRoute = updatedRoute
	}

	if !calleeRoute.isActive || len(calleeRoute.ips) == 0 {
		s.logger.Warn("function route not ready for invocation",
			"name", name,
			"active", calleeRoute.isActive,
			"ipCount", len(calleeRoute.ips))
		return http.StatusServiceUnavailable, nil
	}

	// Record the edge only after the route is confirmed ready for invocation.
	// This avoids polluting callgraph edges with failed cold-start attempts.
	if calleeRoute.callgraphEnabled && s.tracker != nil {
		effectiveCaller := ""
		// If caller's callgraph is disabled but callee is enabled, record as external (caller="")
		if caller != "" && callerRouteOK && callerRoute.callgraphEnabled {
			effectiveCaller = caller
		}
		s.tracker.RecordEdgeWithKind(effectiveCaller, name, callID, callerExecID, startTime, kind)
	}

	// Record activity directly so the autoscaler keeps the function warm.
	if s.autoscalerEnabled() {
		s.autoscaler.RecordActivity(name)
	}

	finishRequest := func() {
		if s.autoscalerEnabled() {
			s.autoscaler.EndInvocation(name)
		}
	}
	if s.autoscalerEnabled() {
		if err := s.autoscaler.StartInvocation(name); err != nil {
			s.logger.Error("failed to mark request start", "name", name, "err", err)
			return http.StatusServiceUnavailable, nil
		}
	}

	// choose random handler
	ip, err := calleeRoute.PickIP()

	if err != nil {
		s.logger.Error("failed to pick function ip", "err", err)
		finishRequest()
		return http.StatusInternalServerError, nil
	}

	s.logger.Debug("chosen function ip", "ip", ip)

	// Trigger prewarming for downstream functions (fire-and-forget, non-blocking).
	// Prewarming requires both callgraph and autoscaler to be enabled. When the
	// callee was just cold-started we already kicked off eager prewarming above,
	// so skip the scheduled path here to avoid triggering it twice.
	if !prewarmTriggered && calleeRoute.callgraphEnabled && s.autoscalerEnabled() && s.tracker != nil && s.tracker.PrewarmEnabled() {
		go s.prewarmDownstream(name)
	}

	// Mark that this function is starting execution
	startedExecution := false
	functionStartTime := time.Now()
	if calleeRoute.callgraphEnabled && s.tracker != nil {
		s.tracker.StartExecution(name, callID, calleeExecID, functionStartTime)
		startedExecution = true
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		s.logger.Error("failed to create request", "err", err)
		if startedExecution {
			s.tracker.EndExecution(name, callID, calleeExecID, time.Now())
		}
		finishRequest()
		return http.StatusInternalServerError, nil
	}

	req.Header = header

	// call function and return results
	defer finishRequest()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.logger.Error("failed to invoke function", "err", err)
		return http.StatusInternalServerError, nil
	}

	defer resp.Body.Close()
	res_body, err := io.ReadAll(resp.Body)

	if err != nil {
		s.logger.Error("failed to read response body", "err", err)
		return http.StatusInternalServerError, nil
	}

	// End execution - records function stats and cleans up context
	if startedExecution {
		s.tracker.EndExecution(name, callID, calleeExecID, time.Now())
	}

	return resp.StatusCode, res_body
}

// ---------------------------------------------------------------------------
// Prewarming
// ---------------------------------------------------------------------------

// prewarmDownstream triggers prewarming for downstream functions based on call
// graph analysis. It should be called asynchronously (fire-and-forget).
func (s *Server) prewarmDownstream(functionName string) {
	targets := s.tracker.GetPrewarmTargets(functionName)
	if len(targets) == 0 {
		return
	}

	s.logger.Info("prewarming downstream functions",
		"caller", functionName,
		"targetCount", len(targets))

	for _, target := range targets {
		s.schedulePrewarm(functionName, target)
	}
}

// prewarmDownstreamEager warms the predicted downstream functions of a caller
// that is itself currently cold-starting. Unlike schedulePrewarm, it fires
// immediately instead of computing a delay against the recorded lead time: the
// caller's in-progress cold start *is* the lead time, and it is typically far
// larger than the small caller->callee gap seen once the caller is warm. This
// is what lets prewarming help the synchronous critical path on a lean runtime,
// where that gap (e.g. ~60ms) is otherwise too short to schedule against.
//
// Only synchronous (and as-yet-unclassified) callees are warmed: they sit on
// the caller's user-visible critical path, whereas async callees are not waited
// on and would only add container-start contention to the caller's own
// in-flight cold start. The most imminent callee is warmed first, and warms are
// bounded by prewarmSem and skipped (not queued) when the budget is exhausted,
// to limit how much they contend with the request-path cold start. Should be
// called asynchronously (fire-and-forget).
func (s *Server) prewarmDownstreamEager(functionName string) {
	targets := s.tracker.GetPrewarmTargets(functionName)
	if len(targets) == 0 {
		return
	}

	// Prioritize synchronous callees (on the critical path), then the smallest
	// lead time (most imminent) first.
	sort.SliceStable(targets, func(i, j int) bool {
		si := targets[i].Kind == callgraph.EdgeKindSync
		sj := targets[j].Kind == callgraph.EdgeKindSync
		if si != sj {
			return si
		}
		return targets[i].LeadTime < targets[j].LeadTime
	})

	for _, target := range targets {
		// Skip asynchronous callees. The caller does not block on them, so they
		// are not on its user-visible critical path; warming them here would only
		// add container-start contention to the caller's own in-flight cold start
		// for no measurable benefit. They are cold-started on their own dispatch
		// path (and may still be scheduled via the regular prewarm path when the
		// caller is warm). EdgeKindUnknown is treated as potentially-critical and
		// kept.
		if target.Kind == callgraph.EdgeKindAsync {
			continue
		}

		if !s.CallgraphEnabled(target.FunctionName) {
			continue
		}

		s.routingTableMux.RLock()
		route, exists := s.routingTable[target.FunctionName]
		isScaledDown := exists && !route.isActive
		s.routingTableMux.RUnlock()
		if !isScaledDown {
			continue
		}

		// Best-effort: if the prewarm budget is exhausted, skip rather than
		// queue, so speculative work never competes with request-path cold
		// starts.
		select {
		case s.prewarmSem <- struct{}{}:
		default:
			s.logger.Info("skipping eager prewarm - budget exhausted",
				"caller", functionName,
				"target", target.FunctionName)
			continue
		}

		go func(target string) {
			defer func() { <-s.prewarmSem }()
			s.logger.Info("eager prewarm during cold start",
				"caller", functionName,
				"target", target)
			s.executePrewarm(target)
		}(target.FunctionName)
	}
}

// schedulePrewarm schedules a prewarm operation for a target function so that it
// is ready just in time for when it is expected to be needed.
func (s *Server) schedulePrewarm(caller string, target callgraph.PrewarmTarget) {
	if !s.CallgraphEnabled(target.FunctionName) {
		return
	}

	s.routingTableMux.RLock()
	route, exists := s.routingTable[target.FunctionName]
	isScaledDown := exists && !route.isActive
	s.routingTableMux.RUnlock()

	if !isScaledDown {
		s.logger.Info("skipping prewarming - function already active",
			"function", target.FunctionName)
		return
	}

	var coldStartTime time.Duration
	if stats, ok := s.tracker.GetFunctionStats(target.FunctionName); ok {
		coldStartTime = stats.AvgColdStartDuration
	}

	// delay = leadTime - coldStartTime - margin
	const safetyMargin = 50 * time.Millisecond
	delay := target.LeadTime - coldStartTime - safetyMargin

	if delay <= 0 {
		// The cold start cannot finish within the observed lead time, so firing
		// now would not make the function ready in time -- it would only add
		// container-start contention on the request path while it races (and
		// loses to) the on-demand cold start. Skip it. The eager cold-start path
		// (prewarmDownstreamEager) is what covers tight critical-path lead times,
		// by warming downstream while the caller itself is still cold-starting.
		s.logger.Info("skipping prewarm - predicted too late",
			"caller", caller,
			"target", target.FunctionName,
			"leadTime", target.LeadTime,
			"coldStartTime", coldStartTime,
			"delay", delay)
		return
	}

	s.logger.Info("scheduling prewarm",
		"caller", caller,
		"target", target.FunctionName,
		"leadTime", target.LeadTime,
		"coldStartTime", coldStartTime,
		"delay", delay)

	time.AfterFunc(delay, func() {
		s.routingTableMux.RLock()
		route, exists := s.routingTable[target.FunctionName]
		stillScaledDown := exists && !route.isActive
		s.routingTableMux.RUnlock()

		if !stillScaledDown {
			s.logger.Info("skipping scheduled prewarm - function became active",
				"function", target.FunctionName)
			return
		}

		s.executePrewarm(target.FunctionName)
	})
}

// executePrewarm performs the actual prewarm operation for a function.
func (s *Server) executePrewarm(funcName string) {
	if err := s.ScaleUp(funcName, false); err != nil {
		s.logger.Info("prewarm downstream function failed",
			"function", funcName,
			"err", err)
	} else {
		s.logger.Info("prewarm downstream function completed",
			"function", funcName)
	}
}

// ---------------------------------------------------------------------------
// Function lifecycle
// ---------------------------------------------------------------------------

func createTempArchiveFile(prefix string) (*os.File, error) {
	if err := os.MkdirAll(TmpDir, 0777); err != nil {
		return nil, err
	}

	return os.CreateTemp(TmpDir, prefix+"-*.zip")
}

func (s *Server) createFunction(name string, env string, replicas int, archivePath string, subfolderPath string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {

	if !util.IsValidFunctionName(name) {
		return fmt.Errorf("function name %s is not valid (must be 1-63 lowercase alphanumeric characters or hyphens, cannot start or end with hyphen)", name)
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	s.logger.Info("creating function", "name", name, "uuid", uuid.String())

	tempDir := path.Join(TmpDir, uuid.String())

	err = os.MkdirAll(tempDir, 0777)
	if err != nil {
		return err
	}

	s.logger.Info("created folder", "path", tempDir, "archivePath", archivePath)

	err = util.Unzip(archivePath, tempDir, s.logger)
	if err != nil {
		return err
	}

	defer func() {
		err = os.RemoveAll(tempDir)
		if err != nil {
			s.logger.Error("error removing folder", "path", tempDir, "err", err)
		}

		s.logger.Info("cleanup completed", "path", tempDir, "archivePath", archivePath)
	}()

	if subfolderPath != "" {
		tempDir = path.Join(tempDir, subfolderPath)
	}

	resolvedLimits, effectiveLimits, backendLimits, err := EffectiveResourceLimits(resources.Limits)
	if err != nil {
		return fmt.Errorf("invalid limits: %w", err)
	}

	// If the function already exists, keep it serving while the replacement is prepared.
	var oldHandler Handler
	s.mux.Lock()
	if existingHandler, ok := s.functionHandlers[name]; ok {
		oldHandler = existingHandler
	}
	s.mux.Unlock()

	// Prepare the replacement handler before touching the current runtime.
	fh, err := s.backend.Create(name, env, replicas, tempDir, envs, labels, backendLimits)
	if err != nil {
		return err
	}

	if oldHandler != nil && s.autoscalerEnabled() {
		if err := s.autoscaler.ScaleDownWhenIdle(name); err != nil {
			_ = fh.Destroy()
			return fmt.Errorf("cannot safely scale down function %s for redeploy: %w", name, err)
		}
	}

	// Measure cold start time for the replacement runtime start.
	coldStartTime := time.Now()
	err = fh.Start()
	coldStartDuration := time.Since(coldStartTime)

	if err != nil {
		s.logger.Error("failed to start function containers", "function", name, "err", err)
		cleanupErr := fh.Destroy()
		if cleanupErr != nil {
			s.logger.Error("failed to cleanup replacement handler after start failure", "function", name, "err", cleanupErr)
			return errors.Join(err, fmt.Errorf("cleanup failed: %w", cleanupErr))
		}
		return err
	}

	s.logger.Info("cold start completed",
		"function", name,
		"duration", coldStartDuration)

	// Register the function route directly (no HTTP round trip to the reverse proxy).
	s.logger.Info("registering function route", "function", name, "ips", fh.IPs())
	if err := s.Add(name, fh.IPs(), labels); err != nil {
		_ = fh.Destroy()
		return fmt.Errorf("failed to register function route: %w", err)
	}

	// If this is a redeployment, reset callgraph stats before recording the fresh cold start.
	if oldHandler != nil {
		s.resetCallgraphStats(name)
	}

	// Record the cold start for callgraph tracking.
	s.recordScaleUp(name, coldStartTime, coldStartDuration, true)

	config := FunctionConfig{
		Name:            name,
		Env:             env,
		Replicas:        replicas,
		Envs:            envs,
		Labels:          labels,
		Limits:          resolvedLimits,
		EffectiveLimits: effectiveLimits,
		Running:         fh.IsRunning(),
	}

	s.mux.Lock()
	s.functionHandlers[name] = fh
	s.functionConfigs[name] = config
	s.mux.Unlock()

	// Register with autoscaler
	if s.autoscaler != nil {
		s.autoscaler.RegisterFunction(name, labels)
	}

	// destroy the old handler if it exists
	if oldHandler != nil {
		err = oldHandler.Destroy()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) UploadArchive(name string, env string, threads int, archivePath string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {
	err := s.createFunction(name, env, threads, archivePath, "", envs, labels, resources)
	if err != nil {
		s.logger.Error("error creating function", "function", name, "err", err)
		return err
	}

	return nil
}

func (s *Server) UrlUpload(name string, env string, replicas int, funcurl string, subfolder string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {
	resp, err := http.Get(funcurl)
	if err != nil {
		s.logger.Error("error downloading function zip", "url", funcurl, "err", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.logger.Error("error downloading function zip", "url", funcurl, "statusCode", resp.StatusCode)
		return fmt.Errorf("unexpected status code downloading function zip: %d", resp.StatusCode)
	}

	archiveFile, err := createTempArchiveFile("url-upload")
	if err != nil {
		s.logger.Error("error creating temporary archive file", "url", funcurl, "err", err)
		return err
	}
	defer func() {
		if err := os.Remove(archiveFile.Name()); err != nil && !os.IsNotExist(err) {
			s.logger.Error("error removing temporary archive file", "path", archiveFile.Name(), "err", err)
		}
	}()

	bytesWritten, err := io.Copy(archiveFile, resp.Body)
	closeErr := archiveFile.Close()
	if err != nil {
		s.logger.Error("error reading function zip", "url", funcurl, "err", err)
		return err
	}
	if closeErr != nil {
		s.logger.Error("error closing temporary archive file", "path", archiveFile.Name(), "err", closeErr)
		return closeErr
	}

	s.logger.Info("downloaded function archive", "url", funcurl, "bytes", bytesWritten, "path", archiveFile.Name())

	err = s.createFunction(name, env, replicas, archiveFile.Name(), subfolder, envs, labels, resources)
	if err != nil {
		s.logger.Error("error creating function", "function", name, "err", err)
		return err
	}

	return nil
}

func (s *Server) Logs() (io.Reader, error) {
	var logs bytes.Buffer
	s.logger.Info("collecting logs from all functions")
	s.mux.Lock()
	names := make([]string, 0, len(s.functionHandlers))
	for name := range s.functionHandlers {
		names = append(names, name)
	}
	s.mux.Unlock()

	for _, name := range names {
		l, err := s.LogsFunction(name)
		if err != nil {
			s.logger.Error("error getting logs for function", "function", name, "err", err)
			return nil, err
		}

		_, err = io.Copy(&logs, l)
		if err != nil {
			return nil, err
		}

		logs.WriteString("\n")
	}

	return &logs, nil
}

func (s *Server) LogsFunction(name string) (io.Reader, error) {
	s.mux.Lock()
	_, ok := s.functionHandlers[name]
	s.mux.Unlock()
	if !ok {
		return nil, fmt.Errorf("function %s not found", name)
	}

	return tflogs.ReadFunction(name)
}

func (s *Server) Get(name string) (FunctionConfig, bool) {
	s.mux.Lock()
	defer s.mux.Unlock()

	h, ok := s.functionHandlers[name]
	if !ok {
		return FunctionConfig{}, false
	}
	cfg, ok := s.functionConfigs[name]
	if !ok {
		cfg = FunctionConfig{Name: name}
	}
	cfg.Running = h.IsRunning()
	return cfg, true
}

func (s *Server) List() []FunctionConfig {
	s.logger.Info("listing functions")

	s.mux.Lock()
	list := make([]FunctionConfig, 0, len(s.functionHandlers))
	for name, h := range s.functionHandlers {
		cfg, ok := s.functionConfigs[name]
		if !ok {
			cfg = FunctionConfig{Name: name}
		}
		cfg.Running = h.IsRunning()
		list = append(list, cfg)
	}
	s.mux.Unlock()

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

func (s *Server) Wipe() error {
	s.logger.Info("wiping all functions")

	s.mux.Lock()
	names := make([]string, 0, len(s.functionHandlers))
	for name := range s.functionHandlers {
		names = append(names, name)
	}
	s.mux.Unlock()

	for _, name := range names {
		s.logger.Info("destroying function", "function", name)
		_ = s.Delete(name)
	}

	return nil
}

func (s *Server) Delete(name string) error {
	s.mux.Lock()
	fh, ok := s.functionHandlers[name]
	if !ok {
		s.mux.Unlock()
		return fmt.Errorf("function %s not found", name)
	}

	s.logger.Info("deleting function", "function", name)
	defer s.mux.Unlock()

	err := fh.Destroy()
	if err != nil {
		return err
	}

	// Remove the function route directly (no HTTP round trip to the reverse proxy).
	if err := s.Del(name); err != nil {
		s.logger.Warn("failed to remove function route", "function", name, "err", err)
	}

	delete(s.functionHandlers, name)
	delete(s.functionConfigs, name)
	s.logger.Info("function deleted", "function", name)

	// Unregister from autoscaler
	if s.autoscaler != nil {
		s.autoscaler.UnregisterFunction(name)
	}

	return nil
}

func (s *Server) Stop() error {
	err := s.Wipe()
	if err != nil {
		return err
	}

	return s.backend.Stop()
}

// ---------------------------------------------------------------------------
// Callgraph recording helpers (direct, in-process)
// ---------------------------------------------------------------------------

// recordScaleUp records a scale-up or cold start event in the callgraph tracker.
func (s *Server) recordScaleUp(name string, timestamp time.Time, duration time.Duration, cold bool) {
	if s.tracker == nil || !s.CallgraphEnabled(name) {
		return
	}
	s.tracker.RecordScaleUp(name, timestamp, duration, cold)
	s.logger.Debug("recorded scale-up",
		"function", name,
		"duration", duration,
		"cold", cold)
}

// recordScaleDown records a scale-down event in the callgraph tracker.
func (s *Server) recordScaleDown(name string, timestamp time.Time, duration time.Duration) {
	if s.tracker == nil || !s.CallgraphEnabled(name) {
		return
	}
	s.tracker.RecordScaleDown(name, timestamp, duration)
	s.logger.Debug("recorded scale-down",
		"function", name,
		"duration", duration)
}

// resetCallgraphStats resets callgraph stats for a redeployed function.
func (s *Server) resetCallgraphStats(name string) {
	if s.tracker == nil || !s.CallgraphEnabled(name) {
		return
	}
	s.tracker.ResetFunctionStats(name)
	s.logger.Info("reset callgraph stats for redeployment", "function", name)
}
