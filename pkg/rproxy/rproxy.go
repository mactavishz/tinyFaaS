package rproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	retry "github.com/avast/retry-go/v5"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"go.uber.org/zap"
)

type Route struct {
	ips      []string
	isActive bool
}

type RProxy struct {
	routingTable        map[string]*Route
	reverseRoutingTable map[string]string
	mode                string
	routingTableMux     sync.RWMutex
	autoscalerEnabled   bool
	tracker             callgraph.FullTracker
	logger              *zap.Logger
}

func (r *Route) PickIP() (string, error) {
	if len(r.ips) == 0 {
		return "", fmt.Errorf("no IP available")
	}
	return r.ips[rand.Intn(len(r.ips))], nil
}

func New(logger *zap.Logger, mode string) *RProxy {
	return &RProxy{
		routingTable:        make(map[string]*Route),
		reverseRoutingTable: make(map[string]string),
		mode:                mode,
		logger:              logger,
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

func (r *RProxy) Add(name string, ips []string) error {
	if len(ips) == 0 {
		return fmt.Errorf("no ips given")
	}

	r.logger.Debug("adding function route", zap.String("name", name), zap.Strings("ips", ips))
	r.routingTableMux.Lock()
	defer r.routingTableMux.Unlock()

	r.routingTable[name] = &Route{
		ips:      ips,
		isActive: true,
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

	r.logger.Debug("deleting function route", zap.String("name", name))

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

	if _, ok := r.routingTable[name]; ok {
		r.logger.Debug("updating function route", zap.String("name", name))
		r.routingTable[name].isActive = !r.routingTable[name].isActive
	}
	return nil
}

func (r *RProxy) Call(name string, payload []byte, async bool, header http.Header) (int, []byte) {
	startTime := time.Now()

	requestID := header.Get("X-Faas-Request-Id")
	if requestID == "" {
		r.logger.Warn("missing X-Faas-Request-Id header", zap.String("function", name))
		// Generate fallback requestID
		requestID = fmt.Sprintf("unknown-%s", uuid.New().String())
	}

	// Extract source IP to detect if this is an internal call
	sourceIP := header.Get("X-Faas-Source-Ip")
	caller := ""
	if sourceIP != "" {
		// Check if this is an internal call (from another function)
		if callerFunc, ok := r.GetFunctionNameByIP(sourceIP); ok {
			caller = callerFunc
			r.logger.Debug("detected internal call",
				zap.String("caller", caller),
				zap.String("callee", name),
				zap.String("requestID", requestID))
		}
	}

	// Record the edge (this will calculate edge time if caller exists)
	r.tracker.RecordEdge(caller, name, requestID, startTime)

	r.routingTableMux.RLock()
	route, ok := r.routingTable[name]
	r.routingTableMux.RUnlock()

	if !ok {
		r.logger.Error("function not found", zap.String("name", name))
		return http.StatusNotFound, nil
	}

	r.logger.Debug("found function route", zap.Strings("ips", route.ips), zap.Bool("active", route.isActive))

	// Check if function is scaled down and trigger cold start if needed
	if r.autoscalerEnabled && !route.isActive {
		r.logger.Info("function is scaled down, triggering cold start", zap.String("name", name))
		// cold=true since this is triggered by a user request
		// Manager will measure and record the cold start time
		err := r.scaleUpFunction(name, true)
		if err != nil {
			r.logger.Error("failed to trigger cold start", zap.String("name", name), zap.Error(err))
			return http.StatusInternalServerError, nil
		}
	}

	go r.heartbeat(name)

	// choose random handler
	ip, err := route.PickIP()

	if err != nil {
		r.logger.Error("failed to pick function ip", zap.Error(err))
		return http.StatusInternalServerError, nil
	}

	r.logger.Debug("chosen function ip", zap.String("ip", ip))

	// Trigger prewarming for downstream functions (fire-and-forget, non-blocking)
	// Prewarming requires both callgraph and autoscaler to be enabled
	if r.tracker.Enabled() && r.autoscalerEnabled {
		go r.prewarmDownstream(name)
	}

	// Mark that this function is starting execution
	functionStartTime := time.Now()
	r.tracker.StartExecution(name, requestID, functionStartTime)

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:8000/fn", ip), bytes.NewBuffer(payload))
	if err != nil {
		r.logger.Error("failed to create request", zap.Error(err))
		return http.StatusInternalServerError, nil
	}

	req.Header = header

	// call function asynchronously
	if async {
		go func() {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			resp.Body.Close()
			// Clean up execution context after async call completes
			r.tracker.EndExecution(name, requestID, time.Now())
		}()
		return http.StatusAccepted, nil
	}

	// call function and return results
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.logger.Error("failed to invoke function", zap.Error(err))
		return http.StatusInternalServerError, nil
	}

	defer resp.Body.Close()
	res_body, err := io.ReadAll(resp.Body)

	if err != nil {
		r.logger.Error("failed to read response body", zap.Error(err))
		return http.StatusInternalServerError, nil
	}

	// End execution - records function stats and cleans up context
	r.tracker.EndExecution(name, requestID, time.Now())

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
		zap.String("caller", functionName),
		zap.Int("targetCount", len(targets)))

	for _, target := range targets {
		r.schedulePrewarm(functionName, target)
	}
}

// schedulePrewarm schedules a prewarm operation for a target function.
// It calculates the delay based on lead time and cold start estimates,
// ensuring the function is ready just in time for when it's needed.
func (r *RProxy) schedulePrewarm(caller string, target callgraph.PrewarmTarget) {
	// Check if the function is currently scaled down
	r.routingTableMux.RLock()
	route, exists := r.routingTable[target.FunctionName]
	isScaledDown := exists && !route.isActive
	r.routingTableMux.RUnlock()

	if !isScaledDown {
		r.logger.Debug("skipping prewarming - function already active",
			zap.String("function", target.FunctionName))
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

	// If delay is negative or zero, prewarm immediately
	if delay <= 0 {
		r.logger.Info("prewarming function immediately",
			zap.String("caller", caller),
			zap.String("target", target.FunctionName),
			zap.Duration("leadTime", target.LeadTime),
			zap.Duration("coldStartTime", coldStartTime))

		go r.executePrewarm(target.FunctionName)
		return
	}

	r.logger.Info("scheduling prewarm",
		zap.String("caller", caller),
		zap.String("target", target.FunctionName),
		zap.Duration("leadTime", target.LeadTime),
		zap.Duration("coldStartTime", coldStartTime),
		zap.Duration("delay", delay))

	// Schedule the prewarm after the calculated delay
	time.AfterFunc(delay, func() {
		// Re-check if function is still scaled down at trigger time
		r.routingTableMux.RLock()
		route, exists := r.routingTable[target.FunctionName]
		stillScaledDown := exists && !route.isActive
		r.routingTableMux.RUnlock()

		if !stillScaledDown {
			r.logger.Debug("skipping scheduled prewarm - function became active",
				zap.String("function", target.FunctionName))
			return
		}

		r.executePrewarm(target.FunctionName)
	})
}

// executePrewarm performs the actual prewarm operation for a function.
func (r *RProxy) executePrewarm(funcName string) {
	if err := r.scaleUpFunction(funcName, false); err != nil {
		r.logger.Debug("prewarm downstream function failed",
			zap.String("function", funcName),
			zap.Error(err))
	} else {
		r.logger.Info("prewarm downstream function completed",
			zap.String("function", funcName))
	}
}

func (r *RProxy) heartbeat(name string) error {
	url := "http://127.0.0.1/system/heartbeat"

	reqData := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	body, err := json.Marshal(reqData)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	err = retry.New(
		retry.Attempts(3),
		retry.Delay(100*time.Millisecond),
		retry.OnRetry(func(attempt uint, err error) {
			r.logger.Debug("heartbeat attempt failed", zap.Uint("attempt", attempt), zap.String("name", name), zap.Error(err))
		}),
	).Do(
		func() error {
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return err
			}

			r.logger.Info("successfully triggered heartbeat", zap.String("name", name))
			return nil
		},
	)

	if err != nil {
		r.logger.Error("failed to trigger heartbeat", zap.String("name", name), zap.Error(err))
		return err
	}
	return nil
}

// scaleUpFunction calls the manager to scale up a function
// cold=true means this is a user-facing cold start
// cold=false means this is a proactive prewarm
func (r *RProxy) scaleUpFunction(name string, cold bool) error {
	url := "http://127.0.0.1/system/scale-up"

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
			r.logger.Debug("scale-up attempt failed", zap.Uint("attempt", attempt), zap.String("name", name), zap.Bool("cold", cold), zap.Error(err))
		}),
	).Do(
		func() error {
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return err
			}

			r.logger.Info("successfully scaled up the function", zap.String("name", name), zap.Bool("cold", cold))
			return nil
		},
	)
	if err != nil {
		r.logger.Error("failed to scale up the function", zap.String("name", name), zap.Bool("cold", cold), zap.Error(err))
	}
	return nil
}
