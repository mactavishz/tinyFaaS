package server

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testBackend struct {
	mu              sync.Mutex
	handlersByName  map[string][]*testHandler
	createdHandlers []*testHandler
	createCalls     int
}

func newTestBackend() *testBackend {
	return &testBackend{handlersByName: make(map[string][]*testHandler)}
}

func (b *testBackend) Create(name string, env string, threads int, filedir string, envs map[string]string, labels map[string]string, limits ResourceLimits) (Handler, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.createCalls++
	idx := len(b.handlersByName[name]) + 1
	h := &testHandler{name: fmt.Sprintf("%s-%d", name, idx), ips: []string{fmt.Sprintf("10.0.0.%d", idx)}}
	b.handlersByName[name] = append(b.handlersByName[name], h)
	b.createdHandlers = append(b.createdHandlers, h)
	return h, nil
}

func (b *testBackend) Stop() error {
	return nil
}

func (b *testBackend) handlers(name string) []*testHandler {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*testHandler(nil), b.handlersByName[name]...)
}

type testHandler struct {
	mu           sync.Mutex
	name         string
	ips          []string
	running      bool
	startCalls   int
	restartCalls int
	stopCalls    int
	destroyCalls int
	stopErr      error
	startCh      chan struct{}
	restartCh    chan struct{}
	restartBlock chan struct{}
	stopCh       chan struct{}
	destroyCh    chan struct{}
}

func (h *testHandler) IPs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.ips...)
}

func (h *testHandler) Start() error {
	h.mu.Lock()
	h.startCalls++
	h.running = true
	ch := h.startCh
	h.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}

func (h *testHandler) Stop() error {
	h.mu.Lock()
	h.stopCalls++
	err := h.stopErr
	if err == nil {
		h.running = false
	}
	ch := h.stopCh
	h.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return err
}

func (h *testHandler) Restart() error {
	h.mu.Lock()
	h.restartCalls++
	h.running = true
	ch := h.restartCh
	block := h.restartBlock
	h.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	if block != nil {
		<-block
	}
	return nil
}

func (h *testHandler) Destroy() error {
	h.mu.Lock()
	h.destroyCalls++
	h.running = false
	ch := h.destroyCh
	h.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}

func (h *testHandler) Logs() (io.Reader, error) {
	return bytes.NewReader(nil), nil
}

func (h *testHandler) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

func (h *testHandler) GetLabels() map[string]string {
	return map[string]string{}
}

func (h *testHandler) counts() (start, stop, destroy int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.startCalls, h.stopCalls, h.destroyCalls
}

func (h *testHandler) restartCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.restartCalls
}

func newManagerWithRouteRecorder(t *testing.T, backend Backend) (*ManagementService, *routeRecorder) {
	t.Helper()

	recorder := newRouteRecorder()
	ms := NewManagementService("test", backend, nopLogger())
	ms.SetRouteHooks(recorder.addRoute, recorder.clearRoute, recorder.deleteRoute)
	ms.SetCallgraphHooks(recorder.recordScaleUp, recorder.recordScaleDown, recorder.resetCallgraph)
	return ms, recorder
}

type routeRecorder struct {
	mu       sync.Mutex
	requests []recordedRequest
}

type recordedRequest struct {
	method string
	path   string
	body   string
}

type scaleUpRecord struct {
	FunctionName string `json:"name"`
	Timestamp    int64  `json:"timestamp"`
	Duration     int64  `json:"duration_ns"`
	Cold         bool   `json:"cold"`
}

func newRouteRecorder() *routeRecorder {
	return &routeRecorder{}
}

func (r *routeRecorder) addRoute(name string, ips []string, labels map[string]string) error {
	body, _ := json.Marshal(struct {
		FunctionName string            `json:"name"`
		FunctionIPs  []string          `json:"ips"`
		Labels       map[string]string `json:"labels,omitempty"`
	}{FunctionName: name, FunctionIPs: ips, Labels: labels})
	r.record(http.MethodPut, "/config", string(body))
	return nil
}

func (r *routeRecorder) clearRoute(name string) error {
	body, _ := json.Marshal(struct {
		FunctionName string `json:"name"`
	}{FunctionName: name})
	r.record(http.MethodPatch, "/config", string(body))
	return nil
}

func (r *routeRecorder) deleteRoute(name string) error {
	body, _ := json.Marshal(struct {
		FunctionName string `json:"name"`
	}{FunctionName: name})
	r.record(http.MethodDelete, "/config", string(body))
	return nil
}

func (r *routeRecorder) recordScaleUp(name string, timestamp time.Time, duration time.Duration, cold bool) {
	body, _ := json.Marshal(scaleUpRecord{
		FunctionName: name,
		Timestamp:    timestamp.UnixNano(),
		Duration:     duration.Nanoseconds(),
		Cold:         cold,
	})
	r.record(http.MethodPost, "/callgraph/scaleup", string(body))
}

func (r *routeRecorder) recordScaleDown(name string, timestamp time.Time, duration time.Duration) {
	body, _ := json.Marshal(struct {
		FunctionName string `json:"name"`
		Timestamp    int64  `json:"timestamp"`
		Duration     int64  `json:"duration_ns"`
	}{FunctionName: name, Timestamp: timestamp.UnixNano(), Duration: duration.Nanoseconds()})
	r.record(http.MethodPost, "/callgraph/scaledown", string(body))
}

func (r *routeRecorder) resetCallgraph(name string) {
	body, _ := json.Marshal(struct {
		FunctionName string `json:"name"`
	}{FunctionName: name})
	r.record(http.MethodPost, "/callgraph/reset", string(body))
}

func (r *routeRecorder) record(method string, path string, body string) {
	r.mu.Lock()
	r.requests = append(r.requests, recordedRequest{method: method, path: path, body: body})
	r.mu.Unlock()
}

func (r *routeRecorder) count(method string, path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, req := range r.requests {
		if req.method == method && req.path == path {
			count++
		}
	}
	return count
}

func (r *routeRecorder) scaleUpRecords() []scaleUpRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	records := make([]scaleUpRecord, 0)
	for _, req := range r.requests {
		if req.method != http.MethodPost || req.path != "/callgraph/scaleup" {
			continue
		}
		var record scaleUpRecord
		if err := json.Unmarshal([]byte(req.body), &record); err == nil {
			records = append(records, record)
		}
	}
	return records
}

func makeZipArchive(t *testing.T) string {
	t.Helper()

	archivePath := filepath.Join(t.TempDir(), "function.zip")
	file, err := os.Create(archivePath)
	require.NoError(t, err)

	zipWriter := zip.NewWriter(file)
	entry, err := zipWriter.Create("handler.txt")
	require.NoError(t, err)
	_, err = entry.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, zipWriter.Close())
	require.NoError(t, file.Close())

	return archivePath
}

func installAutoScaler(ms *ManagementService) *autoscaler.AutoScaler {
	as := autoscaler.New(autoscaler.Config{Enabled: true, Platform: "tinyfaas", DefaultIdleDuration: time.Minute}, NewTinyFaaSScaleOp(ms, nopLogger()), nopLogger())
	ms.SetAutoScaler(as)
	return as
}

func TestCreateFunctionRedeployWaitsForInFlightRequest(t *testing.T) {
	oldTmpDir := TmpDir
	TmpDir = t.TempDir()
	defer func() { TmpDir = oldTmpDir }()

	backend := newTestBackend()
	ms, routeRecorder := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	archive := makeZipArchive(t)
	require.NoError(t, ms.UploadArchive("echo", "python3", 1, archive, nil, map[string]string{"com.openfaas.scale.zero": "true"}, FunctionResourceRequest{}))

	oldHandler := backend.handlers("echo")[0]
	_, stopCalls, destroyCalls := oldHandler.counts()
	assert.Equal(t, 0, stopCalls)
	assert.Equal(t, 0, destroyCalls)

	require.NoError(t, ms.StartRequest("echo"))

	redeployDone := make(chan error, 1)
	go func() {
		redeployDone <- ms.UploadArchive("echo", "python3", 1, archive, nil, map[string]string{"com.openfaas.scale.zero": "true"}, FunctionResourceRequest{})
	}()

	time.Sleep(80 * time.Millisecond)
	handlers := backend.handlers("echo")
	require.Len(t, handlers, 2)
	newHandler := handlers[1]

	startCalls, _, _ := newHandler.counts()
	_, stopCalls, destroyCalls = oldHandler.counts()
	assert.Equal(t, 0, startCalls)
	assert.Equal(t, 0, stopCalls)
	assert.Equal(t, 0, destroyCalls)

	state, ok := as.GetState("echo")
	require.True(t, ok)
	assert.Equal(t, autoscaler.StateBlocked, state)

	ms.EndRequest("echo")
	require.NoError(t, <-redeployDone)

	startCalls, _, _ = newHandler.counts()
	_, stopCalls, destroyCalls = oldHandler.counts()
	assert.Equal(t, 1, startCalls)
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, 1, destroyCalls)
	assert.Equal(t, 2, routeRecorder.count(http.MethodPut, "/config"))
	assert.Equal(t, 1, routeRecorder.count(http.MethodPatch, "/config"))

	status := as.GetFunctionStatus()["echo"]
	assert.Equal(t, autoscaler.StateActive, status.State)
	assert.Equal(t, 0, status.InFlight)
	assert.True(t, ms.functionHandlers["echo"].IsRunning())
	assert.Equal(t, newHandler, ms.functionHandlers["echo"])
}

func TestScaleDownWaitsForInFlightRequest(t *testing.T) {
	backend := newTestBackend()
	ms, routeRecorder := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	as.RegisterFunction("echo", map[string]string{"com.openfaas.scale.zero": "true"})

	require.NoError(t, ms.StartRequest("echo"))

	scaleDownDone := make(chan error, 1)
	go func() {
		scaleDownDone <- as.ScaleDownWhenIdle("echo")
	}()

	time.Sleep(80 * time.Millisecond)
	_, stopCalls, _ := handler.counts()
	assert.Equal(t, 0, stopCalls)
	assert.Equal(t, 0, routeRecorder.count(http.MethodPatch, "/config"))

	ms.EndRequest("echo")
	require.NoError(t, <-scaleDownDone)

	state, ok := as.GetState("echo")
	require.True(t, ok)
	assert.Equal(t, autoscaler.StateScaledDown, state)
	_, stopCalls, _ = handler.counts()
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, 1, routeRecorder.count(http.MethodPatch, "/config"))
	assert.False(t, handler.IsRunning())
}

func TestScaleDownFailureRestoresRoute(t *testing.T) {
	backend := newTestBackend()
	ms, routeRecorder := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	handler := &testHandler{
		name:    "echo",
		ips:     []string{"10.0.0.1"},
		running: true,
		stopErr: errors.New("stop failed"),
	}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	as.RegisterFunction("echo", map[string]string{"com.tinyfaas.scale.zero": "true"})

	err := as.ScaleDownWhenIdle("echo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to stop function echo")

	state, ok := as.GetState("echo")
	require.True(t, ok)
	assert.Equal(t, autoscaler.StateActive, state)

	_, stopCalls, _ := handler.counts()
	assert.Equal(t, 1, stopCalls)
	assert.True(t, handler.IsRunning())
	assert.Equal(t, 1, routeRecorder.count(http.MethodPatch, "/config"))
	assert.Equal(t, 1, routeRecorder.count(http.MethodPut, "/config"))
}

func TestManagementScaleDownStopsActiveFunction(t *testing.T) {
	backend := newTestBackend()
	ms, _ := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	as.RegisterFunction("echo", map[string]string{"com.tinyfaas.scale.zero": "true"})

	require.NoError(t, ms.ScaleDown("echo"))

	state, ok := as.GetState("echo")
	require.True(t, ok)
	assert.Equal(t, autoscaler.StateScaledDown, state)
	_, stopCalls, _ := handler.counts()
	assert.Equal(t, 1, stopCalls)
	assert.False(t, handler.IsRunning())
}

func TestManagementScaleDownIsIdempotent(t *testing.T) {
	backend := newTestBackend()
	ms, _ := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: false}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo"}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateScaledDown)

	// Already scaled down: scaling down again must be a no-op, not an error.
	require.NoError(t, ms.ScaleDown("echo"))

	_, stopCalls, _ := handler.counts()
	assert.Equal(t, 0, stopCalls, "no Stop call expected for an already scaled-down function")
}

func TestManagementScaleDownErrsWhenAutoscalerDisabled(t *testing.T) {
	backend := newTestBackend()
	ms, _ := newManagerWithRouteRecorder(t, backend)
	// No autoscaler installed.

	err := ms.ScaleDown("echo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "autoscaler not enabled")
}

func TestScaleUpRecordsOnlyClaimingPrewarmDuringDemandRace(t *testing.T) {
	backend := newTestBackend()
	ms, routeRecorder := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	restartStarted := make(chan struct{})
	restartRelease := make(chan struct{})
	handler := &testHandler{
		name:         "echo",
		ips:          []string{"10.0.0.1"},
		restartCh:    restartStarted,
		restartBlock: restartRelease,
	}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo"}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateScaledDown)

	prewarmDone := make(chan error, 1)
	go func() {
		prewarmDone <- ms.ScaleUp("echo", false)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-restartStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	demandDone := make(chan error, 1)
	go func() {
		demandDone <- ms.ScaleUp("echo", true)
	}()

	close(restartRelease)
	require.NoError(t, <-prewarmDone)
	require.NoError(t, <-demandDone)

	assert.Equal(t, 1, handler.restartCount())
	records := routeRecorder.scaleUpRecords()
	require.Len(t, records, 1)
	assert.Equal(t, "echo", records[0].FunctionName)
	assert.False(t, records[0].Cold)
}

func TestScaleUpRecordsOnlyClaimingDemandDuringPrewarmRace(t *testing.T) {
	backend := newTestBackend()
	ms, routeRecorder := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	restartStarted := make(chan struct{})
	restartRelease := make(chan struct{})
	handler := &testHandler{
		name:         "echo",
		ips:          []string{"10.0.0.1"},
		restartCh:    restartStarted,
		restartBlock: restartRelease,
	}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo"}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateScaledDown)

	demandDone := make(chan error, 1)
	go func() {
		demandDone <- ms.ScaleUp("echo", true)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-restartStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	prewarmDone := make(chan error, 1)
	go func() {
		prewarmDone <- ms.ScaleUp("echo", false)
	}()

	close(restartRelease)
	require.NoError(t, <-demandDone)
	require.NoError(t, <-prewarmDone)

	assert.Equal(t, 1, handler.restartCount())
	records := routeRecorder.scaleUpRecords()
	require.Len(t, records, 1)
	assert.Equal(t, "echo", records[0].FunctionName)
	assert.True(t, records[0].Cold)
}

func TestScaleUpActiveFunctionDoesNotRecordScaleUp(t *testing.T) {
	backend := newTestBackend()
	ms, routeRecorder := newManagerWithRouteRecorder(t, backend)
	as := installAutoScaler(ms)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateActive)

	require.NoError(t, ms.ScaleUp("echo", true))

	assert.Equal(t, 0, handler.restartCount())
	assert.Empty(t, routeRecorder.scaleUpRecords())
}

func TestGetReturnsFunctionConfig(t *testing.T) {
	backend := newTestBackend()
	ms, _ := newManagerWithRouteRecorder(t, backend)
	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Env: "python3"}

	fn, ok := ms.Get("echo")
	require.True(t, ok)
	assert.Equal(t, "echo", fn.Name)
	assert.True(t, fn.Running)
}
