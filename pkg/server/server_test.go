package server

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

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
	labels       map[string]string
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
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.labels == nil {
		return map[string]string{}
	}
	return h.labels
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

// recordingTracker wraps a real callgraph tracker and records scale-up events so
// tests can assert which scale-up was recorded.
type recordingTracker struct {
	callgraph.FullTracker
	mu       sync.Mutex
	scaleUps []scaleUpRecord
}

type scaleUpRecord struct {
	FunctionName string
	Cold         bool
}

func newRecordingTracker() *recordingTracker {
	return &recordingTracker{FullTracker: callgraph.New(callgraph.WithLogger(nopLogger()))}
}

func (r *recordingTracker) RecordScaleUp(name string, ts time.Time, d time.Duration, cold bool) {
	r.mu.Lock()
	r.scaleUps = append(r.scaleUps, scaleUpRecord{FunctionName: name, Cold: cold})
	r.mu.Unlock()
	r.FullTracker.RecordScaleUp(name, ts, d, cold)
}

func (r *recordingTracker) scaleUpRecords() []scaleUpRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]scaleUpRecord(nil), r.scaleUps...)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newServer(t *testing.T, backend Backend) *Server {
	t.Helper()
	oldTmpDir := TmpDir
	TmpDir = t.TempDir()
	t.Cleanup(func() { TmpDir = oldTmpDir })
	return New("test", "development", backend, nopLogger())
}

func installAutoScaler(s *Server) *autoscaler.AutoScaler {
	as := autoscaler.New(autoscaler.Config{Enabled: true, Platform: "tinyfaas", DefaultIdleDuration: time.Minute}, NewTinyFaaSScaleOp(s, nopLogger()), nopLogger())
	s.SetAutoScaler(as)
	return as
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

func startFunctionRuntimeServer(t *testing.T) func() {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:8000")
	if err != nil {
		t.Skipf("unable to bind function runtime test server on 127.0.0.1:8000: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/fn", func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodPost, req.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Handler: mux}
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(listener)
		close(done)
	}()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}
}

func (s *Server) routeSnapshotForTest(name string) (*Route, bool) {
	return s.getRouteSnapshot(name)
}

// ---------------------------------------------------------------------------
// Function lifecycle
// ---------------------------------------------------------------------------

func TestCreateFunctionRedeployWaitsForInFlightRequest(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	as := installAutoScaler(s)

	archive := makeZipArchive(t)
	require.NoError(t, s.UploadArchive("echo", "python3", 1, archive, nil, map[string]string{"com.openfaas.scale.zero": "true"}, FunctionResourceRequest{}))

	oldHandler := backend.handlers("echo")[0]
	_, stopCalls, destroyCalls := oldHandler.counts()
	assert.Equal(t, 0, stopCalls)
	assert.Equal(t, 0, destroyCalls)

	require.NoError(t, s.StartRequest("echo"))

	redeployDone := make(chan error, 1)
	go func() {
		redeployDone <- s.UploadArchive("echo", "python3", 1, archive, nil, map[string]string{"com.openfaas.scale.zero": "true"}, FunctionResourceRequest{})
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

	s.EndRequest("echo")
	require.NoError(t, <-redeployDone)

	startCalls, _, _ = newHandler.counts()
	_, stopCalls, destroyCalls = oldHandler.counts()
	assert.Equal(t, 1, startCalls)
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, 1, destroyCalls)

	// The route is registered directly and points at the new handler's IPs.
	route, ok := s.routeSnapshotForTest("echo")
	require.True(t, ok)
	assert.True(t, route.isActive)
	assert.Equal(t, newHandler.IPs(), route.ips)

	status := as.GetFunctionStatus()["echo"]
	assert.Equal(t, autoscaler.StateActive, status.State)
	assert.Equal(t, 0, status.InFlight)
	assert.True(t, s.functionHandlers["echo"].IsRunning())
	assert.Equal(t, newHandler, s.functionHandlers["echo"])
}

func TestScaleDownWaitsForInFlightRequest(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	as := installAutoScaler(s)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	s.functionHandlers["echo"] = handler
	s.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	require.NoError(t, s.Add("echo", handler.IPs(), map[string]string{}))
	as.RegisterFunction("echo", map[string]string{"com.openfaas.scale.zero": "true"})

	require.NoError(t, s.StartRequest("echo"))

	scaleDownDone := make(chan error, 1)
	go func() {
		scaleDownDone <- as.ScaleDownWhenIdle("echo")
	}()

	time.Sleep(80 * time.Millisecond)
	_, stopCalls, _ := handler.counts()
	assert.Equal(t, 0, stopCalls)
	route, _ := s.routeSnapshotForTest("echo")
	assert.True(t, route.isActive)

	s.EndRequest("echo")
	require.NoError(t, <-scaleDownDone)

	state, ok := as.GetState("echo")
	require.True(t, ok)
	assert.Equal(t, autoscaler.StateScaledDown, state)
	_, stopCalls, _ = handler.counts()
	assert.Equal(t, 1, stopCalls)
	route, _ = s.routeSnapshotForTest("echo")
	assert.False(t, route.isActive)
	assert.Empty(t, route.ips)
	assert.False(t, handler.IsRunning())
}

func TestScaleDownFailureRestoresRoute(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	as := installAutoScaler(s)

	handler := &testHandler{
		name:    "echo",
		ips:     []string{"10.0.0.1"},
		running: true,
		stopErr: errors.New("stop failed"),
	}
	s.functionHandlers["echo"] = handler
	s.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	require.NoError(t, s.Add("echo", handler.IPs(), map[string]string{}))
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

	// The route is restored after the failed stop.
	route, ok := s.routeSnapshotForTest("echo")
	require.True(t, ok)
	assert.True(t, route.isActive)
	assert.Equal(t, handler.IPs(), route.ips)
}

func TestScaleUpRecordsOnlyClaimingPrewarmDuringDemandRace(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	tracker := newRecordingTracker()
	s.SetTracker(tracker)
	as := installAutoScaler(s)

	restartStarted := make(chan struct{})
	restartRelease := make(chan struct{})
	handler := &testHandler{
		name:         "echo",
		ips:          []string{"10.0.0.1"},
		restartCh:    restartStarted,
		restartBlock: restartRelease,
	}
	s.functionHandlers["echo"] = handler
	s.functionConfigs["echo"] = FunctionConfig{Name: "echo"}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateScaledDown)

	prewarmDone := make(chan error, 1)
	go func() {
		prewarmDone <- s.ScaleUp("echo", false)
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
		demandDone <- s.ScaleUp("echo", true)
	}()

	close(restartRelease)
	require.NoError(t, <-prewarmDone)
	require.NoError(t, <-demandDone)

	assert.Equal(t, 1, handler.restartCount())
	records := tracker.scaleUpRecords()
	require.Len(t, records, 1)
	assert.Equal(t, "echo", records[0].FunctionName)
	assert.False(t, records[0].Cold)
}

func TestScaleUpRecordsOnlyClaimingDemandDuringPrewarmRace(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	tracker := newRecordingTracker()
	s.SetTracker(tracker)
	as := installAutoScaler(s)

	restartStarted := make(chan struct{})
	restartRelease := make(chan struct{})
	handler := &testHandler{
		name:         "echo",
		ips:          []string{"10.0.0.1"},
		restartCh:    restartStarted,
		restartBlock: restartRelease,
	}
	s.functionHandlers["echo"] = handler
	s.functionConfigs["echo"] = FunctionConfig{Name: "echo"}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateScaledDown)

	demandDone := make(chan error, 1)
	go func() {
		demandDone <- s.ScaleUp("echo", true)
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
		prewarmDone <- s.ScaleUp("echo", false)
	}()

	close(restartRelease)
	require.NoError(t, <-demandDone)
	require.NoError(t, <-prewarmDone)

	assert.Equal(t, 1, handler.restartCount())
	records := tracker.scaleUpRecords()
	require.Len(t, records, 1)
	assert.Equal(t, "echo", records[0].FunctionName)
	assert.True(t, records[0].Cold)
}

func TestScaleUpActiveFunctionDoesNotRecordScaleUp(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	tracker := newRecordingTracker()
	s.SetTracker(tracker)
	as := installAutoScaler(s)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	s.functionHandlers["echo"] = handler
	s.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	as.RegisterFunctionWithState("echo", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateActive)

	require.NoError(t, s.ScaleUp("echo", true))

	assert.Equal(t, 0, handler.restartCount())
	assert.Empty(t, tracker.scaleUpRecords())
}

func TestGetReturnsFunctionConfig(t *testing.T) {
	backend := newTestBackend()
	s := newServer(t, backend)
	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	s.functionHandlers["echo"] = handler
	s.functionConfigs["echo"] = FunctionConfig{Name: "echo", Env: "python3"}

	fn, ok := s.Get("echo")
	require.True(t, ok)
	assert.Equal(t, "echo", fn.Name)
	assert.True(t, fn.Running)
}

// ---------------------------------------------------------------------------
// Routing and dispatch
// ---------------------------------------------------------------------------

func TestUpdateDeactivatesRouteIdempotently(t *testing.T) {
	s := newServer(t, newTestBackend())

	require.NoError(t, s.Add("test-func", []string{"10.0.0.2", "10.0.0.3"}, map[string]string{}))

	name, ok := s.GetFunctionNameByIP("10.0.0.2")
	require.True(t, ok)
	require.Equal(t, "test-func", name)

	require.NoError(t, s.Update("test-func"))

	route, ok := s.routeSnapshotForTest("test-func")
	require.True(t, ok)
	assert.False(t, route.isActive)
	assert.Empty(t, route.ips)
	_, ok1 := s.GetFunctionNameByIP("10.0.0.2")
	_, ok2 := s.GetFunctionNameByIP("10.0.0.3")
	assert.False(t, ok1)
	assert.False(t, ok2)

	require.NoError(t, s.Update("test-func"))

	route, ok = s.routeSnapshotForTest("test-func")
	require.True(t, ok)
	assert.False(t, route.isActive)
	assert.Empty(t, route.ips)
}

func TestCallReturns503WhenColdStartTriggerFails(t *testing.T) {
	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	s := newServer(t, newTestBackend())
	s.SetTracker(tracker)
	// Autoscaler enabled, but the function is never registered with it, so the
	// cold-start scale-up will fail.
	installAutoScaler(s)

	s.routingTableMux.Lock()
	s.routingTable["test-func"] = &Route{
		ips:              []string{"10.255.255.1"},
		isActive:         false,
		callgraphEnabled: true,
	}
	s.routingTableMux.Unlock()

	status, body := s.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-coldstart-fail"}})
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Nil(t, body)
	assert.Equal(t, 0, tracker.EdgeCount())
}

func TestCallWaitsForRouteReadyAfterScaleUp(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	s := newServer(t, newTestBackend())
	as := installAutoScaler(s)

	// Function is registered scaled down with a handler that becomes ready on restart.
	handler := &testHandler{name: "test-func", ips: []string{"127.0.0.1"}}
	s.functionHandlers["test-func"] = handler
	s.functionConfigs["test-func"] = FunctionConfig{Name: "test-func"}
	as.RegisterFunctionWithState("test-func", map[string]string{"com.tinyfaas.scale.zero": "true"}, autoscaler.StateScaledDown)

	s.routingTableMux.Lock()
	s.routingTable["test-func"] = &Route{isActive: false}
	s.routingTableMux.Unlock()

	status, body := s.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-route-sync"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)
	assert.Equal(t, 1, handler.restartCount())
}

func TestCallFinishesRequestAfterLocalRequestBuildFailure(t *testing.T) {
	s := newServer(t, newTestBackend())
	as := installAutoScaler(s)

	require.NoError(t, s.Add("test-func", []string{"bad host"}, map[string]string{}))
	as.RegisterFunctionWithState("test-func", map[string]string{}, autoscaler.StateActive)

	status, body := s.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-build-fail"}})
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Nil(t, body)

	// The in-flight request was released even though building the request failed.
	require.Eventually(t, func() bool {
		return as.GetFunctionStatus()["test-func"].InFlight == 0
	}, time.Second, 10*time.Millisecond)
}

func TestCallAsyncFinishesRequestAfterBackgroundInvocation(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	s := newServer(t, newTestBackend())
	as := installAutoScaler(s)

	require.NoError(t, s.Add("test-func", []string{"127.0.0.1"}, map[string]string{}))
	as.RegisterFunctionWithState("test-func", map[string]string{}, autoscaler.StateActive)

	status, body := s.Call("test-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-async"}})
	assert.Equal(t, http.StatusAccepted, status)
	assert.Nil(t, body)

	require.Eventually(t, func() bool {
		return as.GetFunctionStatus()["test-func"].InFlight == 0
	}, time.Second, 10*time.Millisecond)
}

func TestCallAsyncReturns404ForUnknownFunction(t *testing.T) {
	s := newServer(t, newTestBackend())
	installAutoScaler(s)

	status, body := s.Call("missing-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-async-missing"}})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Nil(t, body)
}

func TestCallRecordsEdgeWithSyncKindWhenRouteReady(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	s := newServer(t, newTestBackend())
	s.SetTracker(tracker)

	require.NoError(t, s.Add("test-func", []string{"127.0.0.1"}, map[string]string{}))

	status, body := s.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-edge-record"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)

	edge, ok := tracker.GetEdgeStats("", "test-func")
	require.True(t, ok)
	assert.Equal(t, 1, edge.Count)
	assert.Equal(t, callgraph.EdgeKindSync, edge.Kind)
}

func TestCallRecordsEdgeWithAsyncKind(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	s := newServer(t, newTestBackend())
	s.SetTracker(tracker)

	require.NoError(t, s.Add("test-func", []string{"127.0.0.1"}, map[string]string{}))

	status, _ := s.Call("test-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-edge-async"}})
	assert.Equal(t, http.StatusAccepted, status)

	require.Eventually(t, func() bool {
		edge, ok := tracker.GetEdgeStats("", "test-func")
		return ok && edge.Count == 1
	}, time.Second, 10*time.Millisecond)

	edge, ok := tracker.GetEdgeStats("", "test-func")
	require.True(t, ok)
	assert.Equal(t, callgraph.EdgeKindAsync, edge.Kind)
}
