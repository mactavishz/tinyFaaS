package manager

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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
	stopCalls    int
	destroyCalls int
	startCh      chan struct{}
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
	h.running = false
	ch := h.stopCh
	h.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}

func (h *testHandler) Restart() error {
	h.mu.Lock()
	h.running = true
	h.mu.Unlock()
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

func newManagerWithRProxy(t *testing.T, backend Backend) (*ManagementService, *rproxyRecorder) {
	t.Helper()

	recorder := newRProxyRecorder()
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)

	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	require.NoError(t, err)

	ms := New("test", port, backend, zap.NewNop())
	return ms, recorder
}

type rproxyRecorder struct {
	mu       sync.Mutex
	requests []recordedRequest
}

type recordedRequest struct {
	method string
	path   string
	body   string
}

func newRProxyRecorder() *rproxyRecorder {
	return &rproxyRecorder{}
}

func (r *rproxyRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.requests = append(r.requests, recordedRequest{method: req.Method, path: req.URL.Path, body: string(body)})
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (r *rproxyRecorder) count(method string, path string) int {
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
	as := autoscaler.New(autoscaler.Config{Enabled: true, Platform: "tinyfaas", DefaultIdleDuration: time.Minute}, NewTinyFaaSScaleOp(ms, zap.NewNop()), zap.NewNop())
	ms.SetAutoScaler(as)
	return as
}

func TestCreateFunctionRedeployWaitsForInFlightRequest(t *testing.T) {
	oldTmpDir := TmpDir
	TmpDir = t.TempDir()
	defer func() { TmpDir = oldTmpDir }()

	backend := newTestBackend()
	ms, rproxy := newManagerWithRProxy(t, backend)
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
	assert.Equal(t, 2, rproxy.count(http.MethodPut, "/config"))
	assert.Equal(t, 1, rproxy.count(http.MethodPatch, "/config"))

	status := as.GetFunctionStatus()["echo"]
	assert.Equal(t, autoscaler.StateActive, status.State)
	assert.Equal(t, 0, status.InFlight)
	assert.True(t, ms.functionHandlers["echo"].IsRunning())
	assert.Equal(t, newHandler, ms.functionHandlers["echo"])
}

func TestScaleDownWaitsForInFlightRequest(t *testing.T) {
	backend := newTestBackend()
	ms, rproxy := newManagerWithRProxy(t, backend)
	as := installAutoScaler(ms)

	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Running: true}
	as.RegisterFunction("echo", map[string]string{"com.openfaas.scale.zero": "true"})

	require.NoError(t, ms.StartRequest("echo"))

	scaleDownDone := make(chan error, 1)
	go func() {
		scaleDownDone <- as.ScaleDownWhenIdle(context.Background(), "echo")
	}()

	time.Sleep(80 * time.Millisecond)
	_, stopCalls, _ := handler.counts()
	assert.Equal(t, 0, stopCalls)
	assert.Equal(t, 0, rproxy.count(http.MethodPatch, "/config"))

	ms.EndRequest("echo")
	require.NoError(t, <-scaleDownDone)

	state, ok := as.GetState("echo")
	require.True(t, ok)
	assert.Equal(t, autoscaler.StateScaledDown, state)
	_, stopCalls, _ = handler.counts()
	assert.Equal(t, 1, stopCalls)
	assert.Equal(t, 1, rproxy.count(http.MethodPatch, "/config"))
	assert.False(t, handler.IsRunning())
}

func TestGetReturnsFunctionConfig(t *testing.T) {
	backend := newTestBackend()
	ms, _ := newManagerWithRProxy(t, backend)
	handler := &testHandler{name: "echo", ips: []string{"10.0.0.1"}, running: true}
	ms.functionHandlers["echo"] = handler
	ms.functionConfigs["echo"] = FunctionConfig{Name: "echo", Env: "python3"}

	fn, ok := ms.Get("echo")
	require.True(t, ok)
	assert.Equal(t, "echo", fn.Name)
	assert.True(t, fn.Running)
}
