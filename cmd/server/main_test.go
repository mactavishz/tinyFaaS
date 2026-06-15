package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/OpenFogStack/tinyFaaS/pkg/queue"
	tinyserver "github.com/OpenFogStack/tinyFaaS/pkg/server"
	invstats "github.com/OpenFogStack/tinyFaaS/pkg/stats"
)

type serverTestBackend struct{}

func (b serverTestBackend) Create(name string, env string, threads int, filedir string, envs map[string]string, labels map[string]string, limits tinyserver.ResourceLimits) (tinyserver.Handler, error) {
	return &serverTestHandler{}, nil
}

func (b serverTestBackend) Stop() error { return nil }

type serverTestHandler struct{}

func (h *serverTestHandler) IPs() []string                         { return []string{"127.0.0.1"} }
func (h *serverTestHandler) Start() error                          { return nil }
func (h *serverTestHandler) Stop() error                           { return nil }
func (h *serverTestHandler) Restart() error                        { return nil }
func (h *serverTestHandler) Destroy() error                        { return nil }
func (h *serverTestHandler) Logs() (io.Reader, error)              { return bytes.NewReader(nil), nil }
func (h *serverTestHandler) IsRunning() bool                       { return true }
func (h *serverTestHandler) GetLabels() map[string]string          { return map[string]string{} }
func serverTestLogger() *slog.Logger                               { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func serverTestStats() *invstats.Store                             { return invstats.NewStore() }
func serverTestRouteAdd(string, []string, map[string]string) error { return nil }

type recordingPublisher struct {
	mu  sync.Mutex
	req *queue.Request
	err error
}

func (p *recordingPublisher) Queue(req *queue.Request) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	copyReq := *req
	copyReq.Body = append([]byte(nil), req.Body...)
	copyReq.Header = req.Header.Clone()
	p.req = &copyReq
	return nil
}

func (p *recordingPublisher) Close() error { return nil }

func newAsyncTestService(t *testing.T, publisher queue.Publisher) *service {
	t.Helper()
	oldTmpDir := tinyserver.TmpDir
	tinyserver.TmpDir = t.TempDir()
	t.Cleanup(func() { tinyserver.TmpDir = oldTmpDir })

	backend := serverTestBackend{}
	ms := tinyserver.NewManagementService("test", backend, serverTestLogger())
	ms.SetRouteHooks(serverTestRouteAdd, func(string) error { return nil }, func(string) error { return nil })

	archive := createServerTestArchive(t)
	if err := ms.UploadArchive("echo", "go", 1, archive, nil, nil, tinyserver.FunctionResourceRequest{}); err != nil {
		t.Fatalf("upload test function: %v", err)
	}

	return &service{ms: ms, queue: publisher, stats: serverTestStats(), logger: serverTestLogger()}
}

func createServerTestArchive(t *testing.T) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "function-*.zip")
	if err != nil {
		t.Fatalf("create temp archive: %v", err)
	}
	zw := zip.NewWriter(file)
	w, err := zw.Create("handler")
	if err != nil {
		t.Fatalf("create zip entry: %v", err)
	}
	if _, err := w.Write([]byte("test")); err != nil {
		t.Fatalf("write zip entry: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}
	return file.Name()
}

func TestAsyncInvokeEnqueuesExistingFunction(t *testing.T) {
	publisher := &recordingPublisher{}
	s := newAsyncTestService(t, publisher)

	req := httptest.NewRequest(http.MethodPost, "/async-invoke/echo?x=1", bytes.NewBufferString("payload"))
	req.Header.Set("X-Call-Id", "call-1")
	req.Header.Set("X-Tinyfaas-Async", "true")
	rec := httptest.NewRecorder()

	s.asyncInvokeHandler(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	if publisher.req == nil {
		t.Fatal("expected request to be queued")
	}
	if publisher.req.Function != "echo" || publisher.req.Method != http.MethodPost || publisher.req.QueryString != "x=1" {
		t.Fatalf("unexpected queued request: %#v", publisher.req)
	}
	if string(publisher.req.Body) != "payload" {
		t.Fatalf("unexpected queued body %q", string(publisher.req.Body))
	}
	if publisher.req.Header.Get("X-Call-Id") != "call-1" {
		t.Fatalf("expected call id preserved")
	}
	if publisher.req.Header.Get("X-Tinyfaas-Async") != "" {
		t.Fatalf("expected legacy async header stripped")
	}
}

func TestAsyncInvokeMissingFunctionReturns404(t *testing.T) {
	publisher := &recordingPublisher{}
	s := newAsyncTestService(t, publisher)

	req := httptest.NewRequest(http.MethodPost, "/async-invoke/missing", nil)
	rec := httptest.NewRecorder()

	s.asyncInvokeHandler(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	if publisher.req != nil {
		t.Fatal("missing function should not be queued")
	}
}

func TestAsyncInvokePublishFailureReturns503(t *testing.T) {
	publisher := &recordingPublisher{err: errors.New("nats unavailable")}
	s := newAsyncTestService(t, publisher)

	req := httptest.NewRequest(http.MethodPost, "/async-invoke/echo", nil)
	rec := httptest.NewRecorder()

	s.asyncInvokeHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
