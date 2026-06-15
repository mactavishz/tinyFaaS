package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestStatsStoreRecordAndReset(t *testing.T) {
	store := NewFunctionStatsStore()
	store.Record("echo", InvocationRecord{StatusCode: 200, Success: true})

	stats, ok := store.Get("echo")
	if !ok {
		t.Fatal("expected stats to exist")
	}
	if stats.summary.SuccessfulInvocations != 1 {
		t.Fatalf("expected one success, got %d", stats.summary.SuccessfulInvocations)
	}
	if stats.summary.StatusCodes["200"] != 1 {
		t.Fatalf("expected 200 count 1, got %d", stats.summary.StatusCodes["200"])
	}

	store.Reset("echo")
	if _, ok := store.Get("echo"); ok {
		t.Fatal("expected reset stats to be removed")
	}
}

func TestRecordInvocationStatsMiddleware(t *testing.T) {
	g := New(nopLogger())
	req := httptest.NewRequest(http.MethodPost, "/fn/echo", nil)
	rec := httptest.NewRecorder()

	h := g.recordInvocationStats(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	h.ServeHTTP(rec, req)

	stats, ok := g.stats.Get("echo")
	if !ok {
		t.Fatal("expected recorded stats")
	}
	if len(stats.invocations) != 1 {
		t.Fatalf("expected 1 invocation, got %d", len(stats.invocations))
	}
	if stats.summary.StatusCodes["201"] != 1 {
		t.Fatalf("expected 201 count 1, got %d", stats.summary.StatusCodes["201"])
	}
	if stats.invocations[0].Path != "/fn/echo" {
		t.Fatalf("expected path /fn/echo, got %q", stats.invocations[0].Path)
	}
}

func TestHandleFunctionStats(t *testing.T) {
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/function/echo" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"echo"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer manager.Close()

	_, port, err := net.SplitHostPort(manager.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port failed: %v", err)
	}

	g := New(nopLogger(), WithTinyFaaSPort(port))
	g.stats.Record("echo", InvocationRecord{StatusCode: 200, Success: true, Method: http.MethodPost, Path: "/fn/echo"})

	req := httptest.NewRequest(http.MethodGet, "/system/stats/function/echo", nil)
	rec := httptest.NewRecorder()
	g.HandleFunctionStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp FunctionStatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Function.Name != "echo" || resp.Function.Namespace != DEFAULT_FUNCTION_NS {
		t.Fatalf("unexpected function response: %#v", resp.Function)
	}
	if resp.Summary.StatusCodes["200"] != 1 {
		t.Fatalf("expected 200 count 1, got %d", resp.Summary.StatusCodes["200"])
	}
}
