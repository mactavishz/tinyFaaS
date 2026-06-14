package gateway

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newGatewayBackend(t *testing.T, handler http.HandlerFunc) (*Gateway, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split backend address: %v", err)
	}
	if host != "127.0.0.1" && host != "::1" {
		t.Fatalf("unexpected backend host %q", host)
	}
	return New(nopLogger(), WithTinyFaaSPort(port)), server.Close
}

func TestFunctionInvokeRewritesToSyncEndpointAndStripsAsyncHeader(t *testing.T) {
	var gotPath string
	var gotAsyncHeader string
	g, cleanup := newGatewayBackend(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAsyncHeader = r.Header.Get("X-Tinyfaas-Async")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/fn/echo", nil)
	req.Header.Set("X-Tinyfaas-Async", "true")
	rec := httptest.NewRecorder()

	g.HandleFunctionInvoke(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotPath != "/invoke/echo" {
		t.Fatalf("expected /invoke/echo, got %q", gotPath)
	}
	if gotAsyncHeader != "" {
		t.Fatalf("expected async header stripped, got %q", gotAsyncHeader)
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "ok" {
		t.Fatalf("expected backend body, got %q", string(body))
	}
}

func TestAsyncFunctionInvokeRewritesToAsyncEndpoint(t *testing.T) {
	var gotPath string
	g, cleanup := newGatewayBackend(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	})
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/async-fn/echo", nil)
	rec := httptest.NewRecorder()

	g.HandleAsyncFunctionInvoke(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if gotPath != "/async-invoke/echo" {
		t.Fatalf("expected /async-invoke/echo, got %q", gotPath)
	}
}
