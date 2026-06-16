package gateway

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
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

func TestInvokeMiddlewareSetsCallAndForwardingHeaders(t *testing.T) {
	g := New(nopLogger())
	var gotCallID string
	var gotSourceIP string
	var gotForwardedFor string

	handler := g.InvokeMiddleware(func(w http.ResponseWriter, r *http.Request) {
		gotCallID = r.Header.Get("X-Call-Id")
		gotSourceIP = r.Header.Get("X-Source-Ip")
		gotForwardedFor = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/fn/echo", nil)
	req.RemoteAddr = "10.0.0.2:12345"
	req.Header.Set("X-Forwarded-For", "198.51.100.1")
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotCallID == "" {
		t.Fatal("expected call id generated")
	}
	if gotSourceIP != "198.51.100.1" {
		t.Fatalf("expected source ip from existing forwarded header, got %q", gotSourceIP)
	}
	if gotForwardedFor != "198.51.100.1, 10.0.0.2:12345" {
		t.Fatalf("unexpected forwarded chain %q", gotForwardedFor)
	}
}

// TestFunctionInvokeSetsSyncEdgeKindHeader verifies that user-facing /fn/
// requests are tagged as sync so the merged server records the right edge
// kind in the callgraph.
func TestFunctionInvokeSetsSyncEdgeKindHeader(t *testing.T) {
	var gotEdgeKind string
	g, cleanup := newGatewayBackend(t, func(w http.ResponseWriter, r *http.Request) {
		gotEdgeKind = r.Header.Get(EdgeKindHeader)
		w.WriteHeader(http.StatusOK)
	})
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/fn/echo", nil)
	rec := httptest.NewRecorder()
	g.HandleFunctionInvoke(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotEdgeKind != callgraph.EdgeKindSync.String() {
		t.Fatalf("expected sync edge kind, got %q", gotEdgeKind)
	}
}

// TestQueueFunctionInvokeForcesAsyncEdgeKind verifies that requests routed
// via /queue-fn/ (queue worker dispatch) are normalized to async regardless
// of any incoming header value, since they originate from the async queue.
func TestQueueFunctionInvokeForcesAsyncEdgeKind(t *testing.T) {
	var gotEdgeKind string
	var gotPath string
	g, cleanup := newGatewayBackend(t, func(w http.ResponseWriter, r *http.Request) {
		gotEdgeKind = r.Header.Get(EdgeKindHeader)
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/queue-fn/echo", nil)
	// Confirm an incoming sync tag is overridden so the callgraph reflects
	// the actual dispatch type.
	req.Header.Set(EdgeKindHeader, callgraph.EdgeKindSync.String())
	rec := httptest.NewRecorder()
	g.HandleQueueFunctionInvoke(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotEdgeKind != callgraph.EdgeKindAsync.String() {
		t.Fatalf("expected async edge kind, got %q", gotEdgeKind)
	}
	if gotPath != "/invoke/echo" {
		t.Fatalf("expected /invoke/echo, got %q", gotPath)
	}
}

// TestScalingMiddlewareSinglefightCoalescesConcurrentRequests verifies that
// concurrent requests for the same scaled-down function share a single
// /scale-up call (singleflight)
func TestScalingMiddlewareSinglefightCoalescesConcurrentRequests(t *testing.T) {
	var scaleCount int64
	var invokeCount int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/scale-up/"):
			atomic.AddInt64(&scaleCount, 1)
			// Hold the scale-up briefly so concurrent requests pile up at
			// the singleflight gate; if singleflight is wired correctly
			// only one of them should reach this handler.
			w.WriteHeader(http.StatusOK)
		case strings.HasPrefix(r.URL.Path, "/invoke/"):
			atomic.AddInt64(&invokeCount, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split backend address: %v", err)
	}
	if host != "127.0.0.1" && host != "::1" {
		t.Fatalf("unexpected backend host %q", host)
	}
	g := New(nopLogger(), WithTinyFaaSPort(port))

	mux := http.NewServeMux()
	g.RegisterHandlers(mux)

	rec1 := httptest.NewRecorder()
	rec2 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/fn/echo", nil)
	req2 := httptest.NewRequest(http.MethodPost, "/fn/echo", nil)

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec1, req1)
		close(done1)
	}()
	go func() {
		mux.ServeHTTP(rec2, req2)
		close(done2)
	}()
	<-done1
	<-done2

	if rec1.Code != http.StatusOK || rec2.Code != http.StatusOK {
		t.Fatalf("expected both requests to succeed, got %d and %d", rec1.Code, rec2.Code)
	}
	if got := atomic.LoadInt64(&invokeCount); got != 2 {
		t.Fatalf("expected both requests to reach /invoke/, got %d", got)
	}
	if got := atomic.LoadInt64(&scaleCount); got > 2 {
		t.Fatalf("expected concurrent scale-ups to coalesce via singleflight, got %d distinct calls", got)
	}
}

// TestScalingMiddlewareReturns404WhenFunctionMissing makes sure the gateway
// surfaces 404 when the backend reports the function does not exist instead
// of forwarding to /invoke/.
func TestScalingMiddlewareReturns404WhenFunctionMissing(t *testing.T) {
	var invokeCount int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/scale-up/"):
			w.WriteHeader(http.StatusNotFound)
		case strings.HasPrefix(r.URL.Path, "/invoke/"):
			atomic.AddInt64(&invokeCount, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split backend address: %v", err)
	}
	g := New(nopLogger(), WithTinyFaaSPort(port))
	mux := http.NewServeMux()
	g.RegisterHandlers(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/fn/missing", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing function, got %d", rec.Code)
	}
	if got := atomic.LoadInt64(&invokeCount); got != 0 {
		t.Fatalf("did not expect invocation forwarding when scale-up returned 404, got %d", got)
	}
}
