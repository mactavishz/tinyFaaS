package rproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type pathRecorder struct {
	mu     sync.Mutex
	counts map[string]int
}

func newPathRecorder() *pathRecorder {
	return &pathRecorder{counts: make(map[string]int)}
}

func (r *pathRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.counts[req.URL.Path]++
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (r *pathRecorder) count(path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[path]
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

	server := &http.Server{Handler: mux}
	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-done
	}
}

func TestScaleUpFunctionReturnsError(t *testing.T) {
	t.Run("connection refused", func(t *testing.T) {
		r := New(nopLogger(), "development")
		r.SetGatewayAddr("127.0.0.1:1")

		err := r.scaleUpFunction("test-func", true)
		require.Error(t, err)
	})

	t.Run("non-200 response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			assert.Equal(t, "/system/scale-up", req.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		r := New(nopLogger(), "development")
		r.SetGatewayAddr(strings.TrimPrefix(server.URL, "http://"))

		err := r.scaleUpFunction("test-func", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scale-up returned status")
	})
}

func TestCallReturns503WhenColdStartTriggerFails(t *testing.T) {
	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	r := New(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetAutoScalerEnabled(true)
	r.SetGatewayAddr("127.0.0.1:1")

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:              []string{"10.255.255.1"},
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-coldstart-fail"}})
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Nil(t, body)
	assert.Equal(t, 0, tracker.EdgeCount())
}

func TestCallWaitsForRouteReadyAfterScaleUp(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	r := New(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive: false,
	}
	r.routingTableMux.Unlock()

	scaleUpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/system/scale-up" && req.URL.Path != "/system/request-start" && req.URL.Path != "/system/request-finish" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		}
		if req.URL.Path != "/system/scale-up" {
			w.WriteHeader(http.StatusOK)
			return
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			r.routingTableMux.Lock()
			r.routingTable["test-func"] = &Route{
				ips:      []string{"127.0.0.1"},
				isActive: true,
			}
			r.routingTableMux.Unlock()
		}()

		w.WriteHeader(http.StatusOK)
	}))
	defer scaleUpServer.Close()

	r.SetGatewayAddr(strings.TrimPrefix(scaleUpServer.URL, "http://"))

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-route-sync"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)
}

func TestCallFinishesRequestAfterLocalRequestBuildFailure(t *testing.T) {
	recorder := newPathRecorder()
	server := httptest.NewServer(recorder)
	defer server.Close()

	r := New(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)
	r.SetGatewayAddr(strings.TrimPrefix(server.URL, "http://"))

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:      []string{"bad host"},
		isActive: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-build-fail"}})
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Nil(t, body)
	assert.Equal(t, 1, recorder.count("/system/request-start"))
	assert.Equal(t, 1, recorder.count("/system/request-finish"))
}

func TestCallAsyncFinishesRequestAfterBackgroundInvocation(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	recorder := newPathRecorder()
	server := httptest.NewServer(recorder)
	defer server.Close()

	r := New(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)
	r.SetGatewayAddr(strings.TrimPrefix(server.URL, "http://"))

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:      []string{"127.0.0.1"},
		isActive: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-async"}})
	assert.Equal(t, http.StatusAccepted, status)
	assert.Nil(t, body)
	assert.Equal(t, 1, recorder.count("/system/request-start"))
	require.Eventually(t, func() bool {
		return recorder.count("/system/request-finish") == 1
	}, time.Second, 10*time.Millisecond)
}

func TestCallRecordsEdgeWhenRouteReady(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	r := New(nopLogger(), "development")
	r.SetTracker(tracker)

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:              []string{"127.0.0.1"},
		isActive:         true,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-edge-record"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)

	edge, ok := tracker.GetEdgeStats("", "test-func")
	require.True(t, ok)
	assert.Equal(t, 1, edge.Count)
}

func TestUpdateDeactivatesRouteIdempotently(t *testing.T) {
	r := New(nopLogger(), "development")

	err := r.Add("test-func", []string{"10.0.0.2", "10.0.0.3"}, map[string]string{})
	require.NoError(t, err)

	require.Equal(t, "test-func", r.reverseRoutingTable["10.0.0.2"])
	require.Equal(t, "test-func", r.reverseRoutingTable["10.0.0.3"])

	err = r.Update("test-func")
	require.NoError(t, err)

	r.routingTableMux.RLock()
	route := r.routingTable["test-func"]
	r.routingTableMux.RUnlock()

	require.NotNil(t, route)
	assert.False(t, route.isActive)
	assert.Empty(t, route.ips)
	_, ok1 := r.reverseRoutingTable["10.0.0.2"]
	_, ok2 := r.reverseRoutingTable["10.0.0.3"]
	assert.False(t, ok1)
	assert.False(t, ok2)

	err = r.Update("test-func")
	require.NoError(t, err)

	r.routingTableMux.RLock()
	route = r.routingTable["test-func"]
	r.routingTableMux.RUnlock()

	require.NotNil(t, route)
	assert.False(t, route.isActive)
	assert.Empty(t, route.ips)
}

func TestSendBatchHeartbeatUsesConfiguredGatewayAddr(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		called = true
		assert.Equal(t, "/system/heartbeat", req.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	r := New(nopLogger(), "development")
	r.SetGatewayAddr(strings.TrimPrefix(server.URL, "http://"))

	err := r.sendBatchHeartbeat([]string{"test-func"})
	require.NoError(t, err)
	assert.True(t, called, fmt.Sprintf("expected heartbeat endpoint to be called via %s", r.gatewayAddr))
}
