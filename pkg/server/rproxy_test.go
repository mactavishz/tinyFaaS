package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pathRecorder struct {
	mu     sync.Mutex
	counts map[string]int
}

func newPathRecorder() *pathRecorder {
	return &pathRecorder{counts: make(map[string]int)}
}

func (r *pathRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.record(req.URL.Path)
	w.WriteHeader(http.StatusOK)
}

func (r *pathRecorder) record(path string) {
	r.mu.Lock()
	r.counts[path]++
	r.mu.Unlock()
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

func newPrewarmTestTracker(t *testing.T, prewarmEnabled bool) *callgraph.CallGraphTracker {
	t.Helper()

	config := callgraph.DefaultConfig()
	config.Enabled = true
	config.Prewarm.Enabled = prewarmEnabled

	tracker := callgraph.New(callgraph.WithConfig(config), callgraph.WithLogger(nopLogger()))
	tracker.Start()
	t.Cleanup(tracker.Stop)
	return tracker
}

func seedPrewarmTarget(tracker *callgraph.CallGraphTracker, caller string, target string, leadTime time.Duration, targetColdStart time.Duration, callerColdStart time.Duration) {
	now := time.Now()
	requestID := fmt.Sprintf("learn-%s-%s", caller, target)
	executionID := fmt.Sprintf("exec-%s", caller)

	tracker.RecordScaleUp(target, now, targetColdStart, true)
	if callerColdStart > 0 {
		tracker.RecordScaleUp(caller, now, callerColdStart, true)
	}
	tracker.StartExecution(caller, requestID, executionID, now)
	tracker.RecordEdge(caller, target, requestID, executionID, now.Add(leadTime))
	tracker.EndExecution(caller, requestID, executionID, now.Add(leadTime+time.Millisecond))
}

func TestScaleUpFunctionReturnsError(t *testing.T) {
	t.Run("missing hook", func(t *testing.T) {
		r := NewInvocationRouter(nopLogger(), "development")

		err := r.scaleUpFunction("test-func", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scale-up hook not configured")
	})

	t.Run("hook error", func(t *testing.T) {
		r := NewInvocationRouter(nopLogger(), "development")
		r.SetScaleUpHook(func(name string, cold bool) error {
			return fmt.Errorf("scale-up failed")
		})

		err := r.scaleUpFunction("test-func", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scale-up failed")
	})
}

func TestSchedulePrewarmExecutesImmediatelyWhenDelayIsNonPositive(t *testing.T) {
	recorder := newPathRecorder()

	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()
	tracker.RecordScaleUp("test-func", time.Now(), 500*time.Millisecond, true)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetScaleUpHook(func(name string, cold bool) error {
		recorder.record("scale-up-hook")
		return nil
	})
	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	r.schedulePrewarm("caller", callgraph.PrewarmTarget{
		FunctionName: "test-func",
		LeadTime:     100 * time.Millisecond,
	})

	require.Eventually(t, func() bool {
		return recorder.count("scale-up-hook") == 1
	}, time.Second, 10*time.Millisecond)
}

func TestSchedulePrewarmExecutesWhenDelayIsPositive(t *testing.T) {
	recorder := newPathRecorder()

	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()
	tracker.RecordScaleUp("test-func", time.Now(), 50*time.Millisecond, true)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetScaleUpHook(func(name string, cold bool) error {
		recorder.record("scale-up-hook")
		return nil
	})
	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	r.schedulePrewarm("caller", callgraph.PrewarmTarget{
		FunctionName: "test-func",
		LeadTime:     200 * time.Millisecond,
	})

	require.Eventually(t, func() bool {
		return recorder.count("scale-up-hook") == 1
	}, time.Second, 10*time.Millisecond)
}

func TestSchedulePrewarmWithLeadOffsetExecutesImmediately(t *testing.T) {
	recorder := newPathRecorder()

	tracker := newPrewarmTestTracker(t, true)
	tracker.RecordScaleUp("test-func", time.Now(), 100*time.Millisecond, true)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetScaleUpHook(func(name string, cold bool) error {
		recorder.record("scale-up-hook")
		return nil
	})
	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	r.schedulePrewarmWithLeadOffset("caller", callgraph.PrewarmTarget{
		FunctionName: "test-func",
		LeadTime:     300 * time.Millisecond,
	}, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		return recorder.count("scale-up-hook") == 1
	}, 100*time.Millisecond, 10*time.Millisecond)
}

func TestSchedulePrewarmDeduplicatesSameTarget(t *testing.T) {
	recorder := newPathRecorder()

	tracker := newPrewarmTestTracker(t, true)
	tracker.RecordScaleUp("test-func", time.Now(), 500*time.Millisecond, true)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetScaleUpHook(func(name string, cold bool) error {
		recorder.record("scale-up-hook")
		return nil
	})
	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	target := callgraph.PrewarmTarget{
		FunctionName: "test-func",
		LeadTime:     100 * time.Millisecond,
	}
	r.schedulePrewarmWithLeadOffset("caller", target, 0)
	r.schedulePrewarmWithLeadOffset("caller", target, 0)

	require.Eventually(t, func() bool {
		return recorder.count("scale-up-hook") == 1
	}, time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, recorder.count("scale-up-hook"))
}

func TestCallReturns503WhenColdStartTriggerFails(t *testing.T) {
	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetAutoScalerEnabled(true)
	r.SetScaleUpHook(func(name string, cold bool) error {
		return fmt.Errorf("scale-up failed")
	})
	r.SetRequestHooks(func(name string) error { return nil }, func(name string) {})

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
	assert.Equal(t, 1, tracker.EdgeCount())
	stats, ok := tracker.GetFunctionStats("test-func")
	require.True(t, ok)
	assert.Equal(t, 1, stats.TotalCalls)
}

func TestCallWaitsForRouteReadyAfterScaleUp(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive: false,
	}
	r.routingTableMux.Unlock()

	r.SetScaleUpHook(func(name string, cold bool) error {
		go func() {
			time.Sleep(100 * time.Millisecond)
			r.routingTableMux.Lock()
			r.routingTable["test-func"] = &Route{
				ips:      []string{"127.0.0.1"},
				isActive: true,
			}
			r.routingTableMux.Unlock()
		}()
		return nil
	})
	r.SetRequestHooks(func(name string) error { return nil }, func(name string) {})

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-route-sync"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)
}

func TestColdInvocationStartsExecutionAndPrewarmsBeforeCallerScaleUpCompletes(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	tracker := newPrewarmTestTracker(t, true)
	seedPrewarmTarget(tracker, "caller", "target", 400*time.Millisecond, 100*time.Millisecond, 500*time.Millisecond)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetAutoScalerEnabled(true)
	r.SetRequestHooks(func(name string) error { return nil }, func(name string) {})

	callerScaleEntered := make(chan struct{})
	releaseCallerScale := make(chan struct{})
	targetPrewarmed := make(chan struct{}, 1)

	r.SetScaleUpHook(func(name string, cold bool) error {
		switch name {
		case "caller":
			close(callerScaleEntered)
			<-releaseCallerScale
			r.routingTableMux.Lock()
			r.routingTable["caller"] = &Route{
				ips:              []string{"127.0.0.1"},
				isActive:         true,
				callgraphEnabled: true,
			}
			r.routingTableMux.Unlock()
		case "target":
			require.False(t, cold)
			targetPrewarmed <- struct{}{}
		default:
			t.Fatalf("unexpected scale-up for %s", name)
		}
		return nil
	})

	r.routingTableMux.Lock()
	r.routingTable["caller"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTable["target"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	header := http.Header{"X-Call-Id": []string{"req-cold-prewarm"}}
	done := make(chan struct{})
	var status int
	var body []byte
	go func() {
		status, body = r.Call("caller", []byte("{}"), false, header)
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-callerScaleEntered:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	select {
	case <-targetPrewarmed:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected target prewarm before caller scale-up was released")
	}

	executionID := header.Get("X-Exec-Id")
	require.NotEmpty(t, executionID)
	functionName, ok := tracker.GetExecutionContextFunction("req-cold-prewarm", executionID)
	require.True(t, ok)
	assert.Equal(t, "caller", functionName)

	close(releaseCallerScale)
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)
}

func TestActiveCallerUsesZeroLeadOffsetForPrewarm(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	tracker := newPrewarmTestTracker(t, true)
	seedPrewarmTarget(tracker, "caller", "target", 220*time.Millisecond, 100*time.Millisecond, 500*time.Millisecond)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetAutoScalerEnabled(true)
	r.SetRequestHooks(func(name string) error { return nil }, func(name string) {})

	targetPrewarmed := make(chan struct{}, 1)
	r.SetScaleUpHook(func(name string, cold bool) error {
		if name == "target" {
			targetPrewarmed <- struct{}{}
			return nil
		}
		t.Fatalf("unexpected scale-up for %s", name)
		return nil
	})

	r.routingTableMux.Lock()
	r.routingTable["caller"] = &Route{
		ips:              []string{"127.0.0.1"},
		isActive:         true,
		callgraphEnabled: true,
	}
	r.routingTable["target"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("caller", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-active-zero-offset"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)

	select {
	case <-targetPrewarmed:
		t.Fatal("expected active caller prewarm to use normal delayed scheduling")
	case <-time.After(40 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		select {
		case <-targetPrewarmed:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestColdInvocationDoesNotPrewarmWhenPrewarmDisabled(t *testing.T) {
	tracker := newPrewarmTestTracker(t, false)
	seedPrewarmTarget(tracker, "caller", "target", 400*time.Millisecond, 100*time.Millisecond, 500*time.Millisecond)

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetTracker(tracker)
	r.SetAutoScalerEnabled(true)
	r.SetRequestHooks(func(name string) error { return nil }, func(name string) {})

	callerScaleEntered := make(chan struct{})
	releaseCallerScale := make(chan struct{})
	targetPrewarmed := make(chan struct{}, 1)

	r.SetScaleUpHook(func(name string, cold bool) error {
		switch name {
		case "caller":
			close(callerScaleEntered)
			<-releaseCallerScale
		case "target":
			targetPrewarmed <- struct{}{}
		default:
			t.Fatalf("unexpected scale-up for %s", name)
		}
		return nil
	})

	r.routingTableMux.Lock()
	r.routingTable["caller"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTable["target"] = &Route{
		isActive:         false,
		callgraphEnabled: true,
	}
	r.routingTableMux.Unlock()

	done := make(chan struct{})
	go func() {
		_, _ = r.Call("caller", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-prewarm-disabled"}})
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-callerScaleEntered:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	select {
	case <-targetPrewarmed:
		t.Fatal("did not expect target prewarm when prewarm is disabled")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseCallerScale)
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestCallFinishesRequestAfterLocalRequestBuildFailure(t *testing.T) {
	recorder := newPathRecorder()

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)
	r.SetRequestHooks(func(name string) error {
		recorder.record("request-start-hook")
		return nil
	}, func(name string) {
		recorder.record("request-finish-hook")
	})

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:      []string{"bad host"},
		isActive: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{"X-Call-Id": []string{"req-build-fail"}})
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Nil(t, body)
	assert.Equal(t, 1, recorder.count("request-start-hook"))
	assert.Equal(t, 1, recorder.count("request-finish-hook"))
}

func TestCallLegacyAsyncFlagInvokesSynchronously(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	recorder := newPathRecorder()

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)
	r.SetRequestHooks(func(name string) error {
		recorder.record("request-start-hook")
		return nil
	}, func(name string) {
		recorder.record("request-finish-hook")
	})

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:      []string{"127.0.0.1"},
		isActive: true,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-async"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)
	assert.Equal(t, 1, recorder.count("request-start-hook"))
	assert.Equal(t, 1, recorder.count("request-finish-hook"))
}

func TestCallLegacyAsyncFlagWaitsForColdStart(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)

	recorder := newPathRecorder()
	scaleUpStarted := make(chan struct{})
	scaleUpReleased := make(chan struct{})
	r.SetScaleUpHook(func(name string, cold bool) error {
		recorder.record("scale-up-hook")
		close(scaleUpStarted)
		time.Sleep(200 * time.Millisecond)
		routingReady := func() {
			r.routingTableMux.Lock()
			defer r.routingTableMux.Unlock()
			r.routingTable["test-func"] = &Route{
				ips:      []string{"127.0.0.1"},
				isActive: true,
			}
		}
		routingReady()
		close(scaleUpReleased)
		return nil
	})
	r.SetRequestHooks(func(name string) error {
		recorder.record("request-start-hook")
		return nil
	}, func(name string) {
		recorder.record("request-finish-hook")
	})

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		isActive: false,
	}
	r.routingTableMux.Unlock()

	start := time.Now()
	status, body := r.Call("test-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-async-cold"}})
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []byte("ok"), body)
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case <-scaleUpStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-scaleUpReleased:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return recorder.count("request-start-hook") == 1 && recorder.count("request-finish-hook") == 1
	}, time.Second, 10*time.Millisecond)
}

func TestCallAsyncReturns404ForUnknownFunction(t *testing.T) {
	recorder := newPathRecorder()

	r := NewInvocationRouter(nopLogger(), "development")
	r.SetAutoScalerEnabled(true)
	r.SetScaleUpHook(func(name string, cold bool) error {
		recorder.record("scale-up-hook")
		return nil
	})
	r.SetRequestHooks(func(name string) error {
		recorder.record("request-start-hook")
		return nil
	}, func(name string) {
		recorder.record("request-finish-hook")
	})

	status, body := r.Call("missing-func", []byte("{}"), true, http.Header{"X-Call-Id": []string{"req-async-missing"}})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Nil(t, body)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, recorder.count("scale-up-hook"))
	assert.Equal(t, 0, recorder.count("request-start-hook"))
	assert.Equal(t, 0, recorder.count("request-finish-hook"))
}

func TestCallRecordsEdgeWhenRouteReady(t *testing.T) {
	stopRuntime := startFunctionRuntimeServer(t)
	defer stopRuntime()

	tracker := callgraph.New(callgraph.WithLogger(nopLogger()))
	tracker.Start()
	defer tracker.Stop()

	r := NewInvocationRouter(nopLogger(), "development")
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
	r := NewInvocationRouter(nopLogger(), "development")

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

func TestSendBatchHeartbeatUsesConfiguredHook(t *testing.T) {
	called := false
	r := NewInvocationRouter(nopLogger(), "development")
	r.SetHeartbeatHooks(nil, func(names []string) error {
		called = true
		assert.Equal(t, []string{"test-func"}, names)
		return nil
	})

	err := r.sendBatchHeartbeat([]string{"test-func"})
	require.NoError(t, err)
	assert.True(t, called, "expected heartbeat hook to be called")
}
