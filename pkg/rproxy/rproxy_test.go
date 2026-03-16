package rproxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestScaleUpFunctionReturnsError(t *testing.T) {
	t.Run("connection refused", func(t *testing.T) {
		r := New(zap.NewNop(), "development")
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

		r := New(zap.NewNop(), "development")
		r.SetGatewayAddr(strings.TrimPrefix(server.URL, "http://"))

		err := r.scaleUpFunction("test-func", true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scale-up returned status")
	})
}

func TestCallReturns503WhenColdStartTriggerFails(t *testing.T) {
	r := New(zap.NewNop(), "development")
	r.SetAutoScalerEnabled(true)
	r.SetGatewayAddr("127.0.0.1:1")

	r.routingTableMux.Lock()
	r.routingTable["test-func"] = &Route{
		ips:      []string{"10.255.255.1"},
		isActive: false,
	}
	r.routingTableMux.Unlock()

	status, body := r.Call("test-func", []byte("{}"), false, http.Header{})
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Nil(t, body)
}

func TestUpdateDeactivatesRouteIdempotently(t *testing.T) {
	r := New(zap.NewNop(), "development")

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

	r := New(zap.NewNop(), "development")
	r.SetGatewayAddr(strings.TrimPrefix(server.URL, "http://"))

	err := r.sendBatchHeartbeat([]string{"test-func"})
	require.NoError(t, err)
	assert.True(t, called, fmt.Sprintf("expected heartbeat endpoint to be called via %s", r.gatewayAddr))
}
