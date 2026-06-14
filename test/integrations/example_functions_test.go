package integrations

import (
	"encoding/json"
	"net/http"
	"testing"

	testutil "github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExampleFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	baseURL := testutil.RequireTinyFaaS(t)
	stackPath := testutil.RepoRoot(t) + "/tinyFaaS/test/fns/stack.yaml"

	deploy := func(t *testing.T, filter string) {
		t.Helper()
		testutil.RemoveTinyFaaSStackFilter(t, baseURL, stackPath, filter)
		t.Cleanup(func() {
			testutil.RemoveTinyFaaSStackFilter(t, baseURL, stackPath, filter)
		})
		testutil.DeployTinyFaaSStackFilter(t, baseURL, stackPath, filter)
	}

	t.Run("sieve", func(t *testing.T) {
		fnName := "sieve-of-eratosthenes"
		deploy(t, fnName)

		status, _ := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodGet, nil, nil)
		assert.Equal(t, http.StatusOK, status)
	})

	t.Run("sieve-async", func(t *testing.T) {
		fnName := "sieve-of-eratosthenes"
		deploy(t, fnName)

		status, _ := testutil.InvokeTinyFaaSAsync(t, baseURL, fnName, http.MethodGet, nil, nil)
		assert.Equal(t, http.StatusAccepted, status)
	})

	t.Run("echo-py", func(t *testing.T) {
		fnName := "echo-py"
		deploy(t, fnName)

		payload := []byte("Hello World!")
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("echo-js", func(t *testing.T) {
		fnName := "echo-js"
		deploy(t, fnName)

		payload := []byte("Hello World!")
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("echo-go", func(t *testing.T) {
		fnName := "echo-go"
		deploy(t, fnName)

		payload := []byte("Hello World!")
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("echo-binary", func(t *testing.T) {
		fnName := "echo-binary"
		deploy(t, fnName)

		payload := []byte("Hello World!")
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("show-headers-js", func(t *testing.T) {
		fnName := "show-headers-js"
		deploy(t, fnName)

		headers := map[string]string{"lab": "scalable_software_systems_group"}
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodGet, nil, headers)
		require.Equal(t, http.StatusOK, status)

		var got map[string]string
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "scalable_software_systems_group", got["lab"])
		assert.Contains(t, got["user-agent"], "Go-http-client")
	})

	t.Run("show-headers-py", func(t *testing.T) {
		fnName := "show-headers-py"
		deploy(t, fnName)

		headers := map[string]string{"Lab": "scalable_software_systems_group"}
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodGet, nil, headers)
		require.Equal(t, http.StatusOK, status)

		var got map[string]string
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "scalable_software_systems_group", got["lab"])
		assert.Contains(t, got["user-agent"], "Go-http-client")
	})
}
