package integrations

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/OpenFogStack/tinyFaaS/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExampleFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	baseURL := testutil.RequireTinyFaaS(t)
	fixturesDir := testutil.FunctionsDir(t)

	resourceLimits := &manager.FunctionResources{
		Memory: "256Mi",
		CPU:    "500m",
	}

	t.Run("sieve", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("sieve")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "sieve-of-eratosthenes"), fnName, "nodejs", 1, resourceLimits)

		status, _ := testutil.Invoke(t, baseURL, fnName, http.MethodGet, nil, nil)
		assert.Equal(t, http.StatusOK, status)
	})

	t.Run("sieve-async", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("sieve-async")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "sieve-of-eratosthenes"), fnName, "nodejs", 1, resourceLimits)

		headers := map[string]string{"X-Tinyfaas-Async": "true"}
		status, _ := testutil.Invoke(t, baseURL, fnName, http.MethodGet, nil, headers)
		assert.Equal(t, http.StatusAccepted, status)
	})

	t.Run("echo-py", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("echo-py")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "echo-py"), fnName, "python3", 1, resourceLimits)

		payload := []byte("Hello World!")
		status, body := testutil.Invoke(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("echo-js", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("echo-js")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "echo-js"), fnName, "nodejs", 1, resourceLimits)

		payload := []byte("Hello World!")
		status, body := testutil.Invoke(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("echo-go", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("echo-go")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "echo-go"), fnName, "go", 1, resourceLimits)

		payload := []byte("Hello World!")
		status, body := testutil.Invoke(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("echo-binary", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("echo-binary")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "echo-binary"), fnName, "binary", 1, resourceLimits)

		payload := []byte("Hello World!")
		status, body := testutil.Invoke(t, baseURL, fnName, http.MethodPost, payload, nil)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	})

	t.Run("show-headers-js", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("headers-js")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "show-headers-js"), fnName, "nodejs", 1, resourceLimits)

		headers := map[string]string{"lab": "scalable_software_systems_group"}
		status, body := testutil.Invoke(t, baseURL, fnName, http.MethodGet, nil, headers)
		require.Equal(t, http.StatusOK, status)

		var got map[string]string
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "scalable_software_systems_group", got["lab"])
		assert.Contains(t, got["user-agent"], "Go-http-client")
	})

	t.Run("show-headers-py", func(t *testing.T) {
		fnName := testutil.UniqueFunctionName("headers-py")
		testutil.CleanupDeleteFunction(t, baseURL, fnName)
		testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "show-headers-py"), fnName, "python3", 1, resourceLimits)

		headers := map[string]string{"Lab": "scalable_software_systems_group"}
		status, body := testutil.Invoke(t, baseURL, fnName, http.MethodGet, nil, headers)
		require.Equal(t, http.StatusOK, status)

		var got map[string]string
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "scalable_software_systems_group", got["lab"])
		assert.Contains(t, got["user-agent"], "Go-http-client")
	})
}
