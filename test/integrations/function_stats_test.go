package integrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	testutil "github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type FunctionStatsResponse struct {
	Function struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"function"`
	Summary struct {
		SuccessfulInvocations int            `json:"successful_invocations"`
		FailedInvocations     int            `json:"failed_invocations"`
		StatusCodes           map[string]int `json:"status_codes"`
	} `json:"summary"`
	Invocations []struct {
		StartedAt  time.Time `json:"started_at"`
		FinishedAt time.Time `json:"finished_at"`
		DurationNS int64     `json:"duration_ns"`
		Method     string    `json:"method"`
		Path       string    `json:"path"`
		StatusCode int       `json:"status_code"`
		Success    bool      `json:"success"`
	} `json:"invocations"`
}

func TestFunctionStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	baseURL := testutil.RequireTinyFaaS(t)
	stackPath := testutil.RepoRoot(t) + "/tinyFaaS/test/fns/stack.yaml"
	fnName := "echo-js"

	testutil.RemoveTinyFaaSStackFilter(t, baseURL, stackPath, fnName)
	waitForFunctionStatsStatusEqual(t, baseURL, fnName, http.StatusNotFound, 30*time.Second)
	t.Cleanup(func() {
		testutil.RemoveTinyFaaSStackFilter(t, baseURL, stackPath, fnName)
	})
	testutil.DeployTinyFaaSStackFilter(t, baseURL, stackPath, fnName)
	testutil.WaitForFunctionsPresent(t, []string{fnName}, 60*time.Second)

	requireFunctionStatsEndpointAvailable(t, baseURL, fnName)
	// There might be some existing stats from previous tests, so just record the baseline count and verify it increases after our test invocations
	baseline := getFunctionStats(t, baseURL, fnName)

	// send 3 sync invocations
	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf("sync-%d", i))
		status, body := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodPost, payload, nil)
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, payload, body)
	}

	stats := getFunctionStats(t, baseURL, fnName)
	assert.Equal(t, fnName, stats.Function.Name)
	assert.Equal(t, "tinyfaas", stats.Function.Namespace)
	assert.Equal(t, stats.Summary.SuccessfulInvocations, baseline.Summary.SuccessfulInvocations+3)
	assert.Equal(t, baseline.Summary.FailedInvocations, stats.Summary.FailedInvocations)
	assert.Equal(t, stats.Summary.StatusCodes["200"], baseline.Summary.StatusCodes["200"]+3)
	assert.Equal(t, len(stats.Invocations), len(baseline.Invocations)+3)

	// send 2 additional async invocations
	asyncHeaders := map[string]string{"X-Tinyfaas-Async": "true"}
	for i := 0; i < 2; i++ {
		payload := []byte(fmt.Sprintf("async-%d", i))
		status, _ := testutil.InvokeTinyFaaS(t, baseURL, fnName, http.MethodPost, payload, asyncHeaders)
		require.Equal(t, http.StatusAccepted, status)
	}

	stats = waitForFunctionStatsCountGreaterEqual(t, baseURL, fnName, len(baseline.Invocations)+2, 30*time.Second)
	assert.Equal(t, fnName, stats.Function.Name)
	assert.Equal(t, "tinyfaas", stats.Function.Namespace)
	assert.Equal(t, stats.Summary.SuccessfulInvocations, baseline.Summary.SuccessfulInvocations+5)
	assert.Equal(t, baseline.Summary.FailedInvocations, stats.Summary.FailedInvocations)
	assert.Equal(t, stats.Summary.StatusCodes["202"], baseline.Summary.StatusCodes["202"]+2)
	assert.Equal(t, len(stats.Invocations), len(baseline.Invocations)+5)

	latest := stats.Invocations[len(stats.Invocations)-5:]
	latest200 := 0
	latest202 := 0
	for _, invocation := range latest {
		assert.Equal(t, http.MethodPost, invocation.Method)
		assert.Equal(t, "/fn/"+fnName, invocation.Path)
		assert.Contains(t, []int{http.StatusOK, http.StatusAccepted}, invocation.StatusCode)
		assert.True(t, invocation.Success)
		assert.False(t, invocation.FinishedAt.Before(invocation.StartedAt))
		assert.Greater(t, invocation.DurationNS, int64(0))
		if invocation.StatusCode == http.StatusOK {
			latest200++
		}
		if invocation.StatusCode == http.StatusAccepted {
			latest202++
		}
	}
	assert.Equal(t, 3, latest200)
	assert.Equal(t, 2, latest202)

	testutil.RemoveTinyFaaSStackFilter(t, baseURL, stackPath, fnName)
	waitForFunctionStatsStatusEqual(t, baseURL, fnName, http.StatusNotFound, 30*time.Second)

	testutil.DeployTinyFaaSStackFilter(t, baseURL, stackPath, fnName)
	testutil.WaitForFunctionsPresent(t, []string{fnName}, 60*time.Second)
	stats = waitForFunctionStatsEmpty(t, baseURL, fnName, 30*time.Second)
	assert.Equal(t, fnName, stats.Function.Name)
	assert.Equal(t, "tinyfaas", stats.Function.Namespace)
	assert.Equal(t, 0, stats.Summary.SuccessfulInvocations)
	assert.Equal(t, 0, stats.Summary.FailedInvocations)
	assert.Empty(t, stats.Summary.StatusCodes)
	assert.Empty(t, stats.Invocations)
}

func getFunctionStats(t *testing.T, baseURL string, functionName string) FunctionStatsResponse {
	t.Helper()

	status, body := getFunctionStatsRaw(t, baseURL, functionName)
	require.Equal(t, http.StatusOK, status, "stats body=%s", string(body))

	var out FunctionStatsResponse
	require.NoError(t, json.Unmarshal(body, &out), "invalid stats json: %s", string(body))
	return out
}

func getFunctionStatsRaw(t *testing.T, baseURL string, functionName string) (int, []byte) {
	t.Helper()

	url := fmt.Sprintf("%s/system/stats/function/%s", baseURL, functionName)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

func waitForFunctionStatsCountGreaterEqual(t *testing.T, baseURL string, functionName string, count int, timeout time.Duration) FunctionStatsResponse {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var got FunctionStatsResponse
	var lastStatus int
	var lastBody []byte

	for time.Now().Before(deadline) {
		status, body := getFunctionStatsRaw(t, baseURL, functionName)
		lastStatus = status
		lastBody = body

		if status == http.StatusNotFound && bytes.Contains(bytes.ToLower(body), []byte("page not found")) {
			t.Fatalf("tinyFaaS stats endpoint unavailable for %q (status=%d body=%s). Rebuild tinyFaaS services to include /system/stats/function support.", functionName, status, strings.TrimSpace(string(body)))
		}
		if status != http.StatusOK {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := json.Unmarshal(body, &got); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(got.Invocations) >= count {
			if got.Summary.StatusCodes == nil {
				got.Summary.StatusCodes = map[string]int{}
			}
			return got
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("stats count did not reach %d within %s (last status=%d body=%s)", count, timeout, lastStatus, string(lastBody))

	if got.Summary.StatusCodes == nil {
		got.Summary.StatusCodes = map[string]int{}
	}
	return got
}

func waitForFunctionStatsStatusEqual(t *testing.T, baseURL string, functionName string, expectedStatus int, timeout time.Duration) {
	t.Helper()

	testutil.Eventually(t, timeout, 500*time.Millisecond, func() bool {
		status, _ := getFunctionStatsRaw(t, baseURL, functionName)
		return status == expectedStatus
	})
}

func waitForFunctionStatsEmpty(t *testing.T, baseURL string, functionName string, timeout time.Duration) FunctionStatsResponse {
	t.Helper()

	var got FunctionStatsResponse
	testutil.Eventually(t, timeout, 500*time.Millisecond, func() bool {
		status, body := getFunctionStatsRaw(t, baseURL, functionName)
		if status != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(body, &got); err != nil {
			return false
		}
		return len(got.Invocations) == 0 && got.Summary.SuccessfulInvocations == 0 && got.Summary.FailedInvocations == 0
	})
	if got.Summary.StatusCodes == nil {
		got.Summary.StatusCodes = map[string]int{}
	}
	return got
}

func requireFunctionStatsEndpointAvailable(t *testing.T, baseURL string, functionName string) {
	t.Helper()

	status, body := getFunctionStatsRaw(t, baseURL, functionName)
	if status == http.StatusNotFound && bytes.Contains(bytes.ToLower(body), []byte("page not found")) {
		t.Fatalf("tinyFaaS stats endpoint unavailable for %q (status=%d body=%s). Rebuild tinyFaaS services to include /system/stats/function support.", functionName, status, strings.TrimSpace(string(body)))
	}
}
