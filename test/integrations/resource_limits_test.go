package integrations

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/OpenFogStack/tinyFaaS/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceLimitsInList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	baseURL := testutil.RequireTinyFaaS(t)
	fixturesDir := testutil.FunctionsDir(t)

	fnName := testutil.UniqueFunctionName("echo-go-resources")
	testutil.CleanupDeleteFunction(t, baseURL, fnName)

	limits := &manager.FunctionResources{CPU: "50m", Memory: "96Mi"}
	testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "echo-go"), fnName, "go", 1, limits)

	fn := testutil.GetFunction(t, baseURL, fnName)
	assert.Equal(t, fnName, fn.Name)

	assert.Equal(t, "50m", fn.Limits.CPU)
	assert.Equal(t, "96Mi", fn.Limits.Memory)
	assert.Equal(t, "50m", fn.EffectiveLimits.CPU)
	assert.Equal(t, "96Mi", fn.EffectiveLimits.Memory)
	assert.Equal(t, int64(50_000_000), fn.EffectiveLimits.NanoCPUs)
	assert.Equal(t, int64(96*1024*1024), fn.EffectiveLimits.MemoryBytes)
	assert.Equal(t, 1, fn.Replicas)

	require.True(t, fn.Running)
}

func TestResourceLimitsInDocker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
	}

	baseURL := testutil.RequireTinyFaaS(t)
	fixturesDir := testutil.FunctionsDir(t)

	fnName := testutil.UniqueFunctionName("echo-go-docker-limits")
	testutil.CleanupDeleteFunction(t, baseURL, fnName)

	cpuLimit := "50m"
	memLimit := "96Mi"
	expectedNano, err := manager.ParseCPUNano(cpuLimit)
	require.NoError(t, err)
	expectedMemBytes, err := manager.ParseMemoryBytes(memLimit)
	require.NoError(t, err)

	limits := &manager.FunctionResources{CPU: cpuLimit, Memory: memLimit}
	testutil.UploadFixtureFunction(t, baseURL, filepath.Join(fixturesDir, "echo-go"), fnName, "go", 1, limits)

	status, body := testutil.Invoke(t, baseURL, fnName, http.MethodPost, []byte("Hello"), nil)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, []byte("Hello"), body)

	containerIDs := requireDockerContainersByFunctionName(t, fnName)
	require.Len(t, containerIDs, 1)

	for _, id := range containerIDs {
		nanoCpus := dockerInspectInt64(t, id, "{{.HostConfig.NanoCpus}}")
		memBytes := dockerInspectInt64(t, id, "{{.HostConfig.Memory}}")
		assert.Equal(t, expectedNano, nanoCpus)
		assert.Equal(t, expectedMemBytes, memBytes)

		used, limit := dockerStatsMemUsage(t, id)
		assert.Equal(t, expectedMemBytes, limit)
		assert.LessOrEqual(t, used, expectedMemBytes+1024*1024)
	}
}

func requireDockerContainersByFunctionName(t *testing.T, functionName string) []string {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		cmd := "sudo docker ps -q --filter \"label=tinyfaas-function=" + functionName + "\""
		out := testutil.VagrantSSH(t, "tinyfaas", cmd)
		ids := parseDockerIDs(out)
		if len(ids) > 0 {
			return ids
		}
		time.Sleep(300 * time.Millisecond)
	}

	cmd := "sudo docker ps --filter \"label=tinyfaas-function=" + functionName + "\""
	out := testutil.VagrantSSH(t, "tinyfaas", cmd)
	t.Fatalf("no running containers found for function %q. docker ps output:\n%s", functionName, out)
	return nil
}

func parseDockerIDs(out string) []string {
	seen := map[string]struct{}{}
	ids := make([]string, 0)
	for _, tok := range strings.Fields(out) {
		if !isHexToken(tok) {
			continue
		}
		if _, ok := seen[tok]; ok {
			continue
		}
		seen[tok] = struct{}{}
		ids = append(ids, tok)
	}
	return ids
}

func isHexToken(s string) bool {
	if len(s) < 12 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isLowerHex := c >= 'a' && c <= 'f'
		isUpperHex := c >= 'A' && c <= 'F'
		if !(isDigit || isLowerHex || isUpperHex) {
			return false
		}
	}
	return true
}

func dockerInspectInt64(t *testing.T, containerID string, format string) int64 {
	t.Helper()

	cmd := "sudo docker inspect -f '" + format + "' " + containerID
	out := testutil.VagrantSSH(t, "tinyfaas", cmd)
	v, ok := firstInt64(out)
	if !ok {
		t.Fatalf("failed to parse int64 from docker inspect output for %s: %q", containerID, out)
	}
	return v
}

func dockerStatsMemUsage(t *testing.T, containerID string) (usedBytes int64, limitBytes int64) {
	t.Helper()

	cmd := "sudo docker stats --no-stream --format '{{.MemUsage}}' " + containerID
	out := testutil.VagrantSSH(t, "tinyfaas", cmd)

	line := ""
	for _, l := range strings.Split(out, "\n") {
		l = strings.TrimSpace(l)
		if strings.Contains(l, "/") {
			line = l
			break
		}
	}
	if line == "" {
		t.Fatalf("failed to parse docker stats output for %s: %q", containerID, out)
	}

	left, right, ok := strings.Cut(line, "/")
	if !ok {
		t.Fatalf("failed to parse docker stats mem usage line for %s: %q", containerID, line)
	}

	usedStr := strings.TrimSpace(left)
	limitStr := strings.TrimSpace(right)
	if f := strings.Fields(usedStr); len(f) > 0 {
		usedStr = f[0]
	}
	if f := strings.Fields(limitStr); len(f) > 0 {
		limitStr = f[0]
	}

	used, err := manager.ParseMemoryBytes(usedStr)
	require.NoError(t, err, "failed to parse used memory from %q", usedStr)
	limit, err := manager.ParseMemoryBytes(limitStr)
	require.NoError(t, err, "failed to parse memory limit from %q", limitStr)
	return used, limit
}

func firstInt64(out string) (int64, bool) {
	for _, tok := range strings.Fields(out) {
		if len(tok) == 0 {
			continue
		}
		isInt := true
		for i := 0; i < len(tok); i++ {
			c := tok[i]
			if c < '0' || c > '9' {
				isInt = false
				break
			}
		}
		if !isInt {
			continue
		}
		v, err := strconv.ParseInt(tok, 10, 64)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}
