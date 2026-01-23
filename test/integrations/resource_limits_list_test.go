package integrations

import (
	"path/filepath"
	"testing"

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

	require.True(t, fn.Running)
}

func TestResourceLimitsInDocker(t *testing.T) {
	// TODO: verify resource limits of the deopoyed function container by using Docker CLI
}
