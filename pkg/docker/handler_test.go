package docker

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestStopClearsTrackedContainersAfterScaleDown(t *testing.T) {
	removed := make([]string, 0)
	networkRemoved := false
	imageRemoved := false
	dh := &dockerHandler{
		name:        "test-func",
		containers:  []string{"cid-a", "cid-b"},
		handlerIPs:  []string{"10.0.0.2", "10.0.0.3"},
		isRunning:   true,
		network:     "network-id",
		networkName: "network-name",
		logger:      nopLogger(),
		containerRemover: func(cid string) error {
			removed = append(removed, cid)
			return nil
		},
		networkRemover: func() error {
			networkRemoved = true
			return nil
		},
		imageRemover: func() error {
			imageRemoved = true
			return nil
		},
	}

	err := dh.Stop()
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"cid-a", "cid-b"}, removed)
	assert.Empty(t, dh.containers)
	assert.Nil(t, dh.handlerIPs)
	assert.False(t, dh.isRunning)
	assert.Equal(t, "", dh.network)
	assert.Equal(t, "network-name", dh.networkName)
	assert.True(t, networkRemoved)
	assert.False(t, imageRemoved)
}

func TestDestroyAfterScaleDownIgnoresMissingResources(t *testing.T) {
	dh := &dockerHandler{
		name:        "test-func",
		network:     "network-id",
		networkName: "network-name",
		uniqueName:  "image-name",
		containers:  []string{"cid-a", "cid-b"},
		logger:      nopLogger(),
		containerRemover: func(string) error {
			return errdefs.ErrNotFound
		},
		networkRemover: func() error {
			return errdefs.ErrNotFound
		},
		imageRemover: func() error {
			return errdefs.ErrNotFound
		},
	}

	err := dh.Destroy()
	require.NoError(t, err)

	assert.Empty(t, dh.containers)
	assert.Equal(t, "", dh.network)
	assert.Equal(t, "", dh.networkName)
	assert.False(t, dh.isRunning)
}

func TestRemoveTrackedContainersKeepsOnlyFailed(t *testing.T) {
	dh := &dockerHandler{
		containers: []string{"cid-a", "cid-b", "cid-c"},
		isRunning:  true,
		logger:     nopLogger(),
	}

	dh.containerRemover = func(cid string) error {
		if cid == "cid-b" {
			return errors.New("remove failed")
		}
		return nil
	}
	err := dh.removeContainers()
	require.Error(t, err)

	assert.Equal(t, []string{"cid-b"}, dh.containers)
	assert.Nil(t, dh.handlerIPs)
	assert.False(t, dh.isRunning)
}

func TestEnsureNetworkLockedCreatesNetworkWhenMissing(t *testing.T) {
	dh := &dockerHandler{
		name:          "test-func",
		networkName:   "test-network",
		networkLabels: map[string]string{"tinyfaas-function": "test-func"},
		logger:        nopLogger(),
	}

	dh.networkCreator = func() (string, error) {
		return "network-id", nil
	}

	err := dh.createNetwork()
	require.NoError(t, err)
	assert.Equal(t, "network-id", dh.network)
	assert.Equal(t, "test-network", dh.networkName)
}

func TestLogsReturnsEmptyReaderWhenNoContainers(t *testing.T) {
	dh := &dockerHandler{
		containers: nil,
		logger:     nopLogger(),
	}

	r, err := dh.Logs()
	require.NoError(t, err)

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "", string(b))
}

func TestWaitForContainerIPRetriesUntilAvailable(t *testing.T) {
	attempts := 0
	dh := &dockerHandler{
		logger: nopLogger(),
		ipInspector: func(string) (string, error) {
			attempts++
			if attempts < 3 {
				return "", errors.New("network not ready")
			}
			return "10.0.0.2", nil
		},
	}

	ip, err := dh.waitForContainerIP("cid-test", 250*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2", ip)
	assert.GreaterOrEqual(t, attempts, 3)
}

func TestWaitForContainerIPTimesOut(t *testing.T) {
	dh := &dockerHandler{
		logger: nopLogger(),
		ipInspector: func(string) (string, error) {
			return "", errors.New("network not ready")
		},
	}

	_, err := dh.waitForContainerIP("cid-timeout", 80*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out waiting for container")
}

func TestWaitForContainerReadyRetriesUntilHealthy(t *testing.T) {
	attempts := 0
	dh := &dockerHandler{
		logger: nopLogger(),
		healthChecker: func(string) error {
			attempts++
			if attempts < 3 {
				return errors.New("connection refused")
			}
			return nil
		},
	}

	err := dh.waitForContainerReady("10.0.0.2", "cid-ready", 300*time.Millisecond)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, attempts, 3)
}

func TestWaitForContainerReadyTimesOut(t *testing.T) {
	dh := &dockerHandler{
		logger: nopLogger(),
		healthChecker: func(string) error {
			return errors.New("still starting")
		},
	}

	err := dh.waitForContainerReady("10.0.0.2", "cid-fail", 90*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed readiness checks")
}
