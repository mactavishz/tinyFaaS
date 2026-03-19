package docker

import (
	"errors"
	"io"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStopClearsTrackedContainersAfterScaleDown(t *testing.T) {
	removed := make([]string, 0)
	dh := &dockerHandler{
		name:       "test-func",
		containers: []string{"cid-a", "cid-b"},
		handlerIPs: []string{"10.0.0.2", "10.0.0.3"},
		isRunning:  true,
		logger:     zap.NewNop(),
		containerRemover: func(cid string) error {
			removed = append(removed, cid)
			return nil
		},
	}

	err := dh.Stop()
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"cid-a", "cid-b"}, removed)
	assert.Empty(t, dh.containers)
	assert.Nil(t, dh.handlerIPs)
	assert.False(t, dh.isRunning)
}

func TestDestroyAfterScaleDownIgnoresMissingResources(t *testing.T) {
	dh := &dockerHandler{
		name:        "test-func",
		network:     "network-id",
		networkName: "network-name",
		uniqueName:  "image-name",
		containers:  []string{"cid-a", "cid-b"},
		logger:      zap.NewNop(),
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

func TestCleanupTrackedContainersKeepsOnlyFailed(t *testing.T) {
	dh := &dockerHandler{
		containers: []string{"cid-a", "cid-b", "cid-c"},
		isRunning:  true,
		logger:     zap.NewNop(),
	}

	err := dh.cleanupContainers(func(cid string) error {
		if cid == "cid-b" {
			return errors.New("remove failed")
		}
		return nil
	})
	require.Error(t, err)

	assert.Equal(t, []string{"cid-b"}, dh.containers)
	assert.Nil(t, dh.handlerIPs)
	assert.False(t, dh.isRunning)
}

func TestLogsReturnsEmptyReaderWhenNoContainers(t *testing.T) {
	dh := &dockerHandler{
		containers: nil,
		logger:     zap.NewNop(),
	}

	r, err := dh.Logs()
	require.NoError(t, err)

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "", string(b))
}
