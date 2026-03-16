package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetManagerPort(t *testing.T) {
	t.Run("uses TF_MANAGER_PORT when set", func(t *testing.T) {
		t.Setenv("TF_MANAGER_PORT", "18080")
		assert.Equal(t, "18080", getManagerPort())
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TF_MANAGER_PORT", "")
		assert.Equal(t, "8080", getManagerPort())
	})
}

func TestGetRProxyPort(t *testing.T) {
	t.Run("uses TF_RPROXY_PORT when set", func(t *testing.T) {
		t.Setenv("TF_RPROXY_PORT", "18000")
		assert.Equal(t, "18000", getRProxyPort())
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TF_RPROXY_PORT", "")
		assert.Equal(t, "8000", getRProxyPort())
	})
}
