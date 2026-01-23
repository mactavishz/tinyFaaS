package manager_test

import (
	"testing"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCPUNano(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		want        int64
		wantErr     bool
		errContains string
	}{
		{name: "cores-1", in: "1", want: 1_000_000_000},
		{name: "cores-decimal", in: "0.0625", want: manager.DefaultNanoCPUs},
		{name: "cores-trim", in: " 2 ", want: 2_000_000_000},
		{name: "millicores-50m", in: "50m", want: 50_000_000},
		{name: "millicores-1000m", in: "1000m", want: 1_000_000_000},
		{name: "millicores-fraction", in: "0.5m", want: 500_000},
		{name: "truncate-toward-zero", in: "0.0000000001", want: 0},
		{name: "empty", in: "", wantErr: true, errContains: "cpu is empty"},
		{name: "whitespace-only", in: "   ", wantErr: true, errContains: "cpu is empty"},
		{name: "invalid", in: "abc", wantErr: true, errContains: "invalid cpu value"},
		{name: "malformed-suffix", in: "1mm", wantErr: true, errContains: "invalid cpu value"},
		{name: "missing-number", in: "m", wantErr: true, errContains: "invalid cpu value"},
		{name: "negative", in: "-1", wantErr: true, errContains: "must be non-negative"},
		{name: "negative-millicores", in: "-10m", wantErr: true, errContains: "must be non-negative"},
		{name: "overflow", in: "1000000000000000000000000", wantErr: true, errContains: "overflows int64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := manager.ParseCPUNano(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseMemoryBytes(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		want        int64
		wantErr     bool
		errContains string
	}{
		{name: "bytes-default", in: "1", want: 1},
		{name: "bytes-with-unit", in: "42B", want: 42},
		{name: "bytes-with-bytes", in: "42bytes", want: 42},
		{name: "binary-mi", in: "96Mi", want: 96 * 1024 * 1024},
		{name: "binary-gi-fraction", in: "1.5Gi", want: 1610612736},
		{name: "decimal-k", in: "1k", want: 1000},
		{name: "decimal-mb", in: "1MB", want: 1_000_000},
		{name: "case-insensitive", in: "96MIB", want: 96 * 1024 * 1024},
		{name: "fraction-truncates", in: "1.1Ki", want: 1126},
		{name: "empty", in: "", wantErr: true, errContains: "memory is empty"},
		{name: "whitespace-only", in: "   ", wantErr: true, errContains: "memory is empty"},
		{name: "missing-number", in: "Mi", wantErr: true, errContains: "missing numeric value"},
		{name: "invalid-unit", in: "12foo", wantErr: true, errContains: "invalid memory unit"},
		{name: "negative", in: "-1Mi", wantErr: true},
		{name: "overflow", in: "999999999999999999999Gi", wantErr: true, errContains: "overflows int64"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := manager.ParseMemoryBytes(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEffectiveResourceLimits(t *testing.T) {
	t.Run("defaults-when-nil", func(t *testing.T) {
		resolved, effective, backend, err := manager.EffectiveResourceLimits(nil)
		require.NoError(t, err)
		assert.Equal(t, manager.DefaultCPUString, resolved.CPU)
		assert.Equal(t, manager.DefaultMemoryString, resolved.Memory)
		assert.Equal(t, manager.DefaultNanoCPUs, effective.NanoCPUs)
		assert.Equal(t, manager.DefaultMemoryBytes, effective.MemoryBytes)
		assert.Equal(t, effective.NanoCPUs, backend.NanoCPUs)
		assert.Equal(t, effective.MemoryBytes, backend.MemoryBytes)
	})

	t.Run("defaults-when-empty", func(t *testing.T) {
		limits := &manager.FunctionResources{CPU: "", Memory: "   "}
		resolved, effective, _, err := manager.EffectiveResourceLimits(limits)
		require.NoError(t, err)
		assert.Equal(t, manager.DefaultCPUString, resolved.CPU)
		assert.Equal(t, manager.DefaultMemoryString, resolved.Memory)
		assert.Equal(t, manager.DefaultNanoCPUs, effective.NanoCPUs)
		assert.Equal(t, manager.DefaultMemoryBytes, effective.MemoryBytes)
	})

	t.Run("trims-input", func(t *testing.T) {
		limits := &manager.FunctionResources{CPU: " 50m ", Memory: " 96Mi "}
		resolved, effective, backend, err := manager.EffectiveResourceLimits(limits)
		require.NoError(t, err)
		assert.Equal(t, "50m", resolved.CPU)
		assert.Equal(t, "96Mi", resolved.Memory)
		assert.Equal(t, int64(50_000_000), effective.NanoCPUs)
		assert.Equal(t, int64(96*1024*1024), effective.MemoryBytes)
		assert.Equal(t, effective.NanoCPUs, backend.NanoCPUs)
		assert.Equal(t, effective.MemoryBytes, backend.MemoryBytes)
	})

	t.Run("invalid-cpu", func(t *testing.T) {
		limits := &manager.FunctionResources{CPU: "abc", Memory: "96Mi"}
		_, _, _, err := manager.EffectiveResourceLimits(limits)
		require.Error(t, err)
	})

	t.Run("invalid-memory", func(t *testing.T) {
		limits := &manager.FunctionResources{CPU: "50m", Memory: "bogus"}
		_, _, _, err := manager.EffectiveResourceLimits(limits)
		require.Error(t, err)
	})
}
