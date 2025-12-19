package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractIP(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "IPv4 with port",
			input:    "192.168.1.1:8080",
			expected: "192.168.1.1",
		},
		{
			name:     "IPv6 with port",
			input:    "[::1]:8080",
			expected: "::1",
		},
		{
			name:     "IPv6 without brackets malformed",
			input:    "::1:8080",
			expected: "::1:8080", // Should return as is since no brackets
		},
		{
			name:     "No port",
			input:    "192.168.1.1",
			expected: "192.168.1.1",
		},
		{
			name:     "IPv6 no port",
			input:    "::1",
			expected: "::1",
		},
		{
			name:     "Malformed IPv6",
			input:    "[::1",
			expected: "[::1", // No closing bracket, returns as is
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractIP(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPrivateIP(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
		hasError bool
	}{
		{
			name:     "Private IPv4 in 192.168.0.0/16",
			input:    "192.168.1.1",
			expected: true,
			hasError: false,
		},
		{
			name:     "Private IPv4 in 172.17.0.0/16",
			input:    "172.17.0.1",
			expected: true,
			hasError: false,
		},
		{
			name:     "Private IPv4 in 172.20.0.0/14",
			input:    "172.20.0.1",
			expected: true,
			hasError: false,
		},
		{
			name:     "External IPv4",
			input:    "8.8.8.8",
			expected: false,
			hasError: false,
		},
		{
			name:     "Loopback IPv4",
			input:    "127.0.0.1",
			expected: false, // Not in docker pools
			hasError: false,
		},
		{
			name:     "Invalid IP",
			input:    "invalid.ip",
			expected: false,
			hasError: true,
		},
		{
			name:     "IPv6 loopback",
			input:    "::1",
			expected: false, // Not in docker pools
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := IsPrivateIP(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestIsExternalIP(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
		hasError bool
	}{
		{
			name:     "Private IPv4",
			input:    "192.168.1.1",
			expected: false,
			hasError: false,
		},
		{
			name:     "External IPv4",
			input:    "8.8.8.8",
			expected: true,
			hasError: false,
		},
		{
			name:     "Invalid IP",
			input:    "not.an.ip",
			expected: false,
			hasError: true,
		},
		{
			name:     "Loopback",
			input:    "127.0.0.1",
			expected: true, // Since not private per docker pools
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := IsExternalIP(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
