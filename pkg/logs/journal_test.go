package logs

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournalIdentifier(t *testing.T) {
	assert.Equal(t, "tinyfaas:echo", JournalIdentifier("echo"))
}

func TestBuildJournalctlArgs(t *testing.T) {
	assert.Equal(t,
		[]string{"--utc", "--no-pager", "--output=cat", "--identifier=tinyfaas:echo"},
		buildJournalctlArgs("echo"),
	)
}

func TestReadFunctionRequiresName(t *testing.T) {
	_, err := ReadFunction("  ")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "function name is required")
}

func TestReadFunctionReturnsOutput(t *testing.T) {
	origLookPath := lookPath
	origNewCommand := newCommand
	t.Cleanup(func() {
		lookPath = origLookPath
		newCommand = origNewCommand
	})

	lookPath = func(string) (string, error) {
		return "/usr/bin/journalctl", nil
	}

	newCommand = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.Command("sh", "-c", "printf 'line-1\\nline-2\\n'")
	}

	r, err := ReadFunction("echo")
	require.NoError(t, err)

	b, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "line-1\nline-2\n", string(b))
}

func TestReadFunctionReturnsStderrOnExitError(t *testing.T) {
	origLookPath := lookPath
	origNewCommand := newCommand
	t.Cleanup(func() {
		lookPath = origLookPath
		newCommand = origNewCommand
	})

	lookPath = func(string) (string, error) {
		return "/usr/bin/journalctl", nil
	}

	newCommand = func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.Command("sh", "-c", "printf 'boom' 1>&2; exit 2")
	}

	_, err := ReadFunction("echo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "journalctl failed: boom")
}

func TestReadFunctionHandlesMissingJournalctl(t *testing.T) {
	origLookPath := lookPath
	t.Cleanup(func() {
		lookPath = origLookPath
	})

	lookPath = func(string) (string, error) {
		return "", errors.New("missing")
	}

	_, err := ReadFunction("echo")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "journalctl not available")
}
