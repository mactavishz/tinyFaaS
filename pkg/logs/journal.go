package logs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

const journalIdentifierPrefix = "tinyfaas"

var (
	lookPath   = exec.LookPath
	newCommand = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, name, args...)
	}
)

func JournalIdentifier(functionName string) string {
	return fmt.Sprintf("%s:%s", journalIdentifierPrefix, functionName)
}

func ReadFunction(functionName string) (io.Reader, error) {
	functionName = strings.TrimSpace(functionName)
	if functionName == "" {
		return nil, fmt.Errorf("function name is required")
	}

	if _, err := lookPath("journalctl"); err != nil {
		return nil, fmt.Errorf("journalctl not available: %w", err)
	}

	args := buildJournalctlArgs(functionName)
	cmd := newCommand(context.Background(), "journalctl", args...)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr := strings.TrimSpace(string(exitErr.Stderr))
			if stderr != "" {
				return nil, fmt.Errorf("journalctl failed: %s", stderr)
			}
		}

		return nil, fmt.Errorf("journalctl failed: %w", err)
	}

	return bytes.NewReader(out), nil
}

func buildJournalctlArgs(functionName string) []string {
	return []string{
		"--utc",
		"--no-pager",
		"--output=cat",
		"--identifier=" + JournalIdentifier(functionName),
	}
}
