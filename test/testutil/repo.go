package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	repoRootOnce sync.Once
	repoRoot     string
	repoRootErr  error
)

func RepoRoot(t *testing.T) string {
	t.Helper()

	repoRootOnce.Do(func() {
		wd, err := os.Getwd()
		if err != nil {
			repoRootErr = err
			return
		}

		dir := wd
		for {
			vf := filepath.Join(dir, "Vagrantfile")
			if _, err := os.Stat(vf); err == nil {
				repoRoot = dir
				return
			}

			parent := filepath.Dir(dir)
			if parent == dir {
				repoRootErr = fmt.Errorf("failed to locate repo root (Vagrantfile) from %q", wd)
				return
			}
			dir = parent
		}
	})

	require.NoError(t, repoRootErr)
	return repoRoot
}

func TinyFaaSDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(RepoRoot(t), "tinyFaaS")
}

func FunctionsDir(t *testing.T) string {
	t.Helper()
	p := filepath.Join(RepoRoot(t), "tinyFaaS", "test", "fns")
	_, err := os.Stat(p)
	require.NoError(t, err, "Functions directory not found: %s", p)
	return p
}
