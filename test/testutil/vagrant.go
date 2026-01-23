package testutil

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func RequireVagrant(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("vagrant"); err != nil {
		t.Skip("vagrant not found in PATH")
	}
}

func VMState(t *testing.T, vmName string) string {
	t.Helper()

	cmd := exec.Command("vagrant", "status", vmName, "--machine-readable")
	cmd.Dir = RepoRoot(t)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	require.NoError(t, err, "vagrant status failed: %s", out.String())

	lines := strings.Split(out.String(), "\n")
	for _, line := range lines {
		parts := strings.Split(line, ",")
		if len(parts) < 4 {
			continue
		}
		if parts[1] != vmName {
			continue
		}
		if parts[2] != "state" {
			continue
		}
		return parts[3]
	}

	return ""
}

func RequireVMRunning(t *testing.T, vmName string) {
	t.Helper()

	state := VMState(t, vmName)
	if state != "running" {
		t.Fatalf("vagrant VM %q is not running (state=%q). Run: vagrant up %s", vmName, state, vmName)
	}
}

func VagrantSSH(t *testing.T, vmName string, command string) string {
	t.Helper()

	cmd := exec.Command("vagrant", "ssh", vmName, "-c", command)
	cmd.Dir = RepoRoot(t)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "vagrant ssh failed: %s", string(out))
	return string(out)
}

func RequireTinyFaaSVM(t *testing.T) {
	t.Helper()
	RequireVagrant(t)
	RequireVMRunning(t, "tinyfaas")
}

func DebugTinyFaaSServices(t *testing.T) {
	t.Helper()

	if v := strings.TrimSpace(strings.ToLower(os.Getenv("TINYFAAS_TEST_VAGRANT_DEBUG"))); v != "1" {
		return
	}

	_ = VagrantSSH(t, "tinyfaas", "systemctl is-active tf-gateway tf-manager tf-rproxy || true")
}
