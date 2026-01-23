package testutil

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func GatewayBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("TINYFAAS_TEST_GATEWAY_URL")); v != "" {
		return v
	}
	return "http://127.0.0.1:8888"
}

func WaitForGateway(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("%s/health", strings.TrimRight(baseURL, "/")))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("tinyFaaS gateway not ready at %s within %v", baseURL, timeout)
}

func RequireTinyFaaS(t *testing.T) string {
	t.Helper()

	RequireTinyFaaSVM(t)
	DebugTinyFaaSServices(t)

	baseURL := GatewayBaseURL()
	WaitForGateway(t, baseURL, 60*time.Second)
	return baseURL
}
