package test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	host                  = "localhost"
	gatewayPort           = 80
	serviceStartupTimeout = 30 * time.Second
)

var (
	srcPath = ".."
	fnPath  = filepath.Join(srcPath, "test", "fns")
)

// waitForService waits for a service to become available
func waitForService(t *testing.T, host string, port int, timeout time.Duration) bool {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("http://%s:%d/", host, port))
		if err == nil {
			resp.Body.Close()
			return true
		}
		// HTTP error means service is up but returned an error (e.g., 404)
		if resp != nil {
			resp.Body.Close()
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// zipDirectory creates a zip archive of a directory and returns the bytes
func zipDirectory(t *testing.T, dirPath string) []byte {
	t.Helper()

	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path for zip entry
		relPath, err := filepath.Rel(dirPath, path)
		if err != nil {
			return err
		}

		// Create zip entry
		writer, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}

		// Read and write file content
		fileData, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		_, err = writer.Write(fileData)
		return err
	})

	require.NoError(t, err, "Failed to create zip archive")
	require.NoError(t, zipWriter.Close(), "Failed to close zip writer")

	return buf.Bytes()
}

// startFunction uploads and starts a function using API
func startFunction(t *testing.T, folderName, fnName, env string, threads int) string {
	t.Helper()

	// Get absolute path
	absPath, err := filepath.Abs(folderName)
	require.NoError(t, err, "Failed to get absolute path for %s", folderName)

	// Create zip archive
	zipData := zipDirectory(t, absPath)

	// Encode to base64
	base64Zip := base64.StdEncoding.EncodeToString(zipData)

	// Prepare upload payload
	payload := map[string]interface{}{
		"name":    fnName,
		"env":     env,
		"threads": threads,
		"zip":     base64Zip,
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err, "Failed to marshal payload")

	// Upload via gateway
	uploadURL := fmt.Sprintf("http://%s:%d/system/upload", host, gatewayPort)
	resp, err := http.Post(uploadURL, "application/json", bytes.NewReader(payloadBytes))
	require.NoError(t, err, "Failed to upload function %s", fnName)
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	t.Logf("Upload %s: status=%d, response=%s", fnName, resp.StatusCode, string(responseBody))

	require.Equal(t, http.StatusOK, resp.StatusCode, "Function upload failed: %s", string(responseBody))
	return fnName
}

// deleteFunction deletes a function using API
func deleteFunction(t *testing.T, fnName string) {
	t.Helper()

	// Prepare delete payload
	payload := map[string]string{
		"name": fnName,
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err, "Failed to marshal delete payload")

	// Delete via gateway
	deleteURL := fmt.Sprintf("http://%s:%d/system/delete", host, gatewayPort)
	resp, err := http.Post(deleteURL, "application/json", bytes.NewReader(payloadBytes))
	if err != nil {
		t.Logf("Failed to delete function %s: %v", fnName, err)
		return
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Logf("Delete %s failed: status=%d, response=%s", fnName, resp.StatusCode, string(responseBody))
	} else {
		t.Logf("Successfully deleted function %s", fnName)
	}
}

// TinyFaaSTestSuite is the base test suite
type TinyFaaSTestSuite struct {
	suite.Suite
	host        string
	gatewayPort int
}

// SetupSuite runs once before all tests
func (s *TinyFaaSTestSuite) SetupSuite() {
	// Check that make is installed
	if err := exec.Command("make", "--version").Run(); err != nil {
		s.T().Fatal("Make is not installed or not working")
	}

	// Check that Docker is working
	if err := exec.Command("docker", "ps").Run(); err != nil {
		s.T().Fatal("Docker is not installed or not working")
	}

	// Wait for gateway
	s.T().Log("Waiting for gateway...")
	if !waitForService(s.T(), host, gatewayPort, serviceStartupTimeout) {
		s.T().Fatalf(
			"gateway at %s:%d did not start within %v",
			host, gatewayPort, serviceStartupTimeout,
		)
	}
	s.T().Log("gateway is ready")
}

// TearDownSuite runs once after all tests
func (s *TinyFaaSTestSuite) TearDownSuite() {
	// Individual test suites handle their own cleanup
}

// SetupTest runs before each test
func (s *TinyFaaSTestSuite) SetupTest() {
	s.host = host
	s.gatewayPort = gatewayPort
}

// TestSieve tests the Sieve of Eratosthenes function
type TestSieve struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestSieve) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "sieve-of-eratosthenes"),
		"sieve",
		"nodejs",
		1,
	)
}

func (s *TestSieve) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestSieve) TestInvokeHTTP() {
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)
	resp, err := http.Get(url)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)
}

func (s *TestSieve) TestInvokeHTTPAsync() {
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(s.T(), err)

	req.Header.Set("X-tinyFaaS-Async", "true")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusAccepted, resp.StatusCode)
}

// TestEchoPY tests the Python echo function
type TestEchoPY struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestEchoPY) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "echo-py"),
		"echo-py",
		"python3",
		1,
	)
}

func (s *TestEchoPY) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestEchoPY) TestInvokeHTTP() {
	payload := "Hello World!"
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)

	resp, err := http.Post(url, "text/plain", bytes.NewBufferString(payload))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), payload, string(body))
}

// TestEchoJS tests the JavaScript echo function
type TestEchoJS struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestEchoJS) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "echo-js"),
		"echojs",
		"nodejs",
		1,
	)
}

func (s *TestEchoJS) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestEchoJS) TestInvokeHTTP() {
	payload := "Hello World!"
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(payload))
	require.NoError(s.T(), err)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), payload, string(body))
}

// TestEchoGo tests the Go echo function
type TestEchoGo struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestEchoGo) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "echo-go"),
		"echo-go",
		"go",
		1,
	)
}

func (s *TestEchoGo) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestEchoGo) TestInvokeHTTP() {
	payload := "Hello World!"
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)

	resp, err := http.Post(url, "text/plain", bytes.NewBufferString(payload))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), payload, string(body))
}

// TestBinary tests the binary echo function
type TestBinary struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestBinary) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "echo-binary"),
		"echobinary",
		"binary",
		1,
	)
}

func (s *TestBinary) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestBinary) TestInvokeHTTP() {
	payload := "Hello World!"
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)

	resp, err := http.Post(url, "text/plain", bytes.NewBufferString(payload))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), payload, string(body))
}

// TestShowHeadersJS tests the JavaScript header display function
type TestShowHeadersJS struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestShowHeadersJS) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "show-headers-js"),
		"headersjs",
		"nodejs",
		1,
	)
}

func (s *TestShowHeadersJS) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestShowHeadersJS) TestInvokeHTTP() {
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(s.T(), err)
	req.Header.Set("lab", "scalable_software_systems_group")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)

	var headers map[string]string
	err = json.Unmarshal(body, &headers)
	require.NoError(s.T(), err)

	// Check custom header
	assert.Contains(s.T(), headers, "lab")
	assert.Equal(s.T(), "scalable_software_systems_group", headers["lab"])

	// Check user-agent header
	assert.Contains(s.T(), headers, "user-agent")
	assert.True(s.T(), strings.Contains(headers["user-agent"], "Go-http-client"))
}

// TestShowHeaders tests the Python header display function
type TestShowHeaders struct {
	TinyFaaSTestSuite
	fn string
}

func (s *TestShowHeaders) SetupSuite() {
	s.TinyFaaSTestSuite.SetupSuite()
	s.fn = startFunction(
		s.T(),
		filepath.Join(fnPath, "show-headers"),
		"headers",
		"python3",
		1,
	)
}

func (s *TestShowHeaders) TearDownSuite() {
	deleteFunction(s.T(), s.fn)
	s.TinyFaaSTestSuite.TearDownSuite()
}

func (s *TestShowHeaders) TestInvokeHTTP() {
	// Note: In Python, the http.server module automatically capitalizes
	// the first character of each word in the header keys
	url := fmt.Sprintf("http://%s:%d/fn/%s", s.host, s.gatewayPort, s.fn)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(s.T(), err)
	req.Header.Set("Lab", "scalable_software_systems_group")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	assert.Equal(s.T(), http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(s.T(), err)

	var headers map[string]string
	err = json.Unmarshal(body, &headers)
	require.NoError(s.T(), err)

	// Check custom header (capitalized by Python http.server)
	assert.Contains(s.T(), headers, "Lab")
	assert.Equal(s.T(), "scalable_software_systems_group", headers["Lab"])

	// Check user-agent header (capitalized by Python http.server)
	assert.Contains(s.T(), headers, "User-Agent")
	assert.True(s.T(), strings.Contains(headers["User-Agent"], "Go-http-client"))
}

// TestIntegration runs all test suites
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// Check environment before running tests
	if _, err := os.Stat(fnPath); os.IsNotExist(err) {
		t.Fatalf("Functions directory not found: %s", fnPath)
	}

	// Run test suites
	suite.Run(t, new(TestSieve))
	suite.Run(t, new(TestEchoPY))
	suite.Run(t, new(TestEchoJS))
	suite.Run(t, new(TestEchoGo))
	suite.Run(t, new(TestBinary))
	suite.Run(t, new(TestShowHeadersJS))
	suite.Run(t, new(TestShowHeaders))
}
