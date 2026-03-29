package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/stretchr/testify/require"
)

var nameRand = rand.New(rand.NewSource(time.Now().UnixNano()))

type UploadPayload struct {
	Name     string                     `json:"name"`
	Env      string                     `json:"env"`
	Replicas int                        `json:"replicas"`
	Envs     []string                   `json:"envs,omitempty"`
	Labels   map[string]string          `json:"labels,omitempty"`
	Limits   *manager.FunctionResources `json:"limits,omitempty"`
}

func UniqueFunctionName(prefix string) string {
	prefix = strings.ToLower(strings.TrimSpace(prefix))
	prefix = strings.ReplaceAll(prefix, "_", "-")
	prefix = strings.ReplaceAll(prefix, " ", "-")
	if prefix == "" {
		prefix = "fn"
	}
	// 63 char DNS label limit. Keep this small and safe.
	suffix := fmt.Sprintf("%d-%04d", time.Now().Unix(), nameRand.Intn(10000))
	name := prefix + "-" + suffix
	if len(name) > 63 {
		name = name[:63]
		name = strings.TrimRight(name, "-")
	}
	return name
}

func UploadFunction(t *testing.T, baseURL string, payload UploadPayload, zipData []byte) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	metadataPart, err := writer.CreateFormField("metadata")
	require.NoError(t, err, "failed to create metadata part")
	require.NoError(t, json.NewEncoder(metadataPart).Encode(payload), "failed to encode upload metadata")

	zipPart, err := writer.CreateFormFile("zip", payload.Name+".zip")
	require.NoError(t, err, "failed to create zip part")
	_, err = zipPart.Write(zipData)
	require.NoError(t, err, "failed to write zip payload")
	require.NoError(t, writer.Close(), "failed to finalize multipart upload payload")

	url := fmt.Sprintf("%s/system/upload", strings.TrimRight(baseURL, "/"))
	req, err := http.NewRequest(http.MethodPost, url, &body)
	require.NoError(t, err, "failed to create upload request")
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "failed to upload function %s", payload.Name)
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload failed: %s", string(responseBody))
}

func UploadFixtureFunction(t *testing.T, baseURL string, fixtureDir string, name string, env string, replicas int, limits *manager.FunctionResources) {
	t.Helper()

	absPath, err := filepath.Abs(fixtureDir)
	require.NoError(t, err, "failed to get absolute path for %s", fixtureDir)

	zip := ZipDirectory(t, absPath)
	UploadFunction(t, baseURL, UploadPayload{
		Name:     name,
		Env:      env,
		Replicas: replicas,
		Limits:   limits,
	}, zip)
}

func DeleteFunction(t *testing.T, baseURL string, name string) error {
	t.Helper()

	payload := struct {
		Name string `json:"name"`
	}{Name: name}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/system/delete", strings.TrimRight(baseURL, "/"))
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func CleanupDeleteFunction(t *testing.T, baseURL string, name string) {
	t.Helper()
	t.Cleanup(func() {
		_ = DeleteFunction(t, baseURL, name)
	})
}

func Invoke(t *testing.T, baseURL string, functionName string, method string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()

	url := fmt.Sprintf("%s/fn/%s", strings.TrimRight(baseURL, "/"), functionName)
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	require.NoError(t, err)

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" && len(body) > 0 {
		req.Header.Set("Content-Type", "text/plain")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, respBody
}

func ListFunctions(t *testing.T, baseURL string) []manager.FunctionConfig {
	t.Helper()

	url := fmt.Sprintf("%s/system/list", strings.TrimRight(baseURL, "/"))
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "list failed: %s", string(body))

	var out []manager.FunctionConfig
	require.NoError(t, json.Unmarshal(body, &out), "invalid list JSON: %s", string(body))
	return out
}

func GetFunction(t *testing.T, baseURL string, name string) manager.FunctionConfig {
	t.Helper()

	for _, fn := range ListFunctions(t, baseURL) {
		if fn.Name == name {
			return fn
		}
	}
	t.Fatalf("function %q not found in /system/list", name)
	return manager.FunctionConfig{}
}
