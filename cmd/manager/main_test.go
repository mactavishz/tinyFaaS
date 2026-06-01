package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type mockManagementService struct {
	uploadCalled      bool
	uploadName        string
	uploadEnv         string
	uploadReplicas    int
	uploadArchiveData []byte
	uploadEnvs        map[string]string
	uploadLabels      map[string]string
	uploadResources   manager.FunctionResourceRequest
	uploadErr         error
	started           []string
	finished          []string
	startErr          error
	functionConfig    manager.FunctionConfig
	hasFunction       bool
}

func (f *mockManagementService) UploadArchive(name string, env string, replicas int, archivePath string, envs map[string]string, labels map[string]string, resources manager.FunctionResourceRequest) error {
	f.uploadCalled = true
	f.uploadName = name
	f.uploadEnv = env
	f.uploadReplicas = replicas
	f.uploadEnvs = envs
	f.uploadLabels = labels
	f.uploadResources = resources

	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		return err
	}
	f.uploadArchiveData = archiveData

	return f.uploadErr
}

func (f *mockManagementService) Delete(string) error {
	return nil
}

func (f *mockManagementService) Get(string) (manager.FunctionConfig, bool) {
	return f.functionConfig, f.hasFunction
}

func (f *mockManagementService) List() []manager.FunctionConfig {
	return nil
}

func (f *mockManagementService) Wipe() error {
	return nil
}

func (f *mockManagementService) Logs() (io.Reader, error) {
	return bytes.NewReader(nil), nil
}

func (f *mockManagementService) LogsFunction(string) (io.Reader, error) {
	return bytes.NewReader(nil), nil
}

func (f *mockManagementService) UrlUpload(string, string, int, string, string, map[string]string, map[string]string, manager.FunctionResourceRequest) error {
	return nil
}

func (f *mockManagementService) ScaleUp(string, bool) error {
	return nil
}

func (f *mockManagementService) StartRequest(name string) error {
	f.started = append(f.started, name)
	return f.startErr
}

func (f *mockManagementService) EndRequest(name string) {
	f.finished = append(f.finished, name)
}

func (f *mockManagementService) Heartbeat(string) error {
	return nil
}

func (f *mockManagementService) HeartbeatBatch([]string) error {
	return nil
}

func newMultipartUploadRequest(t *testing.T, metadataBody []byte, zipBody []byte) *http.Request {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if metadataBody != nil {
		metadataPart, err := writer.CreateFormField("metadata")
		require.NoError(t, err)
		_, err = metadataPart.Write(metadataBody)
		require.NoError(t, err)
	}

	if zipBody != nil {
		zipPart, err := writer.CreateFormFile("zip", "function.zip")
		require.NoError(t, err)
		_, err = zipPart.Write(zipBody)
		require.NoError(t, err)
	}

	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func newTestServer(t *testing.T, ms managementService) *server {
	t.Helper()

	oldTmpDir := manager.TmpDir
	manager.TmpDir = t.TempDir()
	t.Cleanup(func() {
		manager.TmpDir = oldTmpDir
	})

	return &server{ms: ms, logger: nopLogger()}
}

func TestGetManagerPort(t *testing.T) {
	t.Run("uses MANAGER_PORT when set", func(t *testing.T) {
		t.Setenv("MANAGER_PORT", "18080")
		assert.Equal(t, "18080", getManagerPort())
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("MANAGER_PORT", "")
		assert.Equal(t, "8001", getManagerPort())
	})
}

func TestGetRProxyPort(t *testing.T) {
	t.Run("uses RPROXY_PORT when set", func(t *testing.T) {
		t.Setenv("RPROXY_PORT", "18000")
		assert.Equal(t, "18000", getRProxyPort())
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("RPROXY_PORT", "")
		assert.Equal(t, "8000", getRProxyPort())
	})
}

func TestUploadHandlerMultipartSuccess(t *testing.T) {
	limits := &manager.FunctionResources{CPU: "50m", Memory: "96Mi"}
	metadata, err := json.Marshal(uploadMetadata{
		FunctionName:     "echo",
		FunctionEnv:      "python3",
		FunctionReplicas: 2,
		FunctionEnvs:     []string{"VALID=value", "invalid-env"},
		Limits:           limits,
	})
	require.NoError(t, err)

	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()

	s.uploadHandler(recorder, newMultipartUploadRequest(t, metadata, []byte("zip-data")))

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.True(t, mockMS.uploadCalled)
	assert.Equal(t, "echo", mockMS.uploadName)
	assert.Equal(t, "python3", mockMS.uploadEnv)
	assert.Equal(t, 2, mockMS.uploadReplicas)
	assert.Equal(t, map[string]string{"VALID": "value"}, mockMS.uploadEnvs)
	assert.Equal(t, map[string]string{}, mockMS.uploadLabels)
	assert.Equal(t, limits, mockMS.uploadResources.Limits)
	assert.Equal(t, []byte("zip-data"), mockMS.uploadArchiveData)
	assert.Contains(t, recorder.Body.String(), "Function echo deployed")
}

func TestUploadHandlerMissingMetadata(t *testing.T) {
	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()

	s.uploadHandler(recorder, newMultipartUploadRequest(t, nil, []byte("zip-data")))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.False(t, mockMS.uploadCalled)
	assert.Contains(t, recorder.Body.String(), "missing upload metadata")
}

func TestRequestStartHandler(t *testing.T) {
	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)

	body := bytes.NewBufferString(`{"name":"echo"}`)
	req := httptest.NewRequest(http.MethodPost, "/request-start", body)
	rec := httptest.NewRecorder()

	s.startRequestHandler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"echo"}, mockMS.started)
}

func TestRequestFinishHandler(t *testing.T) {
	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)

	body := bytes.NewBufferString(`{"name":"echo"}`)
	req := httptest.NewRequest(http.MethodPost, "/request-finish", body)
	rec := httptest.NewRecorder()

	s.endRequestHandler(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, []string{"echo"}, mockMS.finished)
}

func TestUploadHandlerMissingZip(t *testing.T) {
	metadata, err := json.Marshal(uploadMetadata{FunctionName: "echo", FunctionEnv: "python3", FunctionReplicas: 1})
	require.NoError(t, err)

	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()

	s.uploadHandler(recorder, newMultipartUploadRequest(t, metadata, nil))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.False(t, mockMS.uploadCalled)
	assert.Contains(t, recorder.Body.String(), "missing upload zip file")
}

func TestUploadHandlerInvalidMetadata(t *testing.T) {
	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()

	s.uploadHandler(recorder, newMultipartUploadRequest(t, []byte("{"), []byte("zip-data")))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.False(t, mockMS.uploadCalled)
	assert.Contains(t, recorder.Body.String(), "failed to decode upload metadata")
}

func TestUploadHandlerServiceError(t *testing.T) {
	metadata, err := json.Marshal(uploadMetadata{FunctionName: "echo", FunctionEnv: "python3", FunctionReplicas: 1})
	require.NoError(t, err)

	mockMS := &mockManagementService{uploadErr: assert.AnError}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()

	s.uploadHandler(recorder, newMultipartUploadRequest(t, metadata, []byte("zip-data")))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	assert.True(t, mockMS.uploadCalled)
	assert.Contains(t, recorder.Body.String(), "failed to upload function")
}

func TestUploadHandlerRejectsInvalidMethod(t *testing.T) {
	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/upload", nil)

	s.uploadHandler(recorder, req)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.False(t, mockMS.uploadCalled)
	assert.Contains(t, recorder.Body.String(), "invalid method")
}

func TestFunctionHandler(t *testing.T) {
	mockMS := &mockManagementService{functionConfig: manager.FunctionConfig{Name: "echo"}, hasFunction: true}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/function/echo", nil)

	s.functionHandler(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Body.String(), `"name":"echo"`)
}

func TestFunctionHandlerNotFound(t *testing.T) {
	mockMS := &mockManagementService{}
	s := newTestServer(t, mockMS)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/function/missing", nil)

	s.functionHandler(recorder, req)

	require.Equal(t, http.StatusNotFound, recorder.Code)
}
