package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenFogStack/tinyFaaS/pkg/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
)

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubBackend is a no-op backend that lets us construct a real *server.Server
// for handler tests without touching Docker.
type stubBackend struct{}

func (stubBackend) Create(string, string, int, string, map[string]string, map[string]string, server.ResourceLimits) (server.Handler, error) {
	return nil, assertErrUnused
}
func (stubBackend) Stop() error { return nil }

var assertErrUnused = errUnused{}

type errUnused struct{}

func (errUnused) Error() string { return "backend not used in this test" }

func newTestHandlers(t *testing.T) (*handlers, *server.Server) {
	t.Helper()

	oldTmpDir := server.TmpDir
	server.TmpDir = t.TempDir()
	t.Cleanup(func() { server.TmpDir = oldTmpDir })

	srv := server.New("test", "development", stubBackend{}, nopLogger())
	return &handlers{srv: srv, logger: nopLogger()}, srv
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

func TestGetServerPort(t *testing.T) {
	t.Run("uses TINYFAAS_SERVER_PORT when set", func(t *testing.T) {
		t.Setenv("TINYFAAS_SERVER_PORT", "18000")
		assert.Equal(t, "18000", getServerPort())
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TINYFAAS_SERVER_PORT", "")
		assert.Equal(t, "8000", getServerPort())
	})
}

func TestUploadHandlerMissingMetadata(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()

	h.uploadHandler(recorder, newMultipartUploadRequest(t, nil, []byte("zip-data")))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "missing upload metadata")
}

func TestUploadHandlerMissingZip(t *testing.T) {
	metadata, err := json.Marshal(uploadMetadata{FunctionName: "echo", FunctionEnv: "python3", FunctionReplicas: 1})
	require.NoError(t, err)

	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()

	h.uploadHandler(recorder, newMultipartUploadRequest(t, metadata, nil))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "missing upload zip file")
}

func TestUploadHandlerInvalidMetadata(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()

	h.uploadHandler(recorder, newMultipartUploadRequest(t, []byte("{"), []byte("zip-data")))

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "failed to decode upload metadata")
}

func TestUploadHandlerRejectsInvalidMethod(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/upload", nil)

	h.uploadHandler(recorder, req)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "invalid method")
}

func TestFunctionHandlerNotFound(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/function/missing", nil)

	h.functionHandler(recorder, req)

	require.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestListHandlerReturnsEmptyList(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/list", nil)

	h.listHandler(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "[]\n", recorder.Body.String())
}

func TestInvokeHandlerMissingName(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke/", nil)

	h.invokeHandler(recorder, req)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "function name required")
}

func TestInvokeHandlerUnknownFunction(t *testing.T) {
	h, _ := newTestHandlers(t)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/invoke/missing", bytes.NewBufferString("{}"))

	h.invokeHandler(recorder, req)

	require.Equal(t, http.StatusNotFound, recorder.Code)
}
