package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"log/slog"

	"github.com/OpenFogStack/tinyFaaS/pkg/docker"
	"github.com/OpenFogStack/tinyFaaS/pkg/server"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
)

var emptyBody = []byte{}

const (
	DEFAULT_MODE = "development"
	DEFAULT_PORT = "8000"
)

func getServerPort() string {
	return util.GetEnvOrDefault("TINYFAAS_SERVER_PORT", DEFAULT_PORT)
}

type uploadMetadata struct {
	FunctionName     string                    `json:"name"`
	FunctionEnv      string                    `json:"env"`
	FunctionReplicas int                       `json:"replicas"`
	FunctionEnvs     []string                  `json:"envs"`
	FunctionLabels   map[string]string         `json:"labels"`
	Limits           *server.FunctionResources `json:"limits"`
}

// handlers wires the merged server to HTTP routes.
type handlers struct {
	srv    *server.Server
	logger *slog.Logger
}

func parseFunctionEnvs(functionEnvs []string, logger *slog.Logger) map[string]string {
	envs := make(map[string]string)
	for _, e := range functionEnvs {
		k, v, ok := strings.Cut(e, "=")
		if !ok {
			logger.Warn("invalid env", "env", e)
			continue
		}
		envs[k] = v
	}
	return envs
}

func (h *handlers) writeUploadArchive(src io.Reader) (string, int64, error) {
	if err := os.MkdirAll(server.TmpDir, 0777); err != nil {
		return "", 0, err
	}

	archiveFile, err := os.CreateTemp(server.TmpDir, "upload-*.zip")
	if err != nil {
		return "", 0, err
	}

	bytesWritten, err := io.Copy(archiveFile, src)
	closeErr := archiveFile.Close()
	if err != nil {
		_ = os.Remove(archiveFile.Name())
		return "", 0, err
	}
	if closeErr != nil {
		_ = os.Remove(archiveFile.Name())
		return "", 0, closeErr
	}

	return archiveFile.Name(), bytesWritten, nil
}

func main() {
	logger := util.CreateLogger()

	id := uuid.New().String()

	mode := util.GetEnvOrDefault("ENV", DEFAULT_MODE)
	logger.Info("tinyFaaS server ENV", "env", mode)

	// find backend
	backend := util.GetEnvOrDefault("BACKEND", "docker")
	logger.Info("using runtime backend", "backend", backend)

	var tfBackend server.Backend
	switch backend {
	case "docker":
		logger.Info("using docker backend")
		tfBackend = docker.New(id, logger)
	default:
		logger.Error("invalid backend", "backend", backend)
		os.Exit(1)
	}

	srv := server.New(id, mode, tfBackend, logger)

	// Initialize autoscaler
	autoscalerConfig, err := autoscaler.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Error("failed to initialize autoscaler", "err", err)
		os.Exit(1)
	}
	scaleOp := server.NewTinyFaaSScaleOp(srv, logger)
	as := autoscaler.New(autoscalerConfig, scaleOp, logger)
	srv.SetAutoScaler(as)
	if autoscalerConfig.Enabled {
		logger.Info("autoscaler enabled")
	} else {
		logger.Info("autoscaler disabled")
	}
	as.Start()

	// Initialize callgraph (includes prewarming)
	callGraphConfig, err := callgraph.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Error("failed to initialize callgraph config", "err", err)
		os.Exit(1)
	}

	tracker := callgraph.New(
		callgraph.WithLogger(logger),
		callgraph.WithConfig(&callGraphConfig),
	)
	srv.SetTracker(tracker)
	if callGraphConfig.Enabled {
		logger.Info("callgraph tracking enabled")
		if tracker.PrewarmEnabled() && autoscalerConfig.Enabled {
			logger.Info("prewarming enabled")
		} else if tracker.PrewarmEnabled() && !autoscalerConfig.Enabled {
			logger.Warn("prewarming configured but autoscaler is disabled - prewarming will not work")
		} else {
			logger.Info("prewarming disabled")
		}
		switch callGraphConfig.Method {
		case callgraph.ExponentialMovingAverage:
			logger.Info("callgraph config",
				"method", callGraphConfig.Method.String(),
				"alpha", callGraphConfig.EMAConfig.Alpha,
			)
		default:
			logger.Info("callgraph config",
				"method", callGraphConfig.Method.String(),
				"window_size", callGraphConfig.SMAConfig.WindowSize,
			)
		}
	} else {
		logger.Info("callgraph tracking disabled")
	}
	tracker.Start()

	h := &handlers{srv: srv, logger: logger}

	// create handlers
	mux := http.NewServeMux()

	// Management endpoints
	mux.HandleFunc("/upload", h.uploadHandler)
	mux.HandleFunc("/delete", h.deleteHandler)
	mux.HandleFunc("/list", h.listHandler)
	mux.HandleFunc("/function/", h.functionHandler)
	mux.HandleFunc("/wipe", h.wipeHandler)
	mux.HandleFunc("/logs", h.logsHandler)
	mux.HandleFunc("/uploadURL", h.urlUploadHandler)

	// Function invocation endpoint
	mux.HandleFunc("/invoke/", h.invokeHandler)

	// Callgraph analytics API endpoints (only available in development mode)
	if srv.IsDev() {
		logger.Info("registering development mode only callgraph endpoints")
		mux.HandleFunc("/callgraph", h.callgraphHandler)
		mux.HandleFunc("/callgraph/function/", h.callgraphFunctionHandler)
		mux.HandleFunc("/callgraph/edge", h.callgraphEdgeHandler)
	}

	addr := fmt.Sprintf("127.0.0.1:%s", getServerPort())
	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// setup signal handling for graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	// start HTTP server in goroutine
	go func() {
		logger.Info("tinyFaaS server started", "address", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
			os.Exit(1)
		}
	}()

	// wait for shutdown signal
	<-sig

	logger.Info("received interrupt signal")
	logger.Info("initiating graceful shutdown...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	logger.Info("shutting down HTTP server...")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "err", err)
	}

	logger.Info("stopping callgraph tracker...")
	tracker.Stop()

	logger.Info("stopping autoscaler...")
	as.Stop()

	logger.Info("stopping server...")
	if err := srv.Stop(); err != nil {
		logger.Error("server shutdown error", "err", err)
	}

	logger.Info("shutdown complete")
	os.Exit(0)
}

// ---------------------------------------------------------------------------
// Invocation
// ---------------------------------------------------------------------------

func (h *handlers) invokeHandler(w http.ResponseWriter, req *http.Request) {
	functionName := req.URL.Path[len("/invoke/"):]

	if functionName == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("function name required"))
		return
	}

	async := req.Header.Get("X-Tinyfaas-Async") != ""

	// Determine the caller by checking the X-Faas-Source-Ip header.
	sourceIP := req.Header.Get("X-Faas-Source-Ip")
	if sourceIP == "" {
		sourceIP = util.ExtractIP(req.RemoteAddr)
	}

	h.logger.Debug("incoming request", "ip", sourceIP)
	h.logger.Info("invoking function", "name", functionName, "async", async)

	if isInternalIP, err := util.IsPrivateIP(sourceIP); err == nil && isInternalIP {
		if callerName, ok := h.srv.GetFunctionNameByIP(sourceIP); ok {
			h.logger.Info("Intra-platform invocation",
				"caller", callerName,
				"callee", functionName,
				"sourceIP", sourceIP)
		}
	}

	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		h.logger.Error("failed to read request body", "err", err)
		return
	}

	status, res := h.srv.Call(functionName, reqBody, async, req.Header.Clone())

	w.WriteHeader(status)
	if res != nil {
		w.Write(res)
	} else {
		w.Write(emptyBody)
	}
}

// ---------------------------------------------------------------------------
// Management handlers
// ---------------------------------------------------------------------------

func (h *handlers) uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	reader, err := r.MultipartReader()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to parse multipart upload request\n")
		h.logger.Error("failed to parse multipart upload request", "err", err)
		return
	}

	var metadata *uploadMetadata
	archivePath := ""
	archiveBytes := int64(0)
	defer func() {
		if archivePath == "" {
			return
		}

		if err := os.Remove(archivePath); err != nil && !os.IsNotExist(err) {
			h.logger.Error("failed to remove uploaded archive", "path", archivePath, "err", err)
		}
	}()

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "failed to read upload request\n")
			h.logger.Error("failed to read upload request", "err", err)
			return
		}

		switch part.FormName() {
		case "metadata":
			if metadata != nil {
				part.Close()
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "duplicate upload metadata\n")
				return
			}

			var decoded uploadMetadata
			if err := json.NewDecoder(part).Decode(&decoded); err != nil {
				part.Close()
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "failed to decode upload metadata\n")
				h.logger.Error("failed to decode upload metadata", "err", err)
				return
			}
			metadata = &decoded
		case "zip":
			if archivePath != "" {
				part.Close()
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "duplicate upload zip file\n")
				return
			}

			archivePath, archiveBytes, err = h.writeUploadArchive(part)
			if err != nil {
				part.Close()
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to persist upload archive\n")
				h.logger.Error("failed to persist upload archive", "err", err)
				return
			}
		default:
			h.logger.Debug("ignoring unexpected upload part", "part", part.FormName())
			_, _ = io.Copy(io.Discard, part)
		}

		part.Close()
	}

	if metadata == nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "missing upload metadata\n")
		return
	}

	if archivePath == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "missing upload zip file\n")
		return
	}

	h.logger.Info("receive upload request", "name", metadata.FunctionName, "env", metadata.FunctionEnv, "replicas", metadata.FunctionReplicas, "bytes", archiveBytes, "envs", metadata.FunctionEnvs, "labels", metadata.FunctionLabels, "limits", metadata.Limits)
	envs := parseFunctionEnvs(metadata.FunctionEnvs, h.logger)

	if metadata.FunctionLabels == nil {
		metadata.FunctionLabels = make(map[string]string)
	}

	err = h.srv.UploadArchive(metadata.FunctionName, metadata.FunctionEnv, metadata.FunctionReplicas, archivePath, envs, metadata.FunctionLabels, server.FunctionResourceRequest{Limits: metadata.Limits})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to upload function\n")
		h.logger.Error("failed to upload function", "err", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deployed\n", metadata.FunctionName)
}

func (h *handlers) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	d := struct {
		FunctionName string `json:"name"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode delete request\n")
		h.logger.Error("failed to decode delete request", "err", err)
		return
	}

	h.logger.Info("receive delete request", "name", d.FunctionName)

	err = h.srv.Delete(d.FunctionName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to delete function\n")
		h.logger.Error("failed to delete function", "name", d.FunctionName, "err", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deleted\n", d.FunctionName)
}

func (h *handlers) listHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only GET allowed\n")
		return
	}

	h.logger.Info("receive list request")
	l := h.srv.List()

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(l)
}

func (h *handlers) functionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only GET allowed\n")
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/function/")
	if strings.TrimSpace(name) == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "missing function name\n")
		return
	}

	fn, ok := h.srv.Get(name)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fn)
}

func (h *handlers) wipeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	h.logger.Info("receive wipe request")
	err := h.srv.Wipe()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to wipe functions\n")
		h.logger.Error("failed to wipe functions", "err", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "All functions deleted\n")
}

func (h *handlers) logsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only GET allowed\n")
		return
	}

	var logs io.Reader
	name := r.URL.Query().Get("name")
	h.logger.Info("receive logs request", "function_name", name)

	if name == "" {
		l, err := h.srv.Logs()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to get logs\n")
			h.logger.Error("failed to get logs", "err", err)
			return
		}
		logs = l
	}

	if name != "" {
		l, err := h.srv.LogsFunction(name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to get logs for function %s\n", name)
			h.logger.Error("failed to get logs for function", "name", name, "err", err)
			return
		}
		logs = l
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")
	_, err := io.Copy(w, logs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to write logs to response\n")
		h.logger.Error("failed to write logs to response", "err", err)
		return
	}
}

func (h *handlers) urlUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	d := struct {
		FunctionName     string                    `json:"name"`
		FunctionEnv      string                    `json:"env"`
		FunctionReplicas int                       `json:"replicas"`
		FunctionURL      string                    `json:"url"`
		FunctionEnvs     []string                  `json:"envs"`
		SubFolder        string                    `json:"subfolder_path"`
		FunctionLabels   map[string]string         `json:"labels"`
		Limits           *server.FunctionResources `json:"limits"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode url upload request\n")
		h.logger.Error("failed to decode url upload request", "err", err)
		return
	}

	h.logger.Info("receive url upload request", "name", d.FunctionName, "env", d.FunctionEnv, "replicas", d.FunctionReplicas, "url", d.FunctionURL, "envs", d.FunctionEnvs, "subfolder", d.SubFolder, "labels", d.FunctionLabels, "limits", d.Limits)

	envs := parseFunctionEnvs(d.FunctionEnvs, h.logger)

	if d.FunctionLabels == nil {
		d.FunctionLabels = make(map[string]string)
	}

	err = h.srv.UrlUpload(d.FunctionName, d.FunctionEnv, d.FunctionReplicas, d.FunctionURL, d.SubFolder, envs, d.FunctionLabels, server.FunctionResourceRequest{Limits: d.Limits})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to upload function from url\n")
		h.logger.Error("failed to upload function from url", "err", err)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deployed\n", d.FunctionName)
}

// ---------------------------------------------------------------------------
// Callgraph analytics handlers (development mode only)
// ---------------------------------------------------------------------------

func (h *handlers) callgraphHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	callGraph := h.srv.GetTracker().GetCallGraph()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(callGraph); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		h.logger.Error("failed to encode call graph", "err", err)
		return
	}
}

func (h *handlers) callgraphFunctionHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	tracker := h.srv.GetTracker()
	functionName := req.URL.Path[len("/callgraph/function/"):]
	if functionName == "" {
		stats := tracker.GetAllFunctionStats()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			h.logger.Error("failed to encode function stats", "err", err)
			return
		}
		return
	}

	stats, ok := tracker.GetFunctionStats(functionName)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("function not found in call graph"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		h.logger.Error("failed to encode function stats", "err", err)
		return
	}
}

func (h *handlers) callgraphEdgeHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	caller := req.URL.Query().Get("caller")
	callee := req.URL.Query().Get("callee")

	if callee == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("callee parameter is required"))
		return
	}

	edge, ok := h.srv.GetTracker().GetEdgeStats(caller, callee)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("edge not found in call graph"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(edge); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		h.logger.Error("failed to encode edge stats", "err", err)
		return
	}
}
