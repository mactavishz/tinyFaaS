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

	"github.com/OpenFogStack/tinyFaaS/pkg/docker"
	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"go.uber.org/zap"
)

var (
	ManagerPort = getManagerPort()
	RProxyPort  = getRProxyPort()
)

func getManagerPort() string {
	return util.GetEnvOrDefault("MANAGER_PORT", "8080")
}

func getRProxyPort() string {
	return util.GetEnvOrDefault("RPROXY_PORT", "8000")
}

type managementService interface {
	UploadArchive(name string, env string, replicas int, archivePath string, envs map[string]string, labels map[string]string, resources manager.FunctionResourceRequest) error
	Delete(name string) error
	List() []manager.FunctionConfig
	Wipe() error
	Logs() (io.Reader, error)
	LogsFunction(name string) (io.Reader, error)
	UrlUpload(name string, env string, replicas int, funcurl string, subfolder string, envs map[string]string, labels map[string]string, resources manager.FunctionResourceRequest) error
	ScaleUp(functionName string, cold bool) error
	StartRequest(functionName string) error
	EndRequest(functionName string)
	Heartbeat(functionName string) error
	HeartbeatBatch(functionNames []string) error
}

type uploadMetadata struct {
	FunctionName     string                     `json:"name"`
	FunctionEnv      string                     `json:"env"`
	FunctionReplicas int                        `json:"replicas"`
	FunctionEnvs     []string                   `json:"envs"`
	FunctionLabels   map[string]string          `json:"labels"`
	Limits           *manager.FunctionResources `json:"limits"`
}

type server struct {
	ms     managementService
	logger *zap.Logger
}

func parseFunctionEnvs(functionEnvs []string, logger *zap.Logger) map[string]string {
	envs := make(map[string]string)
	for _, e := range functionEnvs {
		k, v, ok := strings.Cut(e, "=")

		if !ok {
			logger.Warn("invalid env", zap.String("env", e))
			continue
		}

		envs[k] = v
	}

	return envs
}

func (s *server) writeUploadArchive(src io.Reader) (string, int64, error) {
	if err := os.MkdirAll(manager.TmpDir, 0777); err != nil {
		return "", 0, err
	}

	archiveFile, err := os.CreateTemp(manager.TmpDir, "upload-*.zip")
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
	defer logger.Sync() // flushes buffer, if any

	// setting backend to docker
	id := uuid.New().String()

	// find backend
	backend := util.GetEnvOrDefault("BACKEND", "docker")
	logger.Info("using runtime backend", zap.String("backend", backend))

	var tfBackend manager.Backend
	switch backend {
	case "docker":
		logger.Info("using docker backend")
		tfBackend = docker.New(id, logger)
	default:
		logger.Fatal("invalid backend", zap.String("backend", backend))
	}

	ms := manager.New(
		id,
		RProxyPort,
		tfBackend,
		logger,
	)

	logger.Info("manager expects rproxy", zap.String("port", RProxyPort))

	// Initialize autoscaler
	autoscalerConfig, err := autoscaler.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Fatal("failed to initialize autoscaler", zap.Error(err))
	}
	scaleOp := manager.NewTinyFaaSScaleOp(ms, logger)
	as := autoscaler.New(autoscalerConfig, scaleOp, logger)
	ms.SetAutoScaler(as)
	as.Start()

	s := &server{
		ms:     ms,
		logger: logger,
	}

	// create handlers
	r := http.NewServeMux()
	r.HandleFunc("/upload", s.uploadHandler)
	r.HandleFunc("/delete", s.deleteHandler)
	r.HandleFunc("/list", s.listHandler)
	r.HandleFunc("/wipe", s.wipeHandler)
	r.HandleFunc("/logs", s.logsHandler)
	r.HandleFunc("/uploadURL", s.urlUploadHandler)
	r.HandleFunc("/scale-up", s.scaleUpHandler)
	r.HandleFunc("/request-start", s.startRequestHandler)
	r.HandleFunc("/request-finish", s.endRequestHandler)
	r.HandleFunc("/heartbeat", s.heartbeatHandler)

	// create HTTP server with graceful shutdown support
	addr := fmt.Sprintf("127.0.0.1:%s", ManagerPort)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// setup signal handling for graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	// start HTTP server in goroutine
	go func() {
		logger.Info("starting HTTP server", zap.String("address", addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	// wait for shutdown signal
	<-sig

	logger.Info("received interrupt signal")
	logger.Info("initiating graceful shutdown...")

	// create context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// gracefully shutdown HTTP server
	logger.Info("shutting down HTTP server...")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	}

	// stop autoscaler
	logger.Info("stopping autoscaler...")
	as.Stop()

	// stop management service (cleans up function containers)
	logger.Info("stopping management service...")
	if err := ms.Stop(); err != nil {
		logger.Error("management service shutdown error", zap.Error(err))
	}

	logger.Info("shutdown complete")
	os.Exit(0)
}

func (s *server) uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	reader, err := r.MultipartReader()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to parse multipart upload request\n")
		s.logger.Error("failed to parse multipart upload request", zap.Error(err))
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
			s.logger.Error("failed to remove uploaded archive", zap.String("path", archivePath), zap.Error(err))
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
			s.logger.Error("failed to read upload request", zap.Error(err))
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
				s.logger.Error("failed to decode upload metadata", zap.Error(err))
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

			archivePath, archiveBytes, err = s.writeUploadArchive(part)
			if err != nil {
				part.Close()
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to persist upload archive\n")
				s.logger.Error("failed to persist upload archive", zap.Error(err))
				return
			}
		default:
			s.logger.Debug("ignoring unexpected upload part", zap.String("part", part.FormName()))
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

	s.logger.Info("receive upload request", zap.String("name", metadata.FunctionName), zap.String("env", metadata.FunctionEnv), zap.Int("replicas", metadata.FunctionReplicas), zap.Int64("bytes", archiveBytes), zap.Strings("envs", metadata.FunctionEnvs), zap.Any("labels", metadata.FunctionLabels), zap.Any("limits", metadata.Limits))
	envs := parseFunctionEnvs(metadata.FunctionEnvs, s.logger)

	if metadata.FunctionLabels == nil {
		metadata.FunctionLabels = make(map[string]string)
	}

	err = s.ms.UploadArchive(metadata.FunctionName, metadata.FunctionEnv, metadata.FunctionReplicas, archivePath, envs, metadata.FunctionLabels, manager.FunctionResourceRequest{Limits: metadata.Limits})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to upload function\n")
		s.logger.Error("failed to upload function", zap.Error(err))
		return
	}

	// return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deployed\n", metadata.FunctionName)

}

func (s *server) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	// parse request
	d := struct {
		FunctionName string `json:"name"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode delete request\n")
		s.logger.Error("failed to decode delete request", zap.Error(err))
		return
	}

	s.logger.Info("receive delete request", zap.String("name", d.FunctionName))

	// delete function
	err = s.ms.Delete(d.FunctionName)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to delete function\n")
		s.logger.Error("failed to delete function", zap.String("name", d.FunctionName), zap.Error(err))
		return
	}

	// return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deleted\n", d.FunctionName)
}

func (s *server) listHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only GET allowed\n")
		return
	}

	s.logger.Info("receive list request")
	l := s.ms.List()

	// return success
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(l)
}

func (s *server) wipeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	s.logger.Info("receive wipe request")
	err := s.ms.Wipe()

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to wipe functions\n")
		s.logger.Error("failed to wipe functions", zap.Error(err))
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "All functions deleted\n")
}

func (s *server) logsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only GET allowed\n")
		return
	}

	// parse request
	var logs io.Reader
	name := r.URL.Query().Get("name")
	s.logger.Info("receive logs request", zap.String("function_name", name))

	if name == "" {
		l, err := s.ms.Logs()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to get logs\n")
			s.logger.Error("failed to get logs", zap.Error(err))
			return
		}
		logs = l
	}

	if name != "" {
		l, err := s.ms.LogsFunction(name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to get logs for function %s\n", name)
			s.logger.Error("failed to get logs for function", zap.String("name", name), zap.Error(err))
			return
		}
		logs = l
	}

	// return success
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")
	_, err := io.Copy(w, logs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to write logs to response\n")
		s.logger.Error("failed to write logs to response", zap.Error(err))
		return
	}
}

func (s *server) urlUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	// parse request
	d := struct {
		FunctionName     string                     `json:"name"`
		FunctionEnv      string                     `json:"env"`
		FunctionReplicas int                        `json:"replicas"`
		FunctionURL      string                     `json:"url"`
		FunctionEnvs     []string                   `json:"envs"`
		SubFolder        string                     `json:"subfolder_path"`
		FunctionLabels   map[string]string          `json:"labels"`
		Limits           *manager.FunctionResources `json:"limits"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode url upload request\n")
		s.logger.Error("failed to decode url upload request", zap.Error(err))
		return
	}

	s.logger.Info("receive url upload request", zap.String("name", d.FunctionName), zap.String("env", d.FunctionEnv), zap.Int("replicas", d.FunctionReplicas), zap.String("url", d.FunctionURL), zap.Strings("envs", d.FunctionEnvs), zap.String("subfolder", d.SubFolder), zap.Any("labels", d.FunctionLabels), zap.Any("limits", d.Limits))

	envs := parseFunctionEnvs(d.FunctionEnvs, s.logger)

	if d.FunctionLabels == nil {
		d.FunctionLabels = make(map[string]string)
	}

	err = s.ms.UrlUpload(d.FunctionName, d.FunctionEnv, d.FunctionReplicas, d.FunctionURL, d.SubFolder, envs, d.FunctionLabels, manager.FunctionResourceRequest{Limits: d.Limits})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to upload function from url\n")
		s.logger.Error("failed to upload function from url", zap.Error(err))
		return
	}

	// return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deployed\n", d.FunctionName)
}

func (s *server) scaleUpHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	// parse request
	d := struct {
		FunctionName string `json:"name"`
		Cold         bool   `json:"cold"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode scale up request\n")
		s.logger.Error("failed to decode scale up request", zap.Error(err))
		return
	}

	s.logger.Info("receive scale up request", zap.String("name", d.FunctionName), zap.Bool("cold", d.Cold))
	// scale up function
	err = s.ms.ScaleUp(d.FunctionName, d.Cold)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to scale up function\n")
		s.logger.Error("failed to scale up function", zap.String("name", d.FunctionName), zap.Error(err))
		return
	}

	// return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s scaled up\n", d.FunctionName)
	s.logger.Info("function scaled up successfully", zap.String("name", d.FunctionName))
}

func (s *server) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	// Support both single heartbeat (name) and batch heartbeat (functions)
	d := struct {
		FunctionName string   `json:"name"`      // Single heartbeat (legacy)
		Functions    []string `json:"functions"` // Batch heartbeat
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode heartbeat request\n")
		s.logger.Error("failed to decode heartbeat request", zap.Error(err))
		return
	}

	// Handle batch heartbeat
	if len(d.Functions) > 0 {
		s.logger.Debug("receive batch heartbeat request", zap.Int("count", len(d.Functions)))

		err = s.ms.HeartbeatBatch(d.Functions)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to record batch heartbeat\n")
			s.logger.Error("failed to record batch heartbeat", zap.Strings("functions", d.Functions), zap.Error(err))
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Batch heartbeat for %d functions recorded\n", len(d.Functions))
		s.logger.Debug("batch heartbeat recorded successfully", zap.Int("count", len(d.Functions)))
		return
	}

	// Handle single heartbeat (legacy)
	if d.FunctionName == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "missing function name or functions array\n")
		return
	}

	s.logger.Debug("receive heartbeat request", zap.String("name", d.FunctionName))

	err = s.ms.Heartbeat(d.FunctionName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to record heartbeat\n")
		s.logger.Error("failed to record heartbeat", zap.String("name", d.FunctionName), zap.Error(err))
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Heartbeat for function %s recorded\n", d.FunctionName)
	s.logger.Debug("heartbeat recorded successfully", zap.String("name", d.FunctionName))
}

func (s *server) startRequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	d := struct {
		FunctionName string `json:"name"`
	}{}

	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode request start request\n")
		s.logger.Error("failed to decode request start request", zap.Error(err))
		return
	}

	if strings.TrimSpace(d.FunctionName) == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "missing function name\n")
		return
	}

	if err := s.ms.StartRequest(d.FunctionName); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to mark request start\n")
		s.logger.Error("failed to mark request start", zap.String("name", d.FunctionName), zap.Error(err))
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Request start for %s recorded\n", d.FunctionName)
}

func (s *server) endRequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid method, only POST allowed\n")
		return
	}

	d := struct {
		FunctionName string `json:"name"`
	}{}

	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode request finish request\n")
		s.logger.Error("failed to decode request finish request", zap.Error(err))
		return
	}

	if strings.TrimSpace(d.FunctionName) == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "missing function name\n")
		return
	}

	s.ms.EndRequest(d.FunctionName)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Request finish for %s recorded\n", d.FunctionName)
}
