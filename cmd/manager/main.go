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
	ManagerPort = util.GetEnvOrDefault("MANAGER_PORT", "8080")
	RProxyPort  = util.GetEnvOrDefault("RPROXY_PORT", "8000")
)

type server struct {
	ms     *manager.ManagementService
	logger *zap.Logger
}

func main() {
	logger := util.CreateLogger()
	defer logger.Sync() // flushes buffer, if any

	// setting backend to docker
	id := uuid.New().String()

	// find backend
	backend, ok := os.LookupEnv("TF_BACKEND")

	if !ok {
		backend = "docker"
		logger.Info("using default backend docker")
	}

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

	// parse request
	d := struct {
		FunctionName     string                     `json:"name"`
		FunctionEnv      string                     `json:"env"`
		FunctionReplicas int                        `json:"replicas"`
		FunctionZip      string                     `json:"zip"`
		FunctionEnvs     []string                   `json:"envs"`
		FunctionLabels   map[string]string          `json:"labels"`
		Limits           *manager.FunctionResources `json:"limits"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&d)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode upload request\n")
		s.logger.Error("failed to decode upload request", zap.Error(err))
		return
	}

	s.logger.Info("receive upload request", zap.String("name", d.FunctionName), zap.String("env", d.FunctionEnv), zap.Int("replicas", d.FunctionReplicas), zap.Int("bytes", len(d.FunctionZip)), zap.Strings("envs", d.FunctionEnvs), zap.Any("labels", d.FunctionLabels), zap.Any("limits", d.Limits))
	envs := make(map[string]string)
	for _, e := range d.FunctionEnvs {
		k, v, ok := strings.Cut(e, "=")

		if !ok {
			s.logger.Warn("invalid env", zap.String("env", e))
			continue
		}

		envs[k] = v
	}

	if d.FunctionLabels == nil {
		d.FunctionLabels = make(map[string]string)
	}

	err = s.ms.Upload(d.FunctionName, d.FunctionEnv, d.FunctionReplicas, d.FunctionZip, envs, d.FunctionLabels, manager.FunctionResourceRequest{Limits: d.Limits})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to upload function\n")
		s.logger.Error("failed to upload function", zap.Error(err))
		return
	}

	// return success
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Function %s deployed\n", d.FunctionName)

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

	envs := make(map[string]string)
	for _, e := range d.FunctionEnvs {
		k, v, ok := strings.Cut(e, "=")

		if !ok {
			s.logger.Warn("invalid env", zap.String("env", e))
			continue
		}

		envs[k] = v
	}

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
