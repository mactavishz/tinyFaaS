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
	"github.com/OpenFogStack/tinyFaaS/pkg/queue"
	tinyserver "github.com/OpenFogStack/tinyFaaS/pkg/server"
	invstats "github.com/OpenFogStack/tinyFaaS/pkg/stats"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph"
	"log/slog"
)

const (
	defaultMode = "development"
	defaultPort = "8000"
)

type service struct {
	ms        *tinyserver.ManagementService
	rp        *tinyserver.InvocationRouter
	tracker   callgraph.FullTracker
	autoscale *autoscaler.AutoScaler
	queue     queue.Publisher
	stats     *invstats.Store
	logger    *slog.Logger
}

type uploadMetadata struct {
	FunctionName     string                        `json:"name"`
	FunctionEnv      string                        `json:"env"`
	FunctionReplicas int                           `json:"replicas"`
	FunctionEnvs     []string                      `json:"envs"`
	FunctionLabels   map[string]string             `json:"labels"`
	Limits           *tinyserver.FunctionResources `json:"limits"`
}

func main() {
	logger := util.CreateLogger()
	mode := util.GetEnvOrDefault("ENV", defaultMode)
	port := util.GetEnvOrDefault("TINYFAAS_PORT", defaultPort)
	id := uuid.New().String()

	var backend tinyserver.Backend
	switch util.GetEnvOrDefault("BACKEND", "docker") {
	case "docker":
		backend = docker.New(id, logger)
	default:
		logger.Error("invalid backend")
		os.Exit(1)
	}

	rp := tinyserver.NewInvocationRouter(logger, mode)

	callGraphConfig, err := callgraph.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Error("failed to initialize callgraph config", "err", err)
		os.Exit(1)
	}
	tracker := callgraph.New(callgraph.WithLogger(logger), callgraph.WithConfig(&callGraphConfig))
	rp.SetTracker(tracker)
	tracker.Start()

	ms := tinyserver.NewManagementService(id, backend, logger)
	ms.SetRouteHooks(rp.Add, rp.Update, rp.Del)
	ms.SetCallgraphHooks(
		func(name string, timestamp time.Time, duration time.Duration, cold bool) {
			if rp.CallgraphEnabled(name) {
				tracker.RecordScaleUp(name, timestamp, duration, cold)
			}
		},
		func(name string, timestamp time.Time, duration time.Duration) {
			if rp.CallgraphEnabled(name) {
				tracker.RecordScaleDown(name, timestamp, duration)
			}
		},
		func(name string) {
			if rp.CallgraphEnabled(name) {
				tracker.ResetFunctionStats(name)
			}
		},
	)

	autoscalerConfig, err := autoscaler.NewConfigFromEnv("tinyfaas")
	if err != nil {
		logger.Error("failed to initialize autoscaler", "err", err)
		os.Exit(1)
	}
	scaleOp := tinyserver.NewTinyFaaSScaleOp(ms, logger)
	as := autoscaler.New(autoscalerConfig, scaleOp, logger)
	ms.SetAutoScaler(as)
	as.Start()
	rp.SetAutoScalerEnabled(autoscalerConfig.Enabled)
	rp.SetScaleUpHook(ms.ScaleUp)
	rp.SetRequestHooks(ms.StartRequest, ms.EndRequest)
	rp.SetHeartbeatHooks(ms.Heartbeat, ms.HeartbeatBatch)
	if autoscalerConfig.Enabled {
		rp.StartHeartbeatWorker()
	}

	publisherConfig := queue.NATSConfig{
		URL:       util.GetEnvOrDefault("TINYFAAS_NATS_URL", "nats://127.0.0.1:4222"),
		ClusterID: util.GetEnvOrDefault("TINYFAAS_NATS_CLUSTER", queue.DefaultClusterID),
		ClientID:  "tinyfaas-server-" + id,
		Subject:   util.GetEnvOrDefault("TINYFAAS_NATS_SUBJECT", queue.DefaultSubject),
	}
	publisher, err := newPublisherWithRetry(publisherConfig, logger, 30, time.Second)
	if err != nil {
		logger.Warn("async queue publisher unavailable", "err", err)
	}
	var publisherIface queue.Publisher = queue.NoopPublisher{}
	if publisher != nil {
		publisherIface = publisher
	}

	s := &service{
		ms:        ms,
		rp:        rp,
		tracker:   tracker,
		autoscale: as,
		queue:     publisherIface,
		stats:     invstats.NewStore(),
		logger:    logger,
	}

	mux := http.NewServeMux()
	s.register(mux)

	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: mux}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		logger.Info("tinyFaaS merged server starting", "address", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-sig
	logger.Info("shutdown requested")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	if autoscalerConfig.Enabled {
		rp.StopHeartbeatWorker()
	}
	tracker.Stop()
	as.Stop()
	_ = s.queue.Close()
	if err := ms.Stop(); err != nil {
		logger.Error("management service shutdown error", "err", err)
	}
}

func newPublisherWithRetry(config queue.NATSConfig, logger *slog.Logger, attempts int, delay time.Duration) (*queue.NATSPublisher, error) {
	var lastErr error
	for i := 1; i <= attempts; i++ {
		publisher, err := queue.NewNATSPublisher(config, logger)
		if err == nil {
			if i > 1 {
				logger.Info("async queue publisher connected", "attempt", i)
			}
			return publisher, nil
		}
		lastErr = err
		logger.Warn("async queue publisher connect failed", "attempt", i, "attempts", attempts, "err", err)
		time.Sleep(delay)
	}
	return nil, lastErr
}

func (s *service) register(mux *http.ServeMux) {
	mux.HandleFunc("/upload", s.uploadHandler)
	mux.HandleFunc("/delete", s.deleteHandler)
	mux.HandleFunc("/list", s.listHandler)
	mux.HandleFunc("/function/", s.functionHandler)
	mux.HandleFunc("/wipe", s.wipeHandler)
	mux.HandleFunc("/logs", s.logsHandler)
	mux.HandleFunc("/uploadURL", s.urlUploadHandler)
	mux.HandleFunc("/invoke/", s.invokeHandler)
	mux.HandleFunc("/async-invoke/", s.asyncInvokeHandler)
	mux.HandleFunc("/callgraph", s.callgraphHandler)
	mux.HandleFunc("/callgraph/function/", s.callgraphFunctionHandler)
	mux.HandleFunc("/callgraph/edge", s.callgraphEdgeHandler)
	mux.HandleFunc("/stats/function/", s.functionStatsHandler)
}

func (s *service) uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "failed to parse multipart upload request", http.StatusBadRequest)
		return
	}
	var meta *uploadMetadata
	var archivePath string
	defer func() {
		if archivePath != "" {
			_ = os.Remove(archivePath)
		}
	}()
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "failed to read upload request", http.StatusBadRequest)
			return
		}
		switch part.FormName() {
		case "metadata":
			var decoded uploadMetadata
			if err := json.NewDecoder(part).Decode(&decoded); err != nil {
				_ = part.Close()
				http.Error(w, "failed to decode upload metadata", http.StatusBadRequest)
				return
			}
			meta = &decoded
		case "zip":
			path, _, err := s.writeUploadArchive(part)
			if err != nil {
				_ = part.Close()
				http.Error(w, "failed to persist upload archive", http.StatusInternalServerError)
				return
			}
			archivePath = path
		default:
			_, _ = io.Copy(io.Discard, part)
		}
		_ = part.Close()
	}
	if meta == nil || archivePath == "" {
		http.Error(w, "missing upload data", http.StatusBadRequest)
		return
	}
	if meta.FunctionLabels == nil {
		meta.FunctionLabels = map[string]string{}
	}
	if err := s.ms.UploadArchive(meta.FunctionName, meta.FunctionEnv, meta.FunctionReplicas, archivePath, parseFunctionEnvs(meta.FunctionEnvs), meta.FunctionLabels, tinyserver.FunctionResourceRequest{Limits: meta.Limits}); err != nil {
		s.logger.Error("upload failed", "err", err)
		http.Error(w, "failed to upload function", http.StatusInternalServerError)
		return
	}
	s.stats.Reset(meta.FunctionName)
	fmt.Fprintf(w, "Function %s deployed\n", meta.FunctionName)
}

func (s *service) writeUploadArchive(src io.Reader) (string, int64, error) {
	if err := os.MkdirAll(tinyserver.TmpDir, 0777); err != nil {
		return "", 0, err
	}
	archiveFile, err := os.CreateTemp(tinyserver.TmpDir, "upload-*.zip")
	if err != nil {
		return "", 0, err
	}
	written, copyErr := io.Copy(archiveFile, src)
	closeErr := archiveFile.Close()
	if copyErr != nil {
		_ = os.Remove(archiveFile.Name())
		return "", 0, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(archiveFile.Name())
		return "", 0, closeErr
	}
	return archiveFile.Name(), written, nil
}

func parseFunctionEnvs(functionEnvs []string) map[string]string {
	envs := map[string]string{}
	for _, e := range functionEnvs {
		k, v, ok := strings.Cut(e, "=")
		if ok {
			envs[k] = v
		}
	}
	return envs
}

func (s *service) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}
	var d struct {
		FunctionName string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		http.Error(w, "failed to decode delete request", http.StatusBadRequest)
		return
	}
	if err := s.ms.Delete(d.FunctionName); err != nil {
		http.Error(w, "failed to delete function", http.StatusInternalServerError)
		return
	}
	s.stats.Reset(d.FunctionName)
	fmt.Fprintf(w, "Function %s deleted\n", d.FunctionName)
}

func (s *service) listHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.ms.List())
}

func (s *service) functionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/function/")
	fn, ok := s.ms.Get(name)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fn)
}

func (s *service) wipeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}
	if err := s.ms.Wipe(); err != nil {
		http.Error(w, "failed to wipe functions", http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, "All functions deleted\n")
}

func (s *service) logsHandler(w http.ResponseWriter, r *http.Request) {
	var reader io.Reader
	var err error
	if name := r.URL.Query().Get("name"); name != "" {
		reader, err = s.ms.LogsFunction(name)
	} else {
		reader, err = s.ms.Logs()
	}
	if err != nil {
		http.Error(w, "failed to get logs", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.Copy(w, reader)
}

func (s *service) urlUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "invalid method", http.StatusBadRequest)
		return
	}
	var d struct {
		FunctionName     string                        `json:"name"`
		FunctionEnv      string                        `json:"env"`
		FunctionReplicas int                           `json:"replicas"`
		FunctionURL      string                        `json:"url"`
		FunctionEnvs     []string                      `json:"envs"`
		SubFolder        string                        `json:"subfolder_path"`
		FunctionLabels   map[string]string             `json:"labels"`
		Limits           *tinyserver.FunctionResources `json:"limits"`
	}
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		http.Error(w, "failed to decode url upload request", http.StatusBadRequest)
		return
	}
	if d.FunctionLabels == nil {
		d.FunctionLabels = map[string]string{}
	}
	if err := s.ms.UrlUpload(d.FunctionName, d.FunctionEnv, d.FunctionReplicas, d.FunctionURL, d.SubFolder, parseFunctionEnvs(d.FunctionEnvs), d.FunctionLabels, tinyserver.FunctionResourceRequest{Limits: d.Limits}); err != nil {
		http.Error(w, "failed to upload function from url", http.StatusInternalServerError)
		return
	}
	s.stats.Reset(d.FunctionName)
	fmt.Fprintf(w, "Function %s deployed from url\n", d.FunctionName)
}

func (s *service) invokeHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/invoke/")
	if name == "" {
		http.Error(w, "function name required", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	r.Header.Del("X-Tinyfaas-Async")
	start := time.Now().UTC()
	status, resp := s.rp.Call(name, body, false, r.Header.Clone())
	finish := time.Now().UTC()
	s.stats.Record(name, invstats.InvocationRecord{
		StartedAt:  start,
		FinishedAt: finish,
		DurationNS: finish.Sub(start).Nanoseconds(),
		Method:     r.Method,
		Path:       r.URL.Path,
		StatusCode: status,
		Success:    status >= 200 && status < 400,
	})
	w.WriteHeader(status)
	if resp != nil {
		_, _ = w.Write(resp)
	}
}

func (s *service) asyncInvokeHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/async-invoke/")
	if name == "" {
		http.Error(w, "function name required", http.StatusBadRequest)
		return
	}
	if _, ok := s.ms.Get(name); !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	header := r.Header.Clone()
	header.Del("X-Tinyfaas-Async")
	if err := s.queue.Queue(&queue.Request{
		Header:      header,
		Host:        r.Host,
		Body:        body,
		Method:      r.Method,
		QueryString: r.URL.RawQuery,
		Function:    name,
	}); err != nil {
		s.logger.Error("async enqueue failed", "function", name, "err", err)
		http.Error(w, "failed to enqueue async invocation", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *service) callgraphHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.tracker.GetCallGraph())
}

func (s *service) callgraphFunctionHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/callgraph/function/")
	w.Header().Set("Content-Type", "application/json")
	if name == "" {
		_ = json.NewEncoder(w).Encode(s.tracker.GetAllFunctionStats())
		return
	}
	stats, ok := s.tracker.GetFunctionStats(name)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *service) callgraphEdgeHandler(w http.ResponseWriter, r *http.Request) {
	edge, ok := s.tracker.GetEdgeStats(r.URL.Query().Get("caller"), r.URL.Query().Get("callee"))
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(edge)
}

func (s *service) functionStatsHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/stats/function/")
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	_, exists := s.ms.Get(name)
	resp, ok := s.stats.Response(name, exists)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	invstats.WriteJSON(w, resp)
}
