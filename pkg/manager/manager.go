package manager

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"go.uber.org/zap"
)

var (
	// TmpDir can be overridden via TF_TMP_DIR environment variable
	TmpDir = getEnvOrDefault("TF_TMP_DIR", "./tmp")
)

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

type ManagementService struct {
	id               string
	backend          Backend
	functionHandlers map[string]Handler
	functionConfigs  map[string]FunctionConfig
	mux              sync.Mutex
	rproxyPort       string
	autoscaler       *autoscaler.AutoScaler
	logger           *zap.Logger
}

type Backend interface {
	Create(name string, env string, threads int, filedir string, envs map[string]string, labels map[string]string, limits ResourceLimits) (Handler, error)
	Stop() error
}

type Handler interface {
	IPs() []string
	Start() error
	Stop() error
	Restart() error
	Destroy() error
	Logs() (io.Reader, error)
	IsRunning() bool
	GetLabels() map[string]string
}

func New(id string, rproxyPort string, tfBackend Backend, logger *zap.Logger) *ManagementService {

	ms := &ManagementService{
		id:               id,
		backend:          tfBackend,
		functionHandlers: make(map[string]Handler),
		functionConfigs:  make(map[string]FunctionConfig),
		rproxyPort:       rproxyPort,
		logger:           logger,
	}

	return ms
}

func (ms *ManagementService) createFunction(name string, env string, replicas int, funczip []byte, subfolderPath string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {

	// validate function name according to RFC 1035 DNS label rules
	if !util.IsValidFunctionName(name) {
		return fmt.Errorf("function name %s is not valid (must be 1-63 lowercase alphanumeric characters or hyphens, cannot start or end with hyphen)", name)
	}

	// make a uuidv4 for the function
	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	ms.logger.Info("creating function", zap.String("name", name), zap.String("uuid", uuid.String()))

	// create a new function handler

	p := path.Join(TmpDir, uuid.String())

	err = os.MkdirAll(p, 0777)

	if err != nil {
		return err
	}

	ms.logger.Info("created folder", zap.String("path", p))

	// write zip to file
	zipPath := path.Join(TmpDir, uuid.String()+".zip")
	err = os.WriteFile(zipPath, funczip, 0777)

	if err != nil {
		return err
	}

	err = util.Unzip(zipPath, p)

	if err != nil {
		return err
	}

	defer func() {
		// remove folder
		err = os.RemoveAll(p)
		if err != nil {
			ms.logger.Error("error removing folder", zap.String("path", p), zap.Error(err))
		}

		err = os.Remove(zipPath)
		if err != nil {
			ms.logger.Error("error removing zip", zap.String("path", zipPath), zap.Error(err))
		}

		ms.logger.Info("cleanup completed", zap.String("path", p), zap.String("zipPath", zipPath))
	}()

	if subfolderPath != "" {
		p = path.Join(p, subfolderPath)
	}

	resolvedLimits, effectiveLimits, backendLimits, err := EffectiveResourceLimits(resources.Limits)
	if err != nil {
		return fmt.Errorf("invalid limits: %w", err)
	}
	// if function already exists, keep it while deploying the new version
	var oldHandler Handler
	ms.mux.Lock()
	if existingHandler, ok := ms.functionHandlers[name]; ok {
		oldHandler = existingHandler
	}
	ms.mux.Unlock()

	// create new function handler (image build - not counted as cold start)
	fh, err := ms.backend.Create(name, env, replicas, p, envs, labels, backendLimits)

	if err != nil {
		return err
	}

	// Measure cold start time (container creation and start)
	coldStartTime := time.Now()
	err = fh.Start()
	coldStartDuration := time.Since(coldStartTime)

	if err != nil {
		// container did not start properly...
		return err
	}

	ms.logger.Info("cold start completed",
		zap.String("function", name),
		zap.Duration("duration", coldStartDuration))

	// tell rproxy about the new function
	// curl -X PUT http://<rproxyAddr>:<rproxyPort>/config -d '{"name": "<name>", "ips": ["<ip1>", "<ip2>"]}'
	d := struct {
		FunctionName string            `json:"name"`
		FunctionIPs  []string          `json:"ips"`
		Labels       map[string]string `json:"labels,omitempty"`
	}{
		FunctionName: name,
		FunctionIPs:  fh.IPs(),
		Labels:       labels,
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	ms.logger.Info("notify rproxy", zap.String("function", name), zap.Strings("ips", fh.IPs()))

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://127.0.0.1:%s/config", ms.rproxyPort), bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil && !errors.Is(err, io.EOF) {
		ms.logger.Error("error notifying rproxy", zap.String("function", name), zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	r, err := io.ReadAll(resp.Body)
	if err != nil {
		ms.logger.Error("error reading rproxy response", zap.String("function", name), zap.Error(err))
		return err
	}

	ms.logger.Info("rproxy response", zap.String("response", string(r)))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to notify rproxy, status code %d", resp.StatusCode)
	}

	// If this is a redeployment, reset callgraph stats first before recording fresh cold start
	if oldHandler != nil {
		ms.logger.Info("notifying rproxy of callgraph reset for redeployment", zap.String("function", name))
		ms.notifyCallgraphReset(name)
	}

	// Send scale-up data to rproxy for callgraph tracking, marked as cold start since this is a new function
	ms.notifyScaleUp(name, coldStartTime, coldStartDuration, true)

	config := FunctionConfig{
		Name:            name,
		Env:             env,
		Replicas:        replicas,
		Envs:            envs,
		Labels:          labels,
		Limits:          resolvedLimits,
		EffectiveLimits: effectiveLimits,
		Running:         fh.IsRunning(),
	}

	ms.mux.Lock()
	ms.functionHandlers[name] = fh
	ms.functionConfigs[name] = config
	ms.mux.Unlock()

	// Register with autoscaler
	if ms.autoscaler != nil {
		// overwrite registration if function already exists
		ms.autoscaler.RegisterFunction(name, labels)
	}

	// destroy the old handler if it exists
	if oldHandler != nil {
		err = oldHandler.Destroy()
		if err != nil {
			return err
		}
	}

	return nil
}

func (ms *ManagementService) Logs() (io.Reader, error) {

	var logs bytes.Buffer
	ms.logger.Info("collecting logs from all functions")
	ms.mux.Lock()
	names := make([]string, 0, len(ms.functionHandlers))
	for name := range ms.functionHandlers {
		names = append(names, name)
	}
	ms.mux.Unlock()

	for _, name := range names {
		l, err := ms.LogsFunction(name)
		if err != nil {
			ms.logger.Error("error getting logs for function", zap.String("function", name), zap.Error(err))
			return nil, err
		}

		_, err = io.Copy(&logs, l)
		if err != nil {
			return nil, err
		}

		logs.WriteString("\n")
	}

	return &logs, nil
}

func (ms *ManagementService) LogsFunction(name string) (io.Reader, error) {

	ms.mux.Lock()
	fh, ok := ms.functionHandlers[name]
	ms.mux.Unlock()
	if !ok {
		return nil, fmt.Errorf("function %s not found", name)
	}

	return fh.Logs()
}

func (ms *ManagementService) List() []FunctionConfig {
	ms.logger.Info("listing functions")

	ms.mux.Lock()
	list := make([]FunctionConfig, 0, len(ms.functionHandlers))
	for name, h := range ms.functionHandlers {
		cfg, ok := ms.functionConfigs[name]
		if !ok {
			cfg = FunctionConfig{Name: name}
		}
		cfg.Running = h.IsRunning()
		list = append(list, cfg)
	}
	ms.mux.Unlock()

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return list
}

func (ms *ManagementService) Wipe() error {
	ms.logger.Info("wiping all functions")

	ms.mux.Lock()
	names := make([]string, 0, len(ms.functionHandlers))
	for name := range ms.functionHandlers {
		names = append(names, name)
	}
	ms.mux.Unlock()

	for _, name := range names {
		ms.logger.Info("destroying function", zap.String("function", name))
		_ = ms.Delete(name)
	}

	return nil
}

func (ms *ManagementService) Delete(name string) error {
	ms.mux.Lock()
	fh, ok := ms.functionHandlers[name]
	if !ok {
		ms.mux.Unlock()
		return fmt.Errorf("function %s not found", name)
	}

	ms.logger.Info("deleting function", zap.String("function", name))
	defer ms.mux.Unlock()

	err := fh.Destroy()
	if err != nil {
		return err
	}

	// tell rproxy about the delete function
	d := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	ms.logger.Info("notify rproxy", zap.String("function", name))
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://127.0.0.1:%s/config", ms.rproxyPort), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	defer resp.Body.Close()

	r, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	ms.logger.Info("rproxy response", zap.String("response", string(r)))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rproxy returned status code %d", resp.StatusCode)
	}

	delete(ms.functionHandlers, name)
	delete(ms.functionConfigs, name)
	ms.logger.Info("function deleted", zap.String("function", name))

	// Unregister from autoscaler
	if ms.autoscaler != nil {
		ms.autoscaler.UnregisterFunction(name)
	}

	return nil
}

func (ms *ManagementService) Upload(name string, env string, threads int, zipped string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {

	// b64 decode zip
	zip, err := base64.StdEncoding.DecodeString(zipped)
	if err != nil {
		ms.logger.Error("error decoding base64 zip", zap.Error(err))
		return err
	}

	// create function handler
	err = ms.createFunction(name, env, threads, zip, "", envs, labels, resources)

	if err != nil {
		ms.logger.Error("error creating function", zap.String("function", name), zap.Error(err))
		return err
	}

	return nil
}

func (ms *ManagementService) UrlUpload(name string, env string, replicas int, funcurl string, subfolder string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {

	// download url
	resp, err := http.Get(funcurl)
	if err != nil {
		// w.WriteHeader(http.StatusBadRequest)
		ms.logger.Error("error downloading function zip", zap.String("url", funcurl), zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	// reading body to memory
	// not the smartest thing
	zip, err := io.ReadAll(resp.Body)

	if err != nil {
		ms.logger.Error("error reading function zip", zap.String("url", funcurl), zap.Error(err))
		return err
	}

	// create function handler
	err = ms.createFunction(name, env, replicas, zip, subfolder, envs, labels, resources)

	if err != nil {
		ms.logger.Error("error creating function", zap.String("function", name), zap.Error(err))
		return err
	}

	return nil
}

// notifyScaleUp sends scale-up or cold start data to rproxy for callgraph tracking
func (ms *ManagementService) notifyScaleUp(name string, timestamp time.Time, duration time.Duration, cold bool) {
	data := struct {
		FunctionName string `json:"name"`
		Timestamp    int64  `json:"timestamp"`   // Unix nanoseconds
		Duration     int64  `json:"duration_ns"` // Nanoseconds
		Cold         bool   `json:"cold"`        // Whether this is a cold start
	}{
		FunctionName: name,
		Timestamp:    timestamp.UnixNano(),
		Duration:     duration.Nanoseconds(),
		Cold:         cold, // Whether this is a cold start
	}

	body, err := json.Marshal(data)
	if err != nil {
		ms.logger.Error("failed to marshal scale-up data", zap.String("function", name), zap.Error(err))
		return
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/callgraph/scaleup", ms.rproxyPort), bytes.NewBuffer(body))
	if err != nil {
		ms.logger.Error("failed to create scale-up request", zap.String("function", name), zap.Bool("cold", cold), zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		ms.logger.Error("failed to notify rproxy of scale-up", zap.String("function", name), zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ms.logger.Warn("rproxy scale-up notification failed",
			zap.String("function", name),
			zap.Int("statusCode", resp.StatusCode),
			zap.Bool("cold", cold))
	} else {
		ms.logger.Debug("notified rproxy of scale-up",
			zap.String("function", name),
			zap.Duration("duration", duration),
			zap.Bool("cold", cold))
	}
}

// notifyScaleDown sends scale-down data to rproxy for callgraph tracking
func (ms *ManagementService) notifyScaleDown(name string, timestamp time.Time, duration time.Duration) {
	data := struct {
		FunctionName string `json:"name"`
		Timestamp    int64  `json:"timestamp"`   // Unix nanoseconds
		Duration     int64  `json:"duration_ns"` // Nanoseconds
	}{
		FunctionName: name,
		Timestamp:    timestamp.UnixNano(),
		Duration:     duration.Nanoseconds(),
	}

	body, err := json.Marshal(data)
	if err != nil {
		ms.logger.Error("failed to marshal scale-down data", zap.String("function", name), zap.Error(err))
		return
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/callgraph/scaledown", ms.rproxyPort), bytes.NewBuffer(body))
	if err != nil {
		ms.logger.Error("failed to create scale-down request", zap.String("function", name), zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		ms.logger.Error("failed to notify rproxy of scale-down", zap.String("function", name), zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ms.logger.Warn("rproxy scale-down notification failed",
			zap.String("function", name),
			zap.Int("statusCode", resp.StatusCode))
	} else {
		ms.logger.Debug("notified rproxy of scale-down",
			zap.String("function", name),
			zap.Duration("duration", duration))
	}
}

// notifyCallgraphReset notifies rproxy to reset callgraph stats for a redeployed function
func (ms *ManagementService) notifyCallgraphReset(name string) {
	data := struct {
		FunctionName string `json:"name"`
	}{
		FunctionName: name,
	}

	body, err := json.Marshal(data)
	if err != nil {
		ms.logger.Error("failed to marshal callgraph reset data", zap.String("function", name), zap.Error(err))
		return
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/callgraph/reset", ms.rproxyPort), bytes.NewBuffer(body))
	if err != nil {
		ms.logger.Error("failed to create callgraph reset request", zap.String("function", name), zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		ms.logger.Error("failed to notify rproxy of callgraph reset", zap.String("function", name), zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ms.logger.Warn("rproxy callgraph reset notification failed",
			zap.String("function", name),
			zap.Int("statusCode", resp.StatusCode))
	} else {
		ms.logger.Info("notified rproxy of callgraph reset for redeployment",
			zap.String("function", name))
	}
}

func (ms *ManagementService) Stop() error {
	err := ms.Wipe()
	if err != nil {
		return err
	}

	return ms.backend.Stop()
}
