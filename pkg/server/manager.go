package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	tflogs "github.com/OpenFogStack/tinyFaaS/pkg/logs"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler"
	"log/slog"
)

var (
	// TmpDir can be overridden via TMP_DIR environment variable
	TmpDir = util.GetEnvOrDefault("TMP_DIR", "/var/lib/tinyfaas/tmp")
)

type ManagementService struct {
	id               string
	backend          Backend
	functionHandlers map[string]Handler
	functionConfigs  map[string]FunctionConfig
	scaleUpClaims    map[string]struct{}
	mux              sync.Mutex
	autoscaler       *autoscaler.AutoScaler
	logger           *slog.Logger
	routeAddHook     func(name string, ips []string, labels map[string]string) error
	routeUpdateHook  func(name string) error
	routeDeleteHook  func(name string) error
	scaleUpHook      func(name string, timestamp time.Time, duration time.Duration, cold bool)
	scaleDownHook    func(name string, timestamp time.Time, duration time.Duration)
	resetHook        func(name string)
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

func NewManagementService(id string, tfBackend Backend, logger *slog.Logger) *ManagementService {

	ms := &ManagementService{
		id:               id,
		backend:          tfBackend,
		functionHandlers: make(map[string]Handler),
		functionConfigs:  make(map[string]FunctionConfig),
		scaleUpClaims:    make(map[string]struct{}),
		logger:           logger,
	}

	return ms
}

func (ms *ManagementService) SetRouteHooks(add func(name string, ips []string, labels map[string]string) error, update func(name string) error, del func(name string) error) {
	ms.routeAddHook = add
	ms.routeUpdateHook = update
	ms.routeDeleteHook = del
}

func (ms *ManagementService) SetCallgraphHooks(
	scaleUp func(name string, timestamp time.Time, duration time.Duration, cold bool),
	scaleDown func(name string, timestamp time.Time, duration time.Duration),
	reset func(name string),
) {
	ms.scaleUpHook = scaleUp
	ms.scaleDownHook = scaleDown
	ms.resetHook = reset
}

func createTempArchiveFile(prefix string) (*os.File, error) {
	if err := os.MkdirAll(TmpDir, 0777); err != nil {
		return nil, err
	}

	return os.CreateTemp(TmpDir, prefix+"-*.zip")
}

func (ms *ManagementService) createFunction(name string, env string, replicas int, archivePath string, subfolderPath string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {

	// validate function name according to RFC 1035 DNS label rules
	if !util.IsValidFunctionName(name) {
		return fmt.Errorf("function name %s is not valid (must be 1-63 lowercase alphanumeric characters or hyphens, cannot start or end with hyphen)", name)
	}

	// make a uuidv4 for the function
	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	ms.logger.Info("creating function", "name", name, "uuid", uuid.String())

	tempDir := path.Join(TmpDir, uuid.String())

	err = os.MkdirAll(tempDir, 0777)

	if err != nil {
		return err
	}

	ms.logger.Info("created folder", "path", tempDir, "archivePath", archivePath)

	err = util.Unzip(archivePath, tempDir, ms.logger)

	if err != nil {
		return err
	}

	defer func() {
		// remove folder
		err = os.RemoveAll(tempDir)
		if err != nil {
			ms.logger.Error("error removing folder", "path", tempDir, "err", err)
		}

		ms.logger.Info("cleanup completed", "path", tempDir, "archivePath", archivePath)
	}()

	if subfolderPath != "" {
		tempDir = path.Join(tempDir, subfolderPath)
	}

	resolvedLimits, effectiveLimits, backendLimits, err := EffectiveResourceLimits(resources.Limits)
	if err != nil {
		return fmt.Errorf("invalid limits: %w", err)
	}

	// If the function already exists, keep it serving while the replacement is prepared.
	var oldHandler Handler
	ms.mux.Lock()
	if existingHandler, ok := ms.functionHandlers[name]; ok {
		oldHandler = existingHandler
	}
	ms.mux.Unlock()

	// Prepare the replacement handler before touching the current runtime.
	fh, err := ms.backend.Create(name, env, replicas, tempDir, envs, labels, backendLimits)

	if err != nil {
		return err
	}

	if oldHandler != nil && ms.autoscaler != nil && ms.autoscaler.IsEnabled() {
		if err := ms.autoscaler.ScaleDownWhenIdle(name); err != nil {
			_ = fh.Destroy()
			return fmt.Errorf("cannot safely scale down function %s for redeploy: %w", name, err)
		}
	}

	// Measure cold start time for the replacement runtime start.
	coldStartTime := time.Now()
	err = fh.Start()
	coldStartDuration := time.Since(coldStartTime)

	if err != nil {
		// deployment start failed; clean up unpublished replacement handler resources
		ms.logger.Error("failed to start function containers", "function", name, "err", err)
		cleanupErr := fh.Destroy()
		if cleanupErr != nil {
			ms.logger.Error("failed to cleanup replacement handler after start failure", "function", name, "err", cleanupErr)
			return errors.Join(err, fmt.Errorf("cleanup failed: %w", cleanupErr))
		}
		return err
	}

	ms.logger.Info("cold start completed",
		"function", name,
		"duration", coldStartDuration)

	if err := ms.notifyRouteAdd(name, fh.IPs(), labels); err != nil {
		return err
	}

	// If this is a redeployment, reset callgraph stats first before recording fresh cold start
	if oldHandler != nil {
		ms.logger.Info("notifying callgraph tracker of reset for redeployment", "function", name)
		ms.notifyCallgraphReset(name)
	}

	// Record scale-up data for callgraph tracking, marked as cold start since this is a new function
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
			ms.logger.Error("error getting logs for function", "function", name, "err", err)
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
	_, ok := ms.functionHandlers[name]
	ms.mux.Unlock()
	if !ok {
		return nil, fmt.Errorf("function %s not found", name)
	}

	return tflogs.ReadFunction(name)
}

func (ms *ManagementService) Get(name string) (FunctionConfig, bool) {
	ms.mux.Lock()
	defer ms.mux.Unlock()

	h, ok := ms.functionHandlers[name]
	if !ok {
		return FunctionConfig{}, false
	}
	cfg, ok := ms.functionConfigs[name]
	if !ok {
		cfg = FunctionConfig{Name: name}
	}
	cfg.Running = h.IsRunning()
	return cfg, true
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
		ms.logger.Info("destroying function", "function", name)
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

	ms.logger.Info("deleting function", "function", name)
	defer ms.mux.Unlock()

	err := fh.Destroy()
	if err != nil {
		return err
	}

	if err := ms.notifyRouteDelete(name); err != nil {
		return err
	}

	delete(ms.functionHandlers, name)
	delete(ms.functionConfigs, name)
	ms.logger.Info("function deleted", "function", name)

	// Unregister from autoscaler
	if ms.autoscaler != nil {
		ms.autoscaler.UnregisterFunction(name)
	}

	return nil
}

func (ms *ManagementService) UploadArchive(name string, env string, threads int, archivePath string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {
	err := ms.createFunction(name, env, threads, archivePath, "", envs, labels, resources)

	if err != nil {
		ms.logger.Error("error creating function", "function", name, "err", err)
		return err
	}

	return nil
}

func (ms *ManagementService) UrlUpload(name string, env string, replicas int, funcurl string, subfolder string, envs map[string]string, labels map[string]string, resources FunctionResourceRequest) error {

	// download url
	resp, err := http.Get(funcurl)
	if err != nil {
		// w.WriteHeader(http.StatusBadRequest)
		ms.logger.Error("error downloading function zip", "url", funcurl, "err", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		ms.logger.Error("error downloading function zip", "url", funcurl, "statusCode", resp.StatusCode)
		return fmt.Errorf("unexpected status code downloading function zip: %d", resp.StatusCode)
	}

	archiveFile, err := createTempArchiveFile("url-upload")
	if err != nil {
		ms.logger.Error("error creating temporary archive file", "url", funcurl, "err", err)
		return err
	}
	defer func() {
		if err := os.Remove(archiveFile.Name()); err != nil && !os.IsNotExist(err) {
			ms.logger.Error("error removing temporary archive file", "path", archiveFile.Name(), "err", err)
		}
	}()

	bytesWritten, err := io.Copy(archiveFile, resp.Body)
	closeErr := archiveFile.Close()
	if err != nil {
		ms.logger.Error("error reading function zip", "url", funcurl, "err", err)
		return err
	}
	if closeErr != nil {
		ms.logger.Error("error closing temporary archive file", "path", archiveFile.Name(), "err", closeErr)
		return closeErr
	}

	ms.logger.Info("downloaded function archive", "url", funcurl, "bytes", bytesWritten, "path", archiveFile.Name())

	// create function handler
	err = ms.createFunction(name, env, replicas, archiveFile.Name(), subfolder, envs, labels, resources)

	if err != nil {
		ms.logger.Error("error creating function", "function", name, "err", err)
		return err
	}

	return nil
}

func (ms *ManagementService) notifyRouteAdd(name string, ips []string, labels map[string]string) error {
	if ms.routeAddHook != nil {
		return ms.routeAddHook(name, ips, labels)
	}
	return fmt.Errorf("route add hook not configured")
}

func (ms *ManagementService) notifyRouteUpdate(name string) error {
	if ms.routeUpdateHook != nil {
		return ms.routeUpdateHook(name)
	}
	return fmt.Errorf("route update hook not configured")
}

func (ms *ManagementService) notifyRouteDelete(name string) error {
	if ms.routeDeleteHook != nil {
		return ms.routeDeleteHook(name)
	}
	return fmt.Errorf("route delete hook not configured")
}

func (ms *ManagementService) notifyScaleUp(name string, timestamp time.Time, duration time.Duration, cold bool) {
	if ms.scaleUpHook != nil {
		ms.scaleUpHook(name, timestamp, duration, cold)
	} else {
		ms.logger.Debug("scale-up callgraph hook not configured", "function", name)
	}
}

func (ms *ManagementService) notifyScaleDown(name string, timestamp time.Time, duration time.Duration) {
	if ms.scaleDownHook != nil {
		ms.scaleDownHook(name, timestamp, duration)
	} else {
		ms.logger.Debug("scale-down callgraph hook not configured", "function", name)
	}
}

func (ms *ManagementService) notifyCallgraphReset(name string) {
	if ms.resetHook != nil {
		ms.resetHook(name)
	} else {
		ms.logger.Debug("callgraph reset hook not configured", "function", name)
	}
}

func (ms *ManagementService) Stop() error {
	err := ms.Wipe()
	if err != nil {
		return err
	}

	return ms.backend.Stop()
}
