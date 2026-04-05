package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/containerd/errdefs"
	"go.uber.org/zap"

	"github.com/OpenFogStack/tinyFaaS/pkg/util"
)

const (
	TmpDir                = "./tmp"
	DEFAULT_PUBLIC_DOMAIN = "tinyfaas.com"
	containerIPTimeout    = 10 * time.Second
	containerIPInterval   = 25 * time.Millisecond

	containerReadyTimeout         = 10 * time.Second
	containerHealthRequestTimeout = 100 * time.Millisecond
	containerHealthInitialDelay   = 50 * time.Millisecond
	containerHealthMaxDelay       = 250 * time.Millisecond
)

// List of supported runtimes (must match directories in pkg/docker/runtimes)
var RUNTIMES = []string{"binary", "go", "nodejs", "python3"}

type dockerHandler struct {
	name             string
	env              string
	replicas         int
	nanoCPUs         int64
	memoryBytes      int64
	uniqueName       string
	filePath         string
	client           *client.Client
	network          string // per-function network ID
	networkName      string // per-function network name
	containers       []string
	handlerIPs       []string
	isRunning        bool               // track if containers are running
	opMux            sync.Mutex         // mutex for start/stop/restart operations
	labels           map[string]string  // store user-provided labels (for GetLabels())
	containerLabels  map[string]string  // store merged labels (system + user) for container creation
	envVars          []string           // store environment variables for container recreation
	extraHosts       []string           // store extra hosts for container recreation
	containerRemover func(string) error // optional override for tests
	ipInspector      func(string) (string, error)
	healthChecker    func(string) error
	networkRemover   func() error // optional override for tests
	imageRemover     func() error // optional override for tests
	logger           *zap.Logger
}

type readinessResult struct {
	index int
	ip    string
	err   error
}

func (dh *dockerHandler) IPs() []string {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	return dh.handlerIPs
}

func (dh *dockerHandler) Start() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	ctx := context.Background()

	if dh.isRunning {
		dh.logger.Info("function is already started", zap.String("name", dh.name))
		return nil
	}

	// Clear any old container IDs
	dh.containers = make([]string, 0, dh.replicas)
	dh.handlerIPs = make([]string, 0, dh.replicas)

	// Create containers from image
	for i := 0; i < dh.replicas; i++ {
		dh.logger.Info("creating function container", zap.String("name", dh.name), zap.Int("replica", i+1))
		containerResp, err := dh.client.ContainerCreate(
			ctx,
			client.ContainerCreateOptions{
				Config: &container.Config{
					Image:  dh.uniqueName,
					Labels: dh.containerLabels,
					Env:    dh.envVars,
				},
				HostConfig: &container.HostConfig{
					NetworkMode: container.NetworkMode(dh.networkName),
					ExtraHosts:  dh.extraHosts,
					Resources: container.Resources{
						NanoCPUs: dh.nanoCPUs,
						Memory:   dh.memoryBytes,
					},
				},
				Name: dh.uniqueName + fmt.Sprintf("-%d", i),
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create container %d: %w", i, err)
		}
		dh.containers = append(dh.containers, containerResp.ID)
	}

	for _, cid := range dh.containers {
		// Start container
		dh.logger.Info("starting container", zap.String("ID", util.GetShortID(cid)))
		_, err := dh.client.ContainerStart(
			context.Background(),
			cid,
			client.ContainerStartOptions{},
		)
		if err != nil {
			dh.logger.Error("error starting container", zap.Error(err))
			return fmt.Errorf("failed to start container %s: %w", util.GetShortID(cid), err)
		}
	}

	results := make(chan readinessResult, len(dh.containers))

	for i, cid := range dh.containers {
		go func(index int, containerID string) {
			ip, err := dh.waitForContainerIP(containerID, containerIPTimeout)
			if err != nil {
				results <- readinessResult{index: index, err: err}
				return
			}

			dh.logger.Info("container IP", zap.String("ip", ip))
			dh.logger.Info("waiting for container to be ready", zap.String("ip", ip))

			err = dh.waitForContainerReady(ip, containerID, containerReadyTimeout)
			if err != nil {
				results <- readinessResult{index: index, ip: ip, err: err}
				return
			}

			results <- readinessResult{index: index, ip: ip}
		}(i, cid)
	}

	handlerIPs := make([]string, len(dh.containers))
	readinessErrors := make([]error, 0)

	for range dh.containers {
		res := <-results
		if res.err != nil {
			containerID := dh.containers[res.index]
			dh.logger.Error(
				"container failed readiness checks",
				zap.String("ID", util.GetShortID(containerID)),
				zap.String("ip", res.ip),
				zap.Error(res.err),
			)

			logs, logErr := dh.getContainerLogs(containerID)
			if logErr != nil {
				dh.logger.Error("error getting logs for container", zap.String("ID", util.GetShortID(containerID)), zap.Error(logErr))
			} else {
				dh.logger.Info("logs for container", zap.String("ID", util.GetShortID(containerID)), zap.String("logs", logs))
			}

			readinessErrors = append(readinessErrors, fmt.Errorf("container %s: %w", util.GetShortID(containerID), res.err))
			continue
		}

		handlerIPs[res.index] = res.ip
	}

	if len(readinessErrors) > 0 {
		dh.handlerIPs = nil
		return fmt.Errorf("container readiness failed: %w", errors.Join(readinessErrors...))
	}

	dh.handlerIPs = handlerIPs

	dh.logger.Debug("health check completed", zap.Int("total", len(dh.handlerIPs)))
	dh.isRunning = true
	dh.logger.Info("all containers started", zap.Int("count", len(dh.containers)))
	return nil
}

func (dh *dockerHandler) waitForContainerIP(containerID string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		ip, err := dh.inspectContainerIP(containerID)
		if err == nil {
			return ip, nil
		}

		lastErr = err
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out waiting for container %s IP after %s: %w", util.GetShortID(containerID), timeout, lastErr)
		}

		time.Sleep(containerIPInterval)
	}
}

func (dh *dockerHandler) inspectContainerIP(containerID string) (string, error) {
	if dh.ipInspector != nil {
		return dh.ipInspector(containerID)
	}

	inspectRes, err := dh.client.ContainerInspect(
		context.Background(),
		containerID,
		client.ContainerInspectOptions{},
	)
	if err != nil {
		return "", err
	}

	networks := inspectRes.Container.NetworkSettings.Networks
	if len(networks) == 0 {
		return "", errors.New("container network settings not ready")
	}

	networkSettings, ok := networks[dh.networkName]
	if !ok {
		return "", fmt.Errorf("container network %s not attached yet", dh.networkName)
	}

	ip := networkSettings.IPAddress
	if !ip.IsValid() {
		return "", errors.New("container IP address not assigned yet")
	}

	return ip.String(), nil
}

func (dh *dockerHandler) waitForContainerReady(ip string, containerID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	attempt := 0
	var lastErr error

	for {
		err := dh.probeContainerHealth(ip)
		if err == nil {
			dh.logger.Info("container is ready", zap.String("ip", ip))
			return nil
		}

		lastErr = err
		dh.logger.Debug(
			"ready check attempt failed",
			zap.Int("attempt", attempt+1),
			zap.String("ip", ip),
			zap.Error(err),
		)

		if time.Now().After(deadline) {
			return fmt.Errorf("container %s failed readiness checks after %s: %w", util.GetShortID(containerID), timeout, lastErr)
		}

		delay := readinessBackoffDelay(attempt)
		attempt++
		remaining := time.Until(deadline)
		if delay > remaining {
			delay = remaining
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func (dh *dockerHandler) probeContainerHealth(ip string) error {
	if dh.healthChecker != nil {
		return dh.healthChecker(ip)
	}

	httpClient := http.Client{Timeout: containerHealthRequestTimeout}
	resp, err := httpClient.Get("http://" + ip + ":8000/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("container health probe failed with status: %s", resp.Status)
	}

	return nil
}

// Calculate exponential backoff with full jitter for readiness checks
func readinessBackoffDelay(attempt int) time.Duration {
	delay := containerHealthInitialDelay

	// Calculate exponential backoff
	// Ensure we don't overflow before capping at MaxDelay
	for i := 0; i < attempt && delay < containerHealthMaxDelay; i++ {
		delay *= 2
	}

	if delay > containerHealthMaxDelay {
		delay = containerHealthMaxDelay
	}

	// Apply Full Jitter
	// rand.Int64n returns a value in [0, delay)
	if delay <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(delay)))
}

// StopContainers stops all function containers (used by cleanup during startup failures)
func (dh *dockerHandler) StopContainers() {
	dh.logger.Info("stopping all function containers", zap.String("name", dh.name))
	wg := sync.WaitGroup{}
	for _, c := range dh.containers {
		wg.Add(1)
		go func(cid string) {
			defer wg.Done()

			_, err := dh.client.ContainerStop(
				context.Background(),
				cid,
				// default using timeout of 10 seconds
				client.ContainerStopOptions{},
			)
			if err != nil {
				dh.logger.Error("error stopping container", zap.String("ID", cid), zap.Error(err))
			} else {
				dh.logger.Info("container stopped")
			}
		}(c)
	}
	wg.Wait()
	dh.handlerIPs = nil
	dh.isRunning = false
}

// removeContainers stops and removes all function containers (used by scale-to-zero)
// This frees up system resources by completely removing the containers
func (dh *dockerHandler) removeContainers() error {
	remove := dh.containerRemover
	if remove == nil {
		remove = func(cid string) error {
			_, err := dh.client.ContainerRemove(
				context.Background(),
				cid,
				client.ContainerRemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				},
			)
			return err
		}
	}

	return dh.cleanupContainers(remove)
}

func (dh *dockerHandler) Destroy() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	dh.logger.Info("destroying function", zap.String("name", dh.name))

	var cleanupErrors []error
	if err := dh.removeContainers(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	// Remove the per-function network
	if dh.network != "" {
		networkRemove := dh.networkRemover
		if networkRemove == nil {
			networkRemove = func() error {
				_, err := dh.client.NetworkRemove(
					context.Background(),
					dh.network,
					client.NetworkRemoveOptions{},
				)
				return err
			}
		}

		err := networkRemove()
		if err != nil {
			if isNotFoundError(err) {
				dh.logger.Info("network already removed", zap.String("network", util.GetShortID(dh.network)))
			} else {
				dh.logger.Error("error removing network", zap.String("network", util.GetShortID(dh.network)), zap.Error(err))
				cleanupErrors = append(cleanupErrors, fmt.Errorf("network remove: %w", err))
			}
		} else {
			dh.logger.Info("network removed", zap.String("network", util.GetShortID(dh.network)))
		}
		dh.network = ""
		dh.networkName = ""
	}

	// remove image
	// docker rmi <image>
	imageRemove := dh.imageRemover
	if imageRemove == nil {
		imageRemove = func() error {
			_, err := dh.client.ImageRemove(
				context.Background(),
				dh.uniqueName,
				client.ImageRemoveOptions{
					Force:         true,
					PruneChildren: true,
				},
			)
			return err
		}
	}
	err := imageRemove()

	if err != nil {
		if isNotFoundError(err) {
			dh.logger.Info("image already removed", zap.String("name", dh.name))
		} else {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("image remove: %w", err))
		}
	} else {
		dh.logger.Info("image removed", zap.String("name", dh.name))
	}

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("destroy cleanup failed: %w", errors.Join(cleanupErrors...))
	}

	return nil
}

func (dh *dockerHandler) cleanupContainers(removeFn func(string) error) error {
	failedContainers := make([]string, 0)
	cleanupErrors := make([]error, 0)

	for _, cid := range dh.containers {
		err := removeFn(cid)
		if err != nil {
			if isNotFoundError(err) {
				dh.logger.Info("container already removed", zap.String("ID", util.GetShortID(cid)))
				continue
			}

			dh.logger.Error("error removing container", zap.String("ID", util.GetShortID(cid)), zap.Error(err))
			failedContainers = append(failedContainers, cid)
			cleanupErrors = append(cleanupErrors, fmt.Errorf("container %s: %w", util.GetShortID(cid), err))
			continue
		}

		dh.logger.Info("container removed", zap.String("ID", util.GetShortID(cid)))
	}

	dh.containers = failedContainers
	dh.handlerIPs = nil
	dh.isRunning = false

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("container cleanup failed: %w", errors.Join(cleanupErrors...))
	}

	return nil
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return errdefs.IsNotFound(err)
}

func (dh *dockerHandler) getContainerLogs(c string) (string, error) {
	l, err := dh.client.ContainerLogs(
		context.Background(),
		c,
		client.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Timestamps: true,
		},
	)
	if err != nil {
		return "", err
	}
	defer l.Close()

	var lstdout bytes.Buffer
	var lstderr bytes.Buffer

	_, err = stdcopy.StdCopy(&lstdout, &lstderr, l)
	if err != nil {
		return "", err
	}

	// Combine stdout and stderr
	var logs bytes.Buffer
	logs.WriteString(lstdout.String())
	logs.WriteString(lstderr.String())

	return logs.String(), nil
}

func (dh *dockerHandler) Logs() (io.Reader, error) {
	// get container logs
	// docker logs <container>
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	var logs bytes.Buffer

	for i, c := range dh.containers {
		if len(dh.containers) > 1 {
			if i > 0 {
				logs.WriteString("\n")
			}
			logs.WriteString(fmt.Sprintf("====================> Container %d/%d: %s \n", i+1, len(dh.containers), c))
		}

		l, err := dh.getContainerLogs(c)
		if err != nil {
			return nil, err
		}

		logs.WriteString(l)

		if len(dh.containers) > 1 {
			logs.WriteString(fmt.Sprintf("====================> End of Container %d/%d \n", i+1, len(dh.containers)))
		}
	}

	return &logs, nil
}

// Stop stops and deletes the function containers (for scale-to-zero)
// Containers are removed to free up system resources, and will be recreated on scale-up
func (dh *dockerHandler) Stop() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()

	if !dh.isRunning && len(dh.containers) == 0 {
		dh.logger.Info("function is already stopped", zap.String("name", dh.name))
		return nil
	}

	dh.logger.Info("removing function containers", zap.String("name", dh.name))
	if err := dh.removeContainers(); err != nil {
		return err
	}
	dh.logger.Info("function scaled down", zap.String("name", dh.name))
	return nil
}

// Restart recreates and starts function containers from cached image (for scale-up from scale-to-zero)
func (dh *dockerHandler) Restart() error {
	// Restart is now identical to Start since both create containers from the cached image
	// and start them. We delegate to Start() for consistent behavior and better error handling.
	return dh.Start()
}

// IsRunning returns whether the function containers are currently running
func (dh *dockerHandler) IsRunning() bool {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	return dh.isRunning
}

// GetLabels returns the container labels for this function
func (dh *dockerHandler) GetLabels() map[string]string {
	return dh.labels
}
