package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"log/slog"

	"github.com/containerd/errdefs"

	tflogs "github.com/OpenFogStack/tinyFaaS/pkg/logs"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
)

const (
	TmpDir                = "./tmp"
	DEFAULT_PUBLIC_DOMAIN = "tinyfaas.com"
	containerIPTimeout    = 10 * time.Second
	containerIPInterval   = 25 * time.Millisecond

	functionLogDriver = "journald"

	containerReadyTimeout         = 60 * time.Second
	containerHealthRequestTimeout = 100 * time.Millisecond
	containerHealthPollInterval   = 25 * time.Millisecond
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
	networkName      string // stable per-function network name
	networkLabels    map[string]string
	containers       []string
	handlerIPs       []string
	isRunning        bool               // track if containers are running
	opMux            sync.Mutex         // mutex for start/stop/restart operations
	labels           map[string]string  // store user-provided labels (for GetLabels())
	containerLabels  map[string]string  // store merged labels (system + user) for container creation
	envVars          []string           // store environment variables for container recreation
	extraHosts       []string           // store extra hosts for container recreation
	httpClient       *http.Client
	containerRemover func(string) error // optional override for tests
	networkCreator   func() (string, error)
	ipInspector      func(string) (string, error)
	healthChecker    func(string) error
	networkRemover   func() error // optional override for tests
	imageRemover     func() error // optional override for tests
	logger           *slog.Logger
}

func newHealthClient() *http.Client {
	return &http.Client{
		Timeout: containerHealthRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
			IdleConnTimeout:     containerReadyTimeout,
			DisableCompression:  true,
			DialContext: (&net.Dialer{
				Timeout: containerHealthRequestTimeout,
			}).DialContext,
		},
	}
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
		dh.logger.Info("function is already started", "name", dh.name)
		return nil
	}

	if err := dh.createNetwork(); err != nil {
		return err
	}

	// Clear any old container IDs
	dh.containers = make([]string, 0, dh.replicas)
	dh.handlerIPs = make([]string, 0, dh.replicas)

	// Create containers from image
	for i := 0; i < dh.replicas; i++ {
		dh.logger.Info("creating function container", "name", dh.name, "replica", i+1)
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
					LogConfig: container.LogConfig{
						Type: functionLogDriver,
						Config: map[string]string{
							"tag": tflogs.JournalIdentifier(dh.name),
						},
					},
					Resources: container.Resources{
						NanoCPUs: dh.nanoCPUs,
						Memory:   dh.memoryBytes,
					},
				},
				Name: dh.uniqueName + fmt.Sprintf("-%d", i),
			},
		)
		if err != nil {
			baseErr := fmt.Errorf("failed to create container %d: %w", i, err)
			if rollbackErr := dh.rollback(); rollbackErr != nil {
				return errors.Join(baseErr, fmt.Errorf("startup rollback failed: %w", rollbackErr))
			}
			return baseErr
		}
		dh.containers = append(dh.containers, containerResp.ID)
	}

	for _, cid := range dh.containers {
		// Start container
		dh.logger.Info("starting container", "ID", util.GetShortID(cid))
		_, err := dh.client.ContainerStart(
			context.Background(),
			cid,
			client.ContainerStartOptions{},
		)
		if err != nil {
			dh.logger.Error("error starting container", "err", err)
			baseErr := fmt.Errorf("failed to start container %s: %w", util.GetShortID(cid), err)
			if rollbackErr := dh.rollback(); rollbackErr != nil {
				return errors.Join(baseErr, fmt.Errorf("startup rollback failed: %w", rollbackErr))
			}
			return baseErr
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

			dh.logger.Info("container IP", "ip", ip)
			dh.logger.Info("waiting for container to be ready", "ip", ip)

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
				"ID", util.GetShortID(containerID),
				"ip", res.ip,
				"err", res.err,
			)

			logs, logErr := dh.getContainerLogs(containerID)
			if logErr != nil {
				dh.logger.Error("error getting logs for container", "ID", util.GetShortID(containerID), "err", logErr)
			} else {
				dh.logger.Info("logs for container", "ID", util.GetShortID(containerID), "logs", logs)
			}

			readinessErrors = append(readinessErrors, fmt.Errorf("container %s: %w", util.GetShortID(containerID), res.err))
			continue
		}

		handlerIPs[res.index] = res.ip
	}

	if len(readinessErrors) > 0 {
		baseErr := fmt.Errorf("container readiness failed: %w", errors.Join(readinessErrors...))
		if rollbackErr := dh.rollback(); rollbackErr != nil {
			return errors.Join(baseErr, fmt.Errorf("startup rollback failed: %w", rollbackErr))
		}
		return baseErr
	}

	dh.handlerIPs = handlerIPs

	dh.logger.Debug("health check completed", "total", len(dh.handlerIPs))
	dh.isRunning = true
	dh.logger.Info("all containers started", "count", len(dh.containers))
	return nil
}

func (dh *dockerHandler) rollback() error {
	var cleanupErrors []error

	if err := dh.removeContainers(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	if err := dh.removeNetwork(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	if len(cleanupErrors) > 0 {
		return errors.Join(cleanupErrors...)
	}

	return nil
}

func (dh *dockerHandler) createNetwork() error {
	if dh.network != "" {
		return nil
	}

	if dh.networkName == "" {
		dh.networkName = dh.uniqueName
	}

	create := dh.networkCreator
	if create == nil {
		create = func() (string, error) {
			labels := dh.networkLabels
			if len(labels) == 0 {
				labels = map[string]string{
					"tinyfaas-function": dh.name,
				}
			}

			networkResp, err := dh.client.NetworkCreate(
				context.Background(),
				dh.networkName,
				client.NetworkCreateOptions{
					Driver:   "bridge",
					Internal: false,
					Labels:   labels,
				},
			)
			if err != nil {
				return "", err
			}

			return networkResp.ID, nil
		}
	}

	networkID, err := create()
	if err != nil {
		return fmt.Errorf("failed to create network %s: %w", dh.networkName, err)
	}

	dh.network = networkID
	dh.logger.Info("network ready",
		"name", dh.name,
		"network", util.GetShortID(networkID),
		"network_name", dh.networkName)

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
			dh.logger.Info("container is ready", "ip", ip)
			return nil
		}

		lastErr = err
		dh.logger.Debug(
			"ready check attempt failed",
			"attempt", attempt+1,
			"ip", ip,
			"err", err,
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

	resp, err := dh.httpClient.Get("http://" + ip + ":8000/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("container health probe failed with status: %s", resp.Status)
	}

	return nil
}

func readinessBackoffDelay(_ int) time.Duration {
	return containerHealthPollInterval
}

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

	failedContainers := make([]string, 0)
	cleanupErrors := make([]error, 0)

	for _, cid := range dh.containers {
		err := remove(cid)
		if err != nil {
			if isNotFoundError(err) {
				dh.logger.Info("container already removed", "ID", util.GetShortID(cid))
				continue
			}

			dh.logger.Error("error removing container", "ID", util.GetShortID(cid), "err", err)
			failedContainers = append(failedContainers, cid)
			cleanupErrors = append(cleanupErrors, fmt.Errorf("container %s: %w", util.GetShortID(cid), err))
			continue
		}

		dh.logger.Info("container removed", "ID", util.GetShortID(cid))
	}

	dh.containers = failedContainers
	dh.handlerIPs = nil
	dh.isRunning = false

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("container cleanup failed: %w", errors.Join(cleanupErrors...))
	}

	return nil
}

func (dh *dockerHandler) removeNetwork() error {
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
				dh.logger.Info("network already removed", "network", util.GetShortID(dh.network))
			} else {
				dh.logger.Error("error removing network", "network", util.GetShortID(dh.network), "err", err)
				return fmt.Errorf("network remove: %w", err)
			}
		} else {
			dh.logger.Info("network removed", "network", util.GetShortID(dh.network))
		}
		dh.network = ""
	}

	return nil
}

func (dh *dockerHandler) removeImage() error {
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
			dh.logger.Info("image already removed", "name", dh.name)
		} else {
			return fmt.Errorf("image remove: %w", err)
		}
	} else {
		dh.logger.Info("image removed", "name", dh.name)
	}

	return nil
}

func (dh *dockerHandler) removeRuntime() error {
	cleanupErrors := make([]error, 0)

	if err := dh.removeContainers(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	if err := dh.removeNetwork(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("runtime cleanup failed: %w", errors.Join(cleanupErrors...))
	}

	return nil
}

func (dh *dockerHandler) Destroy() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	dh.logger.Info("destroying function", "name", dh.name)

	cleanupErrors := make([]error, 0)

	if err := dh.removeRuntime(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	if err := dh.removeImage(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	// Destroy is terminal for this handler; clear network name to prevent accidental reuse.
	dh.networkName = ""

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("destroy cleanup failed: %w", errors.Join(cleanupErrors...))
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

// Stop scales the function to zero by removing runtime resources.
// Containers and the per-function network are removed; the function image is kept.
func (dh *dockerHandler) Stop() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()

	if !dh.isRunning && len(dh.containers) == 0 && dh.network == "" {
		dh.logger.Info("function is already stopped", "name", dh.name)
		return nil
	}

	dh.logger.Info("scaling function to zero", "name", dh.name)
	if err := dh.removeRuntime(); err != nil {
		return err
	}
	dh.logger.Info("function scaled down", "name", dh.name)
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
