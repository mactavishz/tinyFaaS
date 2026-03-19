package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/containerd/errdefs"
	"go.uber.org/zap"

	"github.com/OpenFogStack/tinyFaaS/pkg/util"

	retry "github.com/avast/retry-go/v5"
)

const (
	TmpDir                = "./tmp"
	DEFAULT_PUBLIC_DOMAIN = "tinyfaas.com"
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
	networkRemover   func() error       // optional override for tests
	imageRemover     func() error       // optional override for tests
	logger           *zap.Logger
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

	// Track which containers have successfully started and their IPs
	containerIPs := make(map[string]string)
	var ipMux sync.Mutex

	// Retry entire start process up to 10 times
	err := retry.New(
		retry.Attempts(10),
		retry.Delay(50*time.Millisecond),
		retry.DelayType(retry.FixedDelay),
		retry.OnRetry(func(attempt uint, err error) {
			dh.logger.Debug("start attempt failed", zap.Uint("attempt", attempt))
		}),
	).Do(func() error {
		// Start only containers that haven't been successfully started yet
		var startErrors []error
		var startMux sync.Mutex
		wg := sync.WaitGroup{}

		for _, c := range dh.containers {
			ipMux.Lock()
			alreadyStarted := containerIPs[c] != ""
			ipMux.Unlock()

			if alreadyStarted {
				continue // Skip containers that are already running with IP
			}

			wg.Add(1)
			go func(cid string) {
				defer wg.Done()

				// Start container
				dh.logger.Info("starting container", zap.String("ID", util.GetShortID(cid)))
				_, err := dh.client.ContainerStart(
					context.Background(),
					cid,
					client.ContainerStartOptions{},
				)
				if err != nil {
					dh.logger.Error("error starting container", zap.Error(err))
					startMux.Lock()
					startErrors = append(startErrors, fmt.Errorf("failed to start container %s: %w", util.GetShortID(cid), err))
					startMux.Unlock()
					return
				}

				// Get container IP immediately after starting
				inspectRes, err := dh.client.ContainerInspect(
					context.Background(),
					cid,
					client.ContainerInspectOptions{},
				)
				if err != nil {
					dh.logger.Error("failed to inspect container", zap.Error(err))
					startMux.Lock()
					startErrors = append(startErrors, fmt.Errorf("failed to inspect container %s: %w", util.GetShortID(cid), err))
					startMux.Unlock()
					return
				}

				ip := inspectRes.Container.NetworkSettings.Networks[dh.networkName].IPAddress
				if !ip.IsValid() {
					dh.logger.Error("container IP address not found", zap.String("network", dh.networkName))
					startMux.Lock()
					startErrors = append(startErrors, fmt.Errorf("container %s has no IP address", util.GetShortID(cid)))
					startMux.Unlock()
					return
				}

				dh.logger.Info("container IP", zap.String("ip", ip.String()))
				ipMux.Lock()
				containerIPs[cid] = ip.String()
				ipMux.Unlock()
			}(c)
		}
		wg.Wait()

		// If any container failed to start or get IP, return error to trigger retry
		if len(startErrors) > 0 {
			return fmt.Errorf("failed to start %d containers: %v", len(startErrors), startErrors)
		}

		// Build ordered IP list matching container order
		tempHandlerIPs := make([]string, 0, len(dh.containers))
		for _, cid := range dh.containers {
			ipMux.Lock()
			ip := containerIPs[cid]
			ipMux.Unlock()

			if ip == "" {
				return fmt.Errorf("missing IP in container %s", cid)
			}
			tempHandlerIPs = append(tempHandlerIPs, ip)
		}

		// Store IPs for health checks
		dh.handlerIPs = tempHandlerIPs

		// Wait for all containers to be ready
		var healthErrors []error
		var healthMux sync.Mutex

		for i, ip := range dh.handlerIPs {
			wg.Add(1)
			go func(index int, cip string) {
				defer wg.Done()
				dh.logger.Info("waiting for container to be ready", zap.String("ip", cip))

				err := retry.New(
					retry.Attempts(50),
					retry.DelayType(retry.FixedDelay),
					retry.Delay(50*time.Millisecond),
					retry.OnRetry(func(retryAttempt uint, err error) {
						dh.logger.Debug("ready check attempt failed", zap.Uint("attempt", retryAttempt), zap.String("ip", cip))
					}),
				).Do(func() error {
					client := http.Client{
						Timeout: 1 * time.Second,
					}
					resp, err := client.Get("http://" + cip + ":8000/health")
					if err != nil {
						return err
					}
					defer resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						dh.logger.Info("container is ready", zap.String("ip", cip))
						return nil
					}
					return fmt.Errorf("container health probe failed with status: %s", resp.Status)
				})

				if err != nil {
					containerID := dh.containers[index]
					dh.logger.Error("container not ready after health check retries", zap.String("ip", cip), zap.Error(err))
					healthMux.Lock()
					healthErrors = append(healthErrors, fmt.Errorf("container %s, ip: %s not ready: %w", util.GetShortID(containerID), cip, err))
					healthMux.Unlock()
				}
			}(i, ip)
		}

		wg.Wait()

		// If any health check failed, return error to trigger retry
		if len(healthErrors) > 0 {
			// Log container details for debugging
			for i := range dh.handlerIPs {
				containerID := dh.containers[i]
				logs, logErr := dh.getContainerLogs(containerID)
				if logErr != nil {
					dh.logger.Error("error getting logs for container", zap.String("ID", util.GetShortID(containerID)), zap.Error(logErr))
				} else {
					dh.logger.Info("logs for container", zap.String("ID", util.GetShortID(containerID)), zap.String("logs", logs))
				}
			}
			return fmt.Errorf("%d containers failed health checks: %v", len(healthErrors), healthErrors)
		}

		// All containers started and healthy - success!
		return nil
	})

	if err != nil {
		dh.logger.Error("failed to start all containers after max retries", zap.Error(err))
		dh.logger.Info("cleaning up partially started containers")
		dh.stopContainers()
		return fmt.Errorf("start failed after max retries: %w", err)
	}

	dh.isRunning = true
	dh.logger.Info("all containers started", zap.Int("count", len(dh.containers)))
	return nil
}

// stopContainers stops all function containers (used by cleanup during startup failures)
func (dh *dockerHandler) stopContainers() {
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
