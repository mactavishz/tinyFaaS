package docker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
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
	name        string
	env         string
	threads     int
	uniqueName  string
	filePath    string
	client      *client.Client
	network     string // per-function network ID
	networkName string // per-function network name
	containers  []string
	handlerIPs  []string
	isRunning   bool              // track if containers are running
	opMux       sync.Mutex        // mutex for start/stop/restart operations
	labels      map[string]string // store container labels
	envVars     []string          // store environment variables for container recreation
	extraHosts  []string          // store extra hosts for container recreation
	logger      *zap.Logger
}

type DockerBackend struct {
	client       *client.Client
	tinyFaaSID   string
	gatewayIP    string // host gateway IP for --add-host (where Caddy runs)
	publicDomain string // public domain for --add-host (e.g., tinyfaas.com)
	logger       *zap.Logger
}

func New(tinyFaaSID string, logger *zap.Logger) *DockerBackend {
	// create docker client
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		logger.Fatal("error creating docker client", zap.Error(err))
		return nil
	}

	db := &DockerBackend{
		client:     client,
		tinyFaaSID: tinyFaaSID,
		logger:     logger,
	}

	// Get public domain from environment variable
	db.publicDomain = os.Getenv("TINYFAAS_PUBLIC_DOMAIN")
	if db.publicDomain == "" {
		db.publicDomain = DEFAULT_PUBLIC_DOMAIN
		logger.Info("TINYFAAS_PUBLIC_DOMAIN not set, using default", zap.String("publicDomain", db.publicDomain))
	}

	// Get gateway IP from environment variable
	// This should be the host IP where Caddy is running
	db.gatewayIP = os.Getenv("TINYFAAS_GATEWAY_IP")
	if db.gatewayIP == "" {
		// Default to host.docker.internal for Docker Desktop
		// On Linux, this might need to be set explicitly to the host IP
		db.gatewayIP = "host-gateway"
		logger.Info("TINYFAAS_GATEWAY_IP not set, using default", zap.String("gatewayIP", db.gatewayIP))
	}

	// Note: Runtime base images must be pre-built using 'make build-runtime-images'
	// or the build script. Manager expects images to already exist.
	logger.Info("verifying runtime base images...")

	// Verify all runtime base images exist
	if err := db.verifyRuntimeImages(); err != nil {
		logger.Fatal("runtime base images not found, please run 'make build-runtime-images' first", zap.Error(err))
		return nil
	}

	logger.Info("all runtime base images verified")
	return db
}

// verifyRuntimeImages checks that all required runtime base images exist
func (db *DockerBackend) verifyRuntimeImages() error {
	var missing []string
	for _, runtime := range RUNTIMES {
		baseImageTag := db.getRuntimeBaseImage(runtime)
		_, _, err := db.client.ImageInspectWithRaw(context.Background(), baseImageTag)
		if err != nil {
			missing = append(missing, baseImageTag)
		} else {
			db.logger.Info("found runtime base image", zap.String("image", baseImageTag))
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing runtime images: %v", missing)
	}

	return nil
}

func (db *DockerBackend) Stop() error {
	return nil
}

// getRuntimeBaseImage returns the tag for a runtime's base image
// Assumes the image was pre-built and exists in Docker
func (db *DockerBackend) getRuntimeBaseImage(runtime string) string {
	return fmt.Sprintf("tinyfaas-runtime-%s", runtime)
}

func (db *DockerBackend) Create(name string, env string, threads int, filedir string, envs map[string]string, labels map[string]string) (manager.Handler, error) {

	// make a unique function name by appending uuid string to function name
	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	dh := &dockerHandler{
		name:       name,
		env:        env,
		client:     db.client,
		threads:    threads,
		containers: make([]string, 0, threads),
		handlerIPs: make([]string, 0, threads),
		isRunning:  false,
		labels:     labels,
		logger:     db.logger,
	}

	dh.uniqueName = name + "-" + uuid.String()
	db.logger.Info("creating function", zap.String("name", name), zap.String("uniqueName", dh.uniqueName))

	// Get runtime base image tag (verified at startup)
	baseImageTag := db.getRuntimeBaseImage(dh.env)

	// make a folder for the function
	// mkdir <folder>
	dh.filePath = path.Join(TmpDir, dh.uniqueName)

	err = os.MkdirAll(dh.filePath, 0777)
	if err != nil {
		return nil, err
	}

	// Copy the runtime Dockerfile (already configured to use the base image)
	dockerfilePath := path.Join(runtimesDir, dh.env, "Dockerfile")
	dockerfileData, err := runtimes.ReadFile(dockerfilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read runtime Dockerfile: %w", err)
	}

	err = os.WriteFile(path.Join(dh.filePath, "Dockerfile"), dockerfileData, 0644)
	if err != nil {
		return nil, err
	}

	db.logger.Info("using runtime Dockerfile for runtime", zap.String("env", dh.env), zap.String("baseImage", baseImageTag))

	// copy function into folder
	// cp <file> <folder>/fn
	err = os.MkdirAll(path.Join(dh.filePath, "fn"), 0777)
	if err != nil {
		return nil, err
	}

	err = util.CopyAll(filedir, path.Join(dh.filePath, "fn"))
	if err != nil {
		return nil, err
	}

	// build image (now just adding function layer on top of base)
	// docker build -t <image> <folder>
	tar, err := archive.TarWithOptions(dh.filePath, &archive.TarOptions{})
	if err != nil {
		return nil, err
	}

	r, err := db.client.ImageBuild(
		context.Background(),
		tar,
		types.ImageBuildOptions{
			Tags:       []string{dh.uniqueName},
			Remove:     true,
			Dockerfile: "Dockerfile",
			Labels: map[string]string{
				"tinyfaas-function": dh.name,
				"tinyFaaS":          db.tinyFaaSID,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	defer r.Body.Close()
	scanner := bufio.NewScanner(r.Body)
	for scanner.Scan() {
		db.logger.Debug(scanner.Text())
	}

	db.logger.Info("built image", zap.String("uniqueName", dh.uniqueName), zap.String("baseImage", baseImageTag))

	// Create per-function isolated network
	// Each function gets its own network, so functions cannot directly communicate with each other
	// They can only reach the host (where Caddy runs) via the gateway
	networkResp, err := db.client.NetworkCreate(
		context.Background(),
		dh.uniqueName,
		network.CreateOptions{
			Driver: "bridge",
			// Enable isolation - containers can only access the gateway, not other networks
			Internal: false, // Must be false to allow internet/host access
			Labels: map[string]string{
				"tinyfaas-function": dh.name,
				"tinyFaaS":          db.tinyFaaSID,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	dh.network = networkResp.ID
	dh.networkName = dh.uniqueName
	db.logger.Info("created isolated network", zap.String("networkName", dh.uniqueName), zap.String("networkID", networkResp.ID))

	e := make([]string, 0, len(envs))

	for k, v := range envs {
		e = append(e, fmt.Sprintf("%s=%s", k, v))
	}

	// Build extra hosts for --add-host
	// This maps the public domain to the host gateway (where Caddy is running)
	// Using "host-gateway" is a special Docker value that resolves to the host's IP
	extraHosts := []string{}
	if db.publicDomain != "" && db.gatewayIP != "" {
		extraHosts = append(extraHosts, fmt.Sprintf("%s:%s", db.publicDomain, db.gatewayIP))
		db.logger.Info("adding extra host", zap.String("host", db.publicDomain), zap.String("ip", db.gatewayIP))
	}

	// Merge custom labels with system labels
	containerLabels := map[string]string{
		"tinyfaas-function": dh.name,
		"tinyFaaS":          db.tinyFaaSID,
	}
	for k, v := range labels {
		containerLabels[k] = v
	}

	// Store config for later container recreation (scale-up)
	dh.envVars = e
	dh.extraHosts = extraHosts

	// create containers
	// docker run -d --network <network> --name <container> <image>
	for i := 0; i < dh.threads; i++ {
		containerResp, err := db.client.ContainerCreate(
			context.Background(),
			&container.Config{
				Image:  dh.uniqueName,
				Labels: containerLabels,
				Env:    e,
			},
			&container.HostConfig{
				NetworkMode: container.NetworkMode(dh.networkName),
				ExtraHosts:  extraHosts,
			},
			nil,
			nil,
			dh.uniqueName+fmt.Sprintf("-%d", i),
		)

		if err != nil {
			return nil, err
		}

		db.logger.Info("created container", zap.String("containerID", containerResp.ID))

		dh.containers = append(dh.containers, containerResp.ID)
	}

	// remove folder
	// rm -rf <folder>
	err = os.RemoveAll(dh.filePath)
	if err != nil {
		return nil, err
	}

	db.logger.Info("removed folder", zap.String("folder", dh.filePath))
	return dh, nil

}

func (dh *dockerHandler) IPs() []string {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	return dh.handlerIPs
}

func (dh *dockerHandler) Start() error {
	dh.logger.Info("starting function containers", zap.Int("count", len(dh.containers)))
	dh.opMux.Lock()
	defer dh.opMux.Unlock()

	// Track which containers have successfully started and their IPs
	containerIPs := make(map[string]string)
	var ipMux sync.Mutex

	// Retry entire start process up to 10 times
	err := retry.New(
		retry.Attempts(10),
		retry.Delay(200*time.Millisecond),
		retry.OnRetry(func(attempt uint, err error) {
			dh.logger.Debug("start attempt failed", zap.Uint("attempt", attempt), zap.Error(err))
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
			go func(containerID string) {
				defer wg.Done()

				// Start container
				err := dh.client.ContainerStart(
					context.Background(),
					containerID,
					container.StartOptions{},
				)
				if err != nil {
					dh.logger.Error("error starting container", zap.String("containerID", containerID), zap.Error(err))
					startMux.Lock()
					startErrors = append(startErrors, fmt.Errorf("failed to start container %s: %w", containerID, err))
					startMux.Unlock()
					return
				}
				dh.logger.Info("started container", zap.String("containerID", containerID))

				// Get container IP immediately after starting
				c, err := dh.client.ContainerInspect(
					context.Background(),
					containerID,
				)
				if err != nil {
					dh.logger.Error("failed to inspect container after start", zap.String("containerID", containerID), zap.Error(err))
					startMux.Lock()
					startErrors = append(startErrors, fmt.Errorf("failed to inspect container %s: %w", containerID, err))
					startMux.Unlock()
					return
				}

				ip := c.NetworkSettings.Networks[dh.networkName].IPAddress
				if ip == "" {
					dh.logger.Error("container has no IP address after start", zap.String("containerID", containerID))
					startMux.Lock()
					startErrors = append(startErrors, fmt.Errorf("container %s has no IP address", containerID))
					startMux.Unlock()
					return
				}

				dh.logger.Info("got IP for container", zap.String("containerID", containerID), zap.String("ip", ip))
				ipMux.Lock()
				containerIPs[containerID] = ip
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
		for _, containerID := range dh.containers {
			ipMux.Lock()
			ip := containerIPs[containerID]
			ipMux.Unlock()

			if ip == "" {
				return fmt.Errorf("container %s missing IP in map", containerID)
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
			go func(index int, containerIP string) {
				defer wg.Done()
				dh.logger.Info("waiting for container to be ready", zap.String("containerID", dh.containers[index]), zap.String("ip", containerIP))

				err := retry.New(
					retry.Attempts(5),
					retry.Delay(100*time.Millisecond),
					retry.OnRetry(func(retryAttempt uint, err error) {
						dh.logger.Debug("health check attempt failed", zap.Uint("attempt", retryAttempt), zap.String("containerID", dh.containers[index]), zap.String("ip", containerIP), zap.Error(err))
					}),
				).Do(func() error {
					client := http.Client{
						Timeout: 3 * time.Second,
					}
					resp, err := client.Get("http://" + containerIP + ":8000/health")
					if err != nil {
						return err
					}
					defer resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						dh.logger.Info("container is ready", zap.String("ip", containerIP))
						return nil
					}
					return fmt.Errorf("container health endpoint failed with status: %s", resp.Status)
				})

				if err != nil {
					containerID := dh.containers[index]
					dh.logger.Error("container not ready after health check retries", zap.String("containerID", containerID), zap.String("ip", containerIP), zap.Error(err))
					healthMux.Lock()
					healthErrors = append(healthErrors, fmt.Errorf("container %s, ip: %s not ready: %w", containerID, containerIP, err))
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
					dh.logger.Error("error getting logs for container", zap.String("containerID", containerID), zap.Error(logErr))
				} else {
					dh.logger.Info("logs for container", zap.String("containerID", containerID), zap.String("logs", logs))
				}
			}
			return fmt.Errorf("%d containers failed health checks: %v", len(healthErrors), healthErrors)
		}

		// All containers started and healthy - success!
		return nil
	})

	if err != nil {
		dh.logger.Error("failed to start all containers after 10 retries", zap.Error(err))
		dh.logger.Info("cleaning up partially started containers")
		dh.stopContainers()
		return fmt.Errorf("start failed after 10 retries: %w", err)
	}

	dh.isRunning = true
	dh.logger.Info("all containers started successfully", zap.Int("count", len(dh.containers)))
	return nil
}

// stopContainers stops all function containers (used by cleanup during startup failures)
func (dh *dockerHandler) stopContainers() {
	wg := sync.WaitGroup{}
	for _, c := range dh.containers {
		wg.Add(1)
		go func(containerID string) {
			defer wg.Done()

			timeout := 5
			err := dh.client.ContainerStop(
				context.Background(),
				containerID,
				container.StopOptions{
					Timeout: &timeout,
				},
			)
			if err != nil {
				dh.logger.Error("error stopping container", zap.String("containerID", containerID), zap.Error(err))
			} else {
				dh.logger.Info("stopped container", zap.String("containerID", containerID))
			}
		}(c)
	}
	wg.Wait()
	dh.handlerIPs = nil
	dh.isRunning = false
}

// deleteContainers stops and removes all function containers (used by scale-to-zero)
// This frees up system resources by completely removing the containers
func (dh *dockerHandler) deleteContainers() {
	wg := sync.WaitGroup{}
	for _, c := range dh.containers {
		wg.Add(1)
		go func(containerID string) {
			defer wg.Done()

			// Stop container first
			timeout := 5
			err := dh.client.ContainerStop(
				context.Background(),
				containerID,
				container.StopOptions{
					Timeout: &timeout,
				},
			)
			if err != nil {
				dh.logger.Error("error stopping container", zap.String("containerID", containerID), zap.Error(err))
			} else {
				dh.logger.Info("stopped container", zap.String("containerID", containerID))
			}

			// Remove container
			err = dh.client.ContainerRemove(
				context.Background(),
				containerID,
				container.RemoveOptions{},
			)
			if err != nil {
				dh.logger.Error("error removing container", zap.String("containerID", containerID), zap.Error(err))
			} else {
				dh.logger.Info("removed container", zap.String("containerID", containerID))
			}
		}(c)
	}
	wg.Wait()
	dh.handlerIPs = nil
	dh.isRunning = false
}

func (dh *dockerHandler) Destroy() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()
	dh.logger.Info("destroying function", zap.String("name", dh.name))

	wg := sync.WaitGroup{}
	dh.logger.Info("stopping containers", zap.Any("containers", dh.containers))
	for _, c := range dh.containers {
		dh.logger.Info("removing container", zap.String("containerID", c))

		wg.Add(1)
		go func(c string) {
			dh.logger.Info("stopping container", zap.String("containerID", c))

			timeout := 1 // seconds

			err := dh.client.ContainerStop(
				context.Background(),
				c,
				container.StopOptions{
					Timeout: &timeout,
				},
			)
			if err != nil {
				dh.logger.Error("error stopping container", zap.String("containerID", c), zap.Error(err))
			}

			dh.logger.Info("stopped container", zap.String("containerID", c))

			err = dh.client.ContainerRemove(
				context.Background(),
				c,
				container.RemoveOptions{},
			)
			wg.Done()
			if err != nil {
				dh.logger.Error("error removing container", zap.String("containerID", c), zap.Error(err))
			}
		}(c)

		dh.logger.Info("removed container", zap.String("containerID", c))
	}
	wg.Wait()

	// Remove the per-function network
	err := dh.client.NetworkRemove(
		context.Background(),
		dh.network,
	)
	if err != nil {
		dh.logger.Error("error removing network", zap.String("network", dh.network), zap.Error(err))
	} else {
		dh.logger.Info("removed network", zap.String("network", dh.network))
	}

	// remove image
	// docker rmi <image>
	_, err = dh.client.ImageRemove(
		context.Background(),
		dh.uniqueName,
		image.RemoveOptions{},
	)

	if err != nil {
		return err
	}

	dh.logger.Info("removed image", zap.String("image", dh.uniqueName))
	return nil
}

func (dh *dockerHandler) getContainerLogs(c string) (string, error) {
	l, err := dh.client.ContainerLogs(
		context.Background(),
		c,
		container.LogsOptions{
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

	if !dh.isRunning {
		dh.logger.Info("function is already stopped", zap.String("name", dh.name))
		return nil
	}

	dh.logger.Info("stopping and deleting function containers", zap.String("name", dh.name))
	dh.deleteContainers()
	dh.logger.Info("function scaled down (containers deleted)", zap.String("name", dh.name))
	return nil
}

// Restart recreates and starts function containers from cached image (for scale-up from scale-to-zero)
func (dh *dockerHandler) Restart() error {
	dh.opMux.Lock()
	defer dh.opMux.Unlock()

	if dh.isRunning {
		dh.logger.Info("function is already running", zap.String("name", dh.name))
		return nil
	}

	dh.logger.Info("recreating function containers from cached image", zap.String("name", dh.name), zap.String("image", dh.uniqueName))

	// Clear old container IDs since we're creating new containers
	dh.containers = make([]string, 0, dh.threads)

	// Recreate containers from cached image
	for i := 0; i < dh.threads; i++ {
		containerResp, err := dh.client.ContainerCreate(
			context.Background(),
			&container.Config{
				Image:  dh.uniqueName,
				Labels: dh.labels,
				Env:    dh.envVars,
			},
			&container.HostConfig{
				NetworkMode: container.NetworkMode(dh.networkName),
				ExtraHosts:  dh.extraHosts,
			},
			nil,
			nil,
			dh.uniqueName+fmt.Sprintf("-%d", i),
		)

		if err != nil {
			return fmt.Errorf("failed to create container %d: %w", i, err)
		}

		dh.logger.Info("created new container", zap.String("containerID", containerResp.ID), zap.Int("index", i))
		dh.containers = append(dh.containers, containerResp.ID)
	}

	// Start all containers
	wg := sync.WaitGroup{}
	for _, c := range dh.containers {
		wg.Add(1)
		go func(containerID string) {
			defer wg.Done()

			err := dh.client.ContainerStart(
				context.Background(),
				containerID,
				container.StartOptions{},
			)
			if err != nil {
				dh.logger.Error("error starting container", zap.String("containerID", containerID), zap.Error(err))
			} else {
				dh.logger.Info("started new container", zap.String("containerID", containerID))
			}
		}(c)
	}
	wg.Wait()

	// Get new container IPs
	dh.handlerIPs = make([]string, 0, len(dh.containers))
	for _, containerID := range dh.containers {
		c, err := dh.client.ContainerInspect(
			context.Background(),
			containerID,
		)
		if err != nil {
			return fmt.Errorf("failed to inspect container %s: %w", containerID, err)
		}

		ip := c.NetworkSettings.Networks[dh.networkName].IPAddress
		dh.handlerIPs = append(dh.handlerIPs, ip)
		dh.logger.Info("got ip for recreated container", zap.String("ip", ip), zap.String("containerID", containerID))
	}

	// Wait for containers to be ready
	for _, ip := range dh.handlerIPs {
		dh.logger.Info("waiting for recreated container to be ready", zap.String("ip", ip))

		err := retry.New(
			retry.Attempts(5),
			retry.Delay(100*time.Millisecond),
			retry.OnRetry(func(attempt uint, err error) {
				dh.logger.Debug("health check failed for container, retrying...", zap.String("ip", ip), zap.Uint("attempt", attempt), zap.Error(err))
			}),
		).Do(func() error {
			client := http.Client{
				Timeout: 3 * time.Second,
			}
			resp, err := client.Get("http://" + ip + ":8000/health")
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				dh.logger.Info("recreated container is ready", zap.String("ip", ip))
				return nil
			}
			return fmt.Errorf("container health endpoint failed with status: %s", resp.Status)
		})

		if err != nil {
			return fmt.Errorf("recreated container %s not ready after maximum retries: %w", ip, err)
		}
	}

	dh.isRunning = true
	dh.logger.Info("function scaled up (new containers created and started)", zap.String("name", dh.name))
	return nil
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
