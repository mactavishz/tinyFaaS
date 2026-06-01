package docker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/OpenFogStack/tinyFaaS/pkg/manager"
	"github.com/OpenFogStack/tinyFaaS/pkg/util"
	"github.com/google/uuid"
	"github.com/moby/go-archive"
	"github.com/moby/go-archive/compression"
	"github.com/moby/moby/client"
	"github.com/moby/moby/client/pkg/jsonmessage"
	"log/slog"
)

type DockerBackend struct {
	client       *client.Client
	tinyFaaSID   string
	gatewayIP    string // host gateway IP for --add-host (where the Gateway runs)
	publicDomain string // public domain for --add-host (e.g., tinyfaas.com)
	gatewayPort  string // gateway port injected into containers as TINYFAAS_GATEWAY_URL
	logger       *slog.Logger
}

func New(tinyFaaSID string, logger *slog.Logger) *DockerBackend {
	if logger == nil {
		logger = slog.Default()
	}

	// create docker client
	client, err := client.New(client.FromEnv)
	if err != nil {
		logger.Error("error creating docker client", "err", err)
		os.Exit(1)
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
		logger.Info("TINYFAAS_PUBLIC_DOMAIN not set, using default", "publicDomain", db.publicDomain)
	}

	// Get gateway IP from environment variable
	// This should be the host IP where the Gateway is running
	db.gatewayIP = os.Getenv("TINYFAAS_GATEWAY_IP")
	if db.gatewayIP == "" {
		// Default to host.docker.internal for Docker Desktop
		// On Linux, this might need to be set explicitly to the host IP
		db.gatewayIP = "host-gateway"
		logger.Info("TINYFAAS_GATEWAY_IP not set, using default", "gatewayIP", db.gatewayIP)
	}

	// Get gateway port from environment variable (same var used by the gateway service)
	db.gatewayPort = os.Getenv("GATEWAY_PORT")
	if db.gatewayPort == "" {
		db.gatewayPort = "80"
		logger.Info("GATEWAY_PORT not set, using default", "gatewayPort", db.gatewayPort)
	}

	// Note: Runtime base images must be pre-built using 'make build-runtime-images'
	// or the build script. Manager expects images to already exist.
	logger.Info("verifying runtime base images...")

	// Verify all runtime base images exist
	if err := db.verifyRuntimeImages(); err != nil {
		logger.Error("runtime base images not found, please run 'make build-runtime-images' first", "err", err)
		os.Exit(1)
	}

	logger.Info("all runtime base images verified")
	return db
}

// verifyRuntimeImages checks that all required runtime base images exist
func (db *DockerBackend) verifyRuntimeImages() error {
	var missing []string
	for _, runtime := range RUNTIMES {
		baseImageTag := db.getRuntimeBaseImage(runtime)
		_, err := db.client.ImageInspect(context.Background(), baseImageTag)
		if err != nil {
			missing = append(missing, baseImageTag)
		} else {
			db.logger.Info("found runtime base image", "image", baseImageTag)
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

func (db *DockerBackend) Create(name string, env string, replicas int, filedir string, envs map[string]string, labels map[string]string, limits manager.ResourceLimits) (manager.Handler, error) {

	// make a unique function name by appending uuid string to function name
	ctx := context.Background()
	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	dh := &dockerHandler{
		name:        name,
		env:         env,
		client:      db.client,
		replicas:    replicas,
		nanoCPUs:    limits.NanoCPUs,
		memoryBytes: limits.MemoryBytes,
		containers:  make([]string, 0, replicas),
		handlerIPs:  make([]string, 0, replicas),
		isRunning:   false,
		labels:      labels,
		logger:      db.logger,
	}

	dh.uniqueName = name + "-" + uuid.String()
	db.logger.Info("creating function", "name", dh.name)

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

	db.logger.Info("Function runtime", "env", dh.env, "baseImage", baseImageTag)

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
	tar, err := archive.Tar(dh.filePath, compression.None)
	if err != nil {
		return nil, err
	}

	buildStart := time.Now()
	db.logger.Info("building image", "function", dh.name, "image", dh.uniqueName, "runtime", dh.env)
	r, err := db.client.ImageBuild(
		ctx,
		tar,
		client.ImageBuildOptions{
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
	var buildLog bytes.Buffer
	err = jsonmessage.DisplayJSONMessagesStream(
		r.Body,
		&buildLog,
		0,
		false,
		nil, // auxCallback (optional: used to capture the final Image ID)
	)
	buildDuration := time.Since(buildStart)
	if err != nil {
		db.logger.Error("image build failed",
			"function", dh.name,
			"image", dh.uniqueName,
			"runtime", dh.env,
			"duration", buildDuration,
			"err", err,
			"build_log", buildLog.String())
		return nil, fmt.Errorf("failed to build image %s: %w", dh.uniqueName, err)
	}

	db.logger.Info("image built",
		"function", dh.name,
		"image", dh.uniqueName,
		"runtime", dh.env,
		"duration", buildDuration)

	// Create per-function isolated network
	// Each function gets its own network, so functions cannot directly communicate with each other
	// They can only reach the host via the gateway
	db.logger.Info("creating isolated network", "name", dh.name)
	networkResp, err := db.client.NetworkCreate(
		ctx,
		dh.uniqueName,
		client.NetworkCreateOptions{
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

	e := make([]string, 0, len(envs))

	for k, v := range envs {
		e = append(e, fmt.Sprintf("%s=%s", k, v))
	}

	// Inject the full gateway URL so function handlers can call other functions
	// regardless of the port the gateway is running on.
	gatewayURL := fmt.Sprintf("http://%s:%s", db.publicDomain, db.gatewayPort)
	e = append(e, fmt.Sprintf("TINYFAAS_GATEWAY_URL=%s", gatewayURL))

	// Build extra hosts for --add-host
	// This maps the public domain to the host gateway (where the Gateway is running)
	// Using "host-gateway" is a special Docker value that resolves to the host's IP
	extraHosts := []string{}
	if db.publicDomain != "" && db.gatewayIP != "" {
		extraHosts = append(extraHosts, fmt.Sprintf("%s:%s", db.publicDomain, db.gatewayIP))
		db.logger.Info("adding extra host", "host", db.publicDomain, "ip", db.gatewayIP)
	}

	// Merge custom labels with system labels
	containerLabels := map[string]string{
		"tinyfaas-function": dh.name,
		"tinyFaaS":          db.tinyFaaSID,
	}
	networkLabels := map[string]string{
		"tinyfaas-function": dh.name,
		"tinyFaaS":          db.tinyFaaSID,
	}
	for k, v := range labels {
		containerLabels[k] = v
	}

	// Store config for later container creation in Start()
	dh.envVars = e
	dh.extraHosts = extraHosts
	dh.containerLabels = containerLabels
	dh.networkLabels = networkLabels

	// NOTE: Containers are NOT created here - they will be created in Start()
	// This ensures cold start measurement includes container creation time

	// remove folder
	// rm -rf <folder>
	err = os.RemoveAll(dh.filePath)
	if err != nil {
		return nil, err
	}
	return dh, nil

}
