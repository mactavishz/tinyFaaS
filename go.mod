module github.com/OpenFogStack/tinyFaaS

go 1.25.4

require (
	github.com/avast/retry-go/v5 v5.0.0
	github.com/containerd/errdefs v1.0.0
	github.com/google/uuid v1.6.0
	github.com/mactavishz/FaaS-Platform-Knowledge-Optimization v0.0.0
	github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler v0.0.0
	github.com/moby/go-archive v0.2.0
	github.com/moby/moby/api v1.53.0
	github.com/moby/moby/client v0.2.2
	github.com/nats-io/stan.go v0.10.4
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/drone/envsubst v1.0.3 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/nats-io/nats-server/v2 v2.14.2 // indirect
	github.com/nats-io/nats-streaming-server v0.25.6 // indirect
	github.com/nats-io/nats.go v1.51.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/openfaas/go-sdk v0.0.0 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/apimachinery v0.34.1 // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
)

require (
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/go-connections v0.6.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph v0.0.0
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.opentelemetry.io/otel/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/autoscaler => ../autoscaler

replace github.com/mactavishz/FaaS-Platform-Knowledge-Optimization/callgraph => ../callgraph

replace github.com/mactavishz/FaaS-Platform-Knowledge-Optimization => ..

replace github.com/openfaas/go-sdk => ../go-sdk
