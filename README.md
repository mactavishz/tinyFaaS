# tinyFaaS: Lightweight FaaS for Edge Environments

This repository contains a tinyFaaS fork used as a research prototype.

tinyFaaS is a lightweight Function-as-a-Service platform focused on constrained environments. The platform currently runs as a public gateway, a merged internal server, a NATS Streaming broker, and an async queue worker, plus per-function containers.

## Safety Notice

tinyFaaS manages Docker containers and can run arbitrary uploaded code. Only run it in environments you trust.

This software is research-grade and not production-ready.

## Prerequisites

- Go 1.22+
- Docker 24+
- Make
- `zip`, `curl`, `uuidgen` (required by helper scripts)
- `systemd` and `sudo` access (`make down`)
- [Vagrant](https://developer.hashicorp.com/vagrant) for testing

## Quick Start

Assuming you have a Linux environment with the prerequisites, you can start tinyFaaS services after installing with:

```sh
make install
```

This command builds the binary artifacts, container runtimes, and creates `systemd` service units. If you want to customize configuration, create a file at `/etc/default/tinyfaas` with environment variable overrides (see Configuration section below).

After installation, start the services with:

```sh
sudo systemctl daemon-reload
sudo systemctl enable tf-gateway tf-nats tf-server tf-queue-worker
sudo systemctl start tf-gateway tf-nats tf-server tf-queue-worker
```

Stop and uninstall services:

```sh
make down
```

## Service Model and Ports

| Service | Default Bind | Purpose |
| --- | --- | --- |
| Gateway (`tf-gateway`) | `0.0.0.0:8080` | Public entrypoint. Routes `/fn/*`, `/async-fn/*`, and `/system/*`. |
| Server (`tf-server`) | `127.0.0.1:8000` | Deploy/list/delete/logs, sync routing, autoscaler, callgraph, and async enqueue. |
| NATS Streaming (`tf-nats`) | `127.0.0.1:4222` | Async invocation queue. |
| Queue worker (`tf-queue-worker`) | n/a | Dequeues async requests and invokes `tf-server` via `/invoke/*`. |

Standard synchronous invocation path is through the gateway: `/fn/{name}`. Async invocation uses `/async-fn/{name}`.

## Deploy and Manage Functions

Helper scripts are in [`./scripts`](./scripts).

| Command | Description |
| --- | --- |
| `./scripts/upload.sh <folder> <name> <env> <replicas>` | Upload local function source as zip. |
| `./scripts/uploadURL.sh <url> <subfolder_path> <name> <env> <replicas>` | Upload function source from a remote zip URL. |
| `./scripts/list.sh` | List deployed functions. |
| `./scripts/delete.sh <name>` | Delete one function. |
| `./scripts/logs.sh [name]` | Stream logs (all functions or one function). |
| `./scripts/wipe-functions.sh` | Delete all functions. |

Script defaults are `GATEWAY_HOST=localhost` and `GATEWAY_PORT=80`.

Supported runtime values for `<env>` are `nodejs`, `python3`, `go`, and `binary`.

## Runtime Contracts

Reference examples are in [`./test/fns`](./test/fns).

### Node.js

- Provide `handler.js`.
- Export a default Express-style handler `(req, res) => { ... }`.
- Return responses via `res.send(...)`.

### Python 3

- Provide `handler.py`.
- Implement `handle(request)` and return a dict with keys `body`, `headers` and `statusCode`. You can also return a flask-compatible response type such as a tuple `(body, statusCode, headers)` or a `Response` object. The runtime normalizes these into the standard dict format. For example:

```python
def handle(request):
    return {
        "body": "Hello World!",
        "headers": {
            "Content-Type": "text/plain"
        },
        "statusCode": 200
    }
    # or simply return the body with default headers and status code:
    # return "Hello World!"
    # or return a tuple with body and status code:
    # return "Hello World!", 200
    # or return a tuple with body, status code, and headers:
    # return "Hello World!", 200, {"Content-Type": "text/plain"}
```

- The `request` argument is the Flask request object exposed by the runtime.
- The runtime serializes the returned dictionary as JSON.
- Both synchronous and asynchronous handlers are supported. For example:

```python
def handle(request) -> dict:
    ...
```

- Optional `requirements.txt` is installed during function build.

### Go

- Provide `handler.go` with package `main`.
- Export symbol:

```go
func Handle(body []byte, headers map[string]string) (string, error)
```

### Binary

- Provide executable `handler.sh`.
- Read request body from `stdin` and write response to `stdout`.

## Invoke Functions

Use HTTP `GET` or `POST`:

```sh
curl -X POST http://localhost/fn/echo-js -d 'hello'
```

Asynchronous invocation uses `/async-fn/{name}`:

```sh
curl -X POST http://localhost/async-fn/sieve -d 'hello'
```

Async requests return `202 Accepted` with an empty body. Header-based async invocation is no longer supported; `/fn/{name}` is always synchronous.

## Management API (Gateway)

All endpoints below are exposed through gateway path prefix `/system`.

| Endpoint | Method | Request Body | Notes |
| --- | --- | --- | --- |
| `/system/upload` | `POST` (multipart) | `metadata` JSON part + `zip` file part | Deploy from uploaded zip archive. |
| `/system/uploadURL` | `POST` | JSON: `name`, `env`, `replicas`, `url`, `subfolder_path`, optional `envs`, `labels`, `limits` | Deploy from remote zip. |
| `/system/delete` | `POST` | JSON: `{"name":"<fn>"}` | Delete one function. |
| `/system/list` | `GET` | None | List functions as JSON. |
| `/system/wipe` | `POST` | Empty body | Delete all functions. |
| `/system/logs` | `GET` | Query optional: `?name=<fn>` | Function logs. |
| `/system/scale-up` | `POST` | JSON: `name`, `cold` | Internal-only, localhost-restricted. |
| `/system/heartbeat` | `POST` | JSON: `name` or `functions` array | Internal-only, localhost-restricted. |
| `/system/callgraph` | `GET` | None | Development mode only. |
| `/system/callgraph/function/{name}` | `GET` | None | Development mode only. |
| `/system/callgraph/edge?callee=<fn>&caller=<fn>` | `GET` | None | Development mode only (`caller` is optional). |

## Cold Start Readiness Semantics

- During scale-up, manager startup waits for container readiness checks to complete before the function is considered running.
- Readiness checks require each replica to have:
  - a valid container IP on the function network
  - `GET /health` returning `200 OK` on port `8000`
- Replica readiness checks run concurrently, but route activation still requires **all configured replicas** to become ready.
- RProxy records callgraph edges only after a function route is confirmed ready for invocation. Failed cold-start attempts that cannot route traffic do not create edge records.

### `/system/upload` Metadata Schema

The multipart `metadata` JSON supports:

```json
{
  "name": "function-name",
  "env": "nodejs|python3|go|binary",
  "replicas": 1,
  "envs": ["KEY=value"],
  "labels": {"key": "value"},
  "limits": {
    "memory": "512Mi",
    "cpu": "500m"
  }
}
```

## Configuration

You can create a file at `/etc/default/tinyfaas` with environment variable overrides to customize configuration. Available environment variables and defaults are listed below. If the file is not present, defaults and any environment variables set in the shell will be used.

| Environment Variable | Default | Description |
| --- | --- | --- |
| `GATEWAY_IP` | `0.0.0.0` | Gateway bind address. |
| `GATEWAY_PORT` | `8080` | Gateway port. |
| `TINYFAAS_PORT` | `8000` | Merged internal server port (loopback). |
| `TINYFAAS_NATS_URL` | `nats://127.0.0.1:4222` | NATS Streaming URL. |
| `TINYFAAS_NATS_CLUSTER` | `faas-cluster` | NATS Streaming cluster ID. |
| `TINYFAAS_NATS_SUBJECT` | `faas-request` | Async invocation subject. |
| `TINYFAAS_NATS_QUEUE_GROUP` | `faas` | Queue worker group. |
| `TINYFAAS_QUEUE_ACK_WAIT` | `5m5s` | Queue redelivery wait for unacked messages. |
| `TINYFAAS_QUEUE_MAX_INFLIGHT` | `1` | Maximum in-flight queued messages per worker. |
| `ENV` | `development` | `development` enables callgraph debug endpoints. |
| `BACKEND` | `docker` | Runtime backend. |
| `AUTOSCALER_ENABLED` | `true` | Enable autoscaler integration. |
| `CALLGRAPH_ENABLED` | `true` | Enable callgraph tracking. |
| `CALLGRAPH_METHOD` | `SMA` | Averaging method for callgraph stats: `SMA` or `EMA` (case-insensitive). |
| `CALLGRAPH_SMA_WINDOW_SIZE` | `10` | SMA window size used when method resolves to `SMA`; invalid values fall back to `10`. |
| `CALLGRAPH_EMA_ALPHA` | `0.3` | EMA alpha used when method resolves to `EMA`; invalid values fall back to `0.3`. |
| `DEFAULT_SCALE_TO_ZERO_IDLE_DURATION` | `5m` | Default scale-to-zero idle duration. |

## Debugging and Troubleshooting

Check service status:

```sh
sudo systemctl status tf-gateway tf-nats tf-server tf-queue-worker
```

Stream logs:

```sh
sudo journalctl -u tf-gateway -o cat -f
sudo journalctl -u tf-nats -o cat -f
sudo journalctl -u tf-server -o cat -f
sudo journalctl -u tf-queue-worker -o cat -f
```

## Testing

- Unit tests:

```sh
make unit-test
```

- Integration tests:

```sh
make integration-test
```

You can override gateway URL for integration tests with `TINYFAAS_TEST_GATEWAY_URL`.

## Development Commands

| Command | Description |
| --- | --- |
| `make build` | Build `tf-server`, `tf-queue-worker`, and `tf-gateway`. |
| `make build-runtime-images` | Pre-build runtime base images. |
| `make install` | Install binaries and `systemd` units. |
| `make down` | Stop services, uninstall binaries/units, and clean artifacts. |
| `make clean` | Remove build artifacts and tinyFaaS Docker assets. |
| `make clean-all` | `make clean` plus embedded runtime artifacts and base images. |
| `make unit-test` | Run package unit tests. |
| `make integration-test` | Run end-to-end integration tests. |

## License

Licensed under [MIT](./LICENSE).
