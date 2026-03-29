# tinyFaaS: Lightweight FaaS for Edge Environments

This repository contains a tinyFaaS fork used as a research prototype.

tinyFaaS is a lightweight Function-as-a-Service platform focused on constrained environments. The platform currently runs as three services: gateway, manager, and reverse proxy (rproxy), plus per-function containers.

## Safety Notice

tinyFaaS manages Docker containers and can run arbitrary uploaded code. Only run it in environments you trust.

This software is research-grade and not production-ready.

## Prerequisites

- Go 1.22+
- Docker 24+
- Make
- `zip`, `curl`, `uuidgen` (required by helper scripts)
- `systemd` and `sudo` access (required by `make up` / `make down`)
- [Vagrant](https://developer.hashicorp.com/vagrant) for testing

## Quick Start

Assuming you have a Linux environment with the prerequisites, you can start tinyFaaS services with:

```sh
make up
```

This command rebuilds, installs, writes `/etc/default/tinyfaas`, and starts `tf-gateway`, `tf-rproxy`, and `tf-manager` via `systemd`.

Stop and uninstall services:

```sh
make down
```

## Service Model and Ports

| Service | Default Bind | Purpose |
| --- | --- | --- |
| Gateway (`tf-gateway`) | `0.0.0.0:80` | Public entrypoint. Routes `/fn/*` and `/system/*`. |
| Manager (`tf-manager`) | `127.0.0.1:8080` | Deploy/list/delete/logs/scale/heartbeat control plane. |
| RProxy (`tf-rproxy`) | `127.0.0.1:8000` | Invocation routing and callgraph/autoscaler integration. |

Standard invocation path is through the gateway: `/fn/{name}`.

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
- Implement:

```python
from typing import Dict, Optional, Union

def handle(input: Optional[str], headers: Optional[Dict[str, str]]) -> Optional[Union[str, dict]]:
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

Asynchronous invocation is enabled by sending header `X-Tinyfaas-Async` (any value):

```sh
curl -H "X-Tinyfaas-Async: true" http://localhost/fn/sieve
```

Async requests return `202 Accepted` without function output.

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

`make up` reads optional local overrides from `.tinyfaas.env` and writes runtime config to `/etc/default/tinyfaas`.

| Environment Variable | Default | Description |
| --- | --- | --- |
| `TF_GATEWAY_IP` | `0.0.0.0` | Gateway bind address. |
| `TF_GATEWAY_PORT` | `80` | Gateway port. |
| `TF_MANAGER_PORT` | `8080` | Manager port (loopback). |
| `TF_RPROXY_PORT` | `8000` | RProxy port (loopback). |
| `TF_ENV` | `development` | `development` enables callgraph debug endpoints. |
| `TF_BACKEND` | `docker` | Runtime backend. |
| `TF_AUTOSCALER_ENABLED` | `true` | Enable autoscaler integration. |
| `TF_CALLGRAPH_ENABLED` | `true` | Enable callgraph tracking. |
| `TF_DEFAULT_SCALE_TO_ZERO_IDLE_DURATION` | `5m` | Default scale-to-zero idle duration. |

## Debugging and Troubleshooting

Check service status:

```sh
sudo systemctl status tf-gateway tf-rproxy tf-manager
```

Stream logs:

```sh
sudo journalctl -u tf-gateway -o cat -f
sudo journalctl -u tf-rproxy -o cat -f
sudo journalctl -u tf-manager -o cat -f
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
| `make build` | Build `tf-manager`, `tf-rproxy`, `tf-gateway`. |
| `make build-runtime-images` | Pre-build runtime base images. |
| `make install` | Install binaries and `systemd` units. |
| `make up` | Rebuild, reinstall, configure env, and start services. |
| `make down` | Stop services, uninstall binaries/units, and clean artifacts. |
| `make clean` | Remove build artifacts and tinyFaaS Docker assets. |
| `make clean-all` | `make clean` plus embedded runtime artifacts and base images. |
| `make unit-test` | Run package unit tests. |
| `make integration-test` | Run end-to-end integration tests. |

## License

Licensed under [MIT](./LICENSE).
