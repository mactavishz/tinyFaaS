# tinyFaaS: A Lightweight FaaS Platform for Edge Environments

This repo is a fork of tinyFaaS.

tinyFaaS is a lightweight FaaS (Function-as-a-Service) platform for edge environment with a focus on performance in constrained environments.

## License

The code in this repository is licensed under the terms of the [MIT](./LICENSE) license.

## Instructions

**Disclaimer**: Please note that this will use your computer's Docker instance to manage containers and will allow anyone in your network to start Docker containers with arbitrary code.
If you don't know what this means you do _not_ want to run this on your computer.
Additionally, note that this software is provided as a research prototype and is not production-ready.

### About

tinyFaaS comprises the _management service_, the _reverse proxy_, and a number of _function handlers_.
In order to run tinyFaaS, the management service has to be deployed.
It will then automatically start the reverse proxy.
Once a function is deployed to tinyFaaS, function handlers are created automatically.

### Prerequisites

Before you get started, make sure you have the following dependencies installed:

- Go (>=v1.22) to compile management service and reverse proxy
- Docker (>=v24)
- Make
- a writable directory (tinyFaaS writes temporary files to a `./tmp` directory)

Note that tinyFaaS is intended for Linux hosts (`x86_64` and `arm64`).
Due to limitations of Docker Desktop for Mac, installing and running [`docker-mac-net-connect`](https://github.com/chipmk/docker-mac-net-connect) is necessary to run tinyFaaS on macOS hosts.
Running tinyFaaS on Windows computers (native or through WSL) is probably possible but has not been tested and is thus not recommended.

### Getting Started

Start tinyFaaS with:

```sh
make start
```

The reverse proxy will be started automatically.
Please note that you cannot use tinyFaaS until the reverse proxy is running.

### Managing Functions

To manage functions on tinyFaaS, use the included scripts included in `./src/scripts`.

To upload a function, run `upload.sh {FOLDER} {NAME} {ENV} {REPLICAS}`, where `{FOLDER}` is the path to your function code, `{NAME}` is the name for your function, `{ENV}` is the environment you would like to use (`python3`, `nodejs`, or `binary`), and `{REPLICAS}` is the desired number of function handlers for your function.
For example, you might call `./scripts/upload.sh "./test/fns/sieve-of-eratosthenes" "sieve" "nodejs" 1` to upload the _sieve of Eratosthenes_ example function included in this repository.
This requires the `zip` and `curl` utilities.

Alternatively, you can also upload functions from a zipped file available at some URL.
Use the included script as a starting point: `uploadURL.sh {URL} {SUBFOLDER_PATH} {NAME} {ENV} {REPLICAS}`, where `{URL}` is the URL to a zip that has your function code, `{SUBFOLDER_PATH}` is the folder of the code within that zip (use `/` if the code is in the top-level), `{NAME}` is the name for your function, `{ENV}` is the environment, and `{REPLICAS}` is the desired number of function handlers for your function.
For example, you might call `uploadURL.sh "https://github.com/OpenFogStack/tinyFaas/archive/main.zip" "tinyFaaS-main/test/fns/sieve-of-eratosthenes" "sieve" "nodejs" 1` to upload the _sieve of Eratosthenes_ example function included in this repository.

To get a list of existing functions, run `list.sh`.

To delete a function, run `delete.sh {NAME}`, where `{NAME}` is the name of the function you want to remove.

Additionally, we provide scripts to read logs from your function and to wipe all functions from tinyFaaS.

### Writing Functions

This tinyFaaS prototype only supports functions written for NodeJS 20, Python 3.9, and binary functions.
A good place to get started with writing functions is the selection of test functions in [`./test/fns`](./test/fns/).
HTTP headers and GRPC Metadata are accessible in NodeJS and Python functions as key values. Check the "show-headers" test functions for more information.

#### NodeJS 20

Your function must be supplied as a Node module with the name `fn` that exports a single function that takes the `req` and `res` parameters for request and response, respectively.
`res` supports the `send()` function that has one parameter, a string that is passed to the client as-is.

#### Python 3.9

Your function must be supplied as a file named `fn.py` that exposes a method `fn` that is invoked for every function invocation.
This method must accept a string as an input (that can also be `None`) and must provide a string as a return value.
You may also provide a `requirements.txt` file from which dependencies will be installed alongside your function.
Any other data you provide will be available.

#### Binary

Your function must be provided as a `fn.sh` shell script that is invoked for every function call.
This shell script may also call other binaries as needed.
Input data is provided from `stdin`.
Output responses should be provided on `stdout`.

### Calling Functions

tinyFaaS supports only HTTP function invocations at the moment.

#### HTTP

To call a tinyFaaS function using its HTTP endpoint, make a GET or POST request to `http://{HOST}:{PORT}/{NAME}` where `{HOST}` is the address of the tinyFaaS host, `{PORT}` is the port for the tinyFaaS HTTP endpoint (default is `8000`), and `{NAME}` is the name of your function.
You may include data in any form you want, it will be passed to your function.

TLS is not supported (but contributions are welcome).

To make an asynchronous request, pass the `X-tinyFaaS-Async` header with any value.
An asynchronous request means the client will receive a `202` response code immediately and no function results will be sent back.

```sh
curl --header "X-tinyFaaS-Async: true" "http://localhost:8000/sieve"
```

### Removing tinyFaaS

When you stop the management service with `SIGINT` (`Ctrl+C`), the reverse proxy and all function handlers should be stopped.
You can also use:

```bash
make clean
```

OR

```bash
docker rm -f $$(docker ps -a -q --filter label=tinyFaaS)
docker network rm $$(docker network ls -q --filter label=tinyFaaS)
docker rmi $$(docker image ls -q --filter label=tinyFaaS)
rm -rf ./tmp
```

### Specifying Ports

By default, tinyFaaS will use the following ports:

| Port | Protocol | Description        |
| ---- | -------- | ------------------ |
| 8080 | TCP      | Management Service |
| 8000 | TCP      | Reverse Proxy      |

### Tests

The tests in [`./test`](./test) test the end-to-end functionality of tinyFaaS and are expected to complete successfully.
We use these tests during development to ensure no patches break any functionality.
The tests can also serve as documentation on the expected behavior of tinyFaaS.

Before running the tests, make sure tinyFaaS is already running.

If you do not install these requirements, test runs are limited to invocations with HTTP.

Run the tests with:

```sh
make test
```

## Known Issues

On macOS, [`docker-mac-net-connect`](https://github.com/chipmk/docker-mac-net-connect) is necessary to run tinyFaaS.
There is a [known issue in `docker-mac-net-connect`](https://github.com/chipmk/docker-mac-net-connect/issues/36) that silently breaks the tunnel when Docker Desktop enters its resource saver mode.
If you find that tinyFaaS does not start properly on your macOS host, try restarting the tunnel:

```sh
sudo brew services restart docker-mac-net-connect
```
