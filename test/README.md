# tinyFaaS Tests

This directory contains integration tests for tinyFaaS.

Function fixtures are defined in `tinyFaaS/test/fns/stack.yaml` and tests deploy via `faas-cli` (not `/system/upload`).
Single-function deploy/remove is done with stack filtering, for example:

```bash
faas-cli deploy --platform tinyfaas --gateway http://127.0.0.1:8080 -f ./tinyFaaS/test/fns/stack.yaml --filter echo-js
faas-cli remove --platform tinyfaas --gateway http://127.0.0.1:8080 -f ./tinyFaaS/test/fns/stack.yaml --filter echo-js
```

To run the tests, ensure you have a working tinyFaaS deployment and execute the following command from the root of the tinyFaaS project:

```bash
make test
```
