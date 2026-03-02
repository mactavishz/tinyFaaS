ARG GO_VERSION=1.26

# Build stage - compile the runtime handler
FROM golang:${GO_VERSION}-bookworm AS builder
ENV CGO_ENABLED=1
RUN apt-get update && apt-get install -y build-essential binutils

WORKDIR /usr/src/build
COPY main.go .
RUN go mod init tfruntime && go mod tidy && go build -o runtime.bin .

# Final stage - lightweight image with Go toolchain for function compilation
FROM builder

WORKDIR /usr/src/app

# Copy pre-built runtime handler from builder
COPY --from=builder /usr/src/build/runtime.bin .
COPY --from=builder /usr/src/build/go.mod . 