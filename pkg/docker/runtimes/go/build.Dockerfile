ARG GO_VERSION=1.25.5
ARG ALPINE_VERSION=3.23

# Build stage - compile the runtime handler
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
ENV CGO_ENABLED=1
RUN apk add build-base binutils-gold

WORKDIR /usr/src/build
COPY main.go .
RUN go mod init tfruntime && go mod tidy && go build -o runtime.bin .

# Final stage - lightweight image with Go toolchain for function compilation
FROM builder

WORKDIR /usr/src/app

# Copy pre-built runtime handler from builder
COPY --from=builder /usr/src/build/runtime.bin .
COPY --from=builder /usr/src/build/go.mod . 