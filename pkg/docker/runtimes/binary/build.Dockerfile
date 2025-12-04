ARG GO_VERSION=1.25.5
ARG ALPINE_VERSION=3.23

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

WORKDIR /usr/src/build
COPY main.go .
RUN GO111MODULE=off CGO_ENABLED=0 go build -o runtime.bin .

FROM alpine:${ALPINE_VERSION}

# Create app directory
WORKDIR /usr/src/app

COPY --from=builder /usr/src/build/runtime.bin .
