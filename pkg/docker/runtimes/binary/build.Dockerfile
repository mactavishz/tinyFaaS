ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-bookworm AS builder

WORKDIR /usr/src/build
COPY main.go .
RUN GO111MODULE=off CGO_ENABLED=0 go build -o runtime.bin .

FROM debian:bookworm-slim

# Create app directory
WORKDIR /usr/src/app

COPY --from=builder /usr/src/build/runtime.bin .
