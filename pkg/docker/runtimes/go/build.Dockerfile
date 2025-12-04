ARG GO_VERSION=1.25.5
ARG ALPINE_VERSION=3.23

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION}

RUN apk add --no-cache build-base binutils-gold

WORKDIR /usr/src/app

# Build the runtime binary
COPY main.go ./
RUN go mod init tfruntime && \
    CGO_ENABLED=1 go build -o runtime.bin .