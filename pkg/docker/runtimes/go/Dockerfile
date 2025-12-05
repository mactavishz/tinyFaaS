# Builder stage: compile the user function plugin
ARG ALPINE_VERSION=3.23
FROM tinyfaas-runtime-go AS builder

ENV CGO_ENABLED=1
WORKDIR /usr/src/app/fn
COPY fn/* ./

RUN go mod tidy && \
    go build -buildmode=plugin -o handler.so .

# Final stage: minimal runtime with only compiled artifacts
ARG ALPINE_VERSION=3.23
FROM alpine:${ALPINE_VERSION} AS runner

WORKDIR /usr/src/app

# Copy runtime binary from base image
COPY --from=tinyfaas-runtime-go /usr/src/app/runtime.bin ./

# Copy compiled plugin from builder
COPY --from=builder /usr/src/app/fn/handler.so ./fn/

EXPOSE 8000

CMD ["./runtime.bin"]