# Builder stage: compile the user function plugin
FROM tinyfaas-runtime-go AS builder

ENV CGO_ENABLED=1
WORKDIR /usr/src/app/fn
COPY fn/* ./

RUN go mod tidy && \
    go build -buildmode=plugin -o handler.so .

# Final stage: keep a glibc-based runner because the runtime binary and
# plugin are built with CGO enabled and will not execute correctly on Alpine.
FROM debian:bookworm-slim AS runner

WORKDIR /usr/src/app

# Copy runtime binary from base image
COPY --from=tinyfaas-runtime-go /usr/src/app/runtime.bin ./

# Copy compiled plugin from builder
COPY --from=builder /usr/src/app/fn/handler.so ./fn/

EXPOSE 8000

CMD ["./runtime.bin"]