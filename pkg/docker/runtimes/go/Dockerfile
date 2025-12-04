ARG GO_VERSION=1.25.5
ARG ALPINE_VERSION=3.23

FROM scratch

# Extract full filesystem with Go toolchain
ADD blob.tar.gz /

ENV PATH="/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

WORKDIR /usr/src/app

# Copy user function code
COPY fn/* ./fn/

# Build the handler as a plugin
WORKDIR /usr/src/app/fn
RUN go mod tidy && \
    go mod download && \
    CGO_ENABLED=1 go build -buildmode=plugin -o handler.so .

WORKDIR /usr/src/app

EXPOSE 8000

CMD ["./runtime.bin"]