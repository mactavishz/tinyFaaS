FROM tinyfaas-runtime-go

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