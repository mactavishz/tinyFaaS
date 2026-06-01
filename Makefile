PROJECT_NAME := "tinyFaaS"
PKG := "github.com/OpenFogStack/$(PROJECT_NAME)"
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/ | grep -v /ext/ | grep -v _test.go)
TEST_DIR := ./test

SUPPORTED_ARCH=amd64 arm arm64
RUNTIMES := $(shell find pkg/docker/runtimes -name Dockerfile | xargs -n1 dirname | xargs -n1 basename)

OS=$(shell go env GOOS)
ARCH=$(shell go env GOARCH)

# Installation paths
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
SYSTEMD_DIR ?= /etc/systemd/system

.PHONY: all
all: build

.PHONY: help
help:
	@echo "tinyFaaS Build System"
	@echo ""
	@echo "Main targets:"
	@echo "  make build                  - Build tf-manager and tf-rproxy binaries"
	@echo "  make install                - Build binaries and install systemd services"
	@echo "  make down                   - Stop tinyFaaS services using systemd"
	@echo "  make unit-test              - Run unit tests"
	@echo "  make integration-test       - Run integration tests"
	@echo "  make clean                  - Clean build artifacts"
	@echo "  make clean-all              - Clean all build artifacts and runtime files and images"
	@echo "  make build-runtime-images   - Pre-build all runtime base images (optional)"

.PHONY: bin-name
bin-name:
	@echo "tf-manager-$(OS)-$(ARCH) tf-rproxy-$(OS)-$(ARCH)"

.PHONY: build
build: tf-manager-${OS}-${ARCH} tf-rproxy-${OS}-${ARCH} tf-gateway-${OS}-${ARCH}

.PHONY: build-manager
build-manager: tf-manager-${OS}-${ARCH}

.PHONY: build-rproxy
build-rproxy: tf-rproxy-${OS}-${ARCH}

.PHONY: build-gateway
build-gateway: tf-gateway-${OS}-${ARCH}

.PHONY: unit-test
unit-test: pkg/docker/runtimes-$(ARCH)
	@echo "Running unit tests..."
	go test -v ./pkg/... --cover

.PHONY: integration-test
integration-test: pkg/docker/runtimes-$(ARCH)
	@echo "Running integration tests..."
	go test -count=1 -v -timeout 10m ./test/integrations/...

.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -f tf-manager-*
	rm -f tf-rproxy-*
	rm -f tf-gateway-*
	rm -f tinyfaas-*
	@echo "Running additional clean-up script..."
	./clean.sh
	@echo "Clean complete (runtime blobs preserved)"

.PHONY: clean-all
clean-all:
	make clean
	@echo "Cleaning embedded runtime files..."
	rm -rf pkg/docker/runtimes-amd64
	rm -rf pkg/docker/runtimes-arm
	rm -rf pkg/docker/runtimes-arm64
	@echo "Cleaning runtime base images from Docker..."
	docker images --filter "label=tinyfaas-type=base-image" -q | xargs -r docker rmi -f 2>/dev/null || true

.PHONY: build-runtime-images
build-runtime-images:
	@echo "Building runtime base images locally..."
	@for runtime in $(RUNTIMES); do \
		echo "Building tinyfaas-runtime-$$runtime..."; \
		docker build -t tinyfaas-runtime-$$runtime \
			--label tinyfaas-runtime=$$runtime \
			--label tinyfaas-type=base-image \
			-f pkg/docker/runtimes/$$runtime/build.Dockerfile \
			pkg/docker/runtimes/$$runtime/ || exit 1; \
	done
	@echo "All runtime base images built successfully"

.PHONY: install
install: clean-all build build-runtime-images
	@echo "Installing binaries to $(BINDIR)..."
	sudo install -d $(BINDIR)
	sudo install -m 755 tf-manager-$(OS)-$(ARCH) $(BINDIR)/tf-manager
	sudo install -m 755 tf-rproxy-$(OS)-$(ARCH) $(BINDIR)/tf-rproxy
	sudo install -m 755 tf-gateway-$(OS)-$(ARCH) $(BINDIR)/tf-gateway
	@echo "Creating working directory..."
	sudo install -d /var/lib/tinyfaas
	@echo "Installing systemd service files to $(SYSTEMD_DIR)..."
	sudo install -d $(SYSTEMD_DIR)
	sudo install -m 644 systemd/tf-rproxy.service $(SYSTEMD_DIR)/
	sudo install -m 644 systemd/tf-manager.service $(SYSTEMD_DIR)/
	sudo install -m 644 systemd/tf-gateway.service $(SYSTEMD_DIR)/
	@echo ""
	@echo "Installation complete. To enable and start the services:"
	@echo "  sudo systemctl daemon-reload"
	@echo "  sudo systemctl enable tf-gateway tf-rproxy tf-manager"
	@echo "  sudo systemctl start tf-gateway tf-rproxy tf-manager"

.PHONY: uninstall
uninstall:
	@echo "Removing binaries and service files..."
	sudo rm -f $(BINDIR)/tf-manager $(BINDIR)/tf-rproxy $(BINDIR)/tf-gateway
	sudo rm -f $(SYSTEMD_DIR)/tf-manager.service $(SYSTEMD_DIR)/tf-rproxy.service $(SYSTEMD_DIR)/tf-gateway.service
	@echo "Uninstall complete"

.PHONY: down
down:
	./down.sh

.PHONY: debug
debug:
	@echo "PROJECT_NAME: $(PROJECT_NAME)"
	@echo "PKG: $(PKG)"
	@echo "GO_FILES: $(GO_FILES)"
	@echo "SUPPORTED_ARCH: $(SUPPORTED_ARCH)"
	@echo "RUNTIMES: $(RUNTIMES)"
	@echo "OS: $(OS)"
	@echo "ARCH: $(ARCH)"

# Copy only Dockerfile to architecture-specific directory for embedding
# (runtime base images must be pre-built, build.Dockerfile and handler files not needed)
define arch_build
pkg/docker/runtimes-$(arch): $(foreach runtime,$(RUNTIMES),pkg/docker/runtimes-$(arch)/$(runtime))
endef
$(foreach arch,$(SUPPORTED_ARCH),$(eval $(arch_build)))

define runtime_build
pkg/docker/runtimes-$(arch)/$(runtime): pkg/docker/runtimes/$(runtime)/Dockerfile
	mkdir -p $$@
	cp pkg/docker/runtimes/$(runtime)/Dockerfile $$@/
	@touch $$@
endef
$(foreach arch,$(SUPPORTED_ARCH),$(foreach runtime,$(RUNTIMES),$(eval $(runtime_build))))

# Build manager binary (embeds runtime source files, base images built at startup)
tf-manager-darwin-%: pkg/docker/runtimes-% $(GO_FILES)
	GOOS=darwin GOARCH=$* go build -buildvcs=false -o $@ -v $(PKG)/cmd/manager

tf-manager-linux-%: pkg/docker/runtimes-% $(GO_FILES)
	GOOS=linux GOARCH=$* go build -buildvcs=false -o $@ -v $(PKG)/cmd/manager

# Build rproxy binary (standalone, no runtime dependencies)
tf-rproxy-darwin-%: $(GO_FILES)
	GOOS=darwin GOARCH=$* go build -buildvcs=false -o $@ -v $(PKG)/cmd/rproxy

tf-rproxy-linux-%: $(GO_FILES)
	GOOS=linux GOARCH=$* go build -buildvcs=false -o $@ -v $(PKG)/cmd/rproxy

# Build gateway binary (standalone, no runtime dependencies)
tf-gateway-darwin-%: $(GO_FILES)
	GOOS=darwin GOARCH=$* go build -buildvcs=false -o $@ -v $(PKG)/cmd/gateway

tf-gateway-linux-%: $(GO_FILES)
	GOOS=linux GOARCH=$* go build -buildvcs=false -o $@ -v $(PKG)/cmd/gateway
