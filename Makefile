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
	@echo "  make start                  - Show commands to start services"
	@echo "  make test                   - Run tests"
	@echo "  make clean                  - Clean build artifacts (preserves embedded runtime files)"
	@echo "  make install                - Install binaries and systemd services"
	@echo ""
	@echo "Runtime management:"
	@echo "  make build-runtime-images   - Pre-build all runtime base images (optional)"
	@echo "  make rebuild-runtime-images - Force rebuild all runtime base images"
	@echo "  make clean-runtime-images   - Remove runtime base images from Docker"
	@echo "  make clean-runtimes         - Clean embedded runtime files (forces re-copy on build)"
	@echo ""
	@echo "IMPORTANT: Runtime base images must be pre-built before starting manager."
	@echo "           Run 'make build-runtime-images' or 'make install' first."

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

.PHONY: start
start: build
	@echo "Starting tinyFaaS services..."
	@echo "First, ensure runtime base images are built:"
	@echo "  make build-runtime-images"
	@echo ""
	@echo "Then start services (rproxy first, then manager):"
	@echo "  ./tf-rproxy-$(OS)-$(ARCH) 127.0.0.1:8081 http:127.0.0.1:8000"
	@echo "  ./tf-manager-$(OS)-$(ARCH)"

.PHONY: unit-test
unit-test:
	@echo "Running unit tests..."
	go test -count=1 -v ./pkg/... --cover

.PHONY: test
test:
	@echo "Running integration tests..."
	go test -count=1 -v -timeout 5m ./test

.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -f tf-manager-*
	rm -f tf-rproxy-*
	rm -f tinyfaas-*
	@echo "Running additional clean-up script..."
	./clean.sh
	@echo "Clean complete (runtime blobs preserved)"
	@echo "To also clean runtime blobs, run: make clean-runtimes"

.PHONY: clean-runtimes
clean-runtimes:
	@echo "Cleaning embedded runtime files..."
	rm -rf pkg/docker/runtimes-amd64
	rm -rf pkg/docker/runtimes-arm
	rm -rf pkg/docker/runtimes-arm64
	@echo "Embedded runtime files cleaned"

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

.PHONY: clean-runtime-images
clean-runtime-images:
	@echo "Removing runtime base images from Docker..."
	@docker images --filter "label=tinyfaas-type=base-image" -q | xargs -r docker rmi -f 2>/dev/null || true
	@echo "Runtime base images removed"

.PHONY: rebuild-runtime-images
rebuild-runtime-images: clean-runtime-images build-runtime-images
	@echo "Runtime images rebuilt"

.PHONY: install
install: clean-runtimes build build-runtime-images
	@echo "Installing binaries to $(BINDIR)..."
	install -d $(BINDIR)
	install -m 755 tf-manager-$(OS)-$(ARCH) $(BINDIR)/tf-manager
	install -m 755 tf-rproxy-$(OS)-$(ARCH) $(BINDIR)/tf-rproxy
	install -m 755 tf-gateway-$(OS)-$(ARCH) $(BINDIR)/tf-gateway
	@echo "Creating working directory..."
	install -d /var/lib/tinyfaas
	@echo "Installing systemd service files to $(SYSTEMD_DIR)..."
	install -d $(SYSTEMD_DIR)
	install -m 644 systemd/tf-rproxy.service $(SYSTEMD_DIR)/
	install -m 644 systemd/tf-manager.service $(SYSTEMD_DIR)/
	install -m 644 systemd/tf-gateway.service $(SYSTEMD_DIR)/
	@echo ""
	@echo "Installation complete. To enable and start the services:"
	@echo "  sudo systemctl daemon-reload"
	@echo "  sudo systemctl enable tf-gateway tf-rproxy tf-manager"
	@echo "  sudo systemctl start tf-gateway tf-rproxy tf-manager"

.PHONY: uninstall
uninstall:
	@echo "Stopping services..."
	-systemctl stop tf-manager tf-rproxy tf-gateway 2>/dev/null || true
	-systemctl disable tf-manager tf-rproxy tf-gateway 2>/dev/null || true
	@echo "Removing binaries and service files..."
	rm -f $(BINDIR)/tf-manager $(BINDIR)/tf-rproxy $(BINDIR)/tf-gateway
	rm -f $(SYSTEMD_DIR)/tf-manager.service $(SYSTEMD_DIR)/tf-rproxy.service $(SYSTEMD_DIR)/tf-gateway.service
	systemctl daemon-reload
	@echo "Uninstall complete"

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
