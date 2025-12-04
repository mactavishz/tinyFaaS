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

.PHONY: bin-name
bin-name:
	@echo "tf-manager-$(OS)-$(ARCH) tf-rproxy-$(OS)-$(ARCH)"

.PHONY: build
build: tf-manager-${OS}-${ARCH} tf-rproxy-${OS}-${ARCH}

.PHONY: build-manager
build-manager: tf-manager-${OS}-${ARCH}

.PHONY: build-rproxy
build-rproxy: tf-rproxy-${OS}-${ARCH}

.PHONY: start
start: build
	@echo "Starting tinyFaaS services..."
	@echo "Start rproxy first, then manager:"
	@echo "  ./tf-rproxy-$(OS)-$(ARCH) 127.0.0.1:8081 http:127.0.0.1:8000"
	@echo "  ./tf-manager-$(OS)-$(ARCH)"

.PHONY: test
test: ${TEST_DIR}/test_all.py
	@python3 ${TEST_DIR}/test_all.py

.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -f tf-manager-*
	rm -f tf-rproxy-*
	rm -f tinyfaas-*
	rm -rf pkg/docker/runtimes-amd64
	rm -rf pkg/docker/runtimes-arm
	rm -rf pkg/docker/runtimes-arm64
	@echo "Running additional clean-up script..."
	./clean.sh
	@echo "Clean complete"

.PHONY: install
install: build
	@echo "Installing binaries to $(BINDIR)..."
	install -d $(BINDIR)
	install -m 755 tf-manager-$(OS)-$(ARCH) $(BINDIR)/tf-manager
	install -m 755 tf-rproxy-$(OS)-$(ARCH) $(BINDIR)/tf-rproxy
	@echo "Creating working directory..."
	install -d /var/lib/tinyfaas
	@echo "Installing systemd service files to $(SYSTEMD_DIR)..."
	install -d $(SYSTEMD_DIR)
	install -m 644 systemd/tf-rproxy.service $(SYSTEMD_DIR)/
	install -m 644 systemd/tf-manager.service $(SYSTEMD_DIR)/
	@echo ""
	@echo "Installation complete. To enable and start the services:"
	@echo "  sudo systemctl daemon-reload"
	@echo "  sudo systemctl enable tf-rproxy tf-manager"
	@echo "  sudo systemctl start tf-rproxy tf-manager"

.PHONY: uninstall
uninstall:
	@echo "Stopping services..."
	-systemctl stop tf-manager tf-rproxy 2>/dev/null || true
	-systemctl disable tf-manager tf-rproxy 2>/dev/null || true
	@echo "Removing binaries and service files..."
	rm -f $(BINDIR)/tf-manager $(BINDIR)/tf-rproxy
	rm -f $(SYSTEMD_DIR)/tf-manager.service $(SYSTEMD_DIR)/tf-rproxy.service
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

define arch_build
pkg/docker/runtimes-$(arch): $(foreach runtime,$(RUNTIMES),pkg/docker/runtimes-$(arch)/$(runtime))
endef
$(foreach arch,$(SUPPORTED_ARCH),$(eval $(arch_build)))

define runtime_build
.PHONY: pkg/docker/runtimes-$(arch)/$(runtime)
pkg/docker/runtimes-$(arch)/$(runtime): pkg/docker/runtimes-$(arch)/$(runtime)/Dockerfile pkg/docker/runtimes-$(arch)/$(runtime)/blob.tar.gz

pkg/docker/runtimes-$(arch)/$(runtime)/blob.tar.gz: pkg/docker/runtimes/$(runtime)/build.Dockerfile
	mkdir -p $$(@D)
	cd $$(<D) ; docker build --platform=linux/$(arch) -t tf-build-$(arch)-$(runtime) -f $$(<F) .
	docker run -d -t --platform=linux/$(arch) --name $${PROJECT_NAME}-$(runtime) --rm tf-build-$(arch)-$(runtime)
	docker export $${PROJECT_NAME}-$(runtime) | gzip > $$@
	docker kill $${PROJECT_NAME}-$(runtime)

pkg/docker/runtimes-$(arch)/$(runtime)/Dockerfile: pkg/docker/runtimes/$(runtime)/Dockerfile
	mkdir -p $$(@D)
	cp -r pkg/docker/runtimes/$(runtime)/Dockerfile $$@
endef
$(foreach arch,$(SUPPORTED_ARCH),$(foreach runtime,$(RUNTIMES),$(eval $(runtime_build))))

# Build manager binary (requires runtimes for docker backend)
tf-manager-darwin-%: pkg/docker/runtimes-% $(GO_FILES)
	GOOS=darwin GOARCH=$* go build -o $@ -v $(PKG)/cmd/manager

tf-manager-linux-%: pkg/docker/runtimes-% $(GO_FILES)
	GOOS=linux GOARCH=$* go build -o $@ -v $(PKG)/cmd/manager

# Build rproxy binary (standalone, no runtime dependencies)
tf-rproxy-darwin-%: $(GO_FILES)
	GOOS=darwin GOARCH=$* go build -o $@ -v $(PKG)/cmd/rproxy

tf-rproxy-linux-%: $(GO_FILES)
	GOOS=linux GOARCH=$* go build -o $@ -v $(PKG)/cmd/rproxy
