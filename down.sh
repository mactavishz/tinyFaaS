#!/bin/bash

set -e

echo "==> Stopping existing tinyFaaS services..."
sudo systemctl disable --now tf-server 2>/dev/null || echo "tf-server not running"
sudo systemctl disable --now tf-gateway 2>/dev/null || echo "tf-gateway not running"

echo "==> Flushing old logs..."
sudo journalctl --rotate --vacuum-time=1s -u tf-server || true
sudo journalctl --rotate --vacuum-time=1s -u tf-gateway || true

sudo rm -f /etc/default/tinyfaas

echo "==> Uninstalling tinyFaaS services..."
make uninstall

echo "==> Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "==> Cleaning builds..."
make clean