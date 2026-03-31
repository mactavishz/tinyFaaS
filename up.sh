#!/bin/bash

set -e

echo "==> Shutting down the old services..."
make down

echo "==> Building and installing tinyFaaS..."
make install

echo "==> Configuring tinyFaaS environment..."

# Set default values
TF_AUTOSCALER_ENABLED=${TF_AUTOSCALER_ENABLED:-true}
TF_CALLGRAPH_ENABLED=${TF_CALLGRAPH_ENABLED:-true}
TF_CALLGRAPH_METHOD=${TF_CALLGRAPH_METHOD:-SMA}
TF_DEFAULT_SCALE_TO_ZERO_IDLE_DURATION=${TF_DEFAULT_SCALE_TO_ZERO_IDLE_DURATION:-5m}
TF_GATEWAY_IP=${TF_GATEWAY_IP:-0.0.0.0}
TF_GATEWAY_PORT=${TF_GATEWAY_PORT:-80}
TF_RPROXY_PORT=${TF_RPROXY_PORT:-8000}
TF_MANAGER_PORT=${TF_MANAGER_PORT:-8080}
TF_ENV=${TF_ENV:-development}

# Load local config if exists (git-ignored)
if [ -f ".tinyfaas.env" ]; then
    echo "==> Loading local configuration from .tinyfaas.env"
    source ".tinyfaas.env"
fi

echo "==> Autoscaler settings: ENABLED=$TF_AUTOSCALER_ENABLED, IDLE_DURATION=$TF_DEFAULT_SCALE_TO_ZERO_IDLE_DURATION"
echo "==> Callgraph settings: ENABLED=$TF_CALLGRAPH_ENABLED, METHOD=$TF_CALLGRAPH_METHOD"
echo "==> Gateway IP: $TF_GATEWAY_IP"
echo "==> Gateway port: $TF_GATEWAY_PORT"
echo "==> Manager port: $TF_MANAGER_PORT"
echo "==> RProxy port: $TF_RPROXY_PORT"
echo "==> Environment: $TF_ENV"

# Create environment files for autoscaler configuration
sudo tee /etc/default/tinyfaas > /dev/null <<EOF
# Autoscaler configuration
TF_AUTOSCALER_ENABLED=$TF_AUTOSCALER_ENABLED
TF_DEFAULT_SCALE_TO_ZERO_IDLE_DURATION=$TF_DEFAULT_SCALE_TO_ZERO_IDLE_DURATION

# Callgraph configuration
TF_CALLGRAPH_ENABLED=$TF_CALLGRAPH_ENABLED
TF_CALLGRAPH_METHOD=$TF_CALLGRAPH_METHOD

# Gateway configuration
TF_GATEWAY_IP=$TF_GATEWAY_IP
TF_GATEWAY_PORT=$TF_GATEWAY_PORT

# RProxy configuration
TF_RPROXY_PORT=$TF_RPROXY_PORT

# Manager configuration
TF_MANAGER_PORT=$TF_MANAGER_PORT

# Environment configuration
TF_ENV=$TF_ENV
EOF

echo "==> Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "==> Starting tinyFaaS services..."
sudo systemctl enable --now tf-gateway
sudo systemctl enable --now tf-rproxy
sudo systemctl enable --now tf-manager

# Wait a moment and check if services are running
sleep 3
if systemctl is-active --quiet tf-gateway && systemctl is-active --quiet tf-rproxy && systemctl is-active --quiet tf-manager; then
    echo "==> tinyFaaS is running successfully!"
    echo "==> Service status:"
    sudo systemctl status tf-gateway --no-pager -l | head -10
    sudo systemctl status tf-rproxy --no-pager -l | head -10
    sudo systemctl status tf-manager --no-pager -l | head -10
    echo "==> View logs with:"
    echo "    journalctl -u tf-gateway -f"
    echo "    journalctl -u tf-manager -f"
    echo "    journalctl -u tf-rproxy -f"
else
    echo "==> ERROR: tinyFaaS failed to start."
    if ! systemctl is-active --quiet tf-gateway; then
        echo "==> tf-gateway failed:"
        sudo systemctl status tf-gateway --no-pager -l
    fi
    if ! systemctl is-active --quiet tf-rproxy; then
        echo "==> tf-rproxy failed:"
        sudo systemctl status tf-rproxy --no-pager -l
    fi
    if ! systemctl is-active --quiet tf-manager; then
        echo "==> tf-manager failed:"
        sudo systemctl status tf-manager --no-pager -l
    fi
    exit 1
fi
