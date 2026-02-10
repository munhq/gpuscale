#!/bin/bash
set -euo pipefail

echo "[gpuscale] Starting bootstrap..."
echo "[gpuscale] Provider: ${PROVIDER:-unknown}"
echo "[gpuscale] Instance: ${INSTANCE_ID:-unknown}"
echo "[gpuscale] GPU Type: ${GPU_TYPE:-unknown}"

# 1. Start Netbird VPN
echo "[gpuscale] Joining VPN network..."
netbird up --setup-key "$NETBIRD_SETUP_KEY" --daemon
sleep 5

# Get VPN IP with retry
NETBIRD_IP=""
for i in $(seq 1 30); do
  NETBIRD_IP=$(ip -4 addr show wt0 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || true)
  if [ -n "$NETBIRD_IP" ]; then
    break
  fi
  echo "[gpuscale] Waiting for VPN IP (attempt $i/30)..."
  sleep 2
done

if [ -z "$NETBIRD_IP" ]; then
  echo "[gpuscale] ERROR: Failed to get VPN IP after 60s"
  exit 1
fi
echo "[gpuscale] VPN IP: $NETBIRD_IP"

# 2. Start K3s agent
echo "[gpuscale] Joining K3s cluster..."
k3s agent \
  --server "$K3S_URL" \
  --token "$K3S_TOKEN" \
  --node-ip "$NETBIRD_IP" \
  --flannel-iface wt0 \
  --node-label "gpuscale.io/managed=true" \
  --node-label "gpuscale.io/provider=$PROVIDER" \
  --node-label "gpuscale.io/gpu-type=$(echo "$GPU_TYPE" | tr ' ' '-')" \
  --node-label "gpuscale.io/instance-id=$INSTANCE_ID" \
  --node-taint "nvidia.com/gpu:NoSchedule" \
  &
K3S_PID=$!

# 3. Pre-cache model weights (parallel)
if [ -n "${MODEL_CACHE_URL:-}" ]; then
  echo "[gpuscale] Pre-caching model weights from $MODEL_CACHE_URL..."
  mkdir -p /opt/models
  rclone sync "$MODEL_CACHE_URL" /opt/models/ --progress &
  CACHE_PID=$!
fi

# 4. Wait for node to be ready
echo "[gpuscale] Waiting for node to be ready..."
HOSTNAME=$(hostname)
for i in $(seq 1 120); do
  if k3s kubectl get node "$HOSTNAME" 2>/dev/null | grep -q " Ready"; then
    echo "[gpuscale] Node is Ready!"
    break
  fi
  if [ "$i" -eq 120 ]; then
    echo "[gpuscale] ERROR: Node did not become Ready within 240s"
    exit 1
  fi
  sleep 2
done

# Wait for model cache if running
if [ -n "${CACHE_PID:-}" ]; then
  echo "[gpuscale] Waiting for model cache to complete..."
  wait "$CACHE_PID" || echo "[gpuscale] WARNING: Model cache sync failed"
fi

echo "[gpuscale] Bootstrap complete. Node is ready for workloads."

# Keep running (K3s agent)
wait $K3S_PID
