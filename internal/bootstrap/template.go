package bootstrap

import (
	"fmt"
	"strings"

	"github.com/munhq/gpuscale/internal/provider"
)

// GenerateScript generates a bootstrap script for a full-node provider instance.
// This script joins the node to the Kubernetes cluster via VPN.
func GenerateScript(config provider.BootstrapConfig) string {
	var sb strings.Builder

	sb.WriteString(`#!/bin/bash
set -euo pipefail

echo "[gpuscale] Starting bootstrap..."
echo "[gpuscale] Provider: ${PROVIDER:-unknown}"
echo "[gpuscale] Instance: ${INSTANCE_ID:-unknown}"

# 1. Start Netbird VPN
echo "[gpuscale] Joining VPN network..."
netbird up --setup-key "$NETBIRD_SETUP_KEY" --daemon
sleep 5

# Get VPN IP
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

# 2. Start Kubernetes agent
echo "[gpuscale] Joining Kubernetes cluster..."
`)

	// Build Kubernetes agent command with labels
	// Uses K3s agent binary — this is the actual command to join the cluster.
	sb.WriteString(fmt.Sprintf(`k3s agent \
  --server "$K8S_URL" \
  --token "$K8S_TOKEN" \
  --node-ip "$NETBIRD_IP" \
  --flannel-iface wt0 \
  --node-label "gpuscale.io/managed=true" \
  --node-label "gpuscale.io/provider=%s" \
  --node-label "gpuscale.io/gpu-type=%s" \
  --node-label "gpuscale.io/instance-id=%s" \
  --node-taint "nvidia.com/gpu:NoSchedule" \
  &
`, config.ProviderName, SanitizeLabel(config.GPUType), config.InstanceID))

	sb.WriteString(`K8S_PID=$!

`)

	// Optional model pre-caching
	sb.WriteString(`# 3. Pre-cache model weights (parallel)
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
  if kubectl get node "$HOSTNAME" 2>/dev/null | grep -q " Ready"; then
    echo "[gpuscale] Node is Ready!"
    break
  fi
  if [ $i -eq 120 ]; then
    echo "[gpuscale] ERROR: Node did not become Ready within 240s"
    exit 1
  fi
  sleep 2
done

# Wait for model cache if running
if [ -n "${CACHE_PID:-}" ]; then
  echo "[gpuscale] Waiting for model cache to complete..."
  wait $CACHE_PID || echo "[gpuscale] WARNING: Model cache failed"
fi

echo "[gpuscale] Bootstrap complete. Node is ready for workloads."

# Keep running (Kubernetes agent)
wait $K8S_PID
`)

	return sb.String()
}

// GenerateEnvVars returns the environment variables map for a node instance.
// For full-node: includes VPN and Kubernetes join credentials.
// For ray-worker: includes model and GPU info.
func GenerateEnvVars(config provider.BootstrapConfig) map[string]string {
	env := map[string]string{
		"GPU_TYPE":    config.GPUType,
		"PROVIDER":    config.ProviderName,
		"INSTANCE_ID": config.InstanceID,
	}

	// Full-node specific env vars
	if config.NodeType == "full-node" {
		env["NETBIRD_SETUP_KEY"] = config.NetbirdKey
		env["K8S_URL"] = config.K8sURL
		env["K8S_TOKEN"] = config.K8sToken
	}

	// Ray-worker specific env vars
	if config.ModelID != "" {
		env["MODEL_ID"] = config.ModelID
	}
	if config.ModelCacheURL != "" {
		env["MODEL_CACHE_URL"] = config.ModelCacheURL
	}
	for k, v := range config.ExtraEnv {
		env[k] = v
	}
	return env
}

// SanitizeLabel sanitizes a string for use as a Kubernetes label value.
// Label values must be <= 63 chars, start/end with alphanumeric, and contain only [-_.a-zA-Z0-9].
func SanitizeLabel(s string) string {
	var result strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			result.WriteRune(c)
		} else if c == ' ' {
			result.WriteRune('-')
		}
	}
	label := result.String()
	if len(label) > 63 {
		label = label[:63]
	}
	return label
}
