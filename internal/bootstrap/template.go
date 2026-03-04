package bootstrap

import (
	"fmt"
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// GenerateScript generates the gpu-agent bootstrap script for a standalone node.
//
// The script:
//  1. Emits bootstrap-event heartbeats to GPU API for progress tracking.
//  2. Verifies NVIDIA driver (nvidia-smi).
//  3. Downloads the gpu-agent binary from the latest GitHub release.
//  4. Exec's gpu-agent, which starts vLLM and opens the outbound WSS tunnel.
//
// gpu-agent takes over from here — it starts vLLM as a child process, waits for
// it to be healthy, then connects the WSS smux tunnel to GPU API and registers.
// No K3s, no Ray, no Netbird. Two processes: gpu-agent + vLLM.
func GenerateScript(config provider.BootstrapConfig) string {
	claimName := config.InstanceID
	gpuLabel := SanitizeLabel(config.GPUType)
	provLabel := SanitizeLabel(config.ProviderName)

	// Derive HTTP base URL from WSS URL for bootstrap-event calls.
	// wss://host → https://host, ws://host → http://host
	apiHTTP := config.GPUAPIURL
	apiHTTP = strings.Replace(apiHTTP, "wss://", "https://", 1)
	apiHTTP = strings.Replace(apiHTTP, "ws://", "http://", 1)

	// HF_TOKEN export (only if set)
	hfTokenExport := ""
	if config.HFToken != "" {
		hfTokenExport = fmt.Sprintf("export HF_TOKEN='%s'\n", config.HFToken)
	}

	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail
CLAIM_NAME='%s'
GPU_API='%s/internal/bootstrap-event'
AUTH='%s'
emit() { curl -sf -X POST "$GPU_API" -H "Authorization: Bearer $AUTH" -H 'Content-Type: application/json' -d "$1" || true; }

echo '[gpuscale] bootstrap start'
emit '{"claim":"'"$CLAIM_NAME"'","step":"bootstrap_start","message":"starting gpu-agent bootstrap (provider=%s gpu=%s)"}'

# Verify NVIDIA driver
if ! nvidia-smi > /dev/null 2>&1; then
  emit '{"claim":"'"$CLAIM_NAME"'","step":"cuda_failed","message":"nvidia-smi not available"}'
  echo '[gpuscale] ERROR: nvidia-smi failed' >&2
  exit 1
fi
emit '{"claim":"'"$CLAIM_NAME"'","step":"cuda_ready","message":"NVIDIA driver ready"}'

# Download gpu-agent binary from latest GitHub release
mkdir -p /usr/local/bin
AGENT_URL='https://github.com/munhq/kubernetes_gpu/releases/latest/download/gpu-agent-linux-amd64'
echo '[gpuscale] downloading gpu-agent...'
curl -fsSL "$AGENT_URL" -o /usr/local/bin/gpu-agent
chmod +x /usr/local/bin/gpu-agent
emit '{"claim":"'"$CLAIM_NAME"'","step":"agent_ready","message":"gpu-agent downloaded"}'

# Configure gpu-agent via environment variables and exec it.
# gpu-agent starts vLLM, waits for health, then connects the WSS tunnel.
export GPU_API_URL='%s'
export NODE_ID='%s'
export AUTH_TOKEN='%s'
export MODELS='%s'
export MODEL_SOURCES='%s'
export GPU_COUNT='%d'
%sexport HF_HOME=/opt/gpu/huggingface
mkdir -p /opt/gpu/huggingface

echo '[gpuscale] starting gpu-agent'
exec /usr/local/bin/gpu-agent
`,
		claimName,
		apiHTTP,
		config.GPUAPIToken,
		provLabel,
		gpuLabel,
		config.GPUAPIURL,
		claimName,
		config.GPUAPIToken,
		config.Models,
		config.ModelSources,
		config.GPUCount,
		hfTokenExport,
	)
}

// GenerateEnvVars returns the environment variables map for a node instance.
// For standalone nodes these are passed to the provider's instance creation API.
func GenerateEnvVars(config provider.BootstrapConfig) map[string]string {
	env := map[string]string{
		"GPU_TYPE":    config.GPUType,
		"PROVIDER":    config.ProviderName,
		"INSTANCE_ID": config.InstanceID,
		"NODE_TYPE":   "standalone",
	}
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
