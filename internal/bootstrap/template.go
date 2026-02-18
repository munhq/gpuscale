package bootstrap

import (
	"fmt"
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// GenerateScript generates a bootstrap script for a full-node provider instance.
// Runs on a fresh Vast.ai Ubuntu 22.04 VM and installs everything from scratch:
//  1. Netbird VPN (apt) → connects outbound to control plane, gets VPN IP on wt0
//  2. NVIDIA Container Toolkit → so K3s containerd can expose GPU to pods
//  3. K3s containerd config → NVIDIA runtime as default
//  4. K3s agent (install script) → joins cluster via VPN IP, runs as systemd service
//
// No chisel tunnels. No inbound ports needed. VPN and K3s agent are outbound-only.
// Credentials come from env vars set by Vast.ai: NETBIRD_SETUP_KEY, K8S_URL, K8S_TOKEN.
// bootstrapScriptURL is the raw GitHub URL of the full bootstrap script.
// The onstart wrapper sets credentials and curls this — keeping onstart tiny
// so it stays under Vast.ai's ~4KB onstart field limit.
const bootstrapScriptURL = "https://raw.githubusercontent.com/munhq/k3s-gpu/main/scripts/node-bootstrap.sh"

func GenerateScript(config provider.BootstrapConfig) string {
	nodeName := fmt.Sprintf("gpuscale-%s", config.InstanceID)

	// Tiny wrapper: sets credentials as env vars, then curls the full script.
	// Credentials are embedded here so the main script can read them regardless
	// of whether Vast.ai KVM mode injects env vars before onstart runs.
	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail
export NETBIRD_SETUP_KEY='%s'
export K8S_URL='%s'
export K8S_TOKEN='%s'
export NODE_NAME='%s'
export GPU_TYPE='%s'
export PROVIDER='%s'
export INSTANCE_ID='%s'
curl -fsSL %s | bash
`, config.NetbirdKey, config.K8sURL, config.K8sToken,
		nodeName, SanitizeLabel(config.GPUType), SanitizeLabel(config.ProviderName),
		config.InstanceID, bootstrapScriptURL)
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
