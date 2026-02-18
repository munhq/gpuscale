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
func GenerateScript(config provider.BootstrapConfig) string {
	var sb strings.Builder

	nodeName := fmt.Sprintf("gpuscale-%s", config.InstanceID)

	sb.WriteString("#!/bin/bash\n")
	sb.WriteString("set -euo pipefail\n\n")

	// Embed credentials directly — Vast.ai VM (KVM) mode does not reliably inject
	// env vars before the onstart script runs. Values are already in the config.
	sb.WriteString(fmt.Sprintf("NETBIRD_SETUP_KEY='%s'\n", config.NetbirdKey))
	sb.WriteString(fmt.Sprintf("K8S_URL='%s'\n", config.K8sURL))
	sb.WriteString(fmt.Sprintf("K8S_TOKEN='%s'\n\n", config.K8sToken))

	sb.WriteString("echo '[gpuscale] Starting full-node bootstrap...'\n")
	sb.WriteString("echo \"[gpuscale] Provider: ${PROVIDER:-unknown}\"\n")
	sb.WriteString("echo \"[gpuscale] Instance: ${INSTANCE_ID:-unknown}\"\n\n")

	// ── 1. Install Netbird ──────────────────────────────────────────────────
	sb.WriteString("# ── 1. Install Netbird VPN ─────────────────────────────────────────────\n")
	sb.WriteString("echo '[gpuscale] Installing Netbird VPN...'\n")
	sb.WriteString("if ! command -v netbird &>/dev/null; then\n")
	sb.WriteString("  export DEBIAN_FRONTEND=noninteractive\n")
	sb.WriteString("  curl -fsSL https://pkgs.netbird.io/debian/public.key | gpg --dearmor -o /usr/share/keyrings/netbird-archive-keyring.gpg\n")
	sb.WriteString("  echo 'deb [signed-by=/usr/share/keyrings/netbird-archive-keyring.gpg] https://pkgs.netbird.io/debian stable main' > /etc/apt/sources.list.d/netbird.list\n")
	sb.WriteString("  apt-get update -qq\n")
	sb.WriteString("  apt-get install -y netbird\n")
	sb.WriteString("fi\n\n")

	// ── 2. Connect to VPN ──────────────────────────────────────────────────
	sb.WriteString("# ── 2. Join VPN network ────────────────────────────────────────────────\n")
	sb.WriteString("echo '[gpuscale] Joining VPN network...'\n")
	sb.WriteString("netbird up \\\n")
	sb.WriteString("  --setup-key \"$NETBIRD_SETUP_KEY\" \\\n")
	sb.WriteString("  --extra-iface-blacklist cni0,flannel.1,docker0,veth\n\n")

	sb.WriteString("NETBIRD_IP=''\n")
	sb.WriteString("for i in $(seq 1 30); do\n")
	sb.WriteString("  NETBIRD_IP=$(ip -4 addr show wt0 2>/dev/null | grep -oP '(?<=inet\\s)\\d+(\\.\\d+){3}' || true)\n")
	sb.WriteString("  if [ -n \"$NETBIRD_IP\" ]; then\n")
	sb.WriteString("    echo \"[gpuscale] VPN IP: $NETBIRD_IP\"\n")
	sb.WriteString("    break\n")
	sb.WriteString("  fi\n")
	sb.WriteString("  echo \"[gpuscale] Waiting for VPN IP (attempt $i/30)...\"\n")
	sb.WriteString("  sleep 2\n")
	sb.WriteString("done\n")
	sb.WriteString("if [ -z \"$NETBIRD_IP\" ]; then\n")
	sb.WriteString("  echo '[gpuscale] ERROR: Failed to get VPN IP after 60s'\n")
	sb.WriteString("  exit 1\n")
	sb.WriteString("fi\n\n")

	// ── 3. Install NVIDIA Container Toolkit ────────────────────────────────
	sb.WriteString("# ── 3. Install NVIDIA Container Toolkit ────────────────────────────────\n")
	sb.WriteString("echo '[gpuscale] Installing NVIDIA Container Toolkit...'\n")
	sb.WriteString("if ! command -v nvidia-container-runtime &>/dev/null; then\n")
	sb.WriteString("  export DEBIAN_FRONTEND=noninteractive\n")
	sb.WriteString("  curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg\n")
	sb.WriteString("  ARCH=$(dpkg --print-architecture)\n")
	sb.WriteString("  echo \"deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://nvidia.github.io/libnvidia-container/stable/deb/${ARCH} /\" > /etc/apt/sources.list.d/nvidia-container-toolkit.list\n")
	sb.WriteString("  apt-get update -qq\n")
	sb.WriteString("  apt-get install -y nvidia-container-toolkit\n")
	sb.WriteString("fi\n")
	sb.WriteString("nvidia-smi -pm 1 || true\n\n")

	// ── 4. Configure K3s containerd for NVIDIA runtime ─────────────────────
	sb.WriteString("# ── 4. Configure K3s containerd for NVIDIA runtime ─────────────────────\n")
	sb.WriteString("echo '[gpuscale] Configuring K3s containerd for NVIDIA runtime...'\n")
	sb.WriteString("mkdir -p /var/lib/rancher/k3s/agent/etc/containerd\n")
	sb.WriteString("cat > /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl << 'CONTAINERD_EOF'\n")
	sb.WriteString("version = 3\n\n")
	sb.WriteString("[plugins.'io.containerd.cri.v1.runtime'.cni]\n")
	sb.WriteString("  bin_dirs = [\"/var/lib/rancher/k3s/data/cni\"]\n")
	sb.WriteString("  conf_dir = \"/var/lib/rancher/k3s/agent/etc/cni/net.d\"\n\n")
	sb.WriteString("[plugins.'io.containerd.cri.v1.runtime'.containerd]\n")
	sb.WriteString("  default_runtime_name = \"nvidia\"\n\n")
	sb.WriteString("[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.nvidia]\n")
	sb.WriteString("  privileged_without_host_devices = false\n")
	sb.WriteString("  runtime_engine = \"\"\n")
	sb.WriteString("  runtime_root = \"\"\n")
	sb.WriteString("  runtime_type = \"io.containerd.runc.v2\"\n\n")
	sb.WriteString("[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.nvidia.options]\n")
	sb.WriteString("  BinaryName = \"/usr/bin/nvidia-container-runtime\"\n")
	sb.WriteString("  SystemdCgroup = true\n")
	sb.WriteString("CONTAINERD_EOF\n\n")

	// ── 5. Install K3s agent via install script ─────────────────────────────
	sb.WriteString("# ── 5. Install K3s agent ───────────────────────────────────────────────\n")
	sb.WriteString("echo '[gpuscale] Downloading K3s installer...'\n")
	sb.WriteString("curl -sfL https://get.k3s.io -o /tmp/k3s-install.sh\n")
	sb.WriteString("chmod +x /tmp/k3s-install.sh\n\n")

	sb.WriteString(fmt.Sprintf("NODE_NAME='%s'\n", nodeName))
	sb.WriteString("GPU_LABEL=$(echo \"${GPU_TYPE:-unknown}\" | tr ' ' '-' | tr '[:upper:]' '[:lower:]')\n\n")

	sb.WriteString("echo \"[gpuscale] Joining K3s cluster as node ${NODE_NAME} with IP ${NETBIRD_IP}...\"\n")
	sb.WriteString("INSTALL_K3S_EXEC=\"agent \\\n")
	sb.WriteString("  --node-name=${NODE_NAME} \\\n")
	sb.WriteString("  --node-ip=${NETBIRD_IP} \\\n")
	sb.WriteString("  --node-external-ip=${NETBIRD_IP} \\\n")
	sb.WriteString("  --flannel-iface=wt0 \\\n")
	sb.WriteString("  --node-label=gpuscale.io/managed=true \\\n")
	sb.WriteString(fmt.Sprintf("  --node-label=gpuscale.io/provider=%s \\\n", SanitizeLabel(config.ProviderName)))
	sb.WriteString("  --node-label=gpuscale.io/gpu-type=${GPU_LABEL} \\\n")
	sb.WriteString(fmt.Sprintf("  --node-label=gpuscale.io/instance-id=%s \\\n", config.InstanceID))
	sb.WriteString("  --node-taint=nvidia.com/gpu:NoSchedule\"\n\n")

	sb.WriteString("INSTALL_K3S_VERSION=\"${K3S_VERSION:-}\" \\\n")
	sb.WriteString("K3S_URL=\"${K8S_URL}\" \\\n")
	sb.WriteString("K3S_TOKEN=\"${K8S_TOKEN}\" \\\n")
	sb.WriteString("INSTALL_K3S_EXEC=\"${INSTALL_K3S_EXEC}\" \\\n")
	sb.WriteString("/tmp/k3s-install.sh\n\n")

	sb.WriteString("echo \"[gpuscale] Bootstrap complete.\"\n")
	sb.WriteString("echo \"[gpuscale] Node: ${NODE_NAME}, VPN IP: ${NETBIRD_IP}\"\n")
	sb.WriteString("echo \"[gpuscale] K3s agent is running as a systemd service.\"\n")

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
