package bootstrap

import (
	"fmt"
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// GenerateScript generates the full-node bootstrap script inline.
// Installs Netbird VPN, NVIDIA Container Toolkit, and K3s agent.
// If config.ModelSources is set, also starts a background huggingface-cli download
// after K3s so the model cache is warm before Ray Serve deploys.
func GenerateScript(config provider.BootstrapConfig) string {
	nodeName := fmt.Sprintf("gpuscale-%s", config.InstanceID)
	gpuLabel := SanitizeLabel(config.GPUType)
	provLabel := SanitizeLabel(config.ProviderName)

	downloadSection := buildModelDownloadSection(config.ModelSources, config.HFToken)

	gpuAPIEvent := "curl -sf -X POST http://gpu-api.gpu-workloads.svc.cluster.local:8000/internal/bootstrap-event -H 'Content-Type: application/json' -d"
	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
CLAIM_NAME='%s'
echo '[gpuscale] bootstrap start'
if ! command -v netbird &>/dev/null; then
curl -fsSL https://pkgs.netbird.io/debian/public.key|gpg --dearmor -o /usr/share/keyrings/netbird.gpg
echo 'deb [signed-by=/usr/share/keyrings/netbird.gpg] https://pkgs.netbird.io/debian stable main'>/etc/apt/sources.list.d/netbird.list
apt-get update -qq && apt-get install -y netbird
fi
netbird up --setup-key '%s' --extra-iface-blacklist cni0,flannel.1,docker0,veth
NB_IP=''
for i in $(seq 1 30); do
NB_IP=$(ip -4 addr show wt0 2>/dev/null|grep -oP '(?<=inet\s)\d+(\.\d+){3}'||true)
[ -n "$NB_IP" ] && break; sleep 2
done
[ -z "$NB_IP" ] && echo 'ERROR: no VPN IP' && exit 1
echo "[gpuscale] VPN IP: $NB_IP"
%s '{"claim":"'"$CLAIM_NAME"'","step":"vpn_ready","message":"VPN IP: '"$NB_IP"'"}' ||true
if ! command -v nvidia-container-runtime &>/dev/null; then
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey|gpg --dearmor -o /usr/share/keyrings/nvcr.gpg
A=$(dpkg --print-architecture)
echo "deb [signed-by=/usr/share/keyrings/nvcr.gpg] https://nvidia.github.io/libnvidia-container/stable/deb/$A /">/etc/apt/sources.list.d/nvcr.list
apt-get update -qq && apt-get install -y nvidia-container-toolkit
fi
nvidia-smi -pm 1||true
%s '{"claim":"'"$CLAIM_NAME"'","step":"nvidia_ready","message":"nvidia-container-runtime installed"}' ||true
mkdir -p /opt/gpu && chmod 777 /opt/gpu
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
cat>/var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl<<'C'
version = 3
[plugins.'io.containerd.cri.v1.runtime'.cni]
bin_dirs = ["/var/lib/rancher/k3s/data/cni"]
conf_dir = "/var/lib/rancher/k3s/agent/etc/cni/net.d"
[plugins.'io.containerd.cri.v1.runtime'.containerd]
default_runtime_name = "nvidia"
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.nvidia]
privileged_without_host_devices = false
runtime_type = "io.containerd.runc.v2"
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.nvidia.options]
BinaryName = "/usr/bin/nvidia-container-runtime"
SystemdCgroup = true
C
curl -sfL https://get.k3s.io -o /tmp/k3s.sh && chmod +x /tmp/k3s.sh
GL=$(echo '%s'|tr ' ' '-'|tr '[:upper:]' '[:lower:]')
INSTALL_K3S_EXEC="agent --node-name=%s --node-ip=$NB_IP --node-external-ip=$NB_IP --flannel-iface=wt0 --node-label=gpuscale.io/managed=true --node-label=gpuscale.io/provider=%s --node-label=gpuscale.io/gpu-type=$GL --node-label=gpuscale.io/instance-id=%s --node-label=nvidia.com/gpu.present=true --node-taint=nvidia.com/gpu:NoSchedule --kubelet-arg=eviction-hard=imagefs.available<0%%,nodefs.available<0%%"
INSTALL_K3S_VERSION='v1.35.0+k3s3' K3S_URL='%s' K3S_TOKEN='%s' INSTALL_K3S_EXEC="$INSTALL_K3S_EXEC" /tmp/k3s.sh
%s '{"claim":"'"$CLAIM_NAME"'","step":"k3s_joined","message":"K3s agent started, joining cluster"}' ||true
%secho '[gpuscale] done'
`, config.InstanceID, config.NetbirdKey,
		gpuAPIEvent, gpuAPIEvent,
		gpuLabel, nodeName, provLabel, config.InstanceID,
		config.K8sURL, config.K8sToken,
		gpuAPIEvent,
		downloadSection)
}

// buildModelDownloadSection returns a bash snippet that pre-downloads the given
// HuggingFace model repos in the background. The snippet is empty if sources is empty.
// Downloads run concurrently with K3s agent registration so cold-start latency is reduced.
func buildModelDownloadSection(sources []string, hfToken string) string {
	if len(sources) == 0 {
		return ""
	}
	gpuAPIEvent := "curl -sf -X POST http://gpu-api.gpu-workloads.svc.cluster.local:8000/internal/bootstrap-event -H 'Content-Type: application/json' -d"
	var b strings.Builder
	b.WriteString("# Pre-download model weights into /opt/gpu (background, runs while K3s agent registers)\n")
	fmt.Fprintf(&b, "%s '{\"claim\":\"'\"$CLAIM_NAME\"'\",\"step\":\"model_downloading\",\"message\":\"starting HuggingFace download\"}' ||true\n", gpuAPIEvent)
	b.WriteString("(\n")
	b.WriteString("  export HF_HOME=/opt/gpu/huggingface\n")
	if hfToken != "" {
		fmt.Fprintf(&b, "  export HF_TOKEN='%s'\n", hfToken)
	}
	b.WriteString("  command -v huggingface-cli || pip3 install --quiet 'huggingface_hub[cli]'\n")
	for _, src := range sources {
		// Strip hf: or hf:// prefix — huggingface-cli takes bare repo IDs.
		repo := strings.TrimPrefix(src, "hf://")
		repo = strings.TrimPrefix(repo, "hf:")
		fmt.Fprintf(&b, "  huggingface-cli download '%s' && echo '[gpuscale] downloaded %s'\n", repo, repo)
	}
	fmt.Fprintf(&b, "  %s '{\"claim\":\"'\"$CLAIM_NAME\"'\",\"step\":\"model_ready\",\"message\":\"model download complete\"}' ||true\n", gpuAPIEvent)
	b.WriteString(") >> /tmp/hf-download.log 2>&1 &\n")
	return b.String()
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
