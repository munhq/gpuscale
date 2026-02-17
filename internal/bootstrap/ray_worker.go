package bootstrap

import (
	"fmt"
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// GenerateRayWorkerScript creates a bootstrap script for a ray-worker that joins
// an existing Ray cluster. The worker provides GPU capacity to the cluster, and
// Ray Serve handles model placement and routing.
//
// This replaces the old standalone vLLM approach — workers no longer run their own
// model. Instead, Ray Serve on the head node decides which models to load where.
func GenerateRayWorkerScript(config provider.BootstrapConfig) string {
	var script strings.Builder

	rayHeadAddr := config.RayHeadAddr
	if rayHeadAddr == "" {
		rayHeadAddr = "localhost"
	}

	// RayHeadAddr may already include a port (e.g., "1.2.3.4:31637" for NodePort).
	// Parse host and port separately so we don't double-append.
	rayHost := rayHeadAddr
	rayPort := "6379"
	if idx := strings.LastIndex(rayHeadAddr, ":"); idx > 0 {
		rayHost = rayHeadAddr[:idx]
		rayPort = rayHeadAddr[idx+1:]
	}
	rayAddr := rayHost + ":" + rayPort

	numGPUs := 1 // Default to 1 GPU
	if config.ExtraEnv != nil {
		if v, ok := config.ExtraEnv["NUM_GPUS"]; ok {
			numGPUs = 0
			for _, c := range v {
				numGPUs = numGPUs*10 + int(c-'0')
			}
		}
	}

	script.WriteString("#!/bin/bash\n")
	script.WriteString("set -euo pipefail\n\n")

	script.WriteString(fmt.Sprintf("echo '[gpuscale] Starting Ray worker on %s'\n", config.ProviderName))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Instance ID: %s'\n", config.InstanceID))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] GPU Type: %s'\n", config.GPUType))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Ray Head: %s'\n\n", rayAddr))

	// Pre-cache models if configured (Ray Serve may need them on this worker)
	if config.ModelCacheURL != "" {
		script.WriteString(fmt.Sprintf("echo '[gpuscale] Pre-caching model from %s...'\n", config.ModelCacheURL))
		script.WriteString("mkdir -p /opt/models\n")
		script.WriteString("if command -v rclone &> /dev/null; then\n")
		script.WriteString(fmt.Sprintf("  rclone sync '%s' /opt/models/ --progress\n", config.ModelCacheURL))
		script.WriteString("else\n")
		script.WriteString("  echo '[gpuscale] rclone not found, skipping pre-cache'\n")
		script.WriteString("fi\n\n")
	}

	// Set HuggingFace cache directory
	script.WriteString("export HF_HOME=/opt/models/huggingface\n")
	script.WriteString("export TRANSFORMERS_CACHE=/opt/models/huggingface\n")
	script.WriteString("export VLLM_WORKER_MULTIPROC_METHOD=spawn\n")
	script.WriteString("mkdir -p /opt/models/huggingface\n\n")

	// Wait for Ray head to be reachable
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Waiting for Ray head at %s...'\n", rayAddr))
	script.WriteString(fmt.Sprintf("for i in $(seq 1 60); do\n"))
	script.WriteString(fmt.Sprintf("  if python3 -c \"import socket; s=socket.socket(); s.settimeout(2); s.connect(('%s', %s)); s.close()\" 2>/dev/null; then\n", rayHost, rayPort))
	script.WriteString("    echo '[gpuscale] Ray head is reachable'\n")
	script.WriteString("    break\n")
	script.WriteString("  fi\n")
	script.WriteString("  echo \"[gpuscale] Waiting for Ray head (attempt $i/60)...\"\n")
	script.WriteString("  sleep 5\n")
	script.WriteString("done\n\n")

	// Resolve the public IP of this machine so the Ray head's GCS health checks
	// can reach the raylet. Without --node-ip-address, ray start auto-detects
	// the Docker bridge IP (172.17.0.2) which is unreachable from Hetzner,
	// causing every worker to die after ~2 minutes due to missed heartbeats.
	script.WriteString("echo '[gpuscale] Resolving public IP...'\n")
	script.WriteString("PUBLIC_IP=$(curl -s --max-time 5 ifconfig.me 2>/dev/null \\\n")
	script.WriteString("  || curl -s --max-time 5 icanhazip.com 2>/dev/null \\\n")
	script.WriteString("  || curl -s --max-time 5 api.ipify.org 2>/dev/null \\\n")
	script.WriteString("  || ip route get 1.1.1.1 2>/dev/null | awk '/src/{print $7}')\n")
	script.WriteString("echo \"[gpuscale] Public IP: $PUBLIC_IP\"\n\n")

	// Join the Ray cluster as a worker node.
	// CONTAINER_ID is injected by Vast.ai at runtime. We use it as instance-id
	// label so the health check can find this specific worker in the Ray dashboard.
	// INSTANCE_ID may also be set by other providers.
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Joining Ray cluster as worker with %d GPUs...'\n", numGPUs))
	// Resolve instance ID: INSTANCE_ID (set by us for other providers),
	// VAST_CONTAINERLABEL (Vast.ai's unique instance name, same as API ID),
	// CONTAINER_ID (Vast.ai Docker container ID — may differ from API ID).
	script.WriteString("GPUSCALE_INSTANCE_ID=\"${INSTANCE_ID:-${VAST_CONTAINERLABEL:-${CONTAINER_ID:-unknown}}}\"\n")
	script.WriteString("echo \"[gpuscale] Instance ID: $GPUSCALE_INSTANCE_ID\"\n")
	script.WriteString(fmt.Sprintf("ray start --address='%s' \\\n", rayAddr))
	script.WriteString(fmt.Sprintf("  --num-gpus=%d \\\n", numGPUs))
	// --node-ip-address: advertise the public IP so GCS health checks work.
	// --node-manager-port: fixed port that Vast.ai exposes (open_ports=10001/tcp).
	script.WriteString("  --node-ip-address=$PUBLIC_IP \\\n")
	script.WriteString("  --node-manager-port=20001 \\\n")
	script.WriteString(fmt.Sprintf("  --labels='{\"gpuscale.io/provider\": \"%s\", \"gpuscale.io/gpu-type\": \"%s\", \"gpuscale.io/instance-id\": \"'\"$GPUSCALE_INSTANCE_ID\"'\"}' \\\n",
		config.ProviderName, config.GPUType))
	script.WriteString("  --block\n")

	return script.String()
}
