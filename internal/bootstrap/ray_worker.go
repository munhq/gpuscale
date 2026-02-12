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
		// Fallback: this shouldn't happen in production
		rayHeadAddr = "localhost"
	}

	// Default GCS port is 6379
	gcsPort := 6379

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
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Ray Head: %s:%d'\n\n", rayHeadAddr, gcsPort))

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
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Waiting for Ray head at %s:%d...'\n", rayHeadAddr, gcsPort))
	script.WriteString(fmt.Sprintf("for i in $(seq 1 60); do\n"))
	script.WriteString(fmt.Sprintf("  if python3 -c \"import socket; s=socket.socket(); s.settimeout(2); s.connect(('%s', %d)); s.close()\" 2>/dev/null; then\n", rayHeadAddr, gcsPort))
	script.WriteString("    echo '[gpuscale] Ray head is reachable'\n")
	script.WriteString("    break\n")
	script.WriteString("  fi\n")
	script.WriteString("  echo \"[gpuscale] Waiting for Ray head (attempt $i/60)...\"\n")
	script.WriteString("  sleep 5\n")
	script.WriteString("done\n\n")

	// Join the Ray cluster as a worker node
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Joining Ray cluster as worker with %d GPUs...'\n", numGPUs))
	script.WriteString(fmt.Sprintf("ray start --address='%s:%d' \\\n", rayHeadAddr, gcsPort))
	script.WriteString(fmt.Sprintf("  --num-gpus=%d \\\n", numGPUs))
	script.WriteString(fmt.Sprintf("  --labels='{\"gpuscale.io/provider\": \"%s\", \"gpuscale.io/gpu-type\": \"%s\", \"gpuscale.io/instance-id\": \"%s\"}' \\\n",
		config.ProviderName, config.GPUType, config.InstanceID))
	script.WriteString("  --block\n")

	return script.String()
}
