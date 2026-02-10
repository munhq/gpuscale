package bootstrap

import (
	"fmt"
	"strings"

	"github.com/munhq/gpuscale/internal/provider"
)

// GenerateRayWorkerScript creates a bootstrap script for Ray Serve workers
func GenerateRayWorkerScript(config provider.BootstrapConfig) string {
	var script strings.Builder

	script.WriteString("#!/bin/bash\n")
	script.WriteString("set -euo pipefail\n\n")

	script.WriteString(fmt.Sprintf("echo '[gpuscale] Starting Ray worker on %s'\n", config.ProviderName))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Instance ID: %s'\n", config.InstanceID))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] GPU Type: %s'\n\n", config.GPUType))

	// Set defaults
	dashPort := config.RayDashPort
	if dashPort == 0 {
		dashPort = 8265
	}
	servePort := config.RayServePort
	if servePort == 0 {
		servePort = 8000
	}

	// Install dependencies if not present
	script.WriteString("# Install dependencies if needed\n")
	script.WriteString("if ! command -v ray &> /dev/null; then\n")
	script.WriteString("  echo '[gpuscale] Installing Ray...'\n")
	script.WriteString("  pip install -q 'ray[serve]' 2>&1 | tail -5\n")
	script.WriteString("fi\n\n")

	// Start Ray
	if config.RayHeadAddr == "" {
		// Run as head node
		script.WriteString("echo '[gpuscale] Starting Ray head node...'\n")
		script.WriteString(fmt.Sprintf("ray start --head \\\n"))
		script.WriteString(fmt.Sprintf("  --port=6379 \\\n"))
		script.WriteString(fmt.Sprintf("  --dashboard-host=0.0.0.0 \\\n"))
		script.WriteString(fmt.Sprintf("  --dashboard-port=%d \\\n", dashPort))
		script.WriteString(fmt.Sprintf("  --num-gpus=1\n\n"))
	} else {
		// Connect to existing head
		script.WriteString(fmt.Sprintf("echo '[gpuscale] Joining Ray cluster at %s...'\n", config.RayHeadAddr))
		script.WriteString(fmt.Sprintf("ray start --address=%s --num-gpus=1\n\n", config.RayHeadAddr))
	}

	// Wait for Ray to be ready
	script.WriteString("echo '[gpuscale] Waiting for Ray to be ready...'\n")
	script.WriteString("for i in $(seq 1 30); do\n")
	script.WriteString("  if ray status 2>/dev/null | grep -q 'Available resources'; then\n")
	script.WriteString("    echo '[gpuscale] Ray is ready!'\n")
	script.WriteString("    break\n")
	script.WriteString("  fi\n")
	script.WriteString("  sleep 2\n")
	script.WriteString("done\n\n")

	// Pre-cache models if configured
	if config.ModelCacheURL != "" {
		script.WriteString(fmt.Sprintf("echo '[gpuscale] Pre-caching models from %s...'\n", config.ModelCacheURL))
		script.WriteString("mkdir -p /opt/models\n")
		script.WriteString(fmt.Sprintf("rclone sync '%s' /opt/models/ --progress &\n", config.ModelCacheURL))
		script.WriteString("CACHE_PID=$!\n\n")
	}

	// Keep Ray running
	script.WriteString("echo '[gpuscale] Ray worker is ready for inference'\n")
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Dashboard: http://$(hostname -I | awk '{print $1}'):%d'\n", dashPort))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Serve endpoint: http://$(hostname -I | awk '{print $1}'):%d'\n\n", servePort))

	// Wait for model cache if running
	if config.ModelCacheURL != "" {
		script.WriteString("if [ -n \"${CACHE_PID:-}\" ]; then\n")
		script.WriteString("  echo '[gpuscale] Waiting for model cache...'\n")
		script.WriteString("  wait \"$CACHE_PID\" || echo '[gpuscale] Model cache sync failed'\n")
		script.WriteString("fi\n\n")
	}

	// Keep container running
	script.WriteString("# Keep alive\n")
	script.WriteString("tail -f /dev/null\n")

	return script.String()
}
