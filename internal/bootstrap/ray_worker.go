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
// Networking: Vast.ai containers are behind NAT with no inbound ports. We use a
// chisel reverse tunnel to expose a block of 10 contiguous ports on the K8s control
// plane that map back to the same ports on the worker. Ray needs multiple inbound
// ports: node-manager (GCS health checks), object-manager (object transfers), and
// worker ports (direct gRPC for Serve proxy → replica actor calls).
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

	// Tunnel server address defaults to the Ray head host on port 8443.
	tunnelServer := config.ExtraEnv["TUNNEL_SERVER"]
	if tunnelServer == "" {
		tunnelServer = rayHost + ":8443"
	}

	script.WriteString("#!/bin/bash\n")
	script.WriteString("set -euo pipefail\n\n")

	// GPU API event emission — same NodePort as full-node bootstrap (30800).
	// rayHost is the Hetzner control-plane IP already parsed from RayHeadAddr.
	script.WriteString(fmt.Sprintf("CLAIM_NAME='%s'\n", config.InstanceID))
	script.WriteString(fmt.Sprintf("GPU_API='http://%s:30800/internal/bootstrap-event'\n", rayHost))
	script.WriteString("emit() { curl -sf -X POST \"$GPU_API\" -H 'Content-Type: application/json' -d \"$1\" || true; }\n\n")

	script.WriteString(fmt.Sprintf("echo '[gpuscale] Starting Ray worker on %s'\n", config.ProviderName))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Instance ID: %s'\n", config.InstanceID))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] GPU Type: %s'\n", config.GPUType))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Ray Head: %s'\n", rayAddr))
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Tunnel Server: %s'\n\n", tunnelServer))

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
	script.WriteString("for i in $(seq 1 60); do\n")
	script.WriteString(fmt.Sprintf("  if python3 -c \"import socket; s=socket.socket(); s.settimeout(2); s.connect(('%s', %s)); s.close()\" 2>/dev/null; then\n", rayHost, rayPort))
	script.WriteString("    echo '[gpuscale] Ray head is reachable'\n")
	script.WriteString("    break\n")
	script.WriteString("  fi\n")
	script.WriteString("  echo \"[gpuscale] Waiting for Ray head (attempt $i/60)...\"\n")
	script.WriteString("  sleep 5\n")
	script.WriteString("done\n\n")

	// --- Chisel reverse tunnel ---
	// Ray needs multiple inbound ports on the worker: node-manager (health checks),
	// object-manager (object transfers), and worker ports (Serve proxy → replica gRPC).
	// We tunnel a contiguous block of 10 ports through a single chisel WebSocket.
	// Port layout within the block:
	//   base+0 = node-manager-port (raylet gRPC, GCS health checks)
	//   base+1 = object-manager-port (object transfers between nodes)
	//   base+2..base+9 = worker ports (actor/task gRPC servers)
	script.WriteString("# --- Reverse tunnel for Ray worker ports ---\n")
	script.WriteString("echo '[gpuscale] Setting up reverse tunnel...'\n")
	script.WriteString("if ! command -v chisel &> /dev/null; then\n")
	script.WriteString("  echo '[gpuscale] Downloading chisel...'\n")
	script.WriteString("  curl -sL 'https://github.com/jpillora/chisel/releases/download/v1.10.1/chisel_1.10.1_linux_amd64.gz' | gunzip > /usr/local/bin/chisel\n")
	script.WriteString("  chmod +x /usr/local/bin/chisel\n")
	script.WriteString("fi\n\n")

	// Pick a random base port. 2000 possible blocks of 10 in range 30000-49990.
	// With 1-5 concurrent workers, collision is near-impossible.
	script.WriteString(fmt.Sprintf("TUNNEL_SERVER='%s'\n", tunnelServer))
	script.WriteString("BASE_PORT=$(( (RANDOM % 2000) * 10 + 30000 ))\n")
	script.WriteString("NODE_MGR_PORT=$((BASE_PORT))\n")
	script.WriteString("OBJ_MGR_PORT=$((BASE_PORT + 1))\n")
	script.WriteString("MIN_WORKER_PORT=$((BASE_PORT + 2))\n")
	script.WriteString("MAX_WORKER_PORT=$((BASE_PORT + 9))\n")
	script.WriteString("echo \"[gpuscale] Tunnel port block: $BASE_PORT - $MAX_WORKER_PORT\"\n\n")

	// Build chisel remotes — one R:port:localhost:port per port in the block.
	script.WriteString("CHISEL_REMOTES=''\n")
	script.WriteString("for p in $(seq $BASE_PORT $MAX_WORKER_PORT); do\n")
	script.WriteString("  CHISEL_REMOTES=\"$CHISEL_REMOTES R:${p}:localhost:${p}\"\n")
	script.WriteString("done\n\n")

	script.WriteString("CHISEL_LOG=/tmp/chisel.log\n")
	script.WriteString("emit \"{\\\"claim\\\":\\\"$CLAIM_NAME\\\",\\\"step\\\":\\\"tunnel_connecting\\\",\\\"message\\\":\\\"connecting chisel tunnel to $TUNNEL_SERVER, port block $BASE_PORT-$MAX_WORKER_PORT\\\"}\"\n")
	script.WriteString("chisel client \"http://$TUNNEL_SERVER\" $CHISEL_REMOTES > \"$CHISEL_LOG\" 2>&1 &\n")
	script.WriteString("CHISEL_PID=$!\n")
	script.WriteString("echo \"[gpuscale] Chisel client started (PID $CHISEL_PID)\"\n\n")

	// Wait for chisel to connect.
	script.WriteString("TUNNEL_OK=0\n")
	script.WriteString("for i in $(seq 1 30); do\n")
	script.WriteString("  sleep 2\n")
	script.WriteString("  if ! kill -0 $CHISEL_PID 2>/dev/null; then\n")
	script.WriteString("    echo '[gpuscale] ERROR: chisel client exited'\n")
	script.WriteString("    cat \"$CHISEL_LOG\"\n")
	script.WriteString("    exit 1\n")
	script.WriteString("  fi\n")
	script.WriteString("  if grep -q 'Connected' \"$CHISEL_LOG\"; then\n")
	script.WriteString("    echo \"[gpuscale] Tunnel established for ports $BASE_PORT-$MAX_WORKER_PORT\"\n")
	script.WriteString("    TUNNEL_OK=1\n")
	script.WriteString("    break\n")
	script.WriteString("  fi\n")
	script.WriteString("  echo \"[gpuscale] Waiting for tunnel (attempt $i/30)...\"\n")
	script.WriteString("done\n\n")

	script.WriteString("if [ \"$TUNNEL_OK\" -ne 1 ]; then\n")
	script.WriteString("  echo '[gpuscale] ERROR: Failed to establish reverse tunnel'\n")
	script.WriteString("  cat \"$CHISEL_LOG\"\n")
	script.WriteString("  exit 1\n")
	script.WriteString("fi\n")
	script.WriteString("emit \"{\\\"claim\\\":\\\"$CLAIM_NAME\\\",\\\"step\\\":\\\"tunnel_ready\\\",\\\"message\\\":\\\"chisel tunnel established, ports $BASE_PORT-$MAX_WORKER_PORT\\\"}\"\n\n")

	// node-ip-address = Hetzner IP (chisel server host). GCS/proxy connect to
	// Hetzner:port → chisel → worker localhost:port for all tunneled ports.
	script.WriteString(fmt.Sprintf("echo '[gpuscale] Joining Ray cluster as worker with %d GPUs...'\n", numGPUs))
	script.WriteString("emit \"{\\\"claim\\\":\\\"$CLAIM_NAME\\\",\\\"step\\\":\\\"ray_joining\\\",\\\"message\\\":\\\"starting ray worker process, joining cluster\\\"}\"\n")
	script.WriteString("GPUSCALE_INSTANCE_ID=\"${INSTANCE_ID:-${VAST_CONTAINERLABEL:-${CONTAINER_ID:-unknown}}}\"\n")
	script.WriteString("echo \"[gpuscale] Instance ID: $GPUSCALE_INSTANCE_ID\"\n")
	script.WriteString(fmt.Sprintf("NODE_IP='%s'\n", rayHost))
	script.WriteString(fmt.Sprintf("ray start --address='%s' \\\n", rayAddr))
	script.WriteString(fmt.Sprintf("  --num-gpus=%d \\\n", numGPUs))
	script.WriteString("  --node-ip-address=$NODE_IP \\\n")
	script.WriteString("  --node-manager-port=$NODE_MGR_PORT \\\n")
	script.WriteString("  --object-manager-port=$OBJ_MGR_PORT \\\n")
	script.WriteString("  --min-worker-port=$MIN_WORKER_PORT \\\n")
	script.WriteString("  --max-worker-port=$MAX_WORKER_PORT \\\n")
	script.WriteString(fmt.Sprintf("  --labels='{\"gpuscale.io/provider\": \"%s\", \"gpuscale.io/gpu-type\": \"%s\", \"gpuscale.io/instance-id\": \"'\"$GPUSCALE_INSTANCE_ID\"'\"}' \\\n",
		config.ProviderName, config.GPUType))
	script.WriteString("  --block\n")

	return script.String()
}
