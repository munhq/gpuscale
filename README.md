# GPUScale

Multi-cloud GPU autoscaler for Kubernetes. Dynamically provisions standalone vLLM inference workers from spot GPU providers (Vast.ai, Verda, RunPod) when demand exceeds in-cluster capacity.

## Why GPUScale?

- **Karpenter** only works on AWS/Azure
- **Cluster Autoscaler** needs pre-defined node groups
- **No open-source autoscaler** provisions GPU workers from marketplace/spot GPU providers

GPUScale fills this gap: it watches for pending GPU workloads, finds the cheapest available GPU across providers, provisions a standalone vLLM instance running Ray Serve, and destroys it when idle.

## Architecture

```
┌─────────────────────────────────────────────┐
│  GPUScale Controller (runs in K8s)          │
│                                              │
│  Provisioning Controller                     │
│    Watch pending GPU pods → batch →          │
│    search offers → provision vLLM worker     │
│                                              │
│  Claim Reconciler                            │
│    Manage GPUNodeClaim lifecycle:             │
│    Pending → Provisioning → Bootstrapping    │
│    → Ready → Draining → Terminated           │
│                                              │
│  Disruption Controller                       │
│    Watch idle workers → destroy after        │
│    cooldown (respects minNodes)              │
│                                              │
│  Interruption Controller                     │
│    Poll provider APIs → detect preemption →  │
│    cleanup → let re-provision                │
│                                              │
│  Worker Metrics Collector                    │
│    Scrape vLLM /metrics from workers →       │
│    re-expose as gpuscale_worker_vllm_*       │
└────────────────┬────────────────────────────┘
                 │ provisions
    ┌────────────┼────────────┐
    ▼            ▼            ▼
┌────────┐ ┌────────┐ ┌────────┐
│Vast.ai │ │ Verda  │ │RunPod  │
│ vLLM   │ │ vLLM   │ │ vLLM   │
│ :8000  │ │ :8000  │ │ :8000  │
└────────┘ └────────┘ └────────┘
  Standalone ray-worker instances
  (not K8s nodes — accessed via HTTP)
```

### How It Works

1. Pending GPU pod detected (e.g., KEDA demand-signal pod)
2. Controller batches requests, finds cheapest GPU offer across providers
3. Provider provisions instance with Ray Serve + vLLM (via `build_openai_app`)
4. Controller polls `GET /v1/models` until worker is healthy (~1-3 min)
5. Claim status moves to Ready with `status.endpoint` set
6. GPU API router adds worker, starts routing inference requests
7. When idle past cooldown → provider destroys instance

## Supported Providers

| Provider | Capacity Types | Auth | Instance Type |
|----------|---------------|------|---------------|
| **Vast.ai** | Spot, On-demand | API Key | Container |
| **Verda** | Spot, On-demand | OAuth2 (client credentials) | VM |
| **RunPod** | Community, Secure | API Key | Container |

## Quick Start

### 1. Deploy via ArgoCD

The Helm chart is at `ansible/argocd/charts/gpuscale/`. ArgoCD deploys it via ApplicationSet.

### 2. Create Provider Credentials

```bash
kubectl -n gpuscale-system create secret generic gpuscale-provider-credentials \
  --from-literal=vastai-api-key="your-vast-api-key" \
  --from-literal=verda-client-id="your-verda-client-id" \
  --from-literal=verda-client-secret="your-verda-client-secret"
```

### 3. Create a GPUNodePool

```yaml
apiVersion: gpuscale.io/v1alpha1
kind: GPUNodePool
metadata:
  name: inference-pool
spec:
  providers:
    - name: vast.ai
      nodeType: ray-worker
      apiKeySecret:
        name: gpuscale-provider-credentials
        namespace: gpuscale-system
      capacityType: spot
      maxPrice: 0.50
  requirements:
    gpuTypes: ["RTX 4090", "A100"]
    minVRAM: 24
  scaling:
    maxNodes: 10
    cooldownPeriod: "10m"
  bootstrap:
    image: "rayproject/ray-llm:2.53.0-py311-cu128"
    rayConfig:
      servePort: 8000
    modelConfig:
      modelId: "THUDM/glm-4-9b-chat"
      maxModelLen: 8192
      dtype: auto
      gpuMemoryUtilization: 0.90
      enablePrefixCaching: true
  limits:
    maxGPUs: 20
    maxCostPerHour: 5.00
```

### 4. Deploy a GPU Workload

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: inference
  annotations:
    gpuscale.io/gpu-vram: "24"
    gpuscale.io/max-price: "0.50"
spec:
  tolerations:
    - key: nvidia.com/gpu
      operator: Exists
      effect: NoSchedule
  containers:
    - name: vllm
      image: vllm/vllm-openai:latest
      resources:
        limits:
          nvidia.com/gpu: 1
```

GPUScale detects the pending pod, finds the cheapest GPU, and provisions a standalone vLLM worker.

### 5. Watch It Work

```bash
# Watch GPUNodeClaims
kubectl get gpunodeclaims -n gpuscale-system -w

# Check worker metrics
curl http://gpuscale-controller:8080/metrics | grep gpuscale_worker
```

## CRDs

### GPUNodePool (cluster-scoped)

Defines which providers to use, GPU requirements, scaling behavior, model configuration, and cost limits.

### GPUNodeClaim (namespace-scoped)

Auto-created by the controller. Tracks the lifecycle of a provisioned GPU worker:
- **Pending** — Claim created, waiting for provisioning
- **Provisioning** — Instance being created on provider
- **Bootstrapping** — Instance running, waiting for vLLM to respond on `/v1/models`
- **Ready** — Worker healthy and serving inference (endpoint in `status.endpoint`)
- **Draining** — Worker being destroyed
- **Terminated** — Instance destroyed

## Pod Annotations

| Annotation | Description | Default |
|------------|-------------|---------|
| `gpuscale.io/gpu-vram` | Minimum VRAM in GB | from pool |
| `gpuscale.io/gpu-type` | Preferred GPU type | any |
| `gpuscale.io/max-price` | Max $/hr | from pool |
| `gpuscale.io/provider` | Preferred provider | any |
| `gpuscale.io/priority` | `spot` or `on-demand` | `spot` |

## Worker Monitoring

The controller scrapes vLLM `/metrics` from each Ready worker every 60s and re-exposes them on `:8080/metrics` as `gpuscale_worker_vllm_*` with labels (endpoint, provider, instance_id, gpu_type). Prometheus picks these up for Grafana dashboards.

Key metrics: `num_requests_running`, `gpu_cache_usage_perc`, `avg_generation_throughput_toks_per_s`, `e2e_request_latency_seconds`.

## Development

```bash
# Build controller
cd gpuscale && go build -o bin/controller ./cmd/controller

# Run tests
cd gpuscale && go test ./...

# Build Docker image
docker build -f docker/controller/Dockerfile -t gpuscale-controller .
```

## Project Structure

```
gpuscale/
├── cmd/controller/        # Entry point
├── api/v1alpha1/          # CRD types (GPUNodePool, GPUNodeClaim)
├── internal/
│   ├── controller/        # Provisioning, disruption, interruption, reconciler
│   ├── provider/          # Provider interface + implementations
│   │   ├── vastai/
│   │   ├── verda/
│   │   └── runpod/
│   ├── scheduler/         # Pod-to-GPU matching, offer selection
│   ├── bootstrap/         # Ray Serve bootstrap script generation, health checks
│   └── metrics/           # Worker vLLM metrics collector
├── docker/controller/     # Controller Dockerfile
└── examples/              # Example GPUNodePool manifests
```
