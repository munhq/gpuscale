# GPUScale

Multi-cloud GPU node autoscaler for Kubernetes. Dynamically provisions GPU instances from cheap cloud providers (Vast.ai, Verda, RunPod) and joins them to your K3s/K8s cluster.

## Why GPUScale?

- **Karpenter** only works on AWS/Azure
- **Cluster Autoscaler** needs pre-defined node groups
- **No open-source autoscaler** provisions GPU nodes from marketplace/spot GPU providers

GPUScale fills this gap: it watches for pending GPU workloads, finds the cheapest available GPU from across providers, provisions the instance, bootstraps it into your cluster via VPN, and scales down when idle.

## Architecture

```
┌─────────────────────────────────────────────┐
│  GPUScale Controller (runs in K3s)          │
│                                              │
│  Provisioning Controller                     │
│    Watch pending GPU pods → batch →          │
│    search offers → provision → bootstrap     │
│                                              │
│  Disruption Controller                       │
│    Watch idle managed nodes → drain →        │
│    destroy after cooldown                    │
│                                              │
│  Interruption Controller                     │
│    Poll provider APIs → detect preemption →  │
│    cleanup → let re-provision                │
│                                              │
│  Claim Reconciler                            │
│    Manage GPUNodeClaim lifecycle:             │
│    Pending → Provisioning → Bootstrapping    │
│    → Ready → Draining → Terminated           │
└─────────────────────────────────────────────┘
```

### Bootstrap Flow

1. Provider provisions instance (30-60s)
2. Bootstrap container starts — joins Netbird VPN (5-15s)
3. K3s agent joins cluster over VPN (10-30s)
4. NVIDIA device plugin detects GPU (10-30s)
5. Pod schedules on new node (5-10s)

**Total: ~1.5-3 minutes** with cached images.

## Supported Providers

| Provider | Capacity Types | Auth |
|----------|---------------|------|
| **Vast.ai** | Spot, On-demand | API Key |
| **Verda** | Spot, On-demand | OAuth2 (client credentials) |
| **RunPod** | Community, Secure | API Key |

## Quick Start

### 1. Install CRDs and Controller

```bash
helm install gpuscale charts/gpuscale/ \
  -n gpuscale-system --create-namespace \
  --set providers.vastai.enabled=true \
  --set providers.vastai.apiKey="your-vast-api-key"
```

### 2. Create Bootstrap Secrets

```bash
kubectl -n gpuscale-system create secret generic gpuscale-netbird-key \
  --from-literal=setup-key="your-netbird-setup-key"

kubectl -n gpuscale-system create secret generic gpuscale-k3s-token \
  --from-literal=token="your-k3s-token"
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
      apiKeySecret:
        name: gpuscale-vast-credentials
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
    image: "ghcr.io/munhq/gpuscale-node:latest"
    vpnSetupKeySecret:
      name: gpuscale-netbird-key
      namespace: gpuscale-system
    k3sTokenSecret:
      name: gpuscale-k3s-token
      namespace: gpuscale-system
    k3sURL: "https://10.100.0.1:6443"
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

GPUScale will detect the pending pod, find the cheapest matching GPU, provision it, and join it to your cluster.

### 5. Watch It Work

```bash
# Watch GPUNodeClaims
kubectl get gpunodeclaims -n gpuscale-system -w

# Watch nodes joining
kubectl get nodes -l gpuscale.io/managed=true -w
```

## CRDs

### GPUNodePool (cluster-scoped)

Defines which providers to use, GPU requirements, scaling behavior, and bootstrap configuration.

### GPUNodeClaim (namespace-scoped)

Auto-created by the controller. Tracks the lifecycle of a provisioned GPU node:
- **Pending** — Claim created, waiting for provisioning
- **Provisioning** — Instance being created on provider
- **Bootstrapping** — Instance running, waiting for K3s join
- **Ready** — Node joined and serving workloads
- **Draining** — Node being drained before destruction
- **Terminated** — Instance destroyed

## Pod Annotations

| Annotation | Description | Default |
|------------|-------------|---------|
| `gpuscale.io/gpu-vram` | Minimum VRAM in GB | from pool |
| `gpuscale.io/gpu-type` | Preferred GPU type | any |
| `gpuscale.io/max-price` | Max $/hr | from pool |
| `gpuscale.io/provider` | Preferred provider | any |
| `gpuscale.io/priority` | `spot` or `on-demand` | `spot` |

## Development

```bash
# Build
make build

# Run locally (needs kubeconfig)
make run

# Build Docker images
make docker-build
make docker-build-node

# Run tests
make test
```

## Project Structure

```
gpuscale/
├── cmd/controller/        # Entry point
├── api/v1alpha1/          # CRD types
├── internal/
│   ├── controller/        # Provisioning, disruption, interruption controllers
│   ├── provider/          # Provider interface + implementations
│   │   ├── vastai/
│   │   ├── verda/
│   │   └── runpod/
│   ├── scheduler/         # Pod-to-GPU matching, offer selection
│   └── bootstrap/         # Bootstrap script generation, health checks
├── charts/gpuscale/       # Helm chart
├── docker/                # Dockerfiles (controller + node)
└── examples/              # Example manifests
```
