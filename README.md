# gpuscale

gpuscale provisions GPU capacity from seven cloud and marketplace providers, runs vLLM on it, and releases it when demand stops. It runs as a Kubernetes controller. The GPU instances it creates are not cluster members: each runs an agent process and vLLM.

You declare the capacity you are willing to buy as a `GPUNodePool`:

```yaml
apiVersion: gpuscale.io/v1alpha1
kind: GPUNodePool
metadata:
  name: inference
spec:
  providers:
    - name: vast.ai
      nodeType: standalone
      capacityType: spot
      maxPrice: 0.50                 # $/hr ceiling, per instance
      apiKeySecret:
        name: gpuscale-provider-credentials
        namespace: gpuscale-system
  requirements:
    gpuTypes: ["RTX 4090", "RTX 3090", "A100"]
    minVRAM: 24                      # GB
    minDisk: 50
    minRAM: 32
  scaling:
    minNodes: 0                      # scale to zero when idle
    maxNodes: 6
    batchWindow: "10s"
    cooldownPeriod: "2m"
  bootstrap:
    image: "vllm/vllm-openai:latest"
  limits:
    maxGPUs: 12
    maxCostPerHour: 3.00
```

Apply the resource. When a GPU workload is pending, the controller searches the configured providers, selects the cheapest offer that meets the requirements, creates the instance, waits for it to serve, and routes traffic to it. When demand stops, it destroys the instance.

## Features

- Offer selection across all configured providers, filtered on GPU type, VRAM, price ceiling, and spot or on-demand capacity.
- Scale to zero. Set `minNodes: 0` and an idle pool costs nothing. A cooldown prevents repeated create and destroy cycles between requests.
- Spot preemption handling. A controller polls provider APIs, detects reclaimed instances, and replaces them if demand remains.
- A hard cost ceiling per instance (`maxPrice`) and per pool (`maxCostPerHour`).
- Consolidation. When bin-packing shows the same models fit on fewer GPUs, the controller migrates them.
- Metrics. vLLM metrics are scraped from each worker and re-exposed as `gpuscale_worker_vllm_*`, alongside controller metrics.

## Background

The platform this came from used Kueue for queueing and Ray for placement. Two problems followed from that design:

- A queued job landed on a worker that had to load the model, so each batch paid the model load time.
- Ray placed work on GPUs that the provisioner already tracked, which duplicated state.

gpuscale replaces both. It schedules against GPU requirements declared as pod annotations:

```
gpuscale.io/gpu-vram    gpuscale.io/gpu-type    gpuscale.io/max-price
gpuscale.io/provider    gpuscale.io/priority
```

A provisioned node runs two processes, an agent and vLLM. It does not run Kubernetes or Ray.

Existing autoscalers did not fit either. Karpenter supports AWS and Azure. Cluster Autoscaler requires node groups defined in advance. Neither can acquire a spot GPU from a marketplace.

## Providers

| Provider | Kind |
|---|---|
| AWS | hyperscaler |
| Azure | hyperscaler |
| GCP | hyperscaler |
| RunPod | GPU cloud |
| Vast.ai | GPU marketplace |
| TensorDock | GPU marketplace |
| Verda | GPU cloud |

All seven providers implement one interface, `pkg/provider`. A provider supplies four operations: offer search, instance create, instance status, and instance destroy. Code above that interface is provider-agnostic. To add a provider, implement the interface. The controllers do not change.

Offer selection filters on GPU count, GPU type, total VRAM, maximum VRAM per GPU, price ceiling per hour, disk, RAM, and spot versus on-demand.

## Node lifecycle

Two custom resources drive it:

- **`GPUNodePool`** — what capacity you are willing to buy: providers, credentials, GPU requirements, scaling bounds and pool limits.
- **`GPUNodeClaim`** — one instance moving through its life:

```
Pending → Provisioning → Bootstrapping → Ready → Draining → Terminated
```

Five controllers do the work:

| Controller | Responsibility |
|---|---|
| Provisioning | Watch pending GPU pods, batch demand, create claims |
| Claim reconciler | Select an offer, provision, bootstrap, wait for Ready, publish the endpoint |
| Disruption | Destroy idle nodes after a cooldown, consolidate the fleet by VRAM bin-packing |
| Interruption | Poll provider APIs for preemption, clean up, re-provision if demand persists |
| Worker metrics | Scrape vLLM metrics from workers and re-expose them as `gpuscale_worker_vllm_*` |

## Failure handling

### Failed offers are blacklisted

When an offer fails to bring up, its ID is added to a blacklist with a time-to-live. Without this, the scheduler selects the same failing offer on the next reconcile. The TTL expiry returns the offer to the pool automatically, so a recovered provider requires no manual reset.

### Idle nodes are destroyed after a cooldown

A node with no active requests is not necessarily idle. Provisioning a replacement takes one to three minutes, so the controller waits for the configured `cooldownPeriod` before destroying an idle node. Nodes serving models marked always-active are never destroyed.

### Consolidation completes before draining

When bin-packing shows the same models fit on fewer nodes, the reconciler waits for the replacement claim to reach Ready and serve traffic, then annotates the original claim for immediate drain, bypassing the cooldown. This avoids a capacity gap during consolidation.

## Configuration

Credentials are read from the environment, or from a `SecretReference` on the pool:

```
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_SUBNET_ID, AWS_SECURITY_GROUP_ID
GCP_PROJECT_ID, GCP_SERVICE_ACCOUNT_JSON, GCP_ZONES
RUNPOD_API_KEY
VASTAI_API_KEY
VERDA_CLIENT_ID, VERDA_CLIENT_SECRET
```

State and demand signals:

```
POSTGRES_URL    instance and claim state
REDIS_URL       demand counters and idle markers
POD_NAMESPACE   namespace the controller runs in
```

Worked examples of `GPUNodePool` are in [`examples/`](examples/): a single-provider pool (`gpunodepool-vast.yaml`), a multi-provider pool (`gpunodepool-multi.yaml`), and a hyperscaler-plus-marketplace mix (`gpunodepool-hybrid.yaml`). `sample-workload.yaml` is a pod that triggers provisioning.

Build the controller image from [`docker/controller/Dockerfile`](docker/controller/Dockerfile).

## Deploy

A Helm chart is in [`deploy/helm/gpuscale`](deploy/helm/gpuscale). It installs the two CRDs, the controller Deployment, a ServiceAccount, ClusterRole and ClusterRoleBinding, and optionally a first `GPUNodePool`:

```
helm install gpuscale deploy/helm/gpuscale \
  --namespace gpu-workloads --create-namespace \
  --set providers.vastai.enabled=true \
  --set providers.vastai.existingSecret=gpuscale-provider-credentials
```

Images are built and published by CI on every `v*` tag — multi-arch amd64 and arm64, to
`ghcr.io/munhq/gpuscale-controller`, with the packaged Helm chart attached to the release.
See [`.github/workflows/release.yml`](.github/workflows/release.yml).

To run your own build instead:

```
docker build -t <your-registry>/gpuscale-controller:dev -f docker/controller/Dockerfile .
helm install gpuscale deploy/helm/gpuscale --set image.repository=<your-registry>/gpuscale-controller --set image.tag=dev
```

Every provider is disabled by default. Enable the ones you hold credentials for, and supply them through `existingSecret` rather than chart values.

`gpuscale` also deploys through ArgoCD as part of a full GPU platform — see the ApplicationSet and values override in [kubernetes_gpu](https://github.com/munhq/kubernetes_gpu) under `ansible/argocd/`, alongside the NVIDIA device plugin, DCGM, KubeRay, Dragonfly and the batch API.

## Requirements

Go 1.25, a Kubernetes cluster for the controller (the GPU instances themselves are standalone and do not join it), Postgres, Redis or Dragonfly, and credentials for at least one provider.

## Design history

This started as the Kubernetes-native path for a private inference platform. That platform has since moved its default provisioning into an API service driving a per-node agent over a WSS tunnel, which removes the Kubernetes dependency from the GPU nodes entirely — worth it there, because the GPU instances never needed to be cluster members.

`gpuscale` remains the right shape if you want GPU capacity expressed as Kubernetes resources and scheduled by Kubernetes: multi-tenant clusters, existing CRD-driven workflows, or anywhere `kubectl get gpunodeclaims` is the interface you want. The provider layer in `pkg/provider` is shared by both designs and is actively used.

Issues and pull requests are welcome, particularly new providers.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
