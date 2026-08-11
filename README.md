# gpuscale

A Kubernetes controller that provisions GPU capacity from **seven cloud and GPU-marketplace providers**, keeps it healthy, and takes it away again when the demand goes.

`gpuscale` watches for pending GPU workloads, searches every configured provider for the cheapest offer that meets the requirement, provisions the instance, waits for it to come up, routes work to it, and terminates it when it goes idle or the provider preempts it.

## Why this exists

It replaces two things that did not fit: the scheduler, and the autoscaler.

**The scheduler.** The platform this came from started on **Kueue plus Ray**. Kueue queues the work correctly, but a queued job still lands on a worker that has to load the model, so every batch paid a cold start — and Ray added an orchestration layer whose only job was to place work on GPUs that `gpuscale` already knows about.

`gpuscale` replaced both. It schedules against GPU requirements declared as pod annotations and provisions to satisfy them:

```
gpuscale.io/gpu-vram    gpuscale.io/gpu-type    gpuscale.io/max-price
gpuscale.io/provider    gpuscale.io/priority
```

A provisioned node runs **two processes: an agent and vLLM.** No K3s on the GPU node, no Ray, no service mesh. The node is a GPU running a model server and reporting its health; everything else is a control-plane concern.

**The autoscaler.** Karpenter provisions on AWS and Azure only. Cluster Autoscaler needs node groups defined in advance. Neither can buy a spot GPU from a marketplace when that is the cheapest capacity available. If your GPU spend is dominated by price differences between providers, no existing open-source autoscaler can act on that.

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

All seven sit behind one interface (`pkg/provider`). A provider supplies offer search, instance create, instance status and instance destroy; everything above it is provider-agnostic. Adding a provider means implementing that interface — nothing in the controllers changes.

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

## Failure handling, and the reasoning behind it

The interesting part of an autoscaler is not how it adds capacity — it is what it does when capacity misbehaves. Three deliberate choices:

**Bad offers are quarantined, not retried.** When an offer fails to bring up, its ID enters a TTL blacklist. Marketplace inventory is uneven; without this the scheduler picks the same broken offer on the next reconcile and burns the loop. The TTL expiry means a provider that has recovered comes back automatically, with no manual reset.

**Idle nodes wait out a cooldown before they die.** A GPU that goes quiet for thirty seconds is not idle, it is between requests. Destroying it means paying the provisioning latency again — one to three minutes — the next time demand arrives. The cooldown is the price of not thrashing. Nodes serving always-active models are never destroyed.

**Consolidation is separate from disruption.** When bin-packing shows the same models fit on fewer nodes, the reconciler brings the replacement to Ready and serving *first*, then annotates the old claim for immediate drain, skipping the normal cooldown. Consolidation should never open a capacity gap.

The general rule behind all three: a fleet that destroys a node on a transient signal wastes more capacity than the fault did. Every automated teardown here is either reversible or gated behind a state that has already been observed to hold.

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
