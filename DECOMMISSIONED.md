# gpuscale — Decommissioned (K8s path)

gpuscale was the Kubernetes-native GPU provisioning operator for this platform.
It used K8s CRDs (`GPUNodeClaim`), controller-runtime, and K8s scheduling signals
to provision and lifecycle-manage GPU nodes.

## Status: Legacy / Opt-in only

**This component is no longer the default.** GPU provisioning has moved into
`gpu-api` directly, using the standalone `gpu-agent` binary on GPU nodes.

The new default stack:
```
gpu-api  ←→  gpu-agent (runs on GPU node, connects via WSS tunnel)
```

- No K8s required on GPU nodes
- No CRDs
- Provisioning is driven by gpu-api calling vendor APIs directly
- Provider implementations live in `pkg/provider/` (imported by gpu-api)

## When to use gpuscale

If a customer explicitly requires Kubernetes-native GPU scheduling (e.g. for
multi-tenant K8s clusters), gpuscale can still be deployed. It is not actively
developed for new features.

## Migration

Existing GPUNodeClaim state is tracked in Postgres `instances` table (managed
by gpu-api). The K8s CRDs and controller-runtime dependency are not needed for
the default gpu-agent path.
