package controller

import "os"

// claimNamespace returns the namespace for GPUNodeClaims.
// Reads POD_NAMESPACE env (set via downward API) with fallback to gpu-workloads.
func claimNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return "gpu-workloads"
}
