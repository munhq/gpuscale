package controller

import (
	"context"
	"fmt"

	"github.com/munhq/gpuscale/pkg/provider"
)

// ProvisioningDecision represents whether to provision and what to provision.
type ProvisioningDecision struct {
	ShouldProvision bool
	Reason          string
	Models          []string // models that need capacity
	RequiredVRAM    int      // total VRAM needed across the instance (GB) — passed as MinVRAM
	MultiGpu        bool     // any model in the bundle allows multi-GPU instances
}

// DecideProvisioning implements bin-packing logic to determine if provisioning is needed.
// This is the core of scenarios 1-4:
//
// Scenario 1: No nodes + 1 model → provision exact fit
// Scenario 2: No nodes + multiple models → provision to fit all
// Scenario 3: Existing node but model won't fit → provision new node
// Scenario 4: Node has capacity → don't provision
func (r *ProvisioningController) DecideProvisioning(
	ctx context.Context,
	demands []*ModelDemand,
	capacity *ClusterCapacity,
) (*ProvisioningDecision, error) {

	if len(demands) == 0 {
		return &ProvisioningDecision{
			ShouldProvision: false,
			Reason:          "no demand",
		}, nil
	}

	// Filter to models that actually need capacity
	modelsNeedingCapacity := r.filterModelsNeedingCapacity(demands, capacity)
	if len(modelsNeedingCapacity) == 0 {
		return &ProvisioningDecision{
			ShouldProvision: false,
			Reason:          "all models already loaded or no queue",
		}, nil
	}

	// Sum VRAM and collect model names; any model with MultiGpu=true enables multi-GPU offers.
	totalVRAMNeeded := 0
	multiGpu := false
	modelNames := make([]string, 0, len(modelsNeedingCapacity))
	for _, d := range modelsNeedingCapacity {
		totalVRAMNeeded += d.VRAMRequired
		if d.MultiGpu {
			multiGpu = true
		}
		modelNames = append(modelNames, d.Model)
	}

	// Scenario 1 & 2: No nodes (cold cluster)
	if len(capacity.Nodes) == 0 || capacity.TotalGPUs == 0 {
		return &ProvisioningDecision{
			ShouldProvision: true,
			Reason: fmt.Sprintf("cold cluster: need %dGB for %d models",
				totalVRAMNeeded, len(modelNames)),
			Models:       modelNames,
			RequiredVRAM: totalVRAMNeeded,
			MultiGpu:     multiGpu,
		}, nil
	}

	// Scenario 4: Check if existing cluster has enough free capacity
	if capacity.FreeVRAM >= totalVRAMNeeded {
		return &ProvisioningDecision{
			ShouldProvision: false,
			Reason: fmt.Sprintf("cluster has %dGB free (need %dGB for %d models)",
				capacity.FreeVRAM, totalVRAMNeeded, len(modelNames)),
		}, nil
	}

	// Scenario 3: Existing nodes but not enough capacity — provision for the gap.
	gap := totalVRAMNeeded - capacity.FreeVRAM
	return &ProvisioningDecision{
		ShouldProvision: true,
		Reason: fmt.Sprintf("capacity gap: need %dGB more (%dGB free, %dGB needed) for %d models",
			gap, capacity.FreeVRAM, totalVRAMNeeded, len(modelNames)),
		Models:       modelNames,
		RequiredVRAM: gap,
		MultiGpu:     multiGpu,
	}, nil
}

// filterModelsNeedingCapacity returns models that have demand but aren't loaded.
func (r *ProvisioningController) filterModelsNeedingCapacity(
	demands []*ModelDemand,
	capacity *ClusterCapacity,
) []*ModelDemand {
	loadedModels := make(map[string]bool)
	for _, m := range capacity.LoadedModels {
		loadedModels[m] = true
	}

	var needed []*ModelDemand
	for _, d := range demands {
		// Need capacity if:
		// 1. Model has queued requests OR active demand
		// 2. Model is always-active but not loaded
		// 3. Model not currently loaded
		hasDemand := d.QueueDepth > 0 || d.ActiveDemand > 0 || d.AlwaysActive
		notLoaded := !loadedModels[d.Model]

		if hasDemand && notLoaded {
			needed = append(needed, d)
		}
	}
	return needed
}

// CreateProvisioningRequirements converts a ProvisioningDecision into provider.GPURequirements.
// GPUCount=0 means: any number of GPUs, as long as total VRAM >= RequiredVRAM.
// The coordinator will accept 1×80GB, 2×48GB, 4×24GB — whatever is available and cheapest.
func (r *ProvisioningController) CreateProvisioningRequirements(decision *ProvisioningDecision) provider.GPURequirements {
	return provider.GPURequirements{
		GPUCount: 0,
		MinVRAM:  decision.RequiredVRAM,
		MultiGpu: decision.MultiGpu,
	}
}
