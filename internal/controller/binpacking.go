package controller

import (
	"context"
	"fmt"
	"math"

	"github.com/munhq/gpuscale/pkg/provider"
)

// ProvisioningDecision represents whether to provision and what to provision.
type ProvisioningDecision struct {
	ShouldProvision bool
	Reason          string
	Models          []string // models that need capacity
	RequiredVRAM    int      // total VRAM needed (GB)
	RequiredGPUs    int      // number of GPUs needed
	GPUType         string   // recommended GPU type
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

	// Calculate total VRAM needed for pending models
	totalVRAMNeeded := 0
	modelNames := make([]string, 0, len(modelsNeedingCapacity))
	for _, d := range modelsNeedingCapacity {
		totalVRAMNeeded += d.VRAMRequired
		modelNames = append(modelNames, d.Model)
	}

	// Scenario 1 & 2: No nodes (cold cluster)
	if len(capacity.Nodes) == 0 || capacity.TotalGPUs == 0 {
		return r.decisionColdCluster(totalVRAMNeeded, modelNames), nil
	}

	// Scenario 4: Check if existing cluster has enough free capacity
	if capacity.FreeVRAM >= totalVRAMNeeded {
		return &ProvisioningDecision{
			ShouldProvision: false,
			Reason: fmt.Sprintf("cluster has %dGB free (need %dGB for %d models)",
				capacity.FreeVRAM, totalVRAMNeeded, len(modelNames)),
		}, nil
	}

	// Scenario 3: Existing nodes but not enough capacity
	gap := totalVRAMNeeded - capacity.FreeVRAM
	return r.decisionCapacityGap(gap, totalVRAMNeeded, modelNames, capacity), nil
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

// decisionColdCluster handles Scenario 1 & 2: no existing nodes.
func (r *ProvisioningController) decisionColdCluster(totalVRAMNeeded int, models []string) *ProvisioningDecision {
	// Determine optimal GPU type and count
	gpuType, gpuCount := r.selectGPUConfiguration(totalVRAMNeeded)

	return &ProvisioningDecision{
		ShouldProvision: true,
		Reason: fmt.Sprintf("cold cluster: need %dGB for %d models",
			totalVRAMNeeded, len(models)),
		Models:       models,
		RequiredVRAM: totalVRAMNeeded,
		RequiredGPUs: gpuCount,
		GPUType:      gpuType,
	}
}

// decisionCapacityGap handles Scenario 3: existing nodes but capacity gap.
func (r *ProvisioningController) decisionCapacityGap(
	gap int,
	totalVRAMNeeded int,
	models []string,
	capacity *ClusterCapacity,
) *ProvisioningDecision {
	// Determine GPU type from existing cluster or select new
	gpuType := ""
	if len(capacity.Nodes) > 0 {
		gpuType = capacity.Nodes[0].GPUType // use same type as existing
	}

	selectedType, gpuCount := r.selectGPUConfiguration(gap)
	if gpuType == "" {
		gpuType = selectedType
	}

	return &ProvisioningDecision{
		ShouldProvision: true,
		Reason: fmt.Sprintf("capacity gap: need %dGB more (%dGB free, %dGB needed) for %d models",
			gap, capacity.FreeVRAM, totalVRAMNeeded, len(models)),
		Models:       models,
		RequiredVRAM: gap,
		RequiredGPUs: gpuCount,
		GPUType:      gpuType,
	}
}

// selectGPUConfiguration selects optimal GPU type and count for required VRAM.
// Returns (gpuType, gpuCount) for a single instance with multiple GPUs (tensor parallelism).
func (r *ProvisioningController) selectGPUConfiguration(vramNeeded int) (gpuType string, gpuCount int) {
	// GPU options ordered by cost-effectiveness
	gpuOptions := []struct {
		name   string
		vramGB int
	}{
		{"RTX 4090", 24},       // cheapest per GB
		{"A100 80GB", 80},      // good balance
		{"H100", 80},           // high performance
		{"H200", 141},          // large models
		{"B200", 180},          // very large models
	}

	// Find smallest GPU type that can fit the model with reasonable count
	for _, gpu := range gpuOptions {
		count := int(math.Ceil(float64(vramNeeded) / float64(gpu.vramGB)))
		if count <= 8 { // max 8 GPUs per instance (typical NVLink limit)
			return gpu.name, count
		}
	}

	// Fallback: use largest GPU type
	largestGPU := gpuOptions[len(gpuOptions)-1]
	count := int(math.Ceil(float64(vramNeeded) / float64(largestGPU.vramGB)))
	return largestGPU.name, count
}

// CreateProvisioningRequirements converts a ProvisioningDecision into provider.GPURequirements.
func (r *ProvisioningController) CreateProvisioningRequirements(decision *ProvisioningDecision) provider.GPURequirements {
	// Calculate per-GPU VRAM needed
	vramPerGPU := 0
	if decision.RequiredGPUs > 0 {
		vramPerGPU = int(math.Ceil(float64(decision.RequiredVRAM) / float64(decision.RequiredGPUs)))
	}

	req := provider.GPURequirements{
		GPUCount: decision.RequiredGPUs,
		MinVRAM:  vramPerGPU,
	}

	if decision.GPUType != "" {
		req.GPUTypes = []string{decision.GPUType}
	}

	return req
}
