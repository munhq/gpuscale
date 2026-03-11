package controller

import (
	"context"
	"testing"
)

// newTestController returns a ProvisioningController with no K8s client.
// DecideProvisioning and filterModelsNeedingCapacity don't touch the client.
func newTestController() *ProvisioningController {
	return &ProvisioningController{}
}

func TestDecideProvisioning_NoDemand(t *testing.T) {
	ctrl := newTestController()
	dec, err := ctrl.DecideProvisioning(context.Background(), nil, &ClusterCapacity{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.ShouldProvision {
		t.Error("no demand: should not provision")
	}
}

func TestDecideProvisioning_AllModelsLoaded(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "llama3", QueueDepth: 5, VRAMRequired: 16},
	}
	capacity := &ClusterCapacity{
		LoadedModels: []string{"llama3"},
		TotalGPUs:    1,
	}
	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.ShouldProvision {
		t.Error("model already loaded: should not provision")
	}
}

func TestDecideProvisioning_ColdCluster(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "llama3", QueueDepth: 3, VRAMRequired: 16},
	}
	capacity := &ClusterCapacity{} // zero nodes

	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dec.ShouldProvision {
		t.Error("cold cluster with demand: should provision")
	}
	if dec.RequiredVRAM != 16 {
		t.Errorf("RequiredVRAM: want 16, got %d", dec.RequiredVRAM)
	}
	if len(dec.Models) != 1 || dec.Models[0] != "llama3" {
		t.Errorf("Models: want [llama3], got %v", dec.Models)
	}
}

func TestDecideProvisioning_CapacityGap(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "llama3", QueueDepth: 1, VRAMRequired: 48},
	}
	capacity := &ClusterCapacity{
		Nodes:     []GPUNode{{NodeID: "n1", GPUCount: 2, VRAMPerGPU: 24}},
		TotalGPUs: 2,
		TotalVRAM: 48,
		FreeVRAM:  16, // only 16GB free, need 48
	}

	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dec.ShouldProvision {
		t.Error("capacity gap: should provision")
	}
	if dec.RequiredVRAM != 32 { // 48 needed - 16 free
		t.Errorf("RequiredVRAM (gap): want 32, got %d", dec.RequiredVRAM)
	}
}

func TestDecideProvisioning_SufficientFreeVRAM(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "phi3", QueueDepth: 2, VRAMRequired: 8},
	}
	capacity := &ClusterCapacity{
		Nodes:     []GPUNode{{NodeID: "n1", GPUCount: 1, VRAMPerGPU: 24}},
		TotalGPUs: 1,
		TotalVRAM: 24,
		FreeVRAM:  20, // 20GB free, need 8
	}

	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dec.ShouldProvision {
		t.Error("sufficient free VRAM: should not provision")
	}
}

func TestDecideProvisioning_AlwaysActiveNoQueue(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "always-on", QueueDepth: 0, ActiveDemand: 0, AlwaysActive: true, VRAMRequired: 24},
	}
	capacity := &ClusterCapacity{}

	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dec.ShouldProvision {
		t.Error("AlwaysActive model not loaded: should provision even with empty queue")
	}
}

func TestDecideProvisioning_MultiGPUFlag(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "llama-70b", QueueDepth: 1, VRAMRequired: 140, MultiGpu: true},
	}
	capacity := &ClusterCapacity{}

	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dec.MultiGpu {
		t.Error("multi-GPU demand should set MultiGpu=true on decision")
	}
}

func TestDecideProvisioning_MultipleDemands(t *testing.T) {
	ctrl := newTestController()
	demands := []*ModelDemand{
		{Model: "model-a", QueueDepth: 1, VRAMRequired: 16},
		{Model: "model-b", QueueDepth: 1, VRAMRequired: 24},
		{Model: "model-c", QueueDepth: 0, AlwaysActive: false, VRAMRequired: 8}, // no demand
	}
	capacity := &ClusterCapacity{}

	dec, err := ctrl.DecideProvisioning(context.Background(), demands, capacity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dec.ShouldProvision {
		t.Error("two models with demand: should provision")
	}
	if dec.RequiredVRAM != 40 { // 16+24, model-c excluded
		t.Errorf("RequiredVRAM: want 40, got %d", dec.RequiredVRAM)
	}
	if len(dec.Models) != 2 {
		t.Errorf("Models: want 2 (not 3), got %d: %v", len(dec.Models), dec.Models)
	}
}

func TestCreateProvisioningRequirements(t *testing.T) {
	ctrl := newTestController()
	dec := &ProvisioningDecision{
		RequiredVRAM: 48,
		MultiGpu:     true,
	}
	req := ctrl.CreateProvisioningRequirements(dec)
	if req.MinVRAM != 48 {
		t.Errorf("MinVRAM: want 48, got %d", req.MinVRAM)
	}
	if !req.MultiGpu {
		t.Error("MultiGpu should be true")
	}
	if req.GPUCount != 0 {
		t.Errorf("GPUCount should be 0 (coordinator picks count): got %d", req.GPUCount)
	}
}
