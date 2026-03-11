package coordinator

import (
	"testing"

	"github.com/munhq/gpuscale/pkg/provider"
)

func makeOffer(gpuCount, vram int, pricePerHour float64, capacityType, gpuType string) provider.Offer {
	return provider.Offer{
		OfferID:      "offer-1",
		ProviderName: "test",
		GPUCount:     gpuCount,
		VRAM:         vram,
		PricePerHour: pricePerHour,
		CapacityType: capacityType,
		GPUType:      gpuType,
		DiskGB:       100,
		RAMGB:        64,
	}
}

func TestFilterByRequirements_NoFilters(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
		makeOffer(1, 48, 2.0, "spot", "A100"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{})
	if len(got) != 2 {
		t.Errorf("no filters: want 2, got %d", len(got))
	}
}

func TestFilterByRequirements_MinVRAM(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
		makeOffer(1, 80, 3.0, "spot", "A100"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{MinVRAM: 40})
	if len(got) != 1 || got[0].VRAM != 80 {
		t.Errorf("MinVRAM filter: want 1 offer with 80GB, got %v", got)
	}
}

func TestFilterByRequirements_MaxVRAMPerGPU(t *testing.T) {
	// 2 GPUs with 96GB total = 48GB per GPU
	offers := []provider.Offer{
		makeOffer(2, 96, 4.0, "spot", "A100-48GB"),
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
	}
	// MaxVRAM=30 means per-GPU VRAM must be ≤30GB
	got := filterByRequirements(offers, provider.GPURequirements{MaxVRAM: 30})
	if len(got) != 1 || got[0].GPUCount != 1 {
		t.Errorf("MaxVRAM per-GPU filter: want single-GPU offer, got %v", got)
	}
}

func TestFilterByRequirements_MaxPrice(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 0.5, "spot", "RTX3090"),
		makeOffer(1, 80, 4.0, "spot", "A100"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{MaxPricePerHour: 1.0})
	if len(got) != 1 || got[0].PricePerHour != 0.5 {
		t.Errorf("MaxPrice filter: want 1 cheap offer, got %v", got)
	}
}

func TestFilterByRequirements_CapacityType(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
		makeOffer(1, 24, 2.0, "on-demand", "RTX4090"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{CapacityType: "on-demand"})
	if len(got) != 1 || got[0].CapacityType != "on-demand" {
		t.Errorf("CapacityType filter: want on-demand offer only, got %v", got)
	}
}

func TestFilterByRequirements_MultiGPUFalse(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
		makeOffer(4, 96, 5.0, "spot", "A100"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{MultiGpu: false})
	if len(got) != 1 || got[0].GPUCount != 1 {
		t.Errorf("MultiGpu=false: want single-GPU offer only, got %v", got)
	}
}

func TestFilterByRequirements_MultiGPUTrue(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
		makeOffer(4, 96, 5.0, "spot", "A100"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{MultiGpu: true})
	if len(got) != 2 {
		t.Errorf("MultiGpu=true: want all offers, got %d", len(got))
	}
}

func TestFilterByRequirements_MinGPUCount(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 1.0, "spot", "RTX4090"),
		makeOffer(4, 96, 5.0, "spot", "A100"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{GPUCount: 4, MultiGpu: true})
	if len(got) != 1 || got[0].GPUCount != 4 {
		t.Errorf("GPUCount filter: want 4-GPU offer, got %v", got)
	}
}

func TestFilterByRequirements_MinDisk(t *testing.T) {
	offers := []provider.Offer{
		{OfferID: "small-disk", GPUCount: 1, VRAM: 24, DiskGB: 50, ProviderName: "test"},
		{OfferID: "big-disk", GPUCount: 1, VRAM: 24, DiskGB: 200, ProviderName: "test"},
	}
	got := filterByRequirements(offers, provider.GPURequirements{MinDisk: 100})
	if len(got) != 1 || got[0].OfferID != "big-disk" {
		t.Errorf("MinDisk filter: want big-disk, got %v", got)
	}
}

func TestFilterByRequirements_MinRAM(t *testing.T) {
	offers := []provider.Offer{
		{OfferID: "low-ram", GPUCount: 1, VRAM: 24, RAMGB: 32, ProviderName: "test"},
		{OfferID: "high-ram", GPUCount: 1, VRAM: 24, RAMGB: 128, ProviderName: "test"},
	}
	got := filterByRequirements(offers, provider.GPURequirements{MinRAM: 64})
	if len(got) != 1 || got[0].OfferID != "high-ram" {
		t.Errorf("MinRAM filter: want high-ram, got %v", got)
	}
}

func TestFilterByRequirements_EmptyOffers(t *testing.T) {
	got := filterByRequirements(nil, provider.GPURequirements{MinVRAM: 24})
	if len(got) != 0 {
		t.Errorf("empty input: want 0, got %d", len(got))
	}
}

func TestFilterByRequirements_AllFiltersExclude(t *testing.T) {
	offers := []provider.Offer{
		makeOffer(1, 24, 10.0, "spot", "RTX3080"),
	}
	got := filterByRequirements(offers, provider.GPURequirements{
		MinVRAM:         40,
		MaxPricePerHour: 2.0,
	})
	if len(got) != 0 {
		t.Errorf("all filters exclude: want 0, got %d", len(got))
	}
}

func TestIsPreferredGPU(t *testing.T) {
	if isPreferredGPU("RTX4090", nil) {
		t.Error("empty preferred list should return false")
	}
	if !isPreferredGPU("NVIDIA A100 SXM4", []string{"a100"}) {
		t.Error("case-insensitive substring match should return true")
	}
	if isPreferredGPU("RTX3080", []string{"a100", "h100"}) {
		t.Error("non-matching GPU should return false")
	}
}

func TestMatchesAnyGPUType(t *testing.T) {
	cases := []struct {
		gpuType string
		wanted  []string
		want    bool
	}{
		{"NVIDIA A100 SXM4 80GB", []string{"a100"}, true},
		{"rtx 4090", []string{"RTX4090", "RTX 4090"}, true},
		{"H100 NVL", []string{"h200", "a100"}, false},
		{"Tesla V100-SXM2-16GB", []string{"v100"}, true},
		{"RTX3090", []string{}, false},
	}
	for _, tc := range cases {
		got := matchesAnyGPUType(tc.gpuType, tc.wanted)
		if got != tc.want {
			t.Errorf("matchesAnyGPUType(%q, %v) = %v, want %v", tc.gpuType, tc.wanted, got, tc.want)
		}
	}
}
