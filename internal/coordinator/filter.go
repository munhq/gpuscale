package coordinator

import (
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// filterByRequirements returns only offers that meet the GPU requirements.
func filterByRequirements(offers []provider.Offer, req provider.GPURequirements) []provider.Offer {
	var result []provider.Offer
	for _, o := range offers {
		if req.GPUCount > 0 && o.GPUCount < req.GPUCount {
			continue
		}
		if req.MinVRAM > 0 && o.VRAM < req.MinVRAM {
			continue
		}
		if req.MaxPrice > 0 && o.PricePerHour > req.MaxPrice {
			continue
		}
		if req.CapacityType != "" && o.CapacityType != req.CapacityType {
			continue
		}
		if len(req.GPUTypes) > 0 && !matchesAnyGPUType(o.GPUType, req.GPUTypes) {
			continue
		}
		if req.MinDisk > 0 && o.DiskGB > 0 && o.DiskGB < req.MinDisk {
			continue
		}
		if req.MinRAM > 0 && o.RAMGB > 0 && o.RAMGB < req.MinRAM {
			continue
		}
		result = append(result, o)
	}
	return result
}

func matchesAnyGPUType(gpuType string, wanted []string) bool {
	gpuLower := strings.ToLower(gpuType)
	for _, w := range wanted {
		wLower := strings.ToLower(w)
		if gpuLower == wLower || strings.Contains(gpuLower, wLower) {
			return true
		}
	}
	return false
}
