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
		if req.MaxVRAM > 0 && o.VRAM > req.MaxVRAM {
			continue
		}
		if req.MaxPrice > 0 && o.PricePerHour > req.MaxPrice {
			continue
		}
		if req.CapacityType != "" && o.CapacityType != req.CapacityType {
			continue
		}
		// GPUTypes is a soft preference (sort order), not a hard filter.
		// MaxVRAM is the hard upper bound. This allows fallback to any GPU
		// when preferred types aren't available.
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

// isPreferredGPU returns true if the offer GPU matches any preferred type.
func isPreferredGPU(gpuType string, preferred []string) bool {
	if len(preferred) == 0 {
		return false
	}
	return matchesAnyGPUType(gpuType, preferred)
}

func matchesAnyGPUType(gpuType string, wanted []string) bool {
	gpuLower := strings.ToLower(gpuType)
	for _, w := range wanted {
		wLower := strings.ToLower(w)
		// Bidirectional: "V100" matches "Tesla V100-SXM2-16GB" because the
		// wanted string contains the short name. Also handles the reverse
		// (full provider name contains the wanted substring).
		if gpuLower == wLower ||
			strings.Contains(gpuLower, wLower) ||
			strings.Contains(wLower, gpuLower) {
			return true
		}
	}
	return false
}
