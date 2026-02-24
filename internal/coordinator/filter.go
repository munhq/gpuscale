package coordinator

import (
	"strings"

	"github.com/munhq/gpuscale/pkg/provider"
)

// filterByRequirements returns only offers that meet the GPU requirements.
// o.VRAM is total VRAM across all GPUs on the instance.
// req.GPUCount is a minimum (0 = any count covering MinVRAM).
// req.MaxPricePerGPU caps $/hr per GPU (offer.PricePerHour / offer.GPUCount).
func filterByRequirements(offers []provider.Offer, req provider.GPURequirements) []provider.Offer {
	var result []provider.Offer
	for _, o := range offers {
		// Minimum GPU count — 0 means no constraint.
		if req.GPUCount > 0 && o.GPUCount < req.GPUCount {
			continue
		}
		// Total VRAM must cover the requirement.
		if req.MinVRAM > 0 && o.VRAM < req.MinVRAM {
			continue
		}
		// MaxVRAM is per GPU — prevent landing on a massively over-sized card.
		if req.MaxVRAM > 0 && o.GPUCount > 0 && o.VRAM/o.GPUCount > req.MaxVRAM {
			continue
		}
		// Per-GPU price cap.
		if req.MaxPricePerGPU > 0 && o.GPUCount > 0 && o.PricePerHour/float64(o.GPUCount) > req.MaxPricePerGPU {
			continue
		}
		if req.CapacityType != "" && o.CapacityType != req.CapacityType {
			continue
		}
		// Enforce single-GPU constraint when MultiGpu is disabled.
		if !req.MultiGpu && o.GPUCount > 1 {
			continue
		}
		// GPUTypes is a soft preference (sort order), not a hard filter.
		// MinVRAM alone drives offer selection when types are not specified.
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
