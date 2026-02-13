package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/go-logr/logr"
)

// Selector searches across providers and selects the best offer.
type Selector struct {
	registry *provider.Registry
	log      logr.Logger
}

// NewSelector creates a new offer selector.
func NewSelector(registry *provider.Registry, log logr.Logger) *Selector {
	return &Selector{
		registry: registry,
		log:      log,
	}
}

// SelectBestOffer queries all providers in parallel and returns the best offer.
func (s *Selector) SelectBestOffer(ctx context.Context, req provider.GPURequirements, preferredProviders []string) (*provider.Offer, error) {
	providers := s.registry.List()
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers registered")
	}

	// If preferred providers specified, filter to those first
	if len(preferredProviders) > 0 {
		filtered := make([]provider.Provider, 0)
		for _, p := range providers {
			for _, name := range preferredProviders {
				if p.Name() == name {
					filtered = append(filtered, p)
					break
				}
			}
		}
		if len(filtered) > 0 {
			providers = filtered
		}
	}

	// Query all providers in parallel
	var (
		mu        sync.Mutex
		allOffers []provider.Offer
		wg        sync.WaitGroup
	)

	for _, p := range providers {
		wg.Add(1)
		go func(prov provider.Provider) {
			defer wg.Done()
			offers, err := prov.SearchOffers(ctx, req)
			if err != nil {
				s.log.Error(err, "Failed to search offers", "provider", prov.Name())
				return
			}
			mu.Lock()
			allOffers = append(allOffers, offers...)
			mu.Unlock()
		}(p)
	}
	wg.Wait()

	// Filter offers that meet requirements
	filtered := filterByRequirements(allOffers, req)

	if len(filtered) == 0 {
		return nil, provider.ErrNoOffersAvailable
	}

	// Sort by price (primary), reliability (secondary)
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].PricePerHour != filtered[j].PricePerHour {
			return filtered[i].PricePerHour < filtered[j].PricePerHour
		}
		return filtered[i].Reliability > filtered[j].Reliability
	})

	best := filtered[0]
	s.log.Info("Selected best offer",
		"provider", best.ProviderName,
		"gpu", best.GPUType,
		"count", best.GPUCount,
		"vram", best.VRAM,
		"price", best.PricePerHour,
		"capacity", best.CapacityType,
	)
	return &best, nil
}

func filterByRequirements(offers []provider.Offer, req provider.GPURequirements) []provider.Offer {
	var result []provider.Offer
	for _, o := range offers {
		// GPU count
		if req.GPUCount > 0 && o.GPUCount < req.GPUCount {
			continue
		}
		// VRAM
		if req.MinVRAM > 0 && o.VRAM < req.MinVRAM {
			continue
		}
		// Price
		if req.MaxPrice > 0 && o.PricePerHour > req.MaxPrice {
			continue
		}
		// Capacity type
		if req.CapacityType != "" && o.CapacityType != req.CapacityType {
			continue
		}
		// GPU type
		if len(req.GPUTypes) > 0 && !matchesAnyGPUType(o.GPUType, req.GPUTypes) {
			continue
		}
		// Disk
		if req.MinDisk > 0 && o.DiskGB > 0 && o.DiskGB < req.MinDisk {
			continue
		}
		// RAM
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
