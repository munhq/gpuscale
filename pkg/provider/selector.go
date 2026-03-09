package provider

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
)

// Selector searches across providers and selects the best offer.
type Selector struct {
	registry *Registry
	log      *slog.Logger
}

// NewSelector creates a new offer selector. If log is nil, uses slog.Default().
func NewSelector(registry *Registry, log *slog.Logger) *Selector {
	if log == nil {
		log = slog.Default()
	}
	return &Selector{
		registry: registry,
		log:      log,
	}
}

// SelectBestOffer queries all providers in parallel and returns the best offer.
func (s *Selector) SelectBestOffer(ctx context.Context, req GPURequirements, preferredProviders []string) (*Offer, error) {
	offers, err := s.SelectTopOffers(ctx, req, preferredProviders, 1)
	if err != nil {
		return nil, err
	}
	if len(offers) == 0 {
		return nil, ErrNoOffersAvailable
	}
	return &offers[0], nil
}

// SelectTopOffers queries all providers in parallel and returns the top N offers sorted by price.
// This is useful for spot market provisioning where offers can become stale quickly.
func (s *Selector) SelectTopOffers(ctx context.Context, req GPURequirements, preferredProviders []string, limit int) ([]Offer, error) {
	providers := s.registry.List()
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers registered")
	}

	// If preferred providers specified, filter to those first
	if len(preferredProviders) > 0 {
		filtered := make([]Provider, 0)
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
		allOffers []Offer
		wg        sync.WaitGroup
	)

	for _, p := range providers {
		wg.Add(1)
		go func(prov Provider) {
			defer wg.Done()
			offers, err := prov.SearchOffers(ctx, req)
			if err != nil {
				s.log.Error("failed to search offers", "provider", prov.Name(), "error", err)
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
		return nil, ErrNoOffersAvailable
	}

	// Sort by price (primary), reliability (secondary)
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].PricePerHour != filtered[j].PricePerHour {
			return filtered[i].PricePerHour < filtered[j].PricePerHour
		}
		return filtered[i].Reliability > filtered[j].Reliability
	})

	// Return top N offers
	if limit > 0 && limit < len(filtered) {
		filtered = filtered[:limit]
	}

	if len(filtered) > 0 {
		s.log.Info("selected top offers",
			"count", len(filtered),
			"best_provider", filtered[0].ProviderName,
			"best_gpu", filtered[0].GPUType,
			"best_price", filtered[0].PricePerHour,
		)
	}

	return filtered, nil
}

// FilterByRequirements is the exported version of filterByRequirements.
// Use this in any path that must enforce all hardware constraints precisely
// (provisioning). Browse paths may apply a subset of filters instead.
func FilterByRequirements(offers []Offer, req GPURequirements) []Offer {
	return filterByRequirements(offers, req)
}

func filterByRequirements(offers []Offer, req GPURequirements) []Offer {
	var result []Offer
	for _, o := range offers {
		if req.GPUCount > 0 && o.GPUCount < req.GPUCount {
			continue
		}
		if req.MinVRAM > 0 && o.VRAM < req.MinVRAM {
			continue
		}
		if req.MaxPricePerHour > 0 && o.PricePerHour > req.MaxPricePerHour {
			continue
		}
		if !req.MultiGpu && o.GPUCount > 1 {
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
