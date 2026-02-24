package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

type cacheEntry struct {
	offers    []provider.Offer
	fetchedAt time.Time
}

// OfferCache caches SearchOffers results per (provider, requirements) tuple.
// Concurrent callers for the same key share a single in-flight search via
// singleflight, preventing thundering herd on cache miss.
type OfferCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	group   singleflight.Group
}

// NewOfferCache creates a cache with the given TTL.
func NewOfferCache(ttl time.Duration) *OfferCache {
	return &OfferCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
	}
}

// cacheKey produces a deterministic key from provider name + requirements.
func cacheKey(providerName string, req provider.GPURequirements) string {
	gpuTypes := "any"
	if len(req.GPUTypes) > 0 {
		gpuTypes = strings.Join(req.GPUTypes, ",")
	}
	return fmt.Sprintf("%s:%d:%d:%.2f:%s:%s:%d:%d",
		providerName,
		req.GPUCount,
		req.MinVRAM,
		req.MaxPricePerGPU,
		req.CapacityType,
		gpuTypes,
		req.MinDisk,
		req.MinRAM,
	)
}

// GetOffers returns cached offers or performs a fresh search.
// The rate limiter token is consumed only on cache miss.
// Concurrent callers with the same key share a single in-flight search.
func (c *OfferCache) GetOffers(
	ctx context.Context,
	prov provider.Provider,
	req provider.GPURequirements,
	limiter *rate.Limiter,
) ([]provider.Offer, error) {
	key := cacheKey(prov.Name(), req)

	// Check cache (fast path).
	c.mu.RLock()
	if entry, ok := c.entries[key]; ok && time.Since(entry.fetchedAt) < c.ttl {
		offers := entry.offers
		c.mu.RUnlock()
		return offers, nil
	}
	c.mu.RUnlock()

	// Cache miss — use singleflight to deduplicate concurrent fetches.
	val, err, _ := c.group.Do(key, func() (interface{}, error) {
		// Consume rate limiter token before calling provider.
		if err := limiter.Wait(ctx); err != nil {
			return nil, err
		}

		offers, err := prov.SearchOffers(ctx, req)
		if err != nil {
			return nil, err
		}

		// Store in cache.
		c.mu.Lock()
		c.entries[key] = &cacheEntry{
			offers:    offers,
			fetchedAt: time.Now(),
		}
		c.mu.Unlock()

		return offers, nil
	})

	if err != nil {
		return nil, err
	}

	return val.([]provider.Offer), nil
}

// Invalidate removes cached entries for a provider.
func (c *OfferCache) Invalidate(providerName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prefix := providerName + ":"
	for key := range c.entries {
		if strings.HasPrefix(key, prefix) {
			delete(c.entries, key)
		}
	}
}
