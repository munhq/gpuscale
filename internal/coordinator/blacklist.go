package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// OfferBlacklist tracks recently-failed offer IDs with automatic TTL expiry.
// In-memory only — ephemeral per-process state.
type OfferBlacklist struct {
	mu      sync.RWMutex
	entries map[string]time.Time // "provider:offerID" → expiry timestamp
	ttl     time.Duration
}

// NewOfferBlacklist creates a blacklist with the given TTL.
func NewOfferBlacklist(ttl time.Duration) *OfferBlacklist {
	return &OfferBlacklist{
		entries: make(map[string]time.Time),
		ttl:     ttl,
	}
}

func blacklistKey(providerName, offerID string) string {
	return fmt.Sprintf("%s:%s", providerName, offerID)
}

// Add blacklists an offer. It will be excluded from results until TTL expires.
func (b *OfferBlacklist) Add(providerName, offerID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries[blacklistKey(providerName, offerID)] = time.Now().Add(b.ttl)
}

// AddWithTTL blacklists an offer with a custom TTL. Use for post-creation
// failures (CDI errors, etc.) that indicate persistent host problems.
func (b *OfferBlacklist) AddWithTTL(providerName, offerID string, ttl time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries[blacklistKey(providerName, offerID)] = time.Now().Add(ttl)
}

// Filter returns offers that are not currently blacklisted.
// Performs lazy cleanup of expired entries.
func (b *OfferBlacklist) Filter(offers []provider.Offer) []provider.Offer {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	// Lazy cleanup of expired entries.
	for k, expiry := range b.entries {
		if now.After(expiry) {
			delete(b.entries, k)
		}
	}

	result := make([]provider.Offer, 0, len(offers))
	for _, o := range offers {
		key := blacklistKey(o.ProviderName, o.OfferID)
		if expiry, blocked := b.entries[key]; blocked && now.Before(expiry) {
			continue
		}
		result = append(result, o)
	}
	return result
}

// Size returns the number of currently blacklisted (non-expired) offers.
func (b *OfferBlacklist) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	now := time.Now()
	count := 0
	for _, expiry := range b.entries {
		if now.Before(expiry) {
			count++
		}
	}
	return count
}
