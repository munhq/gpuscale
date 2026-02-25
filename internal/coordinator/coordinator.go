package coordinator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/munhq/gpuscale/internal/bootstrap"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/go-logr/logr"
	"golang.org/x/time/rate"
)

// ProvisionResult is returned by ProvisionInstance on success.
type ProvisionResult struct {
	Instance *provider.Instance
	Offer    provider.Offer
	Attempts int
}

// Options configures the coordinator.
type Options struct {
	CacheTTL         time.Duration         // default 7s
	BlacklistTTL     time.Duration         // default 60s
	MaxAttempts      int                   // default 5
	OffersPerAttempt int                   // default 3 (top N offers to try per cycle)
	ProviderRates    map[string]rate.Limit // per-provider req/sec, default 1/s
}

func (o Options) withDefaults() Options {
	if o.CacheTTL == 0 {
		o.CacheTTL = 7 * time.Second
	}
	if o.BlacklistTTL == 0 {
		o.BlacklistTTL = 60 * time.Second
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = 5
	}
	if o.OffersPerAttempt == 0 {
		o.OffersPerAttempt = 3
	}
	return o
}

// Coordinator centralizes all provider search+create interactions.
// It provides offer caching, blacklisting of failed offers, and
// per-provider rate limiting. All methods are safe for concurrent use.
type Coordinator struct {
	registry  *provider.Registry
	cache     *OfferCache
	blacklist *OfferBlacklist
	limiters  sync.Map // map[string]*rate.Limiter
	opts      Options
	log       logr.Logger
}

// NewCoordinator creates a coordinator with the given options.
func NewCoordinator(registry *provider.Registry, log logr.Logger, opts Options) *Coordinator {
	opts = opts.withDefaults()
	return &Coordinator{
		registry:  registry,
		cache:     NewOfferCache(opts.CacheTTL),
		blacklist: NewOfferBlacklist(opts.BlacklistTTL),
		opts:      opts,
		log:       log,
	}
}

// getLimiter returns or creates a rate limiter for a provider.
func (c *Coordinator) getLimiter(providerName string) *rate.Limiter {
	if v, ok := c.limiters.Load(providerName); ok {
		return v.(*rate.Limiter)
	}
	limit := rate.Limit(1) // default 1 req/sec
	if c.opts.ProviderRates != nil {
		if r, ok := c.opts.ProviderRates[providerName]; ok {
			limit = r
		}
	}
	limiter := rate.NewLimiter(limit, int(limit)+1) // burst = rate + 1
	actual, _ := c.limiters.LoadOrStore(providerName, limiter)
	return actual.(*rate.Limiter)
}

// ProvisionInstance finds an offer and creates an instance.
// It replaces both Selector.SelectBestOffer and the handlePending search+create loop.
//
// Flow:
//  1. Gather offers from all providers (cached, with singleflight dedup)
//  2. Filter by requirements and blacklist
//  3. Sort by price (primary), reliability (secondary)
//  4. Try ALL filtered offers in order: rate-limit-aware CreateInstance call
//  5. On expired/conflict/no-capacity: blacklist offer, try next
//  6. On rate limit: back off, invalidate cache, retry outer loop
//  7. If all offers exhausted: invalidate cache, retry outer loop (fresh offers)
//  8. On success: return instance + offer used
func (c *Coordinator) ProvisionInstance(
	ctx context.Context,
	req provider.GPURequirements,
	config provider.BootstrapConfig,
	providerNames []string,
) (*ProvisionResult, error) {
	var lastErr error

	for attempt := 1; attempt <= c.opts.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// 1. Gather offers from all eligible providers (cached + singleflight).
		allOffers, searchErrors := c.gatherOffers(ctx, req, providerNames)

		// 2. Filter by requirements + blacklist.
		filtered := filterByRequirements(allOffers, req)
		filtered = c.blacklist.Filter(filtered)

		if len(filtered) == 0 {
			lastErr = provider.ErrNoOffersAvailable
			if len(searchErrors) > 0 {
				lastErr = fmt.Errorf("%w (search errors: %d providers failed)", lastErr, len(searchErrors))
			}
			c.log.Info("No offers available after filtering",
				"attempt", attempt,
				"totalOffers", len(allOffers),
				"afterFilter", len(filtered),
				"blacklisted", c.blacklist.Size(),
			)
			if attempt < c.opts.MaxAttempts {
				c.invalidateProviders(providerNames)
				c.sleep(ctx, attempt)
				continue
			}
			return nil, lastErr
		}

		// 3. Sort: preferred GPUs first, then fewest GPUs (avoid over-provisioning),
		//    then by price (ascending), reliability (descending).
		sort.Slice(filtered, func(i, j int) bool {
			iPref := isPreferredGPU(filtered[i].GPUType, req.GPUTypes)
			jPref := isPreferredGPU(filtered[j].GPUType, req.GPUTypes)
			if iPref != jPref {
				return iPref // preferred GPUs sort first
			}
			if filtered[i].GPUCount != filtered[j].GPUCount {
				return filtered[i].GPUCount < filtered[j].GPUCount // fewer GPUs first
			}
			if filtered[i].PricePerHour != filtered[j].PricePerHour {
				return filtered[i].PricePerHour < filtered[j].PricePerHour
			}
			return filtered[i].Reliability > filtered[j].Reliability
		})

		// 4. Try ALL filtered offers in sorted order.
		// OffersPerAttempt is intentionally not applied here — we exhaust every
		// available option (A100 FIN-01, A100 FIN-02, ..., RTX PRO FIN-01, ...)
		// before giving up and re-fetching. Blacklisting handles skipping offers
		// that are known to be out of capacity.
		rateLimited := false
		for i := 0; i < len(filtered); i++ {
			offer := filtered[i]

			// Rate limit before CreateInstance.
			limiter := c.getLimiter(offer.ProviderName)
			if err := limiter.Wait(ctx); err != nil {
				return nil, ctx.Err()
			}

			prov, ok := c.registry.Get(offer.ProviderName)
			if !ok {
				c.log.Error(nil, "Provider not found in registry", "provider", offer.ProviderName)
				continue
			}

			c.log.Info("Attempting CreateInstance",
				"attempt", attempt,
				"offerIndex", i,
				"provider", offer.ProviderName,
				"offerID", offer.OfferID,
				"gpu", offer.GPUType,
				"gpuCount", offer.GPUCount,
				"price", offer.PricePerHour,
			)

			// Set provider/GPU info on config so bootstrap scripts have correct labels.
			offerConfig := config
			offerConfig.ProviderName = offer.ProviderName
			offerConfig.GPUType = offer.GPUType

			// Generate bootstrap script per-offer, since scripts embed ProviderName and GPUType.
			if offerConfig.NodeType == "full-node" && offerConfig.NetbirdKey != "" {
				offerConfig.OnStartScript = bootstrap.GenerateScript(offerConfig)
				offerConfig.OnStartEnv = bootstrap.GenerateEnvVars(offerConfig)
			} else if offerConfig.NodeType == "ray-worker" && offerConfig.RayHeadAddr != "" {
				offerConfig.OnStartScript = bootstrap.GenerateRayWorkerScript(offerConfig)
				offerConfig.OnStartEnv = bootstrap.GenerateEnvVars(offerConfig)
			}

			instance, err := prov.CreateInstance(ctx, offer, offerConfig)
			if err == nil {
				c.log.Info("Instance created",
					"attempt", attempt,
					"provider", instance.ProviderName,
					"instanceID", instance.InstanceID,
					"gpu", instance.GPUType,
				)
				return &ProvisionResult{
					Instance: instance,
					Offer:    offer,
					Attempts: attempt,
				}, nil
			}

			lastErr = err
			category := ClassifyError(err)
			switch category {
			case ErrorExpired, ErrorConflict:
				c.blacklist.Add(offer.ProviderName, offer.OfferID)
				c.log.Info("Offer blacklisted",
					"provider", offer.ProviderName,
					"offerID", offer.OfferID,
					"reason", category,
					"error", err.Error(),
				)
			case ErrorRateLimited:
				c.log.Info("Rate limited by provider, backing off",
					"provider", offer.ProviderName,
					"attempt", attempt,
				)
				rateLimited = true
			case ErrorPermanent:
				c.log.Error(err, "Permanent provider error, aborting",
					"provider", offer.ProviderName,
					"offerID", offer.OfferID,
				)
				return nil, err
			default: // ErrorTransient
				c.log.Info("Transient error, trying next offer",
					"provider", offer.ProviderName,
					"offerID", offer.OfferID,
					"error", err.Error(),
				)
			}

			if rateLimited {
				break
			}
		}

		// All offers in this attempt failed. Invalidate cache and retry.
		c.invalidateProviders(providerNames)
		if attempt < c.opts.MaxAttempts {
			c.sleep(ctx, attempt)
		}
	}

	return nil, fmt.Errorf("provision failed after %d attempts: %w", c.opts.MaxAttempts, lastErr)
}

// gatherOffers collects offers from all specified providers.
// Uses the cache for each provider independently.
func (c *Coordinator) gatherOffers(
	ctx context.Context,
	req provider.GPURequirements,
	providerNames []string,
) ([]provider.Offer, []error) {
	type result struct {
		offers []provider.Offer
		err    error
	}

	providers := c.resolveProviders(providerNames)
	results := make([]result, len(providers))
	var wg sync.WaitGroup

	for i, prov := range providers {
		wg.Add(1)
		go func(idx int, p provider.Provider) {
			defer wg.Done()
			limiter := c.getLimiter(p.Name())
			offers, err := c.cache.GetOffers(ctx, p, req, limiter)
			results[idx] = result{offers: offers, err: err}
		}(i, prov)
	}
	wg.Wait()

	var allOffers []provider.Offer
	var errs []error
	for _, r := range results {
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		allOffers = append(allOffers, r.offers...)
	}
	return allOffers, errs
}

// resolveProviders returns providers matching the given names, or all if empty.
func (c *Coordinator) resolveProviders(names []string) []provider.Provider {
	all := c.registry.List()
	if len(names) == 0 {
		return all
	}
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}
	var result []provider.Provider
	for _, p := range all {
		if nameSet[p.Name()] {
			result = append(result, p)
		}
	}
	return result
}

// invalidateProviders removes cached entries for the specified providers.
func (c *Coordinator) invalidateProviders(names []string) {
	for _, name := range names {
		c.cache.Invalidate(name)
	}
}

// sleep waits with exponential backoff, respecting context cancellation.
func (c *Coordinator) sleep(ctx context.Context, attempt int) {
	d := time.Duration(attempt) * 2 * time.Second
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// BlacklistOffer blacklists a specific offer so it won't be selected again.
// Uses a 30-minute TTL since post-creation failures (CDI errors, broken GPU
// drivers) indicate persistent host problems that won't self-heal quickly.
func (c *Coordinator) BlacklistOffer(providerName, offerID string) {
	if providerName == "" || offerID == "" {
		return
	}
	c.blacklist.AddWithTTL(providerName, offerID, 30*time.Minute)
	c.log.Info("Offer blacklisted (post-creation failure)",
		"provider", providerName,
		"offerID", offerID,
		"ttl", "30m",
	)
}

// BlacklistSize returns the number of currently blacklisted offers.
func (c *Coordinator) BlacklistSize() int {
	return c.blacklist.Size()
}
