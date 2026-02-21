package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProvisionTrigger subscribes to the gpuscale:provision pub/sub channel
// and creates GPUNodeClaims instantly when a cold-start request arrives.
// It also runs periodic queue reconciliation (every 60s) as a safety net
// to catch orphaned requests from controller restarts or broken autoscalers.
type ProvisionTrigger struct {
	client.Client
	Log         logr.Logger
	DemandStore *DemandStore
}

// NewProvisionTrigger creates a new provision trigger controller.
func NewProvisionTrigger(c client.Client, log logr.Logger, ds *DemandStore) *ProvisionTrigger {
	return &ProvisionTrigger{
		Client:      c,
		Log:         log,
		DemandStore: ds,
	}
}

// Start begins listening for provision trigger events.
func (r *ProvisionTrigger) Start(ctx context.Context) error {
	log := r.Log
	log.Info("Starting provision trigger subscriber")

	// Reconcile on startup: check Redis queues for requests that were
	// orphaned by a controller restart (pub/sub is fire-and-forget).
	time.Sleep(5 * time.Second) // let claims populate first
	r.reconcileQueues(ctx)

	ch := r.DemandStore.SubscribeProvisionTrigger(ctx)

	// Debounce: track recently handled models to avoid duplicate claims
	// from rapid-fire requests for the same model.
	recentlyHandled := make(map[string]time.Time)

	// Safety net: periodic queue reconciliation catches orphaned requests
	// from controller restarts, lost pub/sub messages, or broken Ray autoscaler.
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Provision trigger shutting down")
			return nil
		case <-ticker.C:
			r.reconcileQueues(ctx)
		case model, ok := <-ch:
			if !ok {
				log.Info("Provision trigger channel closed")
				return nil
			}

			// Debounce: skip if we handled this model in the last 10s
			if last, ok := recentlyHandled[model]; ok && time.Since(last) < 10*time.Second {
				log.V(1).Info("Skipping duplicate provision trigger (debounce)", "model", model)
				continue
			}

			log.Info("Received provision trigger", "model", model)
			if err := r.handleTrigger(ctx, model); err != nil {
				log.Error(err, "Failed to handle provision trigger", "model", model)
			} else {
				recentlyHandled[model] = time.Now()
			}

			// Clean old entries from debounce map
			for m, t := range recentlyHandled {
				if time.Since(t) > 30*time.Second {
					delete(recentlyHandled, m)
				}
			}
		}
	}
}

func (r *ProvisionTrigger) handleTrigger(ctx context.Context, model string) error {
	log := r.Log.WithValues("model", model)

	// Check if model is already loaded — no provisioning needed
	if r.DemandStore.IsModelLoaded(ctx, model) {
		log.Info("Model already loaded, skipping provisioning")
		return nil
	}

	// Dedup: check if a claim already exists for this model
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		return fmt.Errorf("listing claims: %w", err)
	}
	for _, c := range claims.Items {
		if c.Spec.ModelID == model &&
			c.Status.Phase != v1alpha1.ClaimPhaseTerminated &&
			c.Status.Phase != v1alpha1.ClaimPhaseDraining {
			log.Info("Claim already exists for model", "claim", c.Name, "phase", c.Status.Phase)
			return nil
		}
	}

	// Get model config from Dragonfly
	cfg, err := r.DemandStore.GetModelConfig(ctx, model)
	if err != nil {
		return fmt.Errorf("getting model config: %w", err)
	}
	if cfg == nil {
		return fmt.Errorf("model %q not found in config", model)
	}

	// Find a pool matching this model's nodeType
	var pools v1alpha1.GPUNodePoolList
	if err := r.List(ctx, &pools); err != nil {
		return fmt.Errorf("listing pools: %w", err)
	}
	if len(pools.Items) == 0 {
		return fmt.Errorf("no GPUNodePools configured")
	}
	pool := findPoolByNodeType(pools.Items, cfg.NodeType)

	// Check pool limits
	activeCount := 0
	for _, c := range claims.Items {
		if c.Spec.PoolRef == pool.Name && c.Status.Phase != v1alpha1.ClaimPhaseTerminated {
			activeCount++
		}
	}
	if activeCount >= pool.Spec.Scaling.MaxNodes {
		return fmt.Errorf("pool limit reached (%d/%d)", activeCount, pool.Spec.Scaling.MaxNodes)
	}

	// Determine node type
	nodeType := "ray-worker"
	if cfg.NodeType != "" {
		nodeType = cfg.NodeType
	} else if len(pool.Spec.Providers) > 0 && pool.Spec.Providers[0].NodeType != "" {
		nodeType = pool.Spec.Providers[0].NodeType
	}

	// Build GPU requirements from model config
	gpuCount := 1
	if cfg.VRAMRequired > 24 {
		gpuCount = (cfg.VRAMRequired + 23) / 24 // ceil(vram/24)
	}
	if cfg.MaxGPUCount > 0 && gpuCount > cfg.MaxGPUCount {
		gpuCount = cfg.MaxGPUCount
	}

	maxPrice := 0.0
	if cfg.MaxPricePerGPU > 0 {
		maxPrice = cfg.MaxPricePerGPU * float64(gpuCount)
	}

	// MaxVRAM: use model config if set, otherwise auto-compute.
	maxVRAM := cfg.MaxVRAMPerGPU
	if maxVRAM == 0 && cfg.VRAMRequired > 0 {
		perGPUNeed := (cfg.VRAMRequired + gpuCount - 1) / gpuCount
		maxVRAM = perGPUNeed * 3
		if maxVRAM > 48 {
			maxVRAM = 48
		}
	}

	// Create the claim
	claimID := uuid.New().String()[:8]
	claim := &v1alpha1.GPUNodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fmt.Sprintf("claim-%s", claimID),
			Namespace:  claimNamespace(),
			Finalizers: []string{"gpuscale.io/instance-cleanup"},
		},
		Spec: v1alpha1.GPUNodeClaimSpec{
			PoolRef:     pool.Name,
			NodeType:    nodeType,
			ModelID:     model,
			ModelSource: cfg.Source,
			Requirements: v1alpha1.ClaimRequirements{
				GPUCount: gpuCount,
				MinVRAM:  cfg.VRAMRequired,
				MaxVRAM:  maxVRAM,
				GPUTypes: cfg.PreferredGPUs,
				MaxPrice: maxPrice,
			},
		},
	}

	if err := r.Create(ctx, claim); err != nil {
		return fmt.Errorf("creating claim: %w", err)
	}

	log.Info("Created GPUNodeClaim from cold-start trigger",
		"claim", claim.Name,
		"nodeType", nodeType,
		"gpuCount", gpuCount,
		"minVRAM", cfg.VRAMRequired,
	)
	return nil
}

// reconcileQueues checks all model queues in Redis for orphaned requests
// that have no active claim. Runs on startup and every 60s as a safety net
// for controller restarts, lost pub/sub messages, or broken Ray autoscaler.
func (r *ProvisionTrigger) reconcileQueues(ctx context.Context) {
	log := r.Log
	demands, err := r.DemandStore.GetAllDemands(ctx)
	if err != nil {
		log.Error(err, "Queue reconcile: failed to get demands")
		return
	}

	for _, d := range demands {
		if d.QueueDepth == 0 && d.ActiveDemand == 0 {
			continue
		}
		log.Info("Queue reconcile: found queued demand",
			"model", d.Model,
			"queue", d.QueueDepth,
			"active", d.ActiveDemand,
		)
		if err := r.handleTrigger(ctx, d.Model); err != nil {
			log.Error(err, "Queue reconcile: failed to trigger", "model", d.Model)
		}
	}
}

// findPoolByNodeType returns the pool whose provider nodeType matches the
// requested nodeType. Falls back to the first pool if no match is found.
func findPoolByNodeType(pools []v1alpha1.GPUNodePool, nodeType string) *v1alpha1.GPUNodePool {
	if nodeType == "" {
		nodeType = "ray-worker"
	}
	for i := range pools {
		for _, p := range pools[i].Spec.Providers {
			if p.NodeType == nodeType {
				return &pools[i]
			}
		}
	}
	return &pools[0]
}

// SetupWithManager registers this controller as a Runnable.
func (r *ProvisionTrigger) SetupWithManager(mgr ctrl.Manager) error {
	return mgr.Add(r)
}
