package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// consolidationCandidate holds a Ready single-model claim and its resolved model config.
// Used by checkConsolidation to identify bin-packing opportunities.
type consolidationCandidate struct {
	claim *v1alpha1.GPUNodeClaim
	cfg   *ModelConfig
}

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

	// Dedup: a model is covered if it appears as primary OR co-located in any active claim.
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		return fmt.Errorf("listing claims: %w", err)
	}
	for i := range claims.Items {
		c := &claims.Items[i]
		if c.Status.Phase == v1alpha1.ClaimPhaseTerminated || c.Status.Phase == v1alpha1.ClaimPhaseDraining {
			continue
		}
		if c.Spec.ModelID != model && !slices.Contains(c.Spec.ModelIDs, model) {
			continue
		}
		log.Info("Model already covered by existing claim", "claim", c.Name, "phase", c.Status.Phase)
		return nil
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
	pool := findPool(pools.Items, cfg.Pool, cfg.NodeType)

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

	// Aggregate co-located models into a single claim via bin-packing.
	modelIDs, totalVRAM, maxDisk, gpuTypes := r.aggregateModelsForClaim(ctx, model, cfg, claims.Items, pool)

	// MaxVRAM: use explicit per-model config if set, otherwise no upper bound.
	// For multi-model claims: always 0 — MinVRAM = sum of all models drives selection.
	maxVRAM := 0
	if len(modelIDs) == 1 {
		maxVRAM = cfg.MaxVRAMPerGPU // 0 if not explicitly set = no upper bound
	}

	// Create the claim. GPUCount=0: the coordinator picks any instance whose
	// total VRAM covers MinVRAM — no hardcoded GPU count assumptions.
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
			ModelIDs:    modelIDs,
			ModelSource: cfg.Source,
			Requirements: v1alpha1.ClaimRequirements{
				MinVRAM:        totalVRAM,
				MaxVRAM:        maxVRAM,
				GPUTypes:       gpuTypes,
				MaxPricePerGPU: cfg.MaxPricePerGPU,
				MinDisk:        maxDisk,
				MultiGpu:       cfg.MultiGpu,
			},
		},
	}

	if err := r.Create(ctx, claim); err != nil {
		return fmt.Errorf("creating claim: %w", err)
	}

	log.Info("Created GPUNodeClaim from cold-start trigger",
		"claim", claim.Name,
		"nodeType", nodeType,
		"models", modelIDs,
		"totalVRAM", totalVRAM,
	)
	return nil
}

// maxBundleVRAMGB is the VRAM ceiling for a single bin-packed claim.
// Set to 96 GB to target RTX Pro 6000 (96 GB) as the largest practical single-GPU node.
// Models whose combined VRAM would exceed this threshold each get their own claim.
const maxBundleVRAMGB = 96

// aggregateModelsForClaim builds the co-located model list for a new claim
// starting from primaryModel. It scans all demands and bundles models that:
//   - share the same nodeType as the primary model
//   - have pending demand (queue or active)
//   - are not already covered by an active (non-Terminated) claim
//   - fit within maxBundleVRAMGB when added to the running total
//
// Returns (modelIDs, totalVRAM, maxDisk, gpuTypes).
// gpuTypes is cleared for multi-model bundles (conflicting preferences would cause
// failed offer searches); MinVRAM alone drives GPU selection.
func (r *ProvisionTrigger) aggregateModelsForClaim(
	ctx context.Context,
	primaryModel string,
	primaryCfg *ModelConfig,
	existingClaims []v1alpha1.GPUNodeClaim,
	pool *v1alpha1.GPUNodePool,
) (modelIDs []string, totalVRAM int, maxDisk int, gpuTypes []string) {
	log := r.Log

	// Start with the primary model.
	modelIDs = []string{primaryModel}
	totalVRAM = primaryCfg.VRAMRequired
	gpuTypes = primaryCfg.PreferredGPUs

	// Fetch all model demands.
	demands, err := r.DemandStore.GetAllDemands(ctx)
	if err != nil {
		log.Error(err, "Failed to fetch all demands for bin-packing; using primary model only")
		return
	}

	for _, d := range demands {
		if d.Model == primaryModel {
			continue // already included
		}
		if d.QueueDepth == 0 && d.ActiveDemand == 0 {
			continue // no demand for this model
		}

		// Fetch the full config to check nodeType and VRAM.
		otherCfg, err := r.DemandStore.GetModelConfig(ctx, d.Model)
		if err != nil || otherCfg == nil {
			continue
		}

		// Only bundle models that target the same node type.
		if otherCfg.NodeType != primaryCfg.NodeType {
			continue
		}

		// Guard: adding this model must not exceed the VRAM ceiling.
		// If it would, the combined node would be impossible to provision.
		if totalVRAM+otherCfg.VRAMRequired > maxBundleVRAMGB {
			log.Info("Skipping model for bin-pack (would exceed VRAM cap)",
				"model", d.Model,
				"currentTotal", totalVRAM,
				"modelVRAM", otherCfg.VRAMRequired,
				"cap", maxBundleVRAMGB)
			continue
		}

		// Skip if this model already has an active claim.
		alreadyCovered := false
		for _, c := range existingClaims {
			if c.Status.Phase == v1alpha1.ClaimPhaseTerminated ||
				c.Status.Phase == v1alpha1.ClaimPhaseDraining ||
				c.Status.Phase == v1alpha1.ClaimPhaseHibernated {
				continue
			}
			if c.Spec.ModelID == d.Model || slices.Contains(c.Spec.ModelIDs, d.Model) {
				alreadyCovered = true
				break
			}
		}
		if alreadyCovered {
			continue
		}

		// Bundle the model into this claim.
		modelIDs = append(modelIDs, d.Model)
		totalVRAM += otherCfg.VRAMRequired
	}

	// Disk = sum of all model weights + single overhead for OS/tools/buffer.
	// Each model needs ~vramRequired GB on disk (HF safetensors ≈ VRAM size).
	// Overhead (50GB) is added once regardless of model count.
	const diskOverheadGB = 50
	maxDisk = totalVRAM + diskOverheadGB

	// For multi-model bin-packed claims: clear gpuTypes.
	// Different models have conflicting preferred GPUs (e.g., V100 vs A100).
	// MinVRAM alone is the correct selection criterion for a shared node.
	if len(modelIDs) > 1 {
		gpuTypes = nil
		log.Info("Bundling models into shared claim (gpuTypes cleared — MinVRAM drives selection)",
			"models", modelIDs, "totalVRAM", totalVRAM)
	}

	return
}

// checkConsolidation looks for Ready single-model claims that could be bin-packed
// onto fewer nodes. When an opportunity is found, it creates a new bin-packed claim.
// Once the bin-packed claim is Ready, the ClaimReconciler annotates the old single-model
// claims with annotationConsolidationDrain, and DisruptionController drains them.
//
// Only one consolidation pair is processed per tick to avoid bursting.
func (r *ProvisionTrigger) checkConsolidation(ctx context.Context, existingClaims []v1alpha1.GPUNodeClaim) {
	log := r.Log

	// Collect all Ready single-model claims grouped by pool/nodeType.
	groups := make(map[string][]consolidationCandidate)
	for i := range existingClaims {
		c := &existingClaims[i]
		if c.Status.Phase != v1alpha1.ClaimPhaseReady {
			continue
		}
		models := claimModelIDs(c)
		if len(models) != 1 {
			continue // skip already-bin-packed or zero-model claims
		}
		cfg, err := r.DemandStore.GetModelConfig(ctx, models[0])
		if err != nil || cfg == nil {
			continue
		}
		key := fmt.Sprintf("%s/%s", c.Spec.PoolRef, c.Status.NodeType)
		groups[key] = append(groups[key], consolidationCandidate{claim: c, cfg: cfg})
	}

	for _, cands := range groups {
		if len(cands) < 2 {
			continue
		}
		// Find the first pair that fits within the VRAM ceiling.
		for i := 0; i < len(cands); i++ {
			for j := i + 1; j < len(cands); j++ {
				a, b := cands[i], cands[j]
				totalVRAM := a.cfg.VRAMRequired + b.cfg.VRAMRequired
				if totalVRAM > maxBundleVRAMGB {
					continue
				}
				// Check that no bin-packed claim already covers both models.
				if r.binPackedClaimExists(existingClaims, a.claim.Spec.ModelID, b.claim.Spec.ModelID) {
					continue
				}
				log.Info("Consolidation opportunity: creating bin-packed claim",
					"model1", a.claim.Spec.ModelID, "model2", b.claim.Spec.ModelID,
					"totalVRAM", totalVRAM)
				if err := r.createBinPackedClaim(ctx, []consolidationCandidate{a, b}); err != nil {
					log.Error(err, "Consolidation: failed to create bin-packed claim")
				}
				// One consolidation per tick — avoid runaway claim creation.
				return
			}
		}
	}
}

// binPackedClaimExists returns true if any non-Terminated/non-Draining claim
// has both model A and model B in its ModelIDs list.
func (r *ProvisionTrigger) binPackedClaimExists(claims []v1alpha1.GPUNodeClaim, modelA, modelB string) bool {
	for _, c := range claims {
		if c.Status.Phase == v1alpha1.ClaimPhaseTerminated || c.Status.Phase == v1alpha1.ClaimPhaseDraining {
			continue
		}
		if len(c.Spec.ModelIDs) < 2 {
			continue
		}
		hasA := slices.Contains(c.Spec.ModelIDs, modelA)
		hasB := slices.Contains(c.Spec.ModelIDs, modelB)
		if hasA && hasB {
			return true
		}
	}
	return false
}

// createBinPackedClaim builds and creates a single GPUNodeClaim that co-locates
// the given candidates. Uses the first candidate's pool and nodeType as the basis.
func (r *ProvisionTrigger) createBinPackedClaim(ctx context.Context, candidates []consolidationCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	primary := candidates[0]

	// Collect all model IDs and aggregate requirements.
	modelIDs := make([]string, 0, len(candidates))
	totalVRAM := 0
	maxPricePerGPU := 0.0
	for _, c := range candidates {
		modelIDs = append(modelIDs, c.claim.Spec.ModelID)
		totalVRAM += c.cfg.VRAMRequired
		if c.cfg.MaxPricePerGPU > maxPricePerGPU {
			maxPricePerGPU = c.cfg.MaxPricePerGPU
		}
	}
	// Disk = sum of all model weights + overhead once.
	const diskOverheadGB = 50
	maxDisk := totalVRAM + diskOverheadGB

	// Get the pool for the primary model.
	var pools v1alpha1.GPUNodePoolList
	if err := r.List(ctx, &pools); err != nil {
		return fmt.Errorf("listing pools: %w", err)
	}
	pool := findPool(pools.Items, primary.cfg.Pool, primary.cfg.NodeType)

	nodeType := primary.cfg.NodeType
	if nodeType == "" {
		nodeType = primary.claim.Status.NodeType
	}

	claimID := uuid.New().String()[:8]
	claim := &v1alpha1.GPUNodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("claim-%s", claimID),
			Namespace: claimNamespace(),
			Finalizers: []string{"gpuscale.io/instance-cleanup"},
			Annotations: map[string]string{
				"gpuscale.io/consolidation": "true",
			},
		},
		Spec: v1alpha1.GPUNodeClaimSpec{
			PoolRef:     pool.Name,
			NodeType:    nodeType,
			ModelID:     primary.claim.Spec.ModelID,
			ModelIDs:    modelIDs,
			ModelSource: primary.cfg.Source,
			Requirements: v1alpha1.ClaimRequirements{
				MinVRAM: totalVRAM,
				// No MaxVRAM cap — MinVRAM drives selection for multi-model claims.
				// No GPUTypes — conflicting preferences across models; let MinVRAM select.
				// MaxPricePerGPU not set — consolidation targets lowest cost overall.
				MinDisk: maxDisk,
			},
		},
	}

	if err := r.Create(ctx, claim); err != nil {
		return fmt.Errorf("creating consolidation claim: %w", err)
	}
	r.Log.Info("Created consolidation bin-packed claim",
		"claim", claim.Name, "models", modelIDs, "totalVRAM", totalVRAM)
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

	// Collect all existing claims once to pass into the trigger per model.
	var claimList v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claimList, client.InNamespace(claimNamespace())); err != nil {
		log.Error(err, "Queue reconcile: failed to list claims")
		return
	}

	// Track models that have been handled in this reconcile pass (via bundling)
	// so we don't create duplicate claims for already-bundled co-located models.
	handled := make(map[string]bool)

	for _, d := range demands {
		if d.QueueDepth == 0 && d.ActiveDemand == 0 {
			continue
		}
		if handled[d.Model] {
			continue
		}

		log.Info("Queue reconcile: found queued demand",
			"model", d.Model,
			"queue", d.QueueDepth,
			"active", d.ActiveDemand,
		)
		if err := r.handleTrigger(ctx, d.Model); err != nil {
			log.Error(err, "Queue reconcile: failed to trigger", "model", d.Model)
			continue
		}

		// Mark all models that would have been bundled with this trigger as handled.
		// We re-check existing claims to pick up the claim just created.
		var updatedClaims v1alpha1.GPUNodeClaimList
		if listErr := r.List(ctx, &updatedClaims, client.InNamespace(claimNamespace())); listErr == nil {
			for _, c := range updatedClaims.Items {
				if c.Status.Phase == v1alpha1.ClaimPhaseTerminated || c.Status.Phase == v1alpha1.ClaimPhaseDraining {
					continue
				}
				// Any model covered by this (newly created) claim is handled.
				handled[c.Spec.ModelID] = true
				for _, mid := range c.Spec.ModelIDs {
					handled[mid] = true
				}
			}
		}
	}

	// After handling demand, check if any Ready single-model claims can be consolidated.
	// This runs on the same 60s tick — one consolidation pair per cycle maximum.
	r.checkConsolidation(ctx, claimList.Items)
}

// findPool returns the pool to use for a given model.
// If poolName is set, it is used as an exact match — this is the expected path
// when models have an explicit "pool" field in their config.
// Falls back to nodeType matching only when poolName is empty (legacy behaviour).
// Panics-safe: returns the first pool if nothing matches.
func findPool(pools []v1alpha1.GPUNodePool, poolName, nodeType string) *v1alpha1.GPUNodePool {
	if len(pools) == 0 {
		return nil
	}
	// Explicit pool name — exact match, no guessing.
	if poolName != "" {
		for i := range pools {
			if pools[i].Name == poolName {
				return &pools[i]
			}
		}
	}
	// Fallback: match by nodeType (first match wins — ambiguous when multiple
	// pools share the same nodeType, but keeps backward compat for models
	// that don't set an explicit pool).
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
