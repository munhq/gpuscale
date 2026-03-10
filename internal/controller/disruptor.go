package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// annotationConsolidationDrain is set by the reconciler when a bin-packed replacement
// claim is Ready and serving all the same models. The DisruptionController skips
// the normal cooldown and drains the claim immediately.
const annotationConsolidationDrain = "gpuscale.io/consolidation-drain"

// DisruptionController watches GPUNodeClaims and destroys idle standalone nodes after cooldown.
// Idle detection is demand-counter-based: demand counters (demand:{model} in Dragonfly DB 3)
// are maintained by GPU API's request queue, and GPU API writes gpu_api:node_idle:{node_id}
// when the node has had zero active inference streams for the configured idle timeout.
// Always-active models are never destroyed.
type DisruptionController struct {
	client.Client
	Log            logr.Logger
	Registry       *provider.Registry
	CooldownPeriod time.Duration
	WorkerStore    *WorkerStore
	DemandStore    *DemandStore // reads demand counters from Dragonfly DB 3
	// ClaimWriter writes claim lifecycle events to Postgres for history.
	ClaimWriter *ClaimWriter
}

// NewDisruptionController creates a new disruption controller.
func NewDisruptionController(c client.Client, log logr.Logger, reg *provider.Registry, cooldown time.Duration) *DisruptionController {
	return &DisruptionController{
		Client:         c,
		Log:            log,
		Registry:       reg,
		CooldownPeriod: cooldown,
	}
}

// Reconcile is the main entry point. It handles standalone node disruption.
func (r *DisruptionController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("claim", req.NamespacedName)

	var claim v1alpha1.GPUNodeClaim
	if err := r.Get(ctx, req.NamespacedName, &claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch claim.Status.Phase {
	case v1alpha1.ClaimPhaseReady:
		// normal path below
	case v1alpha1.ClaimPhaseHibernated:
		// Hibernated claims (legacy) are cleaned up by the ClaimReconciler.
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, nil
	}

	return r.reconcileStandalone(ctx, &claim, log)
}

// --- Standalone path: demand-counter-based idle detection ---
// Standalone nodes serve requests directly via GPU API.
// Idle detection: check demand counters in Dragonfly DB 3 (maintained by GPU API)
// and the node_idle flag set by GPU API when active inference streams drop to zero.
// Always-active models are never destroyed.

func (r *DisruptionController) reconcileStandalone(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	// Check if the provider instance is still alive
	prov, ok := r.Registry.Get(claim.Status.Provider)
	if !ok {
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}
	instance, err := prov.GetInstance(ctx, claim.Status.InstanceID)
	if err != nil {
		log.Error(err, "Failed to check instance status")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if instance.Status == "stopped" || instance.Status == "error" {
		log.Info("Standalone instance died, destroying", "status", instance.Status)
		return r.destroyWorker(ctx, claim, log)
	}

	modelIDs := claimModelIDs(claim)

	// Any co-located model being always-active prevents destruction.
	if r.isAnyModelAlwaysActive(ctx, modelIDs) {
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			_ = r.Status().Update(ctx, claim)
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Consolidation-drain: skip idle/cooldown, destroy immediately.
	if claim.Annotations[annotationConsolidationDrain] == "true" {
		log.Info("Consolidation drain requested, destroying standalone node immediately", "models", modelIDs)
		return r.destroyWorker(ctx, claim, log)
	}

	// Check demand counters for ALL co-located models — any demand keeps the node alive.
	if r.hasAnyModelDemand(ctx, modelIDs) {
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			if err := r.Status().Update(ctx, claim); err != nil {
				log.Error(err, "Failed to clear idle timestamp")
			}
			log.Info("At least one co-located model has demand, standalone node is not idle", "models", modelIDs)
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// No demand for any model. Also check if GPU API says the node is idle
	// (zero active inference streams). Both conditions together → proceed to destroy.
	nodeIdle := r.DemandStore.IsNodeIdle(ctx, claim.Name)
	if !nodeIdle {
		// Demand counters are zero but GPU API hasn't flagged the node as idle yet —
		// there may be in-flight streams that haven't completed. Wait.
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			if err := r.Status().Update(ctx, claim); err != nil {
				log.Error(err, "Failed to clear idle timestamp")
			}
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Both demand counters and GPU API agree the node is idle → enter cooldown.
	return r.handleIdleClaim(ctx, claim, log, func() (ctrl.Result, error) {
		return r.destroyWorker(ctx, claim, log)
	})
}

// hasModelDemand checks if there are active requests for a single model.
func (r *DisruptionController) hasModelDemand(ctx context.Context, modelID string) bool {
	if r.DemandStore == nil || modelID == "" {
		return false
	}
	count, err := r.DemandStore.GetDemand(ctx, modelID)
	if err != nil {
		r.Log.Error(err, "Failed to read demand counter", "model", modelID)
		return false // fail-safe: don't destroy if we can't read
	}
	return count > 0
}

// hasAnyModelDemand returns true if ANY model in the list has active demand.
// For bin-packed multi-model claims: a single model with demand keeps the whole node alive.
func (r *DisruptionController) hasAnyModelDemand(ctx context.Context, modelIDs []string) bool {
	for _, id := range modelIDs {
		if r.hasModelDemand(ctx, id) {
			return true
		}
	}
	return false
}

// isModelAlwaysActive checks if a model is marked as always-active.
func (r *DisruptionController) isModelAlwaysActive(ctx context.Context, modelID string) bool {
	if r.DemandStore == nil || modelID == "" {
		return false
	}
	active, err := r.DemandStore.IsAlwaysActive(ctx, modelID)
	if err != nil {
		return false
	}
	return active
}

// isAnyModelAlwaysActive returns true if ANY model in the list is always-active.
func (r *DisruptionController) isAnyModelAlwaysActive(ctx context.Context, modelIDs []string) bool {
	for _, id := range modelIDs {
		if r.isModelAlwaysActive(ctx, id) {
			return true
		}
	}
	return false
}

func (r *DisruptionController) destroyWorker(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	claim.Status.Phase = v1alpha1.ClaimPhaseDraining
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Draining: %w", err)
	}

	prov, ok := r.Registry.Get(claim.Status.Provider)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("provider %q not found in registry", claim.Status.Provider)
	}
	if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
		log.Error(err, "Failed to destroy standalone instance")
	} else {
		log.Info("Standalone instance destroyed", "instanceID", claim.Status.InstanceID)
	}

	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Terminated: %w", err)
	}

	// Remove from Dragonfly
	if err := r.WorkerStore.RemoveWorker(ctx, claim.Name); err != nil {
		log.Error(err, "Failed to remove worker from Dragonfly")
	}

	// Remove loaded_models keys and signal GPU API to cancel any pending HTTP forwards.
	// GPU API's queue processor may be blocked waiting on a response (up to 30 min timeout).
	// Publishing model_failed cancels that context immediately so it re-enqueues the request.
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			if err := r.DemandStore.RemoveModelLoaded(ctx, modelID); err != nil {
				log.Error(err, "Failed to remove loaded_models key", "model", modelID)
			}
			r.DemandStore.PublishModelFailed(ctx, modelID)
		}
	}

	if r.ClaimWriter != nil {
		if err := r.ClaimWriter.Upsert(ctx, ClaimWriteRecord{
			Name:          claim.Name,
			Pool:          claim.Spec.PoolRef,
			Provider:      claim.Status.Provider,
			GPUType:       claim.Status.GPUType,
			GPUCount:      claim.Status.GPUCount,
			PricePerHour:  claim.Status.PricePerHour,
			ModelID:       claimPrimaryModel(claim),
			NodeType:      claim.Status.NodeType,
			Phase:         string(v1alpha1.ClaimPhaseTerminated),
			ProvisionedAt: provisionedAtTime(claim),
			ReadyAt:       readyAtTime(claim),
		}); err != nil {
			log.Error(err, "Failed to write Terminated standalone claim to Postgres (non-fatal)")
		}
	}

	log.Info("Standalone node destroy complete")
	return ctrl.Result{}, nil
}

// --- Shared helpers ---

// handleIdleClaim processes idle detection and cooldown for any claim type.
// When BillingPeriod is set, it waits until 1 min before the next billing tick
// so we use the time we've already paid for instead of destroying mid-cycle.
// The destroyFn callback performs the type-specific destruction.
func (r *DisruptionController) handleIdleClaim(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger, destroyFn func() (ctrl.Result, error)) (ctrl.Result, error) {
	now := metav1.Now()

	// Read pool for billing period + min-nodes.
	pool, err := r.getPool(ctx, claim.Spec.PoolRef)
	if err != nil {
		log.Error(err, "Failed to get pool")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	var billingPeriod time.Duration
	if pool != nil {
		billingPeriod = pool.Spec.Scaling.BillingPeriod.Duration
	}

	if claim.Status.IdleSince == nil {
		claim.Status.IdleSince = &now
		if err := r.Status().Update(ctx, claim); err != nil {
			log.Error(err, "Failed to set idle timestamp")
		}
		log.Info("Claim became idle, starting cooldown timer")
		return ctrl.Result{RequeueAfter: r.nextDestroyIn(claim, now.Time, billingPeriod)}, nil
	}

	destroyIn := r.nextDestroyIn(claim, now.Time, billingPeriod)
	if destroyIn > 0 {
		log.Info("Idle but waiting for optimal destroy time",
			"destroyIn", destroyIn.Round(time.Second).String(),
		)
		return ctrl.Result{RequeueAfter: destroyIn}, nil
	}

	if pool != nil {
		activeCount := r.countActiveNodes(ctx, pool.Name)
		if activeCount <= pool.Spec.Scaling.MinNodes {
			log.Info("At minimum nodes, not scaling down", "active", activeCount, "min", pool.Spec.Scaling.MinNodes)
			return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
		}
	}

	// MinReplicas guard: don't destroy if this model would drop below its configured floor.
	// Applies to the primary model only — co-located models are covered by their own claims.
	if r.DemandStore != nil && claim.Spec.ModelID != "" {
		if mcfg, cfgErr := r.DemandStore.GetModelConfig(ctx, claim.Spec.ModelID); cfgErr == nil && mcfg != nil && mcfg.MinReplicas > 1 {
			var allClaims v1alpha1.GPUNodeClaimList
			if listErr := r.List(ctx, &allClaims, client.InNamespace(claimNamespace())); listErr == nil {
				activeReplicas := countActiveClaimsForModel(allClaims.Items, claim.Spec.ModelID)
				if activeReplicas <= mcfg.MinReplicas {
					log.Info("At MinReplicas, holding destroy",
						"model", claim.Spec.ModelID,
						"activeReplicas", activeReplicas,
						"minReplicas", mcfg.MinReplicas)
					return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
				}
			}
		}
	}

	log.Info("Idle at optimal destroy time, initiating destroy")
	return destroyFn()
}

// nextDestroyIn returns how long to wait before destroying an idle claim.
// With billingPeriod set: waits until 1 min before the next billing tick so we
// use the time already paid for (e.g. destroy at min 9 or 19, not min 12).
// Without billingPeriod: uses CooldownPeriod as a simple floor from idleSince.
func (r *DisruptionController) nextDestroyIn(claim *v1alpha1.GPUNodeClaim, now time.Time, billingPeriod time.Duration) time.Duration {
	const billingBuffer = 1 * time.Minute

	if billingPeriod > 0 && claim.Status.ProvisionedAt != nil {
		age := now.Sub(claim.Status.ProvisionedAt.Time)
		cyclesDone := int(age / billingPeriod)
		nextTick := claim.Status.ProvisionedAt.Time.Add(time.Duration(cyclesDone+1) * billingPeriod)
		optimalDestroy := nextTick.Add(-billingBuffer)
		if now.Before(optimalDestroy) {
			return optimalDestroy.Sub(now)
		}
		return 0
	}

	// No billing period — simple cooldown from idleSince.
	if claim.Status.IdleSince != nil {
		idleDuration := now.Sub(claim.Status.IdleSince.Time)
		if idleDuration < r.CooldownPeriod {
			return r.CooldownPeriod - idleDuration
		}
	}
	return 0
}

func (r *DisruptionController) getPool(ctx context.Context, name string) (*v1alpha1.GPUNodePool, error) {
	var pool v1alpha1.GPUNodePool
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &pool); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return &pool, nil
}

func (r *DisruptionController) countActiveNodes(ctx context.Context, poolName string) int {
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		return 0
	}
	count := 0
	for _, c := range claims.Items {
		if c.Spec.PoolRef == poolName && c.Status.Phase != v1alpha1.ClaimPhaseTerminated {
			count++
		}
	}
	return count
}

// SetupWithManager sets up the controller with the Manager.
// Watches GPUNodeClaims for standalone node disruption.
func (r *DisruptionController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("disruptor").
		For(&v1alpha1.GPUNodeClaim{}).
		Complete(r)
}
