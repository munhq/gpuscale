package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/munhq/gpuscale/internal/scheduler"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// annotationConsolidationDrain is set by the reconciler when a bin-packed replacement
// claim is Ready and serving all the same models. The DisruptionController skips
// the normal cooldown and drains the claim immediately.
const annotationConsolidationDrain = "gpuscale.io/consolidation-drain"

// annotationWakeRequested is set by ProvisionTrigger when demand arrives for a model
// whose claim is in ClaimPhaseHibernated. The ClaimReconciler wakes the VM on seeing this.
const annotationWakeRequested = "gpuscale.io/wake-requested"

// DisruptionController watches managed nodes and GPUNodeClaims, destroying idle ones after cooldown.
// Supports two modes:
//   - full-node: watches Kubernetes Node objects, pod-based idle detection, drain-and-destroy
//   - ray-worker: watches GPUNodeClaim objects, demand-counter-based idle detection, simple destroy
//
// Demand counters (demand:{model} in Dragonfly DB 3) are maintained by GPU API's request queue.
// Always-active models are never destroyed.
type DisruptionController struct {
	client.Client
	Log            logr.Logger
	Registry       *provider.Registry
	CooldownPeriod time.Duration
	WorkerStore    *WorkerStore
	DemandStore    *DemandStore // reads demand counters from Dragonfly DB 3
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

// Reconcile is the main entry point. It handles both full-node (Node events) and
// ray-worker (GPUNodeClaim events) disruption.
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
		// Hibernated claims are woken by the ClaimReconciler, not the DisruptionController.
		// Nothing to do here.
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, nil
	}

	// Branch based on node type
	if claim.Status.NodeType == "full-node" {
		return r.reconcileFullNode(ctx, &claim, log)
	}
	return r.reconcileRayWorker(ctx, &claim, log)
}

// --- Full-node path: pod-based idle detection on Kubernetes nodes ---

func (r *DisruptionController) reconcileFullNode(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	modelIDs := claimModelIDs(claim)

	// Any co-located model being always-active means the node stays alive.
	if r.isAnyModelAlwaysActive(ctx, modelIDs) {
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			_ = r.Status().Update(ctx, claim)
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	nodeName := claim.Status.NodeName
	if nodeName == "" {
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Fetch the node
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		log.Error(err, "Failed to get node for full-node claim")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Consolidation-drain: another bin-packed claim is now serving all our models.
	// Skip the idle/cooldown check and drain immediately.
	if claim.Annotations[annotationConsolidationDrain] == "true" {
		log.Info("Consolidation drain requested, draining immediately", "models", modelIDs)
		return r.drainAndDestroy(ctx, &node, claim, log)
	}

	// Check if node is idle (no GPU workload pods)
	idle, err := r.isNodeIdle(ctx, &node)
	if err != nil {
		log.Error(err, "Failed to check node idle status")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if !idle {
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			if err := r.Status().Update(ctx, claim); err != nil {
				log.Error(err, "Failed to clear idle timestamp")
			}
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Node is idle (no GPU pods). Enter cooldown.
	// For Vast.ai full-node: hibernate (stop VM, preserve disk) instead of destroy.
	// The model files stay on disk so the next wake-up skips the HuggingFace download.
	return r.handleIdleClaim(ctx, claim, log, func() (ctrl.Result, error) {
		return r.hibernateOrDestroy(ctx, &node, claim, log)
	})
}

func (r *DisruptionController) isNodeIdle(ctx context.Context, node *corev1.Node) (bool, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return false, fmt.Errorf("listing pods on node: %w", err)
	}

	for _, pod := range podList.Items {
		if isDaemonSetPod(&pod) {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if scheduler.IsGPUPod(&pod) {
			// Any non-terminal GPU pod (Pending, Init, Running) means the node is not idle.
			return false, nil
		}
	}
	return true, nil
}

func isDaemonSetPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func (r *DisruptionController) drainAndDestroy(ctx context.Context, node *corev1.Node, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	// Update claim phase to Draining
	claim.Status.Phase = v1alpha1.ClaimPhaseDraining
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Draining: %w", err)
	}

	// Cordon the node
	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		if err := r.Update(ctx, node); err != nil {
			return ctrl.Result{}, fmt.Errorf("cordoning node: %w", err)
		}
		log.Info("Node cordoned")
	}

	// Evict non-DaemonSet pods
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods for drain: %w", err)
	}
	for _, pod := range podList.Items {
		if isDaemonSetPod(&pod) {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if err := r.Delete(ctx, &pod, client.GracePeriodSeconds(30)); err != nil {
			log.Error(err, "Failed to evict pod", "pod", pod.Name)
		}
	}

	// Destroy the provider instance
	prov, ok := r.Registry.Get(claim.Status.Provider)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("provider %q not found in registry", claim.Status.Provider)
	}
	if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
		log.Error(err, "Failed to destroy provider instance")
	} else {
		log.Info("Provider instance destroyed", "instanceID", claim.Status.InstanceID)
	}

	// Delete the Kubernetes node object
	if err := r.Delete(ctx, node); err != nil {
		log.Error(err, "Failed to delete node object")
	}

	// Update claim to Terminated
	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Terminated: %w", err)
	}

	// Remove from Dragonfly
	if err := r.WorkerStore.RemoveWorker(ctx, claim.Name); err != nil {
		log.Error(err, "Failed to remove worker from Dragonfly")
	}

	log.Info("Node drain and destroy complete")
	return ctrl.Result{}, nil
}

func (r *DisruptionController) findClaimForNode(ctx context.Context, node *corev1.Node) (*v1alpha1.GPUNodeClaim, error) {
	instanceID := node.Labels[scheduler.LabelInstanceID]
	if instanceID == "" {
		return nil, nil
	}

	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		return nil, err
	}

	for i, c := range claims.Items {
		if c.Status.InstanceID == instanceID || c.Status.NodeName == node.Name {
			return &claims.Items[i], nil
		}
	}
	return nil, nil
}

// --- Ray-worker path: demand-counter-based idle detection ---
// Ray workers join the Ray cluster and don't serve requests directly.
// Idle detection: check demand counters in Dragonfly DB 3 (maintained by GPU API).
// Always-active models are never destroyed.

func (r *DisruptionController) reconcileRayWorker(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
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
		log.Info("Ray worker instance died, destroying", "status", instance.Status)
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
		log.Info("Consolidation drain requested, destroying worker immediately", "models", modelIDs)
		return r.destroyWorker(ctx, claim, log)
	}

	// Check demand counters for ALL co-located models — any demand keeps the node alive.
	if r.hasAnyModelDemand(ctx, modelIDs) {
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			if err := r.Status().Update(ctx, claim); err != nil {
				log.Error(err, "Failed to clear idle timestamp")
			}
			log.Info("At least one co-located model has demand, worker is not idle", "models", modelIDs)
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// No demand for any model → worker is idle
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

// hibernateOrDestroy checks if the provider supports hibernation (stop without disk loss).
// For Vast.ai full-node VMs: stops the instance and sets claim phase to Hibernated.
//   - The K8s node is drained and deleted so the cluster is clean.
//   - The Vast.ai VM retains its disk (model files stay cached).
//   - On next demand, ProvisionTrigger annotates the claim and ClaimReconciler wakes it.
//
// For providers without hibernation support: falls back to full destroy.
func (r *DisruptionController) hibernateOrDestroy(ctx context.Context, node *corev1.Node, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	prov, ok := r.Registry.Get(claim.Status.Provider)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("provider %q not found in registry", claim.Status.Provider)
	}

	hibernator, canHibernate := prov.(provider.HibernatingProvider)
	if !canHibernate {
		// Provider doesn't support hibernation — do a normal full destroy.
		return r.drainAndDestroy(ctx, node, claim, log)
	}

	// Cordon + drain pods so the node is clean before stopping the VM.
	claim.Status.Phase = v1alpha1.ClaimPhaseDraining
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Draining: %w", err)
	}
	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		if err := r.Update(ctx, node); err != nil {
			return ctrl.Result{}, fmt.Errorf("cordoning node: %w", err)
		}
	}
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name}); err == nil {
		for _, pod := range podList.Items {
			if isDaemonSetPod(&pod) {
				continue
			}
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				continue
			}
			_ = r.Delete(ctx, &pod, client.GracePeriodSeconds(30))
		}
	}

	// Stop the VM — disk is preserved.
	if err := hibernator.StopInstance(ctx, claim.Status.InstanceID); err != nil {
		log.Error(err, "Failed to stop instance for hibernation; falling back to destroy")
		return r.drainAndDestroy(ctx, node, claim, log)
	}
	log.Info("Instance hibernated (stopped, disk preserved)", "instanceID", claim.Status.InstanceID)

	// Delete the K8s node object — it will re-appear when the VM restarts.
	if err := r.Delete(ctx, node); err != nil {
		log.Error(err, "Failed to delete K8s node object after hibernation")
	}

	// Mark claim as Hibernated — keep it alive so ProvisionTrigger can wake it.
	claim.Status.Phase = v1alpha1.ClaimPhaseHibernated
	claim.Status.NodeName = ""
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Hibernated: %w", err)
	}

	log.Info("Claim hibernated", "claim", claim.Name, "models", claimModelIDs(claim))
	return ctrl.Result{}, nil
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
		log.Error(err, "Failed to destroy worker instance")
	} else {
		log.Info("Worker instance destroyed", "instanceID", claim.Status.InstanceID)
	}

	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("updating claim phase to Terminated: %w", err)
	}

	// Remove from Dragonfly
	if err := r.WorkerStore.RemoveWorker(ctx, claim.Name); err != nil {
		log.Error(err, "Failed to remove worker from Dragonfly")
	}

	log.Info("Worker destroy complete")
	return ctrl.Result{}, nil
}

// --- Shared helpers ---

// handleIdleClaim processes idle detection and cooldown for any claim type.
// The destroyFn callback performs the type-specific destruction.
func (r *DisruptionController) handleIdleClaim(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger, destroyFn func() (ctrl.Result, error)) (ctrl.Result, error) {
	now := metav1.Now()
	if claim.Status.IdleSince == nil {
		claim.Status.IdleSince = &now
		if err := r.Status().Update(ctx, claim); err != nil {
			log.Error(err, "Failed to set idle timestamp")
		}
		log.Info("Claim became idle, starting cooldown timer")
		return ctrl.Result{RequeueAfter: r.CooldownPeriod}, nil
	}

	idleDuration := time.Since(claim.Status.IdleSince.Time)
	if idleDuration < r.CooldownPeriod {
		remaining := r.CooldownPeriod - idleDuration
		log.Info("Idle but cooldown not expired", "remaining", remaining.String())
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	// Check pool min-nodes constraint
	pool, err := r.getPool(ctx, claim.Spec.PoolRef)
	if err != nil {
		log.Error(err, "Failed to get pool")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if pool != nil {
		activeCount := r.countActiveNodes(ctx, pool.Name)
		if activeCount <= pool.Spec.Scaling.MinNodes {
			log.Info("At minimum nodes, not scaling down", "active", activeCount, "min", pool.Spec.Scaling.MinNodes)
			return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
		}
	}

	log.Info("Idle past cooldown, initiating destroy",
		"idleDuration", idleDuration.String(),
		"cooldown", r.CooldownPeriod.String(),
	)

	return destroyFn()
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
// Watches both Nodes (for full-node disruption) and GPUNodeClaims (for ray-worker disruption).
func (r *DisruptionController) SetupWithManager(mgr ctrl.Manager) error {
	// Index pods by node name for efficient lookups (full-node path)
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
		pod := obj.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}); err != nil {
		return fmt.Errorf("indexing pods by nodeName: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("disruptor").
		// Watch GPUNodeClaims directly (handles ray-worker claims + full-node claims)
		For(&v1alpha1.GPUNodeClaim{}).
		// Also watch Nodes — map managed nodes to their corresponding GPUNodeClaim
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				node, ok := obj.(*corev1.Node)
				if !ok {
					return nil
				}
				if node.Labels[scheduler.LabelManaged] != "true" {
					return nil
				}
				// Find the claim for this node and enqueue it
				claim, err := r.findClaimForNode(ctx, node)
				if err != nil || claim == nil {
					return nil
				}
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      claim.Name,
						Namespace: claim.Namespace,
					}},
				}
			},
		)).
		Complete(reconcile.Func(func(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
			return r.Reconcile(ctx, req)
		}))
}
