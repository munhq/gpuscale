package controller

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/munhq/gpuscale/internal/scheduler"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ProvisioningController watches for pending GPU pods and creates GPUNodeClaims.
type ProvisioningController struct {
	client.Client
	Log      logr.Logger
	Selector *scheduler.Selector
	Registry *provider.Registry

	// DemandStore reads API queue depth and model configs from Dragonfly DB 3
	DemandStore *DemandStore

	// Batch window for collecting pending pods before provisioning
	BatchWindow time.Duration

	// Track pods we've already started provisioning for
	mu              sync.Mutex
	provisioningFor map[types.UID]bool
	batchTimer      *time.Timer
	pendingBatch    []*corev1.Pod
}

// NewProvisioningController creates a new provisioning controller.
func NewProvisioningController(c client.Client, log logr.Logger, sel *scheduler.Selector, reg *provider.Registry, batchWindow time.Duration) *ProvisioningController {
	return &ProvisioningController{
		Client:          c,
		Log:             log,
		Selector:        sel,
		Registry:        reg,
		BatchWindow:     batchWindow,
		provisioningFor: make(map[types.UID]bool),
	}
}

// Reconcile processes pending GPU pods.
func (r *ProvisioningController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("pod", req.NamespacedName)

	// Fetch the pod
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only process pending GPU pods that are unschedulable
	if pod.Status.Phase != corev1.PodPending {
		return ctrl.Result{}, nil
	}
	if !scheduler.IsGPUPod(&pod) {
		return ctrl.Result{}, nil
	}
	if !scheduler.IsUnschedulable(&pod) {
		// Requeue — might become unschedulable soon
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Check if we're already provisioning for this pod (in-memory fast path)
	r.mu.Lock()
	if r.provisioningFor[pod.UID] {
		r.mu.Unlock()
		return ctrl.Result{}, nil
	}
	r.mu.Unlock()

	// Persistent dedup: check if an existing GPUNodeClaim already covers this pod.
	// This survives controller restarts — the in-memory map is just a fast path.
	if r.claimExistsForPod(ctx, string(pod.UID)) {
		log.Info("Claim already exists for pod, skipping", "podUID", pod.UID)
		r.mu.Lock()
		r.provisioningFor[pod.UID] = true // cache it
		r.mu.Unlock()
		return ctrl.Result{}, nil
	}

	r.mu.Lock()
	// Re-check after persistent lookup (another reconcile may have added it)
	if r.provisioningFor[pod.UID] {
		r.mu.Unlock()
		return ctrl.Result{}, nil
	}

	// Add to batch
	r.pendingBatch = append(r.pendingBatch, &pod)
	r.provisioningFor[pod.UID] = true

	// Start batch timer if not running
	if r.batchTimer == nil {
		r.batchTimer = time.AfterFunc(r.BatchWindow, func() {
			r.processBatch(context.Background())
		})
	}
	r.mu.Unlock()

	log.Info("Pod added to provisioning batch", "gpu", "true")
	return ctrl.Result{}, nil
}

// processBatch processes the collected batch of pending pods.
func (r *ProvisioningController) processBatch(ctx context.Context) {
	r.mu.Lock()
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.batchTimer = nil
	r.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	log := r.Log.WithValues("batchSize", len(batch))
	log.Info("Processing pending pod batch")

	// Query demand data from Dragonfly (Task #1: API queue metrics)
	if r.DemandStore != nil {
		demands, err := r.DemandStore.GetAllDemands(ctx)
		if err != nil {
			log.Error(err, "Failed to query demand data from Dragonfly")
		} else {
			log.Info("Demand data from API queue", "demands", len(demands))
			for _, d := range demands {
				if d.QueueDepth > 0 || d.ActiveDemand > 0 || d.AlwaysActive {
					log.Info("Model demand",
						"model", d.Model,
						"queue", d.QueueDepth,
						"active", d.ActiveDemand,
						"vram", d.VRAMRequired,
						"alwaysActive", d.AlwaysActive,
					)
				}
			}
		}
	}

	// Find the matching GPUNodePool
	var pools v1alpha1.GPUNodePoolList
	if err := r.List(ctx, &pools); err != nil {
		log.Error(err, "Failed to list GPUNodePools")
		r.releaseProvisioningLocks(batch)
		return
	}

	if len(pools.Items) == 0 {
		log.Info("No GPUNodePools configured, cannot provision nodes")
		r.releaseProvisioningLocks(batch)
		return
	}

	// Use first matching pool (future: match based on requirements)
	pool := &pools.Items[0]

	// Check pool limits
	if err := r.checkPoolLimits(ctx, pool); err != nil {
		log.Info("Pool limits reached", "error", err.Error())
		r.releaseProvisioningLocks(batch)
		return
	}

	// Extract and merge requirements
	var reqs []provider.GPURequirements
	for _, pod := range batch {
		reqs = append(reqs, scheduler.ExtractGPURequirements(pod))
	}
	merged := scheduler.MergeRequirements(reqs)

	// Apply pool-level constraints
	if merged.MinVRAM == 0 && pool.Spec.Requirements.MinVRAM > 0 {
		merged.MinVRAM = pool.Spec.Requirements.MinVRAM
	}
	if merged.MinDisk == 0 && pool.Spec.Requirements.MinDisk > 0 {
		merged.MinDisk = pool.Spec.Requirements.MinDisk
	}
	if merged.MinRAM == 0 && pool.Spec.Requirements.MinRAM > 0 {
		merged.MinRAM = pool.Spec.Requirements.MinRAM
	}
	if len(merged.GPUTypes) == 0 && len(pool.Spec.Requirements.GPUTypes) > 0 {
		merged.GPUTypes = pool.Spec.Requirements.GPUTypes
	}

	// Find best offer
	providerNames := make([]string, 0, len(pool.Spec.Providers))
	for _, p := range pool.Spec.Providers {
		providerNames = append(providerNames, p.Name)
	}

	offer, err := r.Selector.SelectBestOffer(ctx, merged, providerNames)
	if err != nil {
		log.Error(err, "No suitable offer found")
		r.releaseProvisioningLocks(batch)
		return
	}

	// Create GPUNodeClaim
	claimID := uuid.New().String()[:8]
	podRefs := make([]v1alpha1.PodReference, 0, len(batch))
	for _, pod := range batch {
		podRefs = append(podRefs, v1alpha1.PodReference{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			UID:       string(pod.UID),
		})
	}

	claim := &v1alpha1.GPUNodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("claim-%s", claimID),
			Namespace: "gpuscale-system",
		},
		Spec: v1alpha1.GPUNodeClaimSpec{
			PoolRef: pool.Name,
			Requirements: v1alpha1.ClaimRequirements{
				GPUCount: merged.GPUCount,
				MinVRAM:  merged.MinVRAM,
				GPUTypes: merged.GPUTypes,
				MaxPrice: merged.MaxPrice,
			},
			PodRefs: podRefs,
		},
	}

	if err := r.Create(ctx, claim); err != nil {
		log.Error(err, "Failed to create GPUNodeClaim")
		r.releaseProvisioningLocks(batch)
		return
	}

	// Determine NodeType from the pool's provider config
	nodeType := "ray-worker" // default
	for _, p := range pool.Spec.Providers {
		if p.Name == offer.ProviderName {
			if p.NodeType != "" {
				nodeType = p.NodeType
			}
			break
		}
	}

	// Update claim status with the selected offer
	claim.Status = v1alpha1.GPUNodeClaimStatus{
		Provider:     offer.ProviderName,
		NodeType:     nodeType,
		GPUType:      offer.GPUType,
		GPUCount:     offer.GPUCount,
		PricePerHour: offer.PricePerHour,
		Phase:        v1alpha1.ClaimPhasePending,
	}
	if err := r.Status().Update(ctx, claim); err != nil {
		log.Error(err, "Failed to update GPUNodeClaim status")
	}

	log.Info("Created GPUNodeClaim",
		"claim", claim.Name,
		"provider", offer.ProviderName,
		"gpu", offer.GPUType,
		"price", offer.PricePerHour,
	)
}

func (r *ProvisioningController) checkPoolLimits(ctx context.Context, pool *v1alpha1.GPUNodePool) error {
	// Count existing claims for this pool
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace("gpuscale-system")); err != nil {
		return fmt.Errorf("listing claims: %w", err)
	}

	activeCount := 0
	totalGPUs := 0
	totalCost := 0.0
	for _, c := range claims.Items {
		if c.Spec.PoolRef != pool.Name {
			continue
		}
		if c.Status.Phase == v1alpha1.ClaimPhaseTerminated {
			continue
		}
		activeCount++
		totalGPUs += c.Status.GPUCount
		totalCost += c.Status.PricePerHour
	}

	if activeCount >= pool.Spec.Scaling.MaxNodes {
		return fmt.Errorf("max nodes limit reached (%d/%d)", activeCount, pool.Spec.Scaling.MaxNodes)
	}
	if pool.Spec.Limits != nil {
		if pool.Spec.Limits.MaxGPUs > 0 && totalGPUs >= pool.Spec.Limits.MaxGPUs {
			return fmt.Errorf("max GPUs limit reached (%d/%d)", totalGPUs, pool.Spec.Limits.MaxGPUs)
		}
		if pool.Spec.Limits.MaxCostPerHour > 0 && totalCost >= pool.Spec.Limits.MaxCostPerHour {
			return fmt.Errorf("max cost/hr limit reached ($%.2f/$%.2f)", totalCost, pool.Spec.Limits.MaxCostPerHour)
		}
	}
	return nil
}

// claimExistsForPod checks whether any active (non-Terminated) GPUNodeClaim
// already references this pod UID. This prevents double-provisioning after
// controller restarts when the in-memory map is lost.
func (r *ProvisioningController) claimExistsForPod(ctx context.Context, podUID string) bool {
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace("gpuscale-system")); err != nil {
		r.Log.Error(err, "Failed to list claims for dedup check")
		return false // fail open — let the batch process handle limits
	}
	for _, c := range claims.Items {
		if c.Status.Phase == v1alpha1.ClaimPhaseTerminated {
			continue
		}
		for _, ref := range c.Spec.PodRefs {
			if ref.UID == podUID {
				return true
			}
		}
	}
	return false
}

func (r *ProvisioningController) releaseProvisioningLocks(pods []*corev1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, pod := range pods {
		delete(r.provisioningFor, pod.UID)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ProvisioningController) SetupWithManager(mgr ctrl.Manager) error {
	// Create an event channel for filtered pod events
	podEvents := make(chan event.GenericEvent, 100)

	// Watch pods, filtering for pending GPU pods
	return ctrl.NewControllerManagedBy(mgr).
		Named("provisioner").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		WatchesRawSource(source.Channel(podEvents, &handler.EnqueueRequestForObject{})).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return nil
				}
				if pod.Status.Phase != corev1.PodPending || !scheduler.IsGPUPod(pod) {
					return nil
				}
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					}},
				}
			},
		)).
		Complete(r)
}

// claimNamespace returns the namespace for GPUNodeClaims.
// Reads POD_NAMESPACE env (set via downward API) with fallback to gpu-workloads.
func claimNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return "gpu-workloads"
}
