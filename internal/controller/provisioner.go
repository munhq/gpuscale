package controller

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/internal/scheduler"
	"github.com/munhq/gpuscale/pkg/provider"
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
// It does NOT select offers — that's the coordinator's job via ClaimReconciler.
type ProvisioningController struct {
	client.Client
	Log logr.Logger

	// DemandStore reads API queue depth and model configs from Dragonfly DB 3
	DemandStore *DemandStore

	// RayCapacityStore queries Ray cluster for current GPU capacity
	RayCapacityStore *RayCapacityStore

	// Batch window for collecting pending pods before provisioning
	BatchWindow time.Duration

	// Track pods we've already started provisioning for
	mu              sync.Mutex
	provisioningFor map[types.UID]bool
	batchTimer      *time.Timer
	pendingBatch    []*corev1.Pod
}

// NewProvisioningController creates a new provisioning controller.
func NewProvisioningController(c client.Client, log logr.Logger, batchWindow time.Duration) *ProvisioningController {
	return &ProvisioningController{
		Client:          c,
		Log:             log,
		BatchWindow:     batchWindow,
		provisioningFor: make(map[types.UID]bool),
	}
}

// Reconcile processes pending GPU pods.
// Dedup is demand-level: we count active (non-Terminated) claims and only
// provision if there are more pending GPU pods than active claims.
// Only GPUScale demand-signal pods (gpuscale.io/demand-signal=true) trigger
// provisioning. KubeRay worker pods are explicitly excluded — they are
// managed by Ray's in-tree autoscaler and must not drive GPUScale provisioning,
// since they carry no model requirements and would use wrong defaults.
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
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// KubeRay worker pods are created by Ray's in-tree autoscaler and do not
	// carry model requirements, so GPUScale cannot provision for them directly
	// (wrong GPU type/VRAM would be selected). Cold-start provisioning is handled
	// by the provision_trigger pub/sub path (ProvisionTriggerSubscriber).
	//
	// However: Ray Serve also creates a "config-update verification" replica
	// (CONFIG_UPDATE_STARTED) even when min_replicas=0 and there is no demand.
	// This leaves a pending worker pod that nothing will ever satisfy.
	// Fix: after a 2-minute grace period (enough time for a real demand pub/sub
	// trigger to have fired), if there is still no demand, delete the stuck
	// DEPLOYING Ray Serve apps so the KubeRay operator re-submits them fresh.
	// With initial_replicas:0 in the new serveConfigV2, Ray won't create a
	// verification replica and the app will settle at 0 replicas.
	if pod.Labels["app.kubernetes.io/created-by"] == "kuberay-operator" {
		if r.DemandStore == nil || r.RayCapacityStore == nil {
			return ctrl.Result{}, nil
		}
		// Grace period: real demand pub/sub triggers fire within seconds.
		// 2 minutes is more than enough; if we still have no demand after
		// that, the pod was created purely for config-update verification.
		if time.Since(pod.CreationTimestamp.Time) < 2*time.Minute {
			return ctrl.Result{RequeueAfter: 2 * time.Minute}, nil
		}
		demands, err := r.DemandStore.GetAllDemands(ctx)
		if err != nil {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		for _, d := range demands {
			if d.QueueDepth > 0 || d.ActiveDemand > 0 || d.AlwaysActive {
				// Real demand exists — the pub/sub path will provision.
				return ctrl.Result{}, nil
			}
		}
		// No demand. Cancel the stuck verification replica.
		log.Info("KubeRay worker pod pending with no demand — cancelling stuck DEPLOYING apps",
			"pod", req.NamespacedName,
			"age", time.Since(pod.CreationTimestamp.Time).Round(time.Second),
		)
		if err := r.RayCapacityStore.DeleteDeployingApps(ctx); err != nil {
			log.Error(err, "Failed to delete stuck DEPLOYING apps")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Demand-level dedup: count active claims vs pending GPU pods.
	// If we already have enough claims in flight, don't create more.
	activeClaims := r.countActiveClaims(ctx)
	pendingGPUPods := r.countPendingGPUPods(ctx)

	if activeClaims >= pendingGPUPods {
		log.V(1).Info("Sufficient claims already exist",
			"activeClaims", activeClaims,
			"pendingGPUPods", pendingGPUPods,
		)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// In-memory dedup to prevent multiple reconciles from creating claims concurrently.
	r.mu.Lock()
	if r.provisioningFor[pod.UID] {
		r.mu.Unlock()
		return ctrl.Result{}, nil
	}

	r.pendingBatch = append(r.pendingBatch, &pod)
	r.provisioningFor[pod.UID] = true

	if r.batchTimer == nil {
		r.batchTimer = time.AfterFunc(r.BatchWindow, func() {
			r.processBatch(context.Background())
		})
	}
	r.mu.Unlock()

	log.Info("Pod added to provisioning batch",
		"activeClaims", activeClaims,
		"pendingGPUPods", pendingGPUPods,
	)
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

	// Re-check dedup at batch processing time — the reconcile-time check
	// may be stale because multiple pods can queue between check and batch fire.
	activeClaims := r.countActiveClaims(ctx)
	pendingGPUPods := r.countPendingGPUPods(ctx)
	log.Info("Processing pending pod batch",
		"activeClaims", activeClaims,
		"pendingGPUPods", pendingGPUPods,
	)
	if activeClaims >= pendingGPUPods {
		log.Info("Skipping batch: sufficient claims already exist")
		r.releaseProvisioningLocks(batch)
		return
	}

	// Health gate: don't provision if Ray Serve has DEPLOY_FAILED apps.
	// Instead, attempt recovery by re-submitting the serve config.
	if r.RayCapacityStore != nil {
		statuses, err := r.RayCapacityStore.GetServeAppStatus(ctx)
		if err == nil {
			for _, app := range statuses {
				if app.Status == "DEPLOY_FAILED" {
					log.Error(nil, "Ray Serve DEPLOY_FAILED — attempting recovery instead of provisioning",
						"app", app.Name)
					if recoverErr := r.RayCapacityStore.ResubmitServeConfig(ctx); recoverErr != nil {
						log.Error(recoverErr, "Failed to resubmit serve config")
					} else {
						log.Info("Resubmitted serve config to reset DEPLOY_FAILED")
					}
					r.releaseProvisioningLocks(batch)
					return
				}
			}
		}
	}

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

	// Query Ray cluster capacity (Task #2: Ray capacity metrics)
	var capacity *ClusterCapacity
	if r.RayCapacityStore != nil {
		var err error
		capacity, err = r.RayCapacityStore.GetCapacity(ctx, r.DemandStore)
		if err != nil {
			log.Error(err, "Failed to query Ray cluster capacity")
		} else {
			log.Info("Ray cluster capacity",
				"nodes", len(capacity.Nodes),
				"totalGPUs", capacity.TotalGPUs,
				"totalVRAM", capacity.TotalVRAM,
				"usedVRAM", capacity.UsedVRAM,
				"freeVRAM", capacity.FreeVRAM,
				"loadedModels", len(capacity.LoadedModels),
			)
			for _, node := range capacity.Nodes {
				log.Info("Ray node",
					"nodeID", node.NodeID,
					"gpuType", node.GPUType,
					"gpuCount", node.GPUCount,
					"totalVRAM", node.TotalVRAM,
					"usedVRAM", node.UsedVRAM,
					"freeVRAM", node.FreeVRAM,
				)
			}
		}
	}

	// Bin-packing decision (Task #3: determine if provisioning needed)
	if r.DemandStore != nil && capacity != nil {
		demands, err := r.DemandStore.GetAllDemands(ctx)
		if err != nil {
			log.Error(err, "Failed to get demands for bin-packing")
		} else {
			decision, err := r.DecideProvisioning(ctx, demands, capacity)
			if err != nil {
				log.Error(err, "Failed to make provisioning decision")
			} else if !decision.ShouldProvision {
				log.Info("Bin-packing decision: skip provisioning",
					"reason", decision.Reason,
				)
				r.releaseProvisioningLocks(batch)
				return
			} else {
				log.Info("Bin-packing decision: provision",
					"reason", decision.Reason,
					"models", decision.Models,
					"requiredVRAM", decision.RequiredVRAM,
					"multiGpu", decision.MultiGpu,
				)
				// Override requirements from bin-packing
				// (will be used below in SearchOffers)
			}
		}
	}

	// Determine model from demand data — needed for pool selection and dedup.
	modelID := ""
	if r.DemandStore != nil {
		demands, err := r.DemandStore.GetAllDemands(ctx)
		if err == nil {
			var maxDemand int64
			for _, d := range demands {
				total := d.QueueDepth + d.ActiveDemand
				if total > maxDemand || d.AlwaysActive {
					maxDemand = total
					modelID = d.Model
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

	// Select pool from model config: explicit pool name first, then nodeType fallback.
	modelNodeType := "ray-worker"
	modelPool := ""
	if r.DemandStore != nil && modelID != "" {
		if mcfg, err := r.DemandStore.GetModelConfig(ctx, modelID); err == nil && mcfg != nil {
			if mcfg.NodeType != "" {
				modelNodeType = mcfg.NodeType
			}
			modelPool = mcfg.Pool
		}
	}
	pool := findPool(pools.Items, modelPool, modelNodeType)

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

	// Create GPUNodeClaim with requirements only — the coordinator picks
	// the provider and offer at provision time via ClaimReconciler.
	claimID := uuid.New().String()[:8]
	podRefs := make([]v1alpha1.PodReference, 0, len(batch))
	for _, pod := range batch {
		podRefs = append(podRefs, v1alpha1.PodReference{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			UID:       string(pod.UID),
		})
	}

	// Determine NodeType from the pool's provider config.
	nodeType := "ray-worker"
	if len(pool.Spec.Providers) > 0 && pool.Spec.Providers[0].NodeType != "" {
		nodeType = pool.Spec.Providers[0].NodeType
	}

	// modelID already determined above for pool selection

	claim := &v1alpha1.GPUNodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("claim-%s", claimID),
			Namespace: claimNamespace(),
		},
		Spec: v1alpha1.GPUNodeClaimSpec{
			PoolRef:  pool.Name,
			NodeType: nodeType,
			ModelID:  modelID,
			Requirements: v1alpha1.ClaimRequirements{
				GPUCount:       merged.GPUCount,
				MinVRAM:        merged.MinVRAM,
				GPUTypes:       merged.GPUTypes,
				MaxPricePerGPU: merged.MaxPricePerGPU,
			},
			PodRefs: podRefs,
		},
	}

	// Add finalizer before creation so it's always present.
	claim.Finalizers = []string{"gpuscale.io/instance-cleanup"}

	if err := r.Create(ctx, claim); err != nil {
		log.Error(err, "Failed to create GPUNodeClaim")
		r.releaseProvisioningLocks(batch)
		return
	}

	log.Info("Created GPUNodeClaim",
		"claim", claim.Name,
		"nodeType", nodeType,
		"gpuCount", merged.GPUCount,
		"minVRAM", merged.MinVRAM,
	)
}

func (r *ProvisioningController) checkPoolLimits(ctx context.Context, pool *v1alpha1.GPUNodePool) error {
	// Count existing claims for this pool
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
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

// countActiveClaims returns the number of non-Terminated GPUNodeClaims.
func (r *ProvisioningController) countActiveClaims(ctx context.Context) int {
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		r.Log.Error(err, "Failed to list claims for dedup")
		return 0
	}
	count := 0
	for _, c := range claims.Items {
		if c.Status.Phase != v1alpha1.ClaimPhaseTerminated {
			count++
		}
	}
	return count
}

// countPendingGPUPods returns the number of pending, unschedulable GPU pods
// that GPUScale manages (demand-signal pods only). KubeRay worker pods are
// excluded — they are not GPUScale demand signals.
func (r *ProvisioningController) countPendingGPUPods(ctx context.Context) int {
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		r.Log.Error(err, "Failed to list pods for dedup")
		return 0
	}
	count := 0
	for _, p := range pods.Items {
		if p.Labels["app.kubernetes.io/created-by"] == "kuberay-operator" {
			continue
		}
		if p.Status.Phase == corev1.PodPending &&
			scheduler.IsGPUPod(&p) &&
			scheduler.IsUnschedulable(&p) {
			count++
		}
	}
	return count
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
