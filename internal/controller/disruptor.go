package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/internal/bootstrap"
	"github.com/munhq/gpuscale/internal/provider"
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

// DisruptionController watches managed nodes and destroys idle ones after cooldown.
type DisruptionController struct {
	client.Client
	Log            logr.Logger
	Registry       *provider.Registry
	CooldownPeriod time.Duration
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

// Reconcile checks if a managed node is idle and should be destroyed.
func (r *DisruptionController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("node", req.Name)

	// Fetch the node
	var node corev1.Node
	if err := r.Get(ctx, req.NamespacedName, &node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only process managed nodes
	if node.Labels[scheduler.LabelManaged] != "true" {
		return ctrl.Result{}, nil
	}

	// Find the corresponding GPUNodeClaim
	claim, err := r.findClaimForNode(ctx, &node)
	if err != nil {
		log.Error(err, "Failed to find claim for node")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if claim == nil {
		log.Info("No GPUNodeClaim found for managed node, skipping")
		return ctrl.Result{}, nil
	}

	// Don't process nodes that are already draining or terminated
	if claim.Status.Phase == v1alpha1.ClaimPhaseDraining || claim.Status.Phase == v1alpha1.ClaimPhaseTerminated {
		return ctrl.Result{}, nil
	}

	// Check if node is idle (no GPU workload pods)
	idle, err := r.isNodeIdle(ctx, &node)
	if err != nil {
		log.Error(err, "Failed to check node idle status")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if !idle {
		// Node has active GPU workloads; clear idle timestamp
		if claim.Status.IdleSince != nil {
			claim.Status.IdleSince = nil
			if err := r.Status().Update(ctx, claim); err != nil {
				log.Error(err, "Failed to clear idle timestamp")
			}
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Node is idle — check if we've been tracking how long
	now := metav1.Now()
	if claim.Status.IdleSince == nil {
		claim.Status.IdleSince = &now
		if err := r.Status().Update(ctx, claim); err != nil {
			log.Error(err, "Failed to set idle timestamp")
		}
		log.Info("Node became idle, starting cooldown timer")
		return ctrl.Result{RequeueAfter: r.CooldownPeriod}, nil
	}

	// Check if cooldown has expired
	idleDuration := time.Since(claim.Status.IdleSince.Time)
	if idleDuration < r.CooldownPeriod {
		remaining := r.CooldownPeriod - idleDuration
		log.Info("Node idle but cooldown not expired", "remaining", remaining.String())
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

	// Cooldown expired — drain and destroy
	log.Info("Node idle past cooldown, initiating drain and destroy",
		"idleDuration", idleDuration.String(),
		"cooldown", r.CooldownPeriod.String(),
	)

	if err := r.drainAndDestroy(ctx, &node, claim); err != nil {
		log.Error(err, "Failed to drain and destroy node")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *DisruptionController) isNodeIdle(ctx context.Context, node *corev1.Node) (bool, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return false, fmt.Errorf("listing pods on node: %w", err)
	}

	for _, pod := range podList.Items {
		// Skip DaemonSet pods
		if isDaemonSetPod(&pod) {
			continue
		}
		// Skip completed/failed pods
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		// If there's any running GPU pod, the node is not idle
		if scheduler.IsGPUPod(&pod) && pod.Status.Phase == corev1.PodRunning {
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

func (r *DisruptionController) drainAndDestroy(ctx context.Context, node *corev1.Node, claim *v1alpha1.GPUNodeClaim) error {
	log := r.Log.WithValues("node", node.Name, "claim", claim.Name)

	// Update claim phase to Draining
	claim.Status.Phase = v1alpha1.ClaimPhaseDraining
	if err := r.Status().Update(ctx, claim); err != nil {
		return fmt.Errorf("updating claim phase to Draining: %w", err)
	}

	// Cordon the node
	if !node.Spec.Unschedulable {
		node.Spec.Unschedulable = true
		if err := r.Update(ctx, node); err != nil {
			return fmt.Errorf("cordoning node: %w", err)
		}
		log.Info("Node cordoned")
	}

	// Evict non-DaemonSet pods
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return fmt.Errorf("listing pods for drain: %w", err)
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
		return fmt.Errorf("provider %q not found in registry", claim.Status.Provider)
	}
	if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
		log.Error(err, "Failed to destroy provider instance")
	} else {
		log.Info("Provider instance destroyed", "instanceID", claim.Status.InstanceID)
	}

	// Delete the K8s node object
	if err := r.Delete(ctx, node); err != nil {
		log.Error(err, "Failed to delete node object")
	}

	// Update claim to Terminated
	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	if err := r.Status().Update(ctx, claim); err != nil {
		return fmt.Errorf("updating claim phase to Terminated: %w", err)
	}

	log.Info("Node drain and destroy complete")
	return nil
}

func (r *DisruptionController) findClaimForNode(ctx context.Context, node *corev1.Node) (*v1alpha1.GPUNodeClaim, error) {
	instanceID := node.Labels[scheduler.LabelInstanceID]
	if instanceID == "" {
		return nil, nil
	}

	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace("gpuscale-system")); err != nil {
		return nil, err
	}

	for i, c := range claims.Items {
		if c.Status.InstanceID == instanceID || c.Status.NodeName == node.Name {
			return &claims.Items[i], nil
		}
	}
	return nil, nil
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
	if err := r.List(ctx, &claims, client.InNamespace("gpuscale-system")); err != nil {
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
func (r *DisruptionController) SetupWithManager(mgr ctrl.Manager) error {
	// Index pods by node name for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
		pod := obj.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}); err != nil {
		return fmt.Errorf("indexing pods by nodeName: %w", err)
	}

	_ = bootstrap.IsNodeReady // reference to prevent unused import (used in reconciler.go)

	return ctrl.NewControllerManagedBy(mgr).
		Named("disruptor").
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(
			func(ctx context.Context, obj client.Object) []reconcile.Request {
				node, ok := obj.(*corev1.Node)
				if !ok {
					return nil
				}
				if node.Labels[scheduler.LabelManaged] != "true" {
					return nil
				}
				return []reconcile.Request{
					{NamespacedName: types.NamespacedName{Name: node.Name}},
				}
			},
		)).
		Complete(r)
}
