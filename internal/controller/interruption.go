package controller

import (
	"context"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/internal/provider"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// InterruptionController polls provider APIs to detect preempted/terminated instances.
type InterruptionController struct {
	client.Client
	Log          logr.Logger
	Registry     *provider.Registry
	PollInterval time.Duration
}

// NewInterruptionController creates a new interruption controller.
func NewInterruptionController(c client.Client, log logr.Logger, reg *provider.Registry, pollInterval time.Duration) *InterruptionController {
	return &InterruptionController{
		Client:       c,
		Log:          log,
		Registry:     reg,
		PollInterval: pollInterval,
	}
}

// Start begins the polling loop for interruption detection.
// This runs as a background goroutine managed by the controller manager.
func (r *InterruptionController) Start(ctx context.Context) error {
	log := r.Log
	log.Info("Starting interruption controller", "pollInterval", r.PollInterval)

	ticker := time.NewTicker(r.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Interruption controller shutting down")
			return nil
		case <-ticker.C:
			r.checkForInterruptions(ctx)
		}
	}
}

func (r *InterruptionController) checkForInterruptions(ctx context.Context) {
	log := r.Log

	// List all active GPUNodeClaims
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace("gpuscale-system")); err != nil {
		log.Error(err, "Failed to list GPUNodeClaims")
		return
	}

	for i := range claims.Items {
		claim := &claims.Items[i]

		// Only check active instances
		if claim.Status.Phase != v1alpha1.ClaimPhaseReady &&
			claim.Status.Phase != v1alpha1.ClaimPhaseBootstrapping &&
			claim.Status.Phase != v1alpha1.ClaimPhaseProvisioning {
			continue
		}

		if claim.Status.InstanceID == "" || claim.Status.Provider == "" {
			continue
		}

		prov, ok := r.Registry.Get(claim.Status.Provider)
		if !ok {
			continue
		}

		// Check instance status with provider
		instance, err := prov.GetInstance(ctx, claim.Status.InstanceID)
		if err != nil {
			log.Error(err, "Failed to get instance status",
				"provider", claim.Status.Provider,
				"instanceID", claim.Status.InstanceID,
			)
			continue
		}

		if instance.Status == "running" || instance.Status == "starting" {
			continue
		}

		// Instance is no longer running — it was preempted or failed
		log.Info("Instance interrupted/terminated",
			"claim", claim.Name,
			"provider", claim.Status.Provider,
			"instanceID", claim.Status.InstanceID,
			"status", instance.Status,
		)

		r.handleInterruption(ctx, claim)
	}
}

func (r *InterruptionController) handleInterruption(ctx context.Context, claim *v1alpha1.GPUNodeClaim) {
	log := r.Log.WithValues("claim", claim.Name)

	// For full-node: cordon and delete the K8s node if it still exists
	if claim.Status.NodeName != "" {
		var node corev1.Node
		if err := r.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, &node); err == nil {
			if !node.Spec.Unschedulable {
				node.Spec.Unschedulable = true
				if err := r.Update(ctx, &node); err != nil {
					log.Error(err, "Failed to cordon interrupted node")
				} else {
					log.Info("Cordoned interrupted node")
				}
			}

			// Delete the node object
			if err := r.Delete(ctx, &node); err != nil {
				log.Error(err, "Failed to delete interrupted node")
			}
		}
	}

	// Update claim status
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "Interrupted",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "ProviderInterruption",
		Message:            "Instance was preempted or terminated by the provider",
	})
	if err := r.Status().Update(ctx, claim); err != nil {
		log.Error(err, "Failed to update claim status after interruption")
	}

	log.Info("Interruption handling complete — pods will re-trigger provisioning")
}

// SetupWithManager registers this controller as a Runnable (background loop).
func (r *InterruptionController) SetupWithManager(mgr ctrl.Manager) error {
	return mgr.Add(r)
}
