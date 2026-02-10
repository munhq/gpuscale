package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/internal/bootstrap"
	"github.com/munhq/gpuscale/internal/provider"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClaimReconciler manages the lifecycle of GPUNodeClaims.
// It drives claims through: Pending → Provisioning → Bootstrapping → Ready → Draining → Terminated
type ClaimReconciler struct {
	client.Client
	Log           logr.Logger
	Registry      *provider.Registry
	HealthChecker *bootstrap.NodeHealthChecker

	// SecretReader is used to fetch bootstrap secrets.
	SecretReader SecretReader
}

// SecretReader fetches secret values needed for bootstrap.
type SecretReader interface {
	GetSecretValue(ctx context.Context, ref v1alpha1.SecretReference, key string) (string, error)
}

// K8sSecretReader reads secrets from the Kubernetes API.
type K8sSecretReader struct {
	client.Client
}

func (r *K8sSecretReader) GetSecretValue(ctx context.Context, ref v1alpha1.SecretReference, key string) (string, error) {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}, &secret); err != nil {
		return "", fmt.Errorf("getting secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	data, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s", key, ref.Namespace, ref.Name)
	}
	return string(data), nil
}

// NewClaimReconciler creates a new claim reconciler.
func NewClaimReconciler(c client.Client, log logr.Logger, reg *provider.Registry) *ClaimReconciler {
	return &ClaimReconciler{
		Client:        c,
		Log:           log,
		Registry:      reg,
		HealthChecker: bootstrap.NewNodeHealthChecker(c),
		SecretReader:  &K8sSecretReader{Client: c},
	}
}

// Reconcile drives the GPUNodeClaim lifecycle.
func (r *ClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("claim", req.NamespacedName)

	var claim v1alpha1.GPUNodeClaim
	if err := r.Get(ctx, req.NamespacedName, &claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch claim.Status.Phase {
	case v1alpha1.ClaimPhasePending:
		return r.handlePending(ctx, &claim, log)
	case v1alpha1.ClaimPhaseProvisioning:
		return r.handleProvisioning(ctx, &claim, log)
	case v1alpha1.ClaimPhaseBootstrapping:
		return r.handleBootstrapping(ctx, &claim, log)
	case v1alpha1.ClaimPhaseReady:
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	case v1alpha1.ClaimPhaseDraining, v1alpha1.ClaimPhaseTerminated:
		return ctrl.Result{}, nil
	default:
		// New claim without phase — set to Pending
		claim.Status.Phase = v1alpha1.ClaimPhasePending
		if err := r.Status().Update(ctx, &claim); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
}

func (r *ClaimReconciler) handlePending(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	log.Info("Claim is Pending, starting provisioning")

	// Get the pool to read bootstrap config
	var pool v1alpha1.GPUNodePool
	if err := r.Get(ctx, types.NamespacedName{Name: claim.Spec.PoolRef}, &pool); err != nil {
		log.Error(err, "Failed to get pool")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Read bootstrap secrets
	netbirdKey, err := r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.VPNSetupKeySecret, "setup-key")
	if err != nil {
		log.Error(err, "Failed to read Netbird setup key")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	k3sToken, err := r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.K3sTokenSecret, "token")
	if err != nil {
		log.Error(err, "Failed to read K3s token")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Get the provider
	prov, ok := r.Registry.Get(claim.Status.Provider)
	if !ok {
		log.Error(fmt.Errorf("provider %q not found", claim.Status.Provider), "Provider not registered")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Build bootstrap config
	instanceID := claim.Name // Use claim name as instance ID
	config := provider.BootstrapConfig{
		Image:         pool.Spec.Bootstrap.Image,
		NetbirdKey:    netbirdKey,
		K3sURL:        pool.Spec.Bootstrap.K3sURL,
		K3sToken:      k3sToken,
		ModelCacheURL: pool.Spec.Bootstrap.ModelCacheURL,
		InstanceID:    instanceID,
		GPUType:       claim.Status.GPUType,
		ProviderName:  claim.Status.Provider,
	}

	// Build an offer from the claim status (the provisioner already selected this)
	offer := provider.Offer{
		ProviderName: claim.Status.Provider,
		OfferID:      "", // Need to re-search if not stored
		GPUType:      claim.Status.GPUType,
		GPUCount:     claim.Status.GPUCount,
		PricePerHour: claim.Status.PricePerHour,
	}

	// Search for a matching offer to get the offer ID
	reqs := provider.GPURequirements{
		GPUCount:     claim.Spec.Requirements.GPUCount,
		MinVRAM:      claim.Spec.Requirements.MinVRAM,
		GPUTypes:     claim.Spec.Requirements.GPUTypes,
		MaxPrice:     claim.Spec.Requirements.MaxPrice,
		CapacityType: "spot",
	}

	offers, err := prov.SearchOffers(ctx, reqs)
	if err != nil || len(offers) == 0 {
		log.Error(err, "Failed to find offer for provisioning")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	offer = offers[0]

	// Transition to Provisioning
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseProvisioning
	claim.Status.ProvisionedAt = &now
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Create instance
	instance, err := prov.CreateInstance(ctx, offer, config)
	if err != nil {
		log.Error(err, "Failed to create instance")
		claim.Status.Phase = v1alpha1.ClaimPhasePending
		claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
			Type:               "ProvisionFailed",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "CreateInstanceFailed",
			Message:            err.Error(),
		})
		_ = r.Status().Update(ctx, claim)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	log.Info("Instance created",
		"provider", instance.ProviderName,
		"instanceID", instance.InstanceID,
		"gpu", instance.GPUType,
	)

	// Update claim with instance details
	claim.Status.InstanceID = instance.InstanceID
	claim.Status.Phase = v1alpha1.ClaimPhaseBootstrapping
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "InstanceCreated",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "InstanceCreated",
		Message:            fmt.Sprintf("Instance %s created on %s", instance.InstanceID, instance.ProviderName),
	})
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue to check bootstrapping status
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *ClaimReconciler) handleProvisioning(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	// If we have an instance ID, transition to bootstrapping
	if claim.Status.InstanceID != "" {
		claim.Status.Phase = v1alpha1.ClaimPhaseBootstrapping
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Otherwise, go back to Pending to retry
	claim.Status.Phase = v1alpha1.ClaimPhasePending
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *ClaimReconciler) handleBootstrapping(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	// Check if the instance is still running
	prov, ok := r.Registry.Get(claim.Status.Provider)
	if !ok {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	instance, err := prov.GetInstance(ctx, claim.Status.InstanceID)
	if err != nil {
		log.Error(err, "Failed to check instance status")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if instance.Status == "stopped" || instance.Status == "error" {
		log.Info("Instance failed during bootstrap", "status", instance.Status)
		claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
		now := metav1.Now()
		claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
			Type:               "BootstrapFailed",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "InstanceDied",
			Message:            fmt.Sprintf("Instance status: %s", instance.Status),
		})
		_ = r.Status().Update(ctx, claim)
		return ctrl.Result{}, nil
	}

	// Check if node has joined the cluster
	node, err := r.findNodeByInstanceID(ctx, claim.Name)
	if err != nil {
		log.Error(err, "Error checking for node")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if node == nil {
		// Node hasn't joined yet — check timeout
		if claim.Status.ProvisionedAt != nil {
			elapsed := time.Since(claim.Status.ProvisionedAt.Time)
			if elapsed > 10*time.Minute {
				log.Info("Bootstrap timeout exceeded", "elapsed", elapsed)
				claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
				now := metav1.Now()
				claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
					Type:               "BootstrapTimeout",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "Timeout",
					Message:            "Node did not join within 10 minutes",
				})
				_ = r.Status().Update(ctx, claim)

				// Clean up the instance
				_ = prov.DestroyInstance(ctx, claim.Status.InstanceID)
				return ctrl.Result{}, nil
			}
		}
		log.Info("Waiting for node to join cluster")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Node joined — check if it's ready
	if !bootstrap.IsNodeReady(node) {
		log.Info("Node joined but not yet Ready")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Node is Ready!
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseReady
	claim.Status.NodeName = node.Name
	claim.Status.ReadyAt = &now
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "NodeJoined",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "NodeReady",
		Message:            fmt.Sprintf("Node %s joined and is Ready", node.Name),
	})

	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Calculate bootstrap duration
	if claim.Status.ProvisionedAt != nil {
		duration := now.Time.Sub(claim.Status.ProvisionedAt.Time)
		log.Info("Node is Ready!",
			"node", node.Name,
			"bootstrapDuration", duration.String(),
			"provider", claim.Status.Provider,
			"gpu", claim.Status.GPUType,
		)
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *ClaimReconciler) findNodeByInstanceID(ctx context.Context, instanceID string) (*corev1.Node, error) {
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList, client.MatchingLabels{
		"gpuscale.io/instance-id": instanceID,
	}); err != nil {
		return nil, err
	}
	if len(nodeList.Items) == 0 {
		return nil, nil
	}
	return &nodeList.Items[0], nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("claim-reconciler").
		For(&v1alpha1.GPUNodeClaim{}).
		Complete(r)
}
