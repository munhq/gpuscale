package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/internal/bootstrap"
	"github.com/munhq/gpuscale/internal/coordinator"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const claimFinalizer = "gpuscale.io/instance-cleanup"

// ClaimReconciler manages the lifecycle of GPUNodeClaims.
// Supports two bootstrap paths:
//   - full-node: VPN + Kubernetes agent join, waits for node to appear
//   - ray-worker: standalone vLLM, direct HTTP health check
//
// Lifecycle: Pending → Provisioning → Bootstrapping → Ready → Draining → Terminated
type ClaimReconciler struct {
	client.Client
	Log           logr.Logger
	Registry      *provider.Registry
	Coordinator   *coordinator.Coordinator
	HealthChecker *bootstrap.NodeHealthChecker

	// SecretReader is used to fetch bootstrap secrets.
	SecretReader SecretReader

	// WorkerStore writes claim status to Dragonfly for observability.
	// Nil if Dragonfly integration is disabled.
	WorkerStore *WorkerStore

	// DemandStore reads/writes model state in Dragonfly DB 3.
	// Used to set/remove loaded_models entries on Ready/Terminated.
	DemandStore *DemandStore

	// RayHeadAddress is the fallback Ray head GCS address (e.g., "1.2.3.4:31637")
	// used when the pool's rayConfig.headAddress is empty.
	// Set from RAY_HEAD_ADDRESS env var.
	RayHeadAddress string

	// RayCapacityStore queries Ray Serve app status to confirm model readiness.
	// Used in handleReady to gate SetModelLoaded until the model is actually RUNNING.
	RayCapacityStore *RayCapacityStore
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

	// Handle deletion: finalizer ensures we destroy the provider instance.
	if !claim.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &claim, log)
	}

	// Ensure finalizer is set on new claims.
	if !controllerutil.ContainsFinalizer(&claim, claimFinalizer) {
		controllerutil.AddFinalizer(&claim, claimFinalizer)
		if err := r.Update(ctx, &claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch claim.Status.Phase {
	case v1alpha1.ClaimPhasePending:
		return r.handlePending(ctx, &claim, log)
	case v1alpha1.ClaimPhaseProvisioning:
		return r.handleProvisioning(ctx, &claim, log)
	case v1alpha1.ClaimPhaseBootstrapping:
		return r.handleBootstrapping(ctx, &claim, log)
	case v1alpha1.ClaimPhaseReady:
		return r.handleReady(ctx, &claim, log)
	case v1alpha1.ClaimPhaseHibernated:
		return r.handleHibernated(ctx, &claim, log)
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

	// Determine provider list: if claim already specifies a provider (legacy),
	// constrain to that; otherwise use all pool providers.
	providerNames := make([]string, 0, len(pool.Spec.Providers))
	if claim.Spec.Provider != "" {
		providerNames = append(providerNames, claim.Spec.Provider)
	} else {
		for _, p := range pool.Spec.Providers {
			providerNames = append(providerNames, p.Name)
		}
	}

	// Build bootstrap config
	nodeType := resolveClaimNodeType(claim)
	config := provider.BootstrapConfig{
		NodeType:      nodeType,
		Image:         pool.Spec.Bootstrap.Image,
		ModelCacheURL: pool.Spec.Bootstrap.ModelCacheURL,
		InstanceID:    claim.Name,
		GPUType:       claim.Status.GPUType,
		MinDisk:       claim.Spec.Requirements.MinDisk,
	}

	// Full-node specific: read VPN and Kubernetes join secrets, and populate model sources
	// for background pre-download during bootstrap.
	// Key names match what the Ansible argocd role writes into gpuscale-provider-credentials:
	//   netbird-setup-key  → vault_gpu_netbird_setup_key
	//   k8s-join-token     → /var/lib/rancher/k3s/server/node-token
	//   k8s-join-url       → https://<wt0-ip>:6443 (Netbird VPN IP of utility-server)
	if nodeType == "full-node" {
		netbirdKey, err := r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.VPNSetupKeySecret, "netbird-setup-key")
		if err != nil {
			log.Error(err, "Failed to read VPN setup key")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		k8sToken, err := r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.K8sTokenSecret, "k8s-join-token")
		if err != nil {
			log.Error(err, "Failed to read Kubernetes join token")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// K8sURL: prefer pool spec (explicit override) but fall back to the secret
		// where Ansible stores it as k8s-join-url (same secret as the token).
		k8sURL := pool.Spec.Bootstrap.K8sURL
		if k8sURL == "" {
			k8sURL, err = r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.K8sTokenSecret, "k8s-join-url")
			if err != nil {
				log.Error(err, "Failed to read Kubernetes server URL from secret")
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
		}
		config.NetbirdKey = netbirdKey
		config.K8sURL = k8sURL
		config.K8sToken = k8sToken

		// Populate model sources for background pre-download during bootstrap.
		// Fetching from DemandStore ensures we get all co-located models for
		// bin-packed claims (which have multiple models in ModelIDs).
		if r.DemandStore != nil {
			for _, modelID := range claimModelIDs(claim) {
				cfg, err := r.DemandStore.GetModelConfig(ctx, modelID)
				if err == nil && cfg != nil && cfg.Source != "" {
					config.ModelSources = append(config.ModelSources, cfg.Source)
				}
			}
		}
	}

	// Ray-worker specific: populate model and ray config
	if nodeType == "ray-worker" {
		config.RayHeadAddr = r.resolveRayHeadAddress(&pool)
		if pool.Spec.Bootstrap.RayConfig != nil {
			config.RayDashPort = pool.Spec.Bootstrap.RayConfig.DashboardPort
			config.RayServePort = pool.Spec.Bootstrap.RayConfig.ServePort
		}
		if pool.Spec.Bootstrap.ModelConfig != nil {
			config.ModelID = pool.Spec.Bootstrap.ModelConfig.ModelID
			config.ModelSource = pool.Spec.Bootstrap.ModelConfig.ModelSource
			config.MaxModelLen = pool.Spec.Bootstrap.ModelConfig.MaxModelLen
			config.DType = pool.Spec.Bootstrap.ModelConfig.DType
			config.GPUMemUtil = pool.Spec.Bootstrap.ModelConfig.GPUMemoryUtilization
			config.TrustRemoteCode = pool.Spec.Bootstrap.ModelConfig.TrustRemoteCode
			config.EnablePrefixCaching = pool.Spec.Bootstrap.ModelConfig.EnablePrefixCaching
			config.MaxOngoingRequests = pool.Spec.Bootstrap.ModelConfig.MaxOngoingRequests
		}

		// For cold-start claims: ProvisionTrigger sets ModelID + ModelSource on the claim spec.
		// Use those when the pool's static ModelConfig didn't provide them.
		if config.ModelID == "" && claim.Spec.ModelID != "" {
			config.ModelID = claim.Spec.ModelID
		}
		if config.ModelSource == "" && claim.Spec.ModelSource != "" {
			config.ModelSource = claim.Spec.ModelSource
		}
	}

	// Ensure ModelID is set for all node types — providers use it to tag OS volumes
	// for model-aware reuse (e.g. Verda volume tracking in Redis).
	// The ray-worker block above sets it from pool config; full-node gets it here.
	if config.ModelID == "" {
		if claim.Spec.ModelID != "" {
			config.ModelID = claim.Spec.ModelID
		} else if ids := claimModelIDs(claim); len(ids) > 0 {
			config.ModelID = ids[0]
		}
	}

	reqs := provider.GPURequirements{
		GPUCount:       claim.Spec.Requirements.GPUCount,
		MinVRAM:        claim.Spec.Requirements.MinVRAM,
		MaxVRAM:        claim.Spec.Requirements.MaxVRAM,
		GPUTypes:       claim.Spec.Requirements.GPUTypes,
		MaxPricePerGPU: claim.Spec.Requirements.MaxPricePerGPU,
		CapacityType:   "spot",
		NodeType:       nodeType,
		MinPCIeGen:     pool.Spec.Requirements.MinPCIeGen,
		MultiGpu:       claim.Spec.Requirements.MultiGpu,
	}

	// Note: full-node bootstrap script is generated inside the coordinator
	// per-offer, since it needs ProviderName and GPUType from the selected offer.

	// Idempotent provisioning: check if we already created an instance on a previous
	// attempt (stored in annotations via merge patch). This prevents orphan instances
	// when a status update conflicts and the reconcile retries.
	existingInstanceID := claim.Annotations["gpuscale.io/instance-id"]
	existingProvider := claim.Annotations["gpuscale.io/provider"]

	var instanceID string
	var providerName string
	var gpuType string
	var gpuCount int
	var pricePerHour float64
	var endpoint string

	if existingInstanceID != "" {
		// Instance already created on a previous attempt — reuse it.
		log.Info("Reusing instance from previous attempt",
			"instanceID", existingInstanceID,
			"provider", existingProvider,
		)
		instanceID = existingInstanceID
		providerName = existingProvider
		// Recover offer details from the provider.
		if prov, ok := r.Registry.Get(providerName); ok {
			if inst, err := prov.GetInstance(ctx, existingInstanceID); err == nil {
				gpuType = inst.GPUType
				endpoint = inst.IP
			}
		}
	} else {
		// No existing instance — provision a new one.

		// Transition to Provisioning before calling coordinator.
		now := metav1.Now()
		claim.Status.Phase = v1alpha1.ClaimPhaseProvisioning
		claim.Status.ProvisionedAt = &now
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}

		result, err := r.Coordinator.ProvisionInstance(ctx, reqs, config, providerNames)
		if err != nil {
			log.Error(err, "Coordinator failed to provision instance")
			if fetchErr := r.Get(ctx, types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}, claim); fetchErr != nil {
				return ctrl.Result{}, fetchErr
			}
			claim.Status.Phase = v1alpha1.ClaimPhasePending
			claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
				Type:               "ProvisionFailed",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				Reason:             "CreateInstanceFailed",
				Message:            err.Error(),
			})
			claim.Status.RetryCount++
			_ = r.Status().Update(ctx, claim)
			return ctrl.Result{RequeueAfter: r.backoffDuration(claim.Status.RetryCount)}, nil
		}

		instance := result.Instance
		offer := result.Offer
		instanceID = instance.InstanceID
		providerName = instance.ProviderName
		gpuType = offer.GPUType
		gpuCount = offer.GPUCount
		pricePerHour = offer.PricePerHour
		endpoint = instance.Endpoint

		log.Info("Instance created via coordinator",
			"provider", providerName,
			"instanceID", instanceID,
			"gpu", gpuType,
			"nodeType", nodeType,
			"attempts", result.Attempts,
		)

		// Immediately persist instanceID via merge patch — this CANNOT conflict.
		// On retry, we'll find this annotation and skip provisioning.
		patch := client.MergeFrom(claim.DeepCopy())
		if claim.Annotations == nil {
			claim.Annotations = make(map[string]string)
		}
		claim.Annotations["gpuscale.io/instance-id"] = instanceID
		claim.Annotations["gpuscale.io/provider"] = providerName
		claim.Annotations["gpuscale.io/offer-id"] = result.Offer.OfferID
		if err := r.Patch(ctx, claim, patch); err != nil {
			log.Error(err, "Failed to persist instance annotation — instance may be orphaned",
				"instanceID", instanceID, "provider", providerName)
			// Don't return error — fall through to status update which might succeed.
		}
	}

	// Re-fetch claim to get latest resourceVersion.
	if err := r.Get(ctx, types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Update claim spec with the provider (if not already set).
	if claim.Spec.Provider == "" {
		claim.Spec.Provider = providerName
		if err := r.Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Get(ctx, types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}, claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Update claim status with instance details.
	successNow := metav1.Now()
	claim.Status.Provider = providerName
	if gpuType != "" {
		claim.Status.GPUType = gpuType
	}
	if gpuCount > 0 {
		claim.Status.GPUCount = gpuCount
	}
	if pricePerHour > 0 {
		claim.Status.PricePerHour = pricePerHour
	}
	claim.Status.InstanceID = instanceID
	claim.Status.Phase = v1alpha1.ClaimPhaseBootstrapping
	claim.Status.RetryCount = 0
	if endpoint != "" {
		claim.Status.Endpoint = endpoint
	}
	if claim.Status.ProvisionedAt == nil {
		claim.Status.ProvisionedAt = &successNow
	}
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "InstanceCreated",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: successNow,
		Reason:             "InstanceCreated",
		Message:            fmt.Sprintf("Instance %s created on %s", instanceID, providerName),
	})
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *ClaimReconciler) handleProvisioning(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	// If instanceID is on status, move to Bootstrapping.
	if claim.Status.InstanceID != "" {
		claim.Status.Phase = v1alpha1.ClaimPhaseBootstrapping
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// If instanceID is in annotations (created but status update failed), go back to
	// Pending so handlePending picks up the annotation and completes the status update.
	if id := claim.Annotations["gpuscale.io/instance-id"]; id != "" {
		log.Info("Instance exists in annotation but not status, retrying status update",
			"instanceID", id)
		claim.Status.Phase = v1alpha1.ClaimPhasePending
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// No instance anywhere — genuinely failed. Go back to Pending for retry.
	log.Info("No instance found, returning to Pending for retry")
	claim.Status.Phase = v1alpha1.ClaimPhasePending
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *ClaimReconciler) handleBootstrapping(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	providerName := resolveClaimProvider(claim)
	prov, ok := r.Registry.Get(providerName)
	if !ok {
		log.Error(fmt.Errorf("provider %q not found", providerName), "Provider not registered")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Check instance is still running
	instance, err := prov.GetInstance(ctx, claim.Status.InstanceID)
	if err != nil {
		if errors.Is(err, provider.ErrInstanceNotFound) {
			log.Info("Instance no longer exists on provider, terminating claim",
				"instanceID", claim.Status.InstanceID, "provider", providerName)
			claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
			now := metav1.Now()
			claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
				Type:               "BootstrapFailed",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "InstanceGone",
				Message:            "Instance no longer exists on the provider",
			})
			if err := r.Status().Update(ctx, claim); err != nil {
				log.Error(err, "Failed to update claim status to Terminated")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			if r.WorkerStore != nil {
				_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
			}
			r.retriggerIfDemandExists(ctx, claim, log)
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to check instance status")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Detect fatal errors in StatusMsg even when status is still "starting".
	// Vast.ai can report OCI runtime errors while ActualStatus is still "created".
	instanceFailed := instance.Status == "stopped" || instance.Status == "error"

	// For containers, "Error" in StatusMsg while starting is suspicious.
	// For KVM VMs, the boot process is much longer and transient errors are normal.
	nodeType := resolveClaimNodeType(claim)
	if !instanceFailed && instance.StatusMsg != "" && strings.Contains(instance.StatusMsg, "Error") && nodeType != "full-node" {
		instanceFailed = true
	}

	if instanceFailed {
		// Skip grace period for known fatal errors that will never self-resolve.
		// CDI/OCI runtime errors indicate a broken GPU driver config on the host.
		fatalError := strings.Contains(instance.StatusMsg, "OCI runtime create failed") ||
			strings.Contains(instance.StatusMsg, "CDI devices")

		// Grace period: don't terminate during early bootstrap.
		// KVM VMs need longer (5 min boot + package install). Containers need 2 min.
		gracePeriod := 2 * time.Minute
		if nodeType == "full-node" {
			gracePeriod = 8 * time.Minute
		}
		if !fatalError && claim.Status.ProvisionedAt != nil && time.Since(claim.Status.ProvisionedAt.Time) < gracePeriod {
			log.Info("Instance reports error but within grace period, retrying",
				"status", instance.Status,
				"statusMsg", instance.StatusMsg,
				"age", time.Since(claim.Status.ProvisionedAt.Time).String(),
			)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		log.Info("Instance failed during bootstrap, destroying", "status", instance.Status, "statusMsg", instance.StatusMsg)
		if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
			log.Error(err, "Failed to destroy failed instance", "instanceID", claim.Status.InstanceID)
		}
		// Blacklist the offer so the coordinator won't pick it again.
		if offerID := claim.Annotations["gpuscale.io/offer-id"]; offerID != "" && r.Coordinator != nil {
			r.Coordinator.BlacklistOffer(providerName, offerID)
		}
		claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
		now := metav1.Now()
		claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
			Type:               "BootstrapFailed",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "InstanceDied",
			Message:            fmt.Sprintf("Instance status: %s", instance.Status),
		})
		if err := r.Status().Update(ctx, claim); err != nil {
			log.Error(err, "Failed to update claim status to Terminated")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if r.WorkerStore != nil {
			_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
		}
		r.retriggerIfDemandExists(ctx, claim, log)
		return ctrl.Result{}, nil
	}

	// Branch based on node type
	nodeType = resolveClaimNodeType(claim)
	if nodeType == "ray-worker" {
		return r.handleBootstrappingRayWorker(ctx, claim, instance, prov, log)
	}
	return r.handleBootstrappingFullNode(ctx, claim, prov, log)
}

// handleBootstrappingFullNode waits for the node to join the Kubernetes cluster.
func (r *ClaimReconciler) handleBootstrappingFullNode(ctx context.Context, claim *v1alpha1.GPUNodeClaim, prov provider.Provider, log logr.Logger) (ctrl.Result, error) {
	node, err := r.findNodeByInstanceID(ctx, claim.Name)
	if err != nil {
		log.Error(err, "Error checking for node")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if node == nil {
		if r.isBootstrapTimedOut(claim) {
			return r.terminateTimedOut(ctx, claim, prov, log)
		}
		log.Info("Waiting for node to join cluster")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !bootstrap.IsNodeReady(node) {
		log.Info("Node joined but not yet Ready")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Node is Ready!
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseReady
	claim.Status.NodeName = node.Name
	claim.Status.NodeType = claim.Spec.NodeType
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

	// Publish worker status to Dragonfly
	if err := r.WorkerStore.SetWorker(ctx, claim); err != nil {
		log.Error(err, "Failed to publish worker to Dragonfly")
	}

	// Mark the volume for this instance as successfully bootstrapped so it is
	// eligible for reuse on next cold start. Volumes from failed instances are
	// never marked ready and will not be reused.
	if r.DemandStore != nil && claim.Status.InstanceID != "" {
		if err := r.DemandStore.MarkVolumeReady(ctx, claim.Status.InstanceID); err != nil {
			log.Error(err, "Failed to mark volume ready")
		}
	}

	// Node is Ready but the model is NOT loaded yet — KubeRay still needs to
	// create a worker pod and Ray Serve needs to deploy the model.
	// Publish a "try drain" event so GPU API's queue processor sends a request
	// to Ray Serve, which triggers autoscaling. The actual loaded_models key
	// is set in handleReady once we confirm the Ray Serve app is RUNNING.
	// Notify all co-located models (multi-model bin-packed claims).
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			r.DemandStore.PublishModelAvailable(ctx, modelID)
		}
		log.Info("Published model available for all co-located models (triggering queue drain attempt)",
			"models", claimModelIDs(claim))
	}

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

// handleBootstrappingRayWorker checks if the ray-worker has joined the Ray cluster.
// Ray workers don't run their own HTTP server — they connect to the Ray head via GCS.
// We check the Ray head dashboard API (/api/cluster_status) for the worker's presence.
func (r *ClaimReconciler) handleBootstrappingRayWorker(ctx context.Context, claim *v1alpha1.GPUNodeClaim, instance *provider.Instance, prov provider.Provider, log logr.Logger) (ctrl.Result, error) {
	// Store instance IP on claim for tracking
	if claim.Status.Endpoint == "" && instance.IP != "" {
		claim.Status.Endpoint = instance.IP // Store raw IP (no port — no HTTP server on worker)
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Get the pool to read Ray head config
	var pool v1alpha1.GPUNodePool
	if err := r.Get(ctx, types.NamespacedName{Name: claim.Spec.PoolRef}, &pool); err != nil {
		log.Error(err, "Failed to get pool for ray head address")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Discover the Ray head service inside the cluster for dashboard access.
	rayDashURL := r.buildRayDashboardURL(ctx, &pool, claim.Namespace)
	if rayDashURL == "" {
		if r.isBootstrapTimedOut(claim) {
			return r.terminateTimedOut(ctx, claim, prov, log)
		}
		log.Info("No Ray head address configured, cannot health-check worker")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Check if the worker has joined the Ray cluster via dashboard API.
	// Match by instance ID label (set in bootstrap --labels), not by IP,
	// since cloud instances report container-internal IPs.
	// The bootstrap script sets gpuscale.io/instance-id label to config.InstanceID,
	// which is claim.Name (not the provider instance ID). Match on that.
	labelInstanceID := claim.Name
	joined := r.checkRayWorkerJoined(ctx, rayDashURL, labelInstanceID, log)
	if !joined {
		if r.isBootstrapTimedOut(claim) {
			return r.terminateTimedOut(ctx, claim, prov, log)
		}
		log.Info("Waiting for ray-worker to join cluster", "instanceID", instance.InstanceID, "labelID", labelInstanceID, "rayDash", rayDashURL)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Worker has joined the Ray cluster
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseReady
	claim.Status.ReadyAt = &now
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "WorkerReady",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "RayWorkerJoined",
		Message:            "Ray worker joined the cluster",
	})

	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Publish worker status to Dragonfly
	if err := r.WorkerStore.SetWorker(ctx, claim); err != nil {
		log.Error(err, "Failed to publish worker to Dragonfly")
	}

	// Worker joined Ray but the model may not be deployed yet.
	// Publish "try drain" event; actual loaded_models set in handleReady
	// once Ray Serve confirms RUNNING.
	// Notify all co-located models (multi-model bin-packed claims).
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			r.DemandStore.PublishModelAvailable(ctx, modelID)
		}
		log.Info("Published model available for all co-located models (triggering queue drain attempt)",
			"models", claimModelIDs(claim))
	}

	if claim.Status.ProvisionedAt != nil {
		duration := now.Time.Sub(claim.Status.ProvisionedAt.Time)
		log.Info("Worker is Ready!",
			"bootstrapDuration", duration.String(),
			"provider", claim.Status.Provider,
			"gpu", claim.Status.GPUType,
		)
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// handleHibernated destroys any claim left in the Hibernated phase.
// Hibernation is no longer used — idle instances are always destroyed.
// This handles cleanup of any claims that were hibernated before this policy change.
func (r *ClaimReconciler) handleHibernated(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	log.Info("Destroying hibernated claim (hibernation no longer used)", "claim", claim.Name, "instanceID", claim.Status.InstanceID)
	return r.destroyHibernatedClaim(ctx, claim, log)
}

// handleReady periodically checks that the provider instance backing a Ready claim
// is still alive. If the instance has been destroyed (host reboot, provider kill,
// non-spot termination), the claim is terminated and provisioning retriggered.
func (r *ClaimReconciler) handleReady(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	providerName := resolveClaimProvider(claim)
	prov, ok := r.Registry.Get(providerName)
	if !ok {
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	instance, err := prov.GetInstance(ctx, claim.Status.InstanceID)
	if err != nil {
		if errors.Is(err, provider.ErrInstanceNotFound) {
			log.Info("Ready instance no longer exists on provider, terminating",
				"instanceID", claim.Status.InstanceID)
			claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
			now := metav1.Now()
			claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
				Type:               "InstanceLost",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "InstanceGone",
				Message:            "Instance no longer exists on the provider",
			})
			_ = r.Status().Update(ctx, claim)
			if r.WorkerStore != nil {
				_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
			}
			if r.DemandStore != nil {
				for _, modelID := range claimModelIDs(claim) {
					if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
						_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
					}
				}
			}
			r.retriggerIfDemandExists(ctx, claim, log)
			return ctrl.Result{}, nil
		}
		// Transient error — don't terminate, just retry
		log.V(1).Info("Failed to check Ready instance status", "error", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Check if instance has died (stopped/error)
	if instance.Status == "stopped" || instance.Status == "error" {
		log.Info("Ready instance died on provider, terminating",
			"instanceID", claim.Status.InstanceID, "status", instance.Status, "statusMsg", instance.StatusMsg)
		if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
			log.Error(err, "Failed to destroy dead instance", "instanceID", claim.Status.InstanceID)
		}
		claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
		now := metav1.Now()
		claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
			Type:               "InstanceLost",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "InstanceDied",
			Message:            fmt.Sprintf("Instance status: %s", instance.Status),
		})
		_ = r.Status().Update(ctx, claim)
		if r.WorkerStore != nil {
			_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
		}
		if r.DemandStore != nil {
			for _, modelID := range claimModelIDs(claim) {
				if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
					_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
				}
			}
		}
		r.retriggerIfDemandExists(ctx, claim, log)
		return ctrl.Result{}, nil
	}

	// Node-type-specific health check.
	// For ray-worker: verify Ray process is still connected to the cluster.
	// For full-node: verify the K8s node is still Ready (K8s itself tracks this).
	nodeType := resolveClaimNodeType(claim)
	if nodeType == "ray-worker" {
		var pool v1alpha1.GPUNodePool
		if err := r.Get(ctx, types.NamespacedName{Name: claim.Spec.PoolRef}, &pool); err != nil {
			log.Error(err, "Failed to get pool for Ray health check")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		rayDashURL := r.buildRayDashboardURL(ctx, &pool, claim.Namespace)
		if rayDashURL != "" {
			workerAlive := r.checkRayWorkerJoined(ctx, rayDashURL, claim.Name, log)
			if !workerAlive {
				log.Info("Ray worker disconnected from cluster, terminating claim",
					"instanceID", claim.Status.InstanceID, "rayDash", rayDashURL)
				if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
					log.Error(err, "Failed to destroy instance with dead Ray worker", "instanceID", claim.Status.InstanceID)
				}
				claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
				now := metav1.Now()
				claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
					Type:               "WorkerLost",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "RayWorkerDisconnected",
					Message:            "Ray worker no longer connected to cluster",
				})
				_ = r.Status().Update(ctx, claim)
				if r.WorkerStore != nil {
					_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
				}
				if r.DemandStore != nil {
					for _, modelID := range claimModelIDs(claim) {
						if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
							_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
						}
					}
				}
				r.retriggerIfDemandExists(ctx, claim, log)
				return ctrl.Result{}, nil
			}
		}
	} else if nodeType == "full-node" && claim.Status.NodeName != "" {
		// For full-node: check that the K8s node is still Ready.
		// If the VM died, the node will go NotReady which we catch here.
		node, err := r.findNodeByInstanceID(ctx, claim.Name)
		if err == nil && node != nil && !bootstrap.IsNodeReady(node) {
			log.Info("Full-node K8s node is NotReady, terminating claim",
				"node", claim.Status.NodeName, "instanceID", claim.Status.InstanceID)
			if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
				log.Error(err, "Failed to destroy instance with NotReady node")
			}
			claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
			now := metav1.Now()
			claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
				Type:               "NodeLost",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "NodeNotReady",
				Message:            "K8s node is no longer Ready",
			})
			_ = r.Status().Update(ctx, claim)
			if r.WorkerStore != nil {
				_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
			}
			if r.DemandStore != nil {
				for _, modelID := range claimModelIDs(claim) {
					if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
						_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
					}
				}
			}
			r.retriggerIfDemandExists(ctx, claim, log)
			return ctrl.Result{}, nil
		}
	}

	// Check if the models are actually serving in Ray Serve.
	// SetModelLoaded is NOT called at transition-to-Ready (it was premature there).
	// Instead, we check here on every 60s tick until each model is confirmed RUNNING.
	// For multi-model bin-packed claims we check all co-located models.
	if r.DemandStore != nil && r.RayCapacityStore != nil {
		statuses, serveErr := r.RayCapacityStore.GetServeAppStatus(ctx)

		// If any model this claim is responsible for is DEPLOY_FAILED, re-submit
		// the serve config to reset Ray's retry counter. Ray stops retrying after
		// 3 failures and will never recover without an explicit re-submission.
		if serveErr == nil {
			for _, app := range statuses {
				for _, modelID := range claimModelIDs(claim) {
					if app.Name == modelID && app.Status == "DEPLOY_FAILED" {
						log.Info("Model is DEPLOY_FAILED, re-submitting serve config to reset retry counter",
							"model", modelID)
						if resubErr := r.RayCapacityStore.ResubmitServeConfig(ctx); resubErr != nil {
							log.Error(resubErr, "Failed to resubmit serve config")
						}
						goto doneResubmit
					}
				}
			}
		}
	doneResubmit:

		for _, modelID := range claimModelIDs(claim) {
			if r.DemandStore.IsModelLoaded(ctx, modelID) {
				continue
			}
			if serveErr == nil {
				for _, app := range statuses {
					if app.Name == modelID && app.Status == "RUNNING" {
						info := LoadedModelInfo{
							ClaimName:  claim.Name,
							Provider:   claim.Status.Provider,
							GPUType:    claim.Status.GPUType,
							GPUCount:   claim.Status.GPUCount,
							InstanceID: claim.Status.InstanceID,
							ReadyAt:    time.Now().Format(time.RFC3339),
						}
						if err := r.DemandStore.SetModelLoaded(ctx, modelID, info); err != nil {
							log.Error(err, "Failed to set model as loaded", "model", modelID)
						} else {
							log.Info("Model confirmed RUNNING in Ray Serve, marked as loaded",
								"model", modelID)
						}
						break
					}
				}
			}
			// Model not RUNNING yet — re-publish availability so queue processor retries
			if !r.DemandStore.IsModelLoaded(ctx, modelID) {
				r.DemandStore.PublishModelAvailable(ctx, modelID)
			}
		}
		if len(claimModelIDs(claim)) > 0 {
			log.Info("Published model available for co-located models", "models", claimModelIDs(claim))
		}
	}

	// --- Fractional GPU allocation for multi-model bin-packed claims ---
	// When multiple models share a single GPU node, each vLLM process must use only
	// its proportional share of GPU memory. We compute fractions from the actual GPU
	// VRAM reported by nvidia-device-plugin (nvidia.com/gpu.memory label, in MiB)
	// and patch Ray Serve's live config on every tick.
	// This corrects any resets caused by ArgoCD re-syncing the Helm chart values.
	if len(claimModelIDs(claim)) > 1 && r.RayCapacityStore != nil && r.DemandStore != nil && claim.Status.NodeName != "" {
		r.patchFractionalGPUAllocation(ctx, claim, log)
	}

	// --- Consolidation drain ---
	// If this is a bin-packed claim that now has all co-located models loaded,
	// annotate any older single-model claims for those models so DisruptionController
	// drains them immediately (they are being replaced by this more efficient node).
	if len(claimModelIDs(claim)) > 1 && r.DemandStore != nil {
		r.triggerConsolidationDrain(ctx, claim, log)
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// patchFractionalGPUAllocation reads the actual GPU VRAM from the K8s node labels
// and calls PatchServeModelFractions so each vLLM replica uses its correct share.
// Safe to call on every 60s tick — it is idempotent and non-fatal on failure.
func (r *ClaimReconciler) patchFractionalGPUAllocation(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) {
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, &node); err != nil {
		return // node not found yet, try again next tick
	}

	memMiBStr, ok := node.Labels["nvidia.com/gpu.memory"]
	if !ok || memMiBStr == "" {
		return // nvidia-device-plugin label not yet set
	}

	memMiB := 0
	if _, err := fmt.Sscanf(memMiBStr, "%d", &memMiB); err != nil || memMiB == 0 {
		log.Error(err, "Failed to parse nvidia.com/gpu.memory label", "value", memMiBStr)
		return
	}
	totalGPUVRAM := memMiB / 1024 // MiB → GB (integer division is fine; 81920 → 80, 98304 → 96)
	if totalGPUVRAM == 0 {
		return
	}

	fractions := make(map[string]float64)
	for _, modelID := range claimModelIDs(claim) {
		cfg, err := r.DemandStore.GetModelConfig(ctx, modelID)
		if err != nil || cfg == nil || cfg.VRAMRequired == 0 {
			continue
		}
		frac := float64(cfg.VRAMRequired) / float64(totalGPUVRAM)
		if frac > 0.95 {
			frac = 0.95 // safety cap — leave 5% for system overhead
		}
		fractions[modelID] = frac
	}

	if len(fractions) == 0 {
		return
	}

	if err := r.RayCapacityStore.PatchServeModelFractions(ctx, fractions); err != nil {
		log.Error(err, "Failed to patch Ray Serve GPU fractions")
		return
	}
	log.Info("Patched Ray Serve GPU memory fractions for co-located models",
		"node", claim.Status.NodeName, "gpuVRAM", totalGPUVRAM, "fractions", fractions)
}

// triggerConsolidationDrain checks if this bin-packed claim now has all co-located
// models loaded in Ray Serve. If so, it annotates any older single-model claims
// for those same models with annotationConsolidationDrain so the DisruptionController
// drains them immediately — they are being replaced by this more efficient shared node.
func (r *ClaimReconciler) triggerConsolidationDrain(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) {
	allLoaded := true
	for _, modelID := range claimModelIDs(claim) {
		if !r.DemandStore.IsModelLoaded(ctx, modelID) {
			allLoaded = false
			break
		}
	}
	if !allLoaded {
		return // bin-packed claim not fully loaded yet; don't drain old nodes
	}

	// Find single-model claims that serve any of our co-located models.
	var claimList v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claimList, client.InNamespace(claimNamespace())); err != nil {
		return
	}

	for i := range claimList.Items {
		c := &claimList.Items[i]
		if c.Name == claim.Name {
			continue // don't drain ourselves
		}
		if c.Status.Phase == v1alpha1.ClaimPhaseTerminated ||
			c.Status.Phase == v1alpha1.ClaimPhaseDraining ||
			c.Status.Phase == v1alpha1.ClaimPhaseHibernated {
			continue
		}
		// Only drain single-model claims (len == 1 means it's not already bin-packed).
		if len(claimModelIDs(c)) != 1 {
			continue
		}
		// Check if this single-model claim serves a model we now cover.
		for _, modelID := range claimModelIDs(claim) {
			if c.Spec.ModelID != modelID && !slices.Contains(c.Spec.ModelIDs, modelID) {
				continue
			}
			if c.Annotations[annotationConsolidationDrain] == "true" {
				break // already annotated
			}
			log.Info("Annotating single-model claim for consolidation drain",
				"claim", c.Name, "model", modelID, "replacedBy", claim.Name)
			if c.Annotations == nil {
				c.Annotations = make(map[string]string)
			}
			c.Annotations[annotationConsolidationDrain] = "true"
			if err := r.Update(ctx, c); err != nil {
				log.Error(err, "Failed to annotate claim for consolidation drain", "claim", c.Name)
			}
			break
		}
	}
}

// resolveRayHeadAddress returns the Ray head address from the pool spec,
// falling back to the controller's RAY_HEAD_ADDRESS env var.
func (r *ClaimReconciler) resolveRayHeadAddress(pool *v1alpha1.GPUNodePool) string {
	if pool.Spec.Bootstrap.RayConfig != nil && pool.Spec.Bootstrap.RayConfig.HeadAddress != "" {
		return pool.Spec.Bootstrap.RayConfig.HeadAddress
	}
	return r.RayHeadAddress
}

// buildRayDashboardURL discovers the Ray head service inside the cluster and
// returns its dashboard URL. The dashboard (port 8265) is only reachable via
// the cluster-internal service — the external RayHeadAddress exposes only the
// GCS NodePort for workers joining from outside.
//
// Discovery: list Services with label ray.io/node-type=head in the claim's
// namespace, find the port named "dashboard", build the cluster-DNS URL.
func (r *ClaimReconciler) buildRayDashboardURL(ctx context.Context, pool *v1alpha1.GPUNodePool, namespace string) string {
	// Allow pool-level override.
	dashPort := 0
	if pool.Spec.Bootstrap.RayConfig != nil && pool.Spec.Bootstrap.RayConfig.DashboardPort != 0 {
		dashPort = pool.Spec.Bootstrap.RayConfig.DashboardPort
	}

	var svcList corev1.ServiceList
	if err := r.List(ctx, &svcList,
		client.InNamespace(namespace),
		client.MatchingLabels{"ray.io/node-type": "head"},
	); err != nil {
		r.Log.Error(err, "Failed to list Ray head services")
		return ""
	}

	if len(svcList.Items) == 0 {
		r.Log.Info("No Ray head service found", "namespace", namespace)
		return ""
	}

	svc := svcList.Items[0]

	// Find the dashboard port from the service spec if not overridden.
	if dashPort == 0 {
		for _, p := range svc.Spec.Ports {
			if p.Name == "dashboard" {
				dashPort = int(p.Port)
				break
			}
		}
	}
	if dashPort == 0 {
		dashPort = 8265 // last-resort default
	}

	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, dashPort)
}

// checkRayWorkerJoined queries the Ray dashboard API to see if a worker with the
// given instance ID has joined the cluster. Workers register with a
// "gpuscale.io/instance-id" label set by the bootstrap script (ray start --labels).
// We match on that label rather than IP, since cloud instances report container-
// internal IPs that don't match the SSH proxy hostname from the provider API.
func (r *ClaimReconciler) checkRayWorkerJoined(ctx context.Context, rayDashURL string, instanceID string, log logr.Logger) bool {
	if instanceID == "" {
		return false
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rayDashURL+"/nodes?view=summary", nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.V(1).Info("Ray dashboard unreachable", "url", rayDashURL, "error", err.Error())
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var result struct {
		Data struct {
			Summary []struct {
				IP     string `json:"ip"`
				State  string `json:"state"`
				Raylet struct {
					Labels map[string]string `json:"labels"`
				} `json:"raylet"`
			} `json:"summary"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.V(1).Info("Failed to parse Ray dashboard response", "error", err.Error())
		return false
	}

	for _, node := range result.Data.Summary {
		labelID := node.Raylet.Labels["gpuscale.io/instance-id"]
		// Vast.ai's VAST_CONTAINERLABEL prepends "C." to the instance ID.
		// Match if the label equals the instanceID or contains it as a suffix.
		matched := labelID == instanceID || strings.HasSuffix(labelID, "."+instanceID)
		// State may be "ALIVE" or empty/nil for nodes that just joined.
		// A node with our label in the summary is alive regardless of state field.
		if matched {
			log.Info("Ray worker joined cluster", "instanceID", instanceID, "labelID", labelID, "nodeIP", node.IP, "state", node.State)
			return true
		}
	}
	return false
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

func (r *ClaimReconciler) isBootstrapTimedOut(claim *v1alpha1.GPUNodeClaim) bool {
	if claim.Status.ProvisionedAt == nil {
		return false
	}
	// ray-worker images (rayproject/ray-llm) are 15GB+ and take longer to pull.
	timeout := 10 * time.Minute
	if claim.Spec.NodeType == "ray-worker" {
		timeout = 15 * time.Minute
	}
	return time.Since(claim.Status.ProvisionedAt.Time) > timeout
}

func (r *ClaimReconciler) terminateTimedOut(ctx context.Context, claim *v1alpha1.GPUNodeClaim, prov provider.Provider, log logr.Logger) (ctrl.Result, error) {
	log.Info("Bootstrap timeout exceeded")
	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	now := metav1.Now()
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "BootstrapTimeout",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "Timeout",
		Message:            "Instance did not become ready within 10 minutes",
	})
	_ = r.Status().Update(ctx, claim)
	_ = prov.DestroyInstance(ctx, claim.Status.InstanceID)

	// Remove from Dragonfly
	if r.WorkerStore != nil {
		if err := r.WorkerStore.RemoveWorker(ctx, claim.Name); err != nil {
			log.Error(err, "Failed to remove worker from Dragonfly")
		}
	}

	// Remove loaded_models entry for all co-located models if this was the last claim.
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
				_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
			}
		}
	}

	r.retriggerIfDemandExists(ctx, claim, log)
	return ctrl.Result{}, nil
}

// destroyHibernatedClaim destroys the provider instance for an expired Hibernated claim
// and transitions it to Terminated. Called when the hibernation TTL is exceeded.
func (r *ClaimReconciler) destroyHibernatedClaim(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	if claim.Status.InstanceID != "" && claim.Status.Provider != "" {
		prov, ok := r.Registry.Get(claim.Status.Provider)
		if ok {
			if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
				log.Error(err, "Failed to destroy expired hibernated instance", "instanceID", claim.Status.InstanceID)
				// Don't block termination — log and fall through.
			} else {
				log.Info("Destroyed expired hibernated instance", "instanceID", claim.Status.InstanceID)
			}
		}
	}

	if r.WorkerStore != nil {
		_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
	}
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
				_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
			}
		}
	}

	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "HibernatedExpired",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "TTLExceeded",
		Message:            "Hibernated claim destroyed (hibernation no longer used)",
	})
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("terminating expired hibernated claim: %w", err)
	}
	log.Info("Hibernated claim expired and terminated", "claim", claim.Name, "models", claimModelIDs(claim))
	return ctrl.Result{}, nil
}

// handleDeletion destroys the provider instance before allowing the claim to be deleted.
func (r *ClaimReconciler) handleDeletion(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(claim, claimFinalizer) {
		return ctrl.Result{}, nil
	}

	// Destroy the provider instance if one was created.
	instanceID := claim.Status.InstanceID
	providerName := resolveClaimProvider(claim)
	if instanceID != "" && instanceID != "0" && providerName != "" {
		prov, ok := r.Registry.Get(providerName)
		if ok {
			if err := prov.DestroyInstance(ctx, instanceID); err != nil {
				log.Error(err, "Failed to destroy instance during deletion", "instanceID", instanceID, "provider", providerName)
				// Don't block deletion forever — log and proceed.
			} else {
				log.Info("Destroyed provider instance on claim deletion", "instanceID", instanceID, "provider", providerName)
			}
		} else {
			log.Error(fmt.Errorf("provider %q not found", providerName), "Cannot destroy instance, provider not registered")
		}
	}

	// Clean up Dragonfly worker entry.
	if r.WorkerStore != nil {
		_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
	}

	// Remove loaded_models entry for all co-located models if this was the last claim.
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			if !r.hasOtherReadyClaims(ctx, modelID, claim.Name) {
				_ = r.DemandStore.RemoveModelLoaded(ctx, modelID)
			}
		}
	}

	// Remove finalizer to allow K8s to delete the object.
	controllerutil.RemoveFinalizer(claim, claimFinalizer)
	if err := r.Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Finalizer completed, claim will be deleted")
	return ctrl.Result{}, nil
}

// resolveClaimProvider returns the provider name, preferring Spec over Status.
// Spec.Provider is set at creation time and survives status update failures.
func resolveClaimProvider(claim *v1alpha1.GPUNodeClaim) string {
	if claim.Spec.Provider != "" {
		return claim.Spec.Provider
	}
	return claim.Status.Provider
}

// resolveClaimNodeType returns the node type, preferring Spec over Status.
func resolveClaimNodeType(claim *v1alpha1.GPUNodeClaim) string {
	if claim.Spec.NodeType != "" {
		return claim.Spec.NodeType
	}
	return claim.Status.NodeType
}

// hasOtherReadyClaims returns true if there are other Ready claims serving the given modelID.
// It checks both ModelID (primary) and ModelIDs (co-located) fields.
func (r *ClaimReconciler) hasOtherReadyClaims(ctx context.Context, modelID string, excludeClaim string) bool {
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		return false
	}
	for _, c := range claims.Items {
		if c.Name == excludeClaim {
			continue
		}
		if c.Status.Phase != v1alpha1.ClaimPhaseReady {
			continue
		}
		if c.Spec.ModelID == modelID {
			return true
		}
		for _, mid := range c.Spec.ModelIDs {
			if mid == modelID {
				return true
			}
		}
	}
	return false
}

// retriggerIfDemandExists re-publishes a provision trigger when a claim terminates
// but there are still queued requests for any co-located model. Without this, requests
// would sit in the queue forever because the ProvisionTrigger only fires on new pub/sub events.
// For multi-model bin-packed claims, re-triggering on the primary model is sufficient:
// ProvisionTrigger will re-bundle co-located models when it creates a new claim.
func (r *ClaimReconciler) retriggerIfDemandExists(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) {
	if r.DemandStore == nil {
		return
	}
	for _, modelID := range claimModelIDs(claim) {
		queueDepth, err := r.DemandStore.GetQueueDepth(ctx, modelID)
		if err != nil {
			log.Error(err, "Failed to check queue depth for re-trigger", "model", modelID)
			continue
		}
		activeDemand, err := r.DemandStore.GetDemand(ctx, modelID)
		if err != nil {
			log.Error(err, "Failed to check active demand for re-trigger", "model", modelID)
			continue
		}
		if queueDepth > 0 || activeDemand > 0 {
			log.Info("Demand exists after claim termination, re-triggering provisioning",
				"model", modelID, "queueDepth", queueDepth, "activeDemand", activeDemand)
			if err := r.DemandStore.PublishProvisionTrigger(ctx, modelID); err != nil {
				log.Error(err, "Failed to publish re-trigger", "model", modelID)
			}
			// Re-triggering on the first model with demand is sufficient; ProvisionTrigger
			// will bundle all co-located models into a new shared claim.
			return
		}
	}
}

// claimModelIDs returns the canonical list of all model IDs served by a claim.
// For multi-model bin-packed claims, ModelIDs contains the full set.
// For single-model claims (ModelIDs empty), falls back to ModelID for backward compatibility.
func claimModelIDs(claim *v1alpha1.GPUNodeClaim) []string {
	if len(claim.Spec.ModelIDs) > 0 {
		return claim.Spec.ModelIDs
	}
	if claim.Spec.ModelID != "" {
		return []string{claim.Spec.ModelID}
	}
	return nil
}

// backoffDuration returns an exponential backoff duration based on retry count.
// The progression is: 30s, 60s, 120s, 240s, capped at 5 minutes.
func (r *ClaimReconciler) backoffDuration(retryCount int) time.Duration {
	base := 30 * time.Second
	d := base
	for i := 1; i < retryCount; i++ {
		d *= 2
	}
	const maxBackoff = 5 * time.Minute
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("claim-reconciler").
		For(&v1alpha1.GPUNodeClaim{}).
		Complete(r)
}
