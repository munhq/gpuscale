package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/munhq/gpuscale/internal/bootstrap"
	"github.com/munhq/gpuscale/internal/coordinator"
	"github.com/munhq/gpuscale/pkg/provider"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const claimFinalizer = "gpuscale.io/instance-cleanup"

// ClaimReconciler manages the lifecycle of GPUNodeClaims.
// Bootstrap path: standalone — gpu-agent connects outbound WSS tunnel to GPU API.
// GPU API registers the node in NodeRegistry (Dragonfly: gpu_api:node:{claim.Name}).
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

	// ClaimWriter writes claim lifecycle events to Postgres for history.
	// Nil-safe: all writes are no-ops when unset.
	ClaimWriter *ClaimWriter
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
	case v1alpha1.ClaimPhaseDraining:
		return r.handleDraining(ctx, &claim, log)
	case v1alpha1.ClaimPhaseTerminated:
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

// handleDraining resumes a Draining claim that was interrupted mid-flight
// (e.g. controller restart while drainAndDestroy was running in the disruptor).
// It ensures the provider instance is destroyed and the claim reaches Terminated.
func (r *ClaimReconciler) handleDraining(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	log.Info("Resuming Draining claim — destroying instance and finalising",
		"instanceID", claim.Status.InstanceID, "provider", claim.Status.Provider)

	providerName := claim.Status.Provider
	if providerName == "" {
		providerName = claim.Spec.Provider
	}

	if claim.Status.InstanceID != "" {
		if prov, ok := r.Registry.Get(providerName); ok {
			if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
				if !errors.Is(err, provider.ErrInstanceNotFound) {
					log.Error(err, "Failed to destroy instance during Draining resume")
					return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
				}
				log.Info("Instance already gone on provider", "instanceID", claim.Status.InstanceID)
			} else {
				log.Info("Instance destroyed", "instanceID", claim.Status.InstanceID)
			}
		}
	}

	r.deleteNodeIfExists(ctx, claim, log)

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

	claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
	now := metav1.Now()
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "DrainResumed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "DrainResumedAfterRestart",
		Message:            "Draining resumed by reconciler after controller restart",
	})
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, fmt.Errorf("setting Draining claim to Terminated: %w", err)
	}

	if r.ClaimWriter != nil {
		_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "terminated",
			fmt.Sprintf("instance %s terminated (drain resumed after controller restart)", claim.Status.InstanceID))
		r.writeTerminatedRecord(ctx, claim, log)
	}

	r.retriggerIfDemandExists(ctx, claim, log)
	return ctrl.Result{}, nil
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

	// Standalone bootstrap: read GPU API URL and bearer token from pool bootstrap spec.
	config.GPUAPIURL = pool.Spec.Bootstrap.GPUAPIUrl
	if pool.Spec.Bootstrap.GPUAPITokenSecret != nil {
		tokenRef := v1alpha1.SecretReference{
			Name:      pool.Spec.Bootstrap.GPUAPITokenSecret.Name,
			Namespace: pool.Spec.Bootstrap.GPUAPITokenSecret.Namespace,
		}
		keyName := pool.Spec.Bootstrap.GPUAPITokenSecret.Key
		if keyName == "" {
			keyName = "gpu-api-token"
		}
		token, err := r.SecretReader.GetSecretValue(ctx, tokenRef, keyName)
		if err != nil {
			log.Error(err, "Failed to read GPU API token")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		config.GPUAPIToken = token
	}

	// Build Models ("model_id:port,...") and ModelSources (comma-separated HF sources)
	// from this claim's model IDs. First model on port 8000, subsequent on 8001, etc.
	if r.DemandStore != nil {
		var modelPairs, modelSourceParts []string
		port := 8000
		for _, modelID := range claimModelIDs(claim) {
			modelPairs = append(modelPairs, fmt.Sprintf("%s:%d", modelID, port))
			port++
			cfg, err := r.DemandStore.GetModelConfig(ctx, modelID)
			if err == nil && cfg != nil && cfg.Source != "" {
				modelSourceParts = append(modelSourceParts, cfg.Source)
			}
		}
		config.Models = strings.Join(modelPairs, ",")
		config.ModelSources = strings.Join(modelSourceParts, ",")
	}

	// ModelID for Verda OS volume tracking (primary/triggering model).
	config.ModelID = claim.Spec.ModelID
	if config.ModelID == "" {
		if ids := claimModelIDs(claim); len(ids) > 0 {
			config.ModelID = ids[0]
		}
	}

	// GPU count for tensor-parallel-size passed to gpu-agent.
	config.GPUCount = claim.Spec.Requirements.GPUCount
	if config.GPUCount == 0 {
		config.GPUCount = 1
	}

	reqs := provider.GPURequirements{
		GPUCount:       claim.Spec.Requirements.GPUCount,
		MinVRAM:        claim.Spec.Requirements.MinVRAM,
		MaxVRAM:        claim.Spec.Requirements.MaxVRAM,
		GPUTypes:       claim.Spec.Requirements.GPUTypes,
		MaxPricePerHour: claim.Spec.Requirements.MaxPricePerGPU,
		CapacityType:   "spot",
		NodeType:       nodeType,
		MinPCIeGen:     pool.Spec.Requirements.MinPCIeGen,
		MultiGpu:       claim.Spec.Requirements.MultiGpu,
	}

	// Bootstrap script is generated inside the coordinator per-offer,
	// since it needs ProviderName and GPUType from the selected offer.

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
		// Recover offer details from annotations (stored atomically when instance was created).
		if gt := claim.Annotations["gpuscale.io/gpu-type"]; gt != "" {
			gpuType = gt
		}
		if gc := claim.Annotations["gpuscale.io/gpu-count"]; gc != "" {
			if n, err := strconv.Atoi(gc); err == nil {
				gpuCount = n
			}
		}
		if ph := claim.Annotations["gpuscale.io/price-per-hour"]; ph != "" {
			if f, err := strconv.ParseFloat(ph, 64); err == nil {
				pricePerHour = f
			}
		}
		// Also recover endpoint from provider (not stored in annotations).
		if prov, ok := r.Registry.Get(providerName); ok {
			if inst, err := prov.GetInstance(ctx, existingInstanceID); err == nil {
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
		if r.ClaimWriter != nil {
			_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "provisioning", "requesting instance from provider")
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
		// Persist offer details so the reuse path can recover them if status update conflicts.
		if gpuType != "" {
			claim.Annotations["gpuscale.io/gpu-type"] = gpuType
		}
		if gpuCount > 0 {
			claim.Annotations["gpuscale.io/gpu-count"] = strconv.Itoa(gpuCount)
		}
		if pricePerHour > 0 {
			claim.Annotations["gpuscale.io/price-per-hour"] = fmt.Sprintf("%.4f", pricePerHour)
		}
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
	// Only emit the "bootstrapping" event when we freshly created the instance,
	// not on retries that reuse the annotation (existingInstanceID != "").
	if r.ClaimWriter != nil && existingInstanceID == "" {
		msg := fmt.Sprintf("instance %s created (%s, %.4f$/hr)", instanceID, claim.Status.GPUType, claim.Status.PricePerHour)
		_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "bootstrapping", msg)
	}
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
			r.deleteNodeIfExists(ctx, claim, log)
			r.retriggerIfDemandExists(ctx, claim, log)
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to check instance status")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Detect fatal errors in StatusMsg even when status is still "starting".
	// Vast.ai can report OCI runtime errors while ActualStatus is still "created".
	instanceFailed := instance.Status == "stopped" || instance.Status == "error"

	// For containers (RunPod/VastAI), "Error" in StatusMsg while starting is suspicious.
	// For KVM VMs (Verda), the boot process is longer and transient errors are normal.
	isKVM := claim.Status.Provider == "verda"
	if !instanceFailed && instance.StatusMsg != "" && strings.Contains(instance.StatusMsg, "Error") && !isKVM {
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
		if isKVM {
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
		r.deleteNodeIfExists(ctx, claim, log)
		r.retriggerIfDemandExists(ctx, claim, log)
		return ctrl.Result{}, nil
	}

	return r.handleBootstrappingStandalone(ctx, claim, prov, log)
}

// handleBootstrappingStandalone waits for the gpu-agent to connect its WSS tunnel to GPU API.
// GPU API registers the node in Dragonfly (gpu_api:node:{claim.Name}) when the tunnel connects.
func (r *ClaimReconciler) handleBootstrappingStandalone(ctx context.Context, claim *v1alpha1.GPUNodeClaim, prov provider.Provider, log logr.Logger) (ctrl.Result, error) {
	if r.DemandStore == nil || !r.DemandStore.IsNodeRegistered(ctx, claim.Name) {
		if r.isBootstrapTimedOut(claim) {
			return r.terminateTimedOut(ctx, claim, prov, log)
		}
		log.Info("Waiting for gpu-agent to connect WSS tunnel")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// gpu-agent connected — node is Ready.
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseReady
	claim.Status.NodeType = "standalone"
	claim.Status.ReadyAt = &now
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "AgentConnected",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "TunnelConnected",
		Message:            "gpu-agent WSS tunnel connected and node registered",
	})

	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Publish worker status to Dragonfly.
	if r.WorkerStore != nil {
		if err := r.WorkerStore.SetWorker(ctx, claim); err != nil {
			log.Error(err, "Failed to publish worker to Dragonfly")
		}
	}

	// Mark the OS volume as successfully bootstrapped for Verda reuse.
	if r.DemandStore != nil && claim.Status.InstanceID != "" {
		if err := r.DemandStore.MarkVolumeReady(ctx, claim.Status.InstanceID); err != nil {
			log.Error(err, "Failed to mark volume ready")
		}
	}

	// GPU API already set loaded_models when the node registered, but publish
	// model_available again to trigger any queued requests that may have been missed.
	if r.DemandStore != nil {
		for _, modelID := range claimModelIDs(claim) {
			r.DemandStore.PublishModelAvailable(ctx, modelID)
		}
		log.Info("Published model available for co-located models", "models", claimModelIDs(claim))
	}

	if claim.Status.ProvisionedAt != nil {
		log.Info("Node is Ready — gpu-agent connected",
			"bootstrapDuration", now.Time.Sub(claim.Status.ProvisionedAt.Time).String(),
			"provider", claim.Status.Provider,
			"gpu", claim.Status.GPUType,
		)
	}

	if r.ClaimWriter != nil {
		readyAt := now.Time
		if err := r.ClaimWriter.Upsert(ctx, ClaimWriteRecord{
			Name:          claim.Name,
			Pool:          claim.Spec.PoolRef,
			Provider:      claim.Status.Provider,
			GPUType:       claim.Status.GPUType,
			GPUCount:      claim.Status.GPUCount,
			PricePerHour:  claim.Status.PricePerHour,
			ModelID:       claimPrimaryModel(claim),
			NodeType:      claim.Status.NodeType,
			Phase:         string(v1alpha1.ClaimPhaseReady),
			ProvisionedAt: provisionedAtTime(claim),
			ReadyAt:       &readyAt,
		}); err != nil {
			log.Error(err, "Failed to write Ready standalone claim to Postgres (non-fatal)")
		}
		_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "agent_connected",
			fmt.Sprintf("gpu-agent WSS tunnel connected (%s, %s)", claim.Status.GPUType, claim.Status.Provider))
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
			if r.ClaimWriter != nil {
				_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "terminated",
					fmt.Sprintf("spot instance %s reclaimed by provider (preempted)", claim.Status.InstanceID))
				r.writeTerminatedRecord(ctx, claim, log)
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
			r.deleteNodeIfExists(ctx, claim, log)
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
		if r.ClaimWriter != nil {
			reason := fmt.Sprintf("instance %s died on provider (status: %s)", claim.Status.InstanceID, instance.Status)
			if instance.StatusMsg != "" {
				reason += " — " + instance.StatusMsg
			}
			_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "terminated", reason)
			r.writeTerminatedRecord(ctx, claim, log)
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
		r.deleteNodeIfExists(ctx, claim, log)
		r.retriggerIfDemandExists(ctx, claim, log)
		return ctrl.Result{}, nil
	}

	// Standalone health check: verify gpu-agent WSS tunnel is still connected.
	// GPU API removes gpu_api:node:{claim.Name} from Dragonfly when the tunnel closes.
	if r.DemandStore != nil && !r.DemandStore.IsNodeRegistered(ctx, claim.Name) {
		log.Info("gpu-agent disconnected (WSS tunnel closed), terminating claim",
			"instanceID", claim.Status.InstanceID)
		if err := prov.DestroyInstance(ctx, claim.Status.InstanceID); err != nil {
			log.Error(err, "Failed to destroy instance with disconnected agent", "instanceID", claim.Status.InstanceID)
		}
		if r.ClaimWriter != nil {
			_ = r.ClaimWriter.WriteEvent(ctx, claim.Name, "terminated",
				fmt.Sprintf("gpu-agent WSS tunnel closed for %s — instance likely preempted", claim.Status.InstanceID))
			r.writeTerminatedRecord(ctx, claim, log)
		}
		claim.Status.Phase = v1alpha1.ClaimPhaseTerminated
		now := metav1.Now()
		claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
			Type:               "AgentLost",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: now,
			Reason:             "TunnelDisconnected",
			Message:            "gpu-agent WSS tunnel closed",
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

	// --- Consolidation drain ---
	// If this is a bin-packed claim that now has all co-located models loaded,
	// annotate any older single-model claims for those models so DisruptionController
	// drains them immediately (they are being replaced by this more efficient node).
	if len(claimModelIDs(claim)) > 1 && r.DemandStore != nil {
		r.triggerConsolidationDrain(ctx, claim, log)
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// writeTerminatedRecord upserts a Terminated record in Postgres, recovering price from
// the annotation if claim.Status.PricePerHour is zero (happens when status update failed
// or the provider didn't return price in the provisioning response).
func (r *ClaimReconciler) writeTerminatedRecord(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) {
	if r.ClaimWriter == nil {
		return
	}
	price := claim.Status.PricePerHour
	if price == 0 {
		if s := claim.Annotations["gpuscale.io/price-per-hour"]; s != "" {
			_, _ = fmt.Sscanf(s, "%f", &price)
		}
	}
	models := claimModelIDs(claim)
	modelID := ""
	if len(models) > 0 {
		modelID = models[0]
	}
	var readyAt *time.Time
	if claim.Status.ReadyAt != nil {
		t := claim.Status.ReadyAt.Time
		readyAt = &t
	}
	if err := r.ClaimWriter.Upsert(ctx, ClaimWriteRecord{
		Name:          claim.Name,
		Pool:          claim.Spec.PoolRef,
		Provider:      claim.Status.Provider,
		GPUType:       claim.Status.GPUType,
		GPUCount:      claim.Status.GPUCount,
		PricePerHour:  price,
		ModelID:       modelID,
		NodeType:      claim.Spec.NodeType,
		Phase:         string(v1alpha1.ClaimPhaseTerminated),
		ProvisionedAt: provisionedAtTime(claim),
		ReadyAt:       readyAt,
	}); err != nil {
		log.Error(err, "Failed to write Terminated claim to Postgres (non-fatal)")
	}
}

// emitRayEventOnce writes a bootstrap event for the given step exactly once per claim.
// It uses a claim annotation as a dedup key so the event is not duplicated on every 60s tick.
// The annotation patch is best-effort; a failure means the event may be emitted more than
// once (acceptable — it will appear as a duplicate in the timeline rather than missing).
func (r *ClaimReconciler) emitRayEventOnce(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger, step, message string) {
	if r.ClaimWriter == nil {
		return
	}
	annotKey := "gpuscale.io/event-" + step
	if claim.Annotations[annotKey] == "1" {
		return
	}
	if err := r.ClaimWriter.WriteEvent(ctx, claim.Name, step, message); err != nil {
		log.Error(err, "Failed to write ray event", "step", step)
		return
	}
	// Mark as emitted via annotation patch so we don't duplicate on next reconcile.
	// Also update bootstrap-step so the Live tab shows current progress (same field
	// the bootstrap script posts to via /internal/bootstrap-event).
	patch := client.MergeFrom(claim.DeepCopy())
	if claim.Annotations == nil {
		claim.Annotations = make(map[string]string)
	}
	claim.Annotations[annotKey] = "1"
	claim.Annotations["gpuscale.io/bootstrap-step"] = step
	if err := r.Patch(ctx, claim, patch); err != nil {
		log.Error(err, "Failed to mark ray event emitted (non-fatal)", "step", step)
	}
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

// deleteNodeIfExists removes the K8s node object for a claim if one exists.
// Called on all terminate paths to prevent stale NotReady nodes accumulating.
func (r *ClaimReconciler) deleteNodeIfExists(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) {
	node, err := r.findNodeByInstanceID(ctx, claim.Name)
	if err != nil {
		log.Error(err, "Failed to look up node for deletion")
		return
	}
	if node == nil {
		return
	}
	if err := r.Delete(ctx, node); err != nil && !apierrors.IsNotFound(err) {
		log.Error(err, "Failed to delete node", "node", node.Name)
		return
	}
	log.Info("Deleted stale K8s node", "node", node.Name)
}

func (r *ClaimReconciler) isBootstrapTimedOut(claim *v1alpha1.GPUNodeClaim) bool {
	if claim.Status.ProvisionedAt == nil {
		return false
	}
	const timeout = 10 * time.Minute
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

	r.deleteNodeIfExists(ctx, claim, log)
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

func claimPrimaryModel(claim *v1alpha1.GPUNodeClaim) string {
	ids := claimModelIDs(claim)
	if len(ids) > 0 {
		return ids[0]
	}
	return ""
}

func provisionedAtTime(claim *v1alpha1.GPUNodeClaim) *time.Time {
	if claim.Status.ProvisionedAt == nil {
		return nil
	}
	t := claim.Status.ProvisionedAt.Time
	return &t
}

func readyAtTime(claim *v1alpha1.GPUNodeClaim) *time.Time {
	if claim.Status.ReadyAt == nil {
		return nil
	}
	t := claim.Status.ReadyAt.Time
	return &t
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
