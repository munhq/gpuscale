package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	instanceID := claim.Name
	nodeType := resolveClaimNodeType(claim)
	config := provider.BootstrapConfig{
		NodeType:      nodeType,
		Image:         pool.Spec.Bootstrap.Image,
		ModelCacheURL: pool.Spec.Bootstrap.ModelCacheURL,
		InstanceID:    instanceID,
		GPUType:       claim.Status.GPUType,
	}

	// Full-node specific: read VPN and Kubernetes join secrets
	if nodeType == "full-node" {
		netbirdKey, err := r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.VPNSetupKeySecret, "setup-key")
		if err != nil {
			log.Error(err, "Failed to read VPN setup key")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		k8sToken, err := r.SecretReader.GetSecretValue(ctx, pool.Spec.Bootstrap.K8sTokenSecret, "token")
		if err != nil {
			log.Error(err, "Failed to read Kubernetes join token")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		config.NetbirdKey = netbirdKey
		config.K8sURL = pool.Spec.Bootstrap.K8sURL
		config.K8sToken = k8sToken
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
	}

	reqs := provider.GPURequirements{
		GPUCount:     claim.Spec.Requirements.GPUCount,
		MinVRAM:      claim.Spec.Requirements.MinVRAM,
		MaxVRAM:      claim.Spec.Requirements.MaxVRAM,
		GPUTypes:     claim.Spec.Requirements.GPUTypes,
		MaxPrice:     claim.Spec.Requirements.MaxPrice,
		CapacityType: "spot",
	}

	// Note: full-node bootstrap script is generated inside the coordinator
	// per-offer, since it needs ProviderName and GPUType from the selected offer.

	// Transition to Provisioning before calling coordinator.
	now := metav1.Now()
	claim.Status.Phase = v1alpha1.ClaimPhaseProvisioning
	claim.Status.ProvisionedAt = &now
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Provision via coordinator: handles offer caching, blacklisting, rate limiting, retries.
	result, err := r.Coordinator.ProvisionInstance(ctx, reqs, config, providerNames)
	if err != nil {
		log.Error(err, "Coordinator failed to provision instance")
		// Re-fetch claim to avoid conflict on status update.
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
	log.Info("Instance created via coordinator",
		"provider", instance.ProviderName,
		"instanceID", instance.InstanceID,
		"gpu", instance.GPUType,
		"nodeType", nodeType,
		"attempts", result.Attempts,
	)

	// Re-fetch claim to get latest resourceVersion before status update.
	if err := r.Get(ctx, types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}, claim); err != nil {
		return ctrl.Result{}, err
	}

	// Update claim spec with the provider chosen by coordinator (if not already set).
	if claim.Spec.Provider == "" {
		claim.Spec.Provider = offer.ProviderName
		if err := r.Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		// Re-fetch after spec update to get latest resourceVersion for status update.
		if err := r.Get(ctx, types.NamespacedName{Name: claim.Name, Namespace: claim.Namespace}, claim); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Update claim status with instance details.
	successNow := metav1.Now()
	claim.Status.Provider = instance.ProviderName
	claim.Status.GPUType = offer.GPUType
	claim.Status.GPUCount = offer.GPUCount
	claim.Status.PricePerHour = offer.PricePerHour
	claim.Status.InstanceID = instance.InstanceID
	claim.Status.Phase = v1alpha1.ClaimPhaseBootstrapping
	claim.Status.RetryCount = 0
	if instance.Endpoint != "" {
		claim.Status.Endpoint = instance.Endpoint
	}
	claim.Status.Conditions = append(claim.Status.Conditions, metav1.Condition{
		Type:               "InstanceCreated",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: successNow,
		Reason:             "InstanceCreated",
		Message:            fmt.Sprintf("Instance %s created on %s", instance.InstanceID, instance.ProviderName),
	})
	if err := r.Status().Update(ctx, claim); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *ClaimReconciler) handleProvisioning(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) (ctrl.Result, error) {
	if claim.Status.InstanceID != "" {
		claim.Status.Phase = v1alpha1.ClaimPhaseBootstrapping
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

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
		log.Error(err, "Failed to check instance status")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if instance.Status == "stopped" || instance.Status == "error" {
		// Grace period: don't terminate on "error" in the first 2 minutes.
		// Providers can report transient error states during instance initialization.
		if instance.Status == "error" && claim.Status.ProvisionedAt != nil {
			if time.Since(claim.Status.ProvisionedAt.Time) < 2*time.Minute {
				log.Info("Instance reports error but within grace period, retrying",
					"status", instance.Status,
					"age", time.Since(claim.Status.ProvisionedAt.Time).String(),
				)
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		}

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
		_ = r.WorkerStore.RemoveWorker(ctx, claim.Name)
		r.retriggerIfDemandExists(ctx, claim, log)
		return ctrl.Result{}, nil
	}

	// Branch based on node type
	nodeType := resolveClaimNodeType(claim)
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

	// Mark model as loaded in Dragonfly — GPU API reads this instantly
	if claim.Spec.ModelID != "" && r.DemandStore != nil {
		info := LoadedModelInfo{
			ClaimName:  claim.Name,
			Provider:   claim.Status.Provider,
			GPUType:    claim.Status.GPUType,
			GPUCount:   claim.Status.GPUCount,
			InstanceID: claim.Status.InstanceID,
			ReadyAt:    now.Time.Format(time.RFC3339),
		}
		if err := r.DemandStore.SetModelLoaded(ctx, claim.Spec.ModelID, info); err != nil {
			log.Error(err, "Failed to set loaded_models", "model", claim.Spec.ModelID)
		} else {
			log.Info("Model marked as loaded", "model", claim.Spec.ModelID)
		}
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

	// Build Ray dashboard URL from the head address
	// RayConfig.HeadAddress is the GCS address (e.g., "1.2.3.4:31637")
	// Dashboard runs on the same host, port 8265
	rayDashURL := r.buildRayDashboardURL(&pool)
	if rayDashURL == "" {
		if r.isBootstrapTimedOut(claim) {
			return r.terminateTimedOut(ctx, claim, prov, log)
		}
		log.Info("No Ray head address configured, cannot health-check worker")
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	// Check if the worker has joined the Ray cluster via dashboard API
	joined := r.checkRayWorkerJoined(ctx, rayDashURL, instance.IP, log)
	if !joined {
		if r.isBootstrapTimedOut(claim) {
			return r.terminateTimedOut(ctx, claim, prov, log)
		}
		log.Info("Waiting for ray-worker to join cluster", "instanceIP", instance.IP, "rayDash", rayDashURL)
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

	// Mark model as loaded in Dragonfly — GPU API reads this instantly
	if claim.Spec.ModelID != "" && r.DemandStore != nil {
		info := LoadedModelInfo{
			ClaimName:  claim.Name,
			Provider:   claim.Status.Provider,
			GPUType:    claim.Status.GPUType,
			GPUCount:   claim.Status.GPUCount,
			InstanceID: claim.Status.InstanceID,
			ReadyAt:    now.Time.Format(time.RFC3339),
		}
		if err := r.DemandStore.SetModelLoaded(ctx, claim.Spec.ModelID, info); err != nil {
			log.Error(err, "Failed to set loaded_models", "model", claim.Spec.ModelID)
		} else {
			log.Info("Model marked as loaded", "model", claim.Spec.ModelID)
		}
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

// resolveRayHeadAddress returns the Ray head address from the pool spec,
// falling back to the controller's RAY_HEAD_ADDRESS env var.
func (r *ClaimReconciler) resolveRayHeadAddress(pool *v1alpha1.GPUNodePool) string {
	if pool.Spec.Bootstrap.RayConfig != nil && pool.Spec.Bootstrap.RayConfig.HeadAddress != "" {
		return pool.Spec.Bootstrap.RayConfig.HeadAddress
	}
	return r.RayHeadAddress
}

// buildRayDashboardURL constructs the Ray dashboard URL from pool config.
func (r *ClaimReconciler) buildRayDashboardURL(pool *v1alpha1.GPUNodePool) string {
	headAddr := r.resolveRayHeadAddress(pool)
	if headAddr == "" {
		return ""
	}
	// HeadAddress is "host:gcsPort" (e.g., "1.2.3.4:31637")
	// Dashboard is on the same host at DashboardPort (default 8265)
	dashPort := pool.Spec.Bootstrap.RayConfig.DashboardPort
	if dashPort == 0 {
		dashPort = 8265
	}
	// Extract host from "host:port"
	host := headAddr
	for i := len(headAddr) - 1; i >= 0; i-- {
		if headAddr[i] == ':' {
			host = headAddr[:i]
			break
		}
	}
	return fmt.Sprintf("http://%s:%d", host, dashPort)
}

// checkRayWorkerJoined queries the Ray dashboard API to see if a worker with the
// given IP has joined the cluster. Returns true if found alive.
func (r *ClaimReconciler) checkRayWorkerJoined(ctx context.Context, rayDashURL string, workerIP string, log logr.Logger) bool {
	if workerIP == "" {
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

	// Parse the response — Ray dashboard /nodes returns JSON with node info
	// We look for any node whose "ip" field matches the worker IP and is alive
	var result struct {
		Data struct {
			Summary []struct {
				IP    string `json:"ip"`
				State string `json:"state"`
			} `json:"summary"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.V(1).Info("Failed to parse Ray dashboard response", "error", err.Error())
		return false
	}

	for _, node := range result.Data.Summary {
		if node.IP == workerIP && node.State == "ALIVE" {
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
	return time.Since(claim.Status.ProvisionedAt.Time) > 10*time.Minute
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
	if err := r.WorkerStore.RemoveWorker(ctx, claim.Name); err != nil {
		log.Error(err, "Failed to remove worker from Dragonfly")
	}

	// Remove loaded_models entry if this was the last claim for this model.
	if claim.Spec.ModelID != "" && r.DemandStore != nil {
		if !r.hasOtherReadyClaims(ctx, claim.Spec.ModelID, claim.Name) {
			_ = r.DemandStore.RemoveModelLoaded(ctx, claim.Spec.ModelID)
		}
	}

	r.retriggerIfDemandExists(ctx, claim, log)
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

	// Remove loaded_models entry if this was the last claim for this model.
	if claim.Spec.ModelID != "" && r.DemandStore != nil {
		if !r.hasOtherReadyClaims(ctx, claim.Spec.ModelID, claim.Name) {
			_ = r.DemandStore.RemoveModelLoaded(ctx, claim.Spec.ModelID)
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

// hasOtherReadyClaims returns true if there are other Ready claims for the same model.
func (r *ClaimReconciler) hasOtherReadyClaims(ctx context.Context, modelID string, excludeClaim string) bool {
	var claims v1alpha1.GPUNodeClaimList
	if err := r.List(ctx, &claims, client.InNamespace(claimNamespace())); err != nil {
		return false
	}
	for _, c := range claims.Items {
		if c.Name == excludeClaim {
			continue
		}
		if c.Spec.ModelID == modelID && c.Status.Phase == v1alpha1.ClaimPhaseReady {
			return true
		}
	}
	return false
}

// retriggerIfDemandExists re-publishes a provision trigger when a claim terminates
// but there are still queued requests for the model. Without this, requests would
// sit in the queue forever because the ProvisionTrigger only fires on new pub/sub events.
func (r *ClaimReconciler) retriggerIfDemandExists(ctx context.Context, claim *v1alpha1.GPUNodeClaim, log logr.Logger) {
	if claim.Spec.ModelID == "" || r.DemandStore == nil {
		return
	}
	depth, err := r.DemandStore.GetQueueDepth(ctx, claim.Spec.ModelID)
	if err != nil {
		log.Error(err, "Failed to check queue depth for re-trigger")
		return
	}
	if depth > 0 {
		log.Info("Demand exists after claim termination, re-triggering provisioning",
			"model", claim.Spec.ModelID, "queueDepth", depth)
		if err := r.DemandStore.PublishProvisionTrigger(ctx, claim.Spec.ModelID); err != nil {
			log.Error(err, "Failed to publish re-trigger")
		}
	}
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
