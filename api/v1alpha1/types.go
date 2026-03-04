package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Max Nodes",type=integer,JSONPath=`.spec.scaling.maxNodes`
// +kubebuilder:printcolumn:name="Min Nodes",type=integer,JSONPath=`.spec.scaling.minNodes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GPUNodePool defines a pool of GPU nodes that can be provisioned from cloud providers.
type GPUNodePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GPUNodePoolSpec   `json:"spec,omitempty"`
	Status GPUNodePoolStatus `json:"status,omitempty"`
}

type GPUNodePoolSpec struct {
	// Providers to use for provisioning, in priority order.
	Providers []ProviderConfig `json:"providers"`

	// GPU requirements for nodes in this pool.
	Requirements GPURequirementsSpec `json:"requirements"`

	// Scaling behavior configuration.
	Scaling ScalingSpec `json:"scaling"`

	// Bootstrap configuration for new nodes.
	Bootstrap BootstrapSpec `json:"bootstrap"`

	// Limits on total resources managed by this pool.
	Limits *PoolLimits `json:"limits,omitempty"`
}

type ProviderConfig struct {
	// Name of the provider (vast.ai, verda, runpod).
	Name string `json:"name"`

	// Reference to a Secret containing provider API credentials.
	APIKeySecret SecretReference `json:"apiKeySecret"`

	// CapacityType: "spot" or "on-demand".
	// +kubebuilder:validation:Enum=spot;on-demand
	// +kubebuilder:default=spot
	CapacityType string `json:"capacityType,omitempty"`

	// MaxPrice is the maximum $/hr per GPU to pay with this provider.
	// 0 means no limit.
	MaxPrice float64 `json:"maxPrice,omitempty"`

	// NodeType specifies how to deploy nodes with this provider.
	// "standalone" = gpu-agent binary + vLLM, connects outbound WSS tunnel to GPU API.
	// +kubebuilder:validation:Enum=standalone
	// +kubebuilder:default=standalone
	NodeType string `json:"nodeType,omitempty"`
}

type SecretReference struct {
	// Name of the Secret.
	Name string `json:"name"`
	// Namespace of the Secret.
	Namespace string `json:"namespace"`
}

type GPURequirementsSpec struct {
	// GPUTypes are the acceptable GPU types (e.g. "RTX 4090", "A100 80GB").
	// Empty means any GPU type.
	GPUTypes []string `json:"gpuTypes,omitempty"`

	// MinVRAM is the minimum GPU VRAM in GB.
	MinVRAM int `json:"minVRAM,omitempty"`

	// MinDisk is the minimum disk space in GB.
	MinDisk int `json:"minDisk,omitempty"`

	// MinRAM is the minimum system RAM in GB.
	MinRAM int `json:"minRAM,omitempty"`

	// MinPCIeGen is the minimum PCIe generation required (e.g., 4 for PCIe 4.0).
	// 0 means no restriction.
	MinPCIeGen int `json:"minPCIeGen,omitempty"`
}

type ScalingSpec struct {
	// BatchWindow is the duration to wait and batch pending pods before provisioning.
	// +kubebuilder:default="10s"
	BatchWindow metav1.Duration `json:"batchWindow,omitempty"`

	// CooldownPeriod is the minimum idle time before destroying a node.
	// With BillingPeriod set, this becomes a floor — the node is kept until
	// the next billing tick regardless.
	// +kubebuilder:default="2m"
	CooldownPeriod metav1.Duration `json:"cooldownPeriod,omitempty"`

	// BillingPeriod is the provider's billing increment (e.g. 10m for Verda).
	// When set, idle nodes are destroyed just before the next billing tick
	// (BillingPeriod - 1 min) to avoid paying for unused time.
	// Zero means no billing-aware scheduling — use CooldownPeriod only.
	BillingPeriod metav1.Duration `json:"billingPeriod,omitempty"`

	// MaxNodes is the maximum number of nodes this pool can manage.
	MaxNodes int `json:"maxNodes"`

	// MinNodes is the minimum number of nodes to keep running (0 = scale to zero).
	// +kubebuilder:default=0
	MinNodes int `json:"minNodes,omitempty"`

	// InterruptionPollInterval is how often to poll providers for instance status changes.
	// +kubebuilder:default="30s"
	InterruptionPollInterval metav1.Duration `json:"interruptionPollInterval,omitempty"`
}

type BootstrapSpec struct {
	// Image is the Docker image for the worker container.
	// e.g., "vllm/vllm-openai:latest" (has vLLM pre-installed; gpu-agent manages it)
	Image string `json:"image"`

	// GPUAPIUrl is the WSS endpoint for gpu-agent to connect its reverse tunnel.
	// e.g., "wss://ai.example.com"
	GPUAPIUrl string `json:"gpuApiURL,omitempty"`

	// GPUAPITokenSecret references the Secret containing the GPU API bearer token.
	// gpu-agent uses this token to authenticate its WSS tunnel connection.
	GPUAPITokenSecret *SecretKeyReference `json:"gpuApiTokenSecret,omitempty"`

	// ModelCacheURL is an optional rclone-compatible URL for pre-caching model weights.
	ModelCacheURL string `json:"modelCacheURL,omitempty"`
}

// SecretKeyReference references a specific key in a Kubernetes Secret.
type SecretKeyReference struct {
	// Name of the Secret.
	Name string `json:"name"`
	// Namespace of the Secret.
	Namespace string `json:"namespace"`
	// Key within the Secret. Defaults to "gpu-api-token" if empty.
	Key string `json:"key,omitempty"`
}

type PoolLimits struct {
	// MaxGPUs is the maximum total GPUs across all managed nodes.
	MaxGPUs int `json:"maxGPUs,omitempty"`

	// MaxCostPerHour is the maximum total $/hr spend across all managed nodes.
	MaxCostPerHour float64 `json:"maxCostPerHour,omitempty"`
}

type GPUNodePoolStatus struct {
	// ActiveNodes is the number of currently active managed nodes.
	ActiveNodes int `json:"activeNodes,omitempty"`

	// TotalGPUs is the total number of GPUs across all managed nodes.
	TotalGPUs int `json:"totalGPUs,omitempty"`

	// CurrentCostPerHour is the current total $/hr spend.
	CurrentCostPerHour float64 `json:"currentCostPerHour,omitempty"`

	// Conditions represent the latest available observations.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

// GPUNodePoolList contains a list of GPUNodePool.
type GPUNodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUNodePool `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.poolRef`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.status.provider`
// +kubebuilder:printcolumn:name="GPU",type=string,JSONPath=`.status.gpuType`
// +kubebuilder:printcolumn:name="Price/hr",type=string,JSONPath=`.status.pricePerHour`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GPUNodeClaim represents a claim for a GPU node being provisioned from a cloud provider.
type GPUNodeClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GPUNodeClaimSpec   `json:"spec,omitempty"`
	Status GPUNodeClaimStatus `json:"status,omitempty"`
}

type GPUNodeClaimSpec struct {
	// PoolRef is the name of the GPUNodePool that created this claim.
	PoolRef string `json:"poolRef"`

	// Provider is the name of the provider selected for this claim.
	// Set at creation time by the provisioner so it survives status update failures.
	Provider string `json:"provider,omitempty"`

	// NodeType is the deployment mode. Currently always "standalone".
	// Set at creation time by the provisioner so it survives status update failures.
	NodeType string `json:"nodeType,omitempty"`

	// ModelID is the model this claim was provisioned for (API name, e.g., "glm-4-7").
	// Used by the disruptor to check demand and always-active status.
	// For multi-model bin-packed claims this holds the primary (triggering) model
	// for backward compatibility. See ModelIDs for the full set.
	ModelID string `json:"modelId,omitempty"`

	// ModelIDs contains all model IDs co-located on this node (multi-model bin-packing).
	// ModelID holds the primary/triggering model for backward compatibility.
	ModelIDs []string `json:"modelIds,omitempty"`

	// ModelSource is the HuggingFace source for the model (e.g., "hf:zai-org/GLM-4.7-Flash").
	// Set by ProvisionTrigger from Dragonfly config. Used by bootstrap to download the correct weights.
	ModelSource string `json:"modelSource,omitempty"`

	// Requirements describe the GPU resources needed.
	Requirements ClaimRequirements `json:"requirements"`

	// PodRefs are the pending pods that triggered this claim.
	PodRefs []PodReference `json:"podRefs,omitempty"`
}

type ClaimRequirements struct {
	// GPUCount is the minimum number of GPUs needed. 0 = any count covering MinVRAM.
	GPUCount int `json:"gpuCount,omitempty"`

	// MinVRAM is the total VRAM required across all GPUs on the instance in GB.
	MinVRAM int `json:"minVRAM,omitempty"`

	// MaxVRAM is the maximum VRAM per GPU in GB.
	// Prevents over-provisioning (e.g., getting an H200 for a 7B model).
	MaxVRAM int `json:"maxVRAM,omitempty"`

	// GPUTypes are the preferred GPU types (soft preference — sort order only).
	GPUTypes []string `json:"gpuTypes,omitempty"`

	// MaxPricePerGPU is the maximum $/hr per GPU (0 = no limit).
	// Total instance price cap = MaxPricePerGPU * offer.GPUCount.
	MaxPricePerGPU float64 `json:"maxPricePerGpu,omitempty"`

	// MinDisk is the minimum OS volume size in GB (model weights + image + OS overhead).
	// Used by VM providers (Verda) to size the OS volume at creation time.
	MinDisk int `json:"minDisk,omitempty"`

	// MultiGpu allows multi-GPU instances (tensor parallel). When false, only
	// single-GPU instances are considered regardless of VRAM requirements.
	MultiGpu bool `json:"multiGpu,omitempty"`
}

type PodReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

// GPUNodeClaimPhase represents the lifecycle phase of a GPUNodeClaim.
// +kubebuilder:validation:Enum=Pending;Provisioning;Bootstrapping;Ready;Draining;Terminated
type GPUNodeClaimPhase string

const (
	ClaimPhasePending       GPUNodeClaimPhase = "Pending"
	ClaimPhaseProvisioning  GPUNodeClaimPhase = "Provisioning"
	ClaimPhaseBootstrapping GPUNodeClaimPhase = "Bootstrapping"
	ClaimPhaseReady         GPUNodeClaimPhase = "Ready"
	ClaimPhaseDraining      GPUNodeClaimPhase = "Draining"
	ClaimPhaseTerminated    GPUNodeClaimPhase = "Terminated"
	// ClaimPhaseHibernated means the provider instance has been stopped (disk preserved).
	// The claim stays alive so it can be woken on next demand without re-provisioning.
	// Supported only for providers that implement HibernatingProvider (currently Vast.ai full-node).
	ClaimPhaseHibernated GPUNodeClaimPhase = "Hibernated"
)

type GPUNodeClaimStatus struct {
	// Provider is the name of the provider that fulfilled this claim.
	Provider string `json:"provider,omitempty"`

	// InstanceID is the provider-specific instance identifier.
	InstanceID string `json:"instanceID,omitempty"`

	// NodeType is the deployment mode. Currently always "standalone".
	NodeType string `json:"nodeType,omitempty"`

	// NodeName is unused for standalone nodes (no K8s join).
	// Kept for backward compatibility.
	NodeName string `json:"nodeName,omitempty"`

	// Endpoint is unused for standalone nodes (connection is inbound WSS from gpu-agent).
	// Kept for backward compatibility.
	Endpoint string `json:"endpoint,omitempty"`

	// GPUType is the actual GPU type provisioned.
	GPUType string `json:"gpuType,omitempty"`

	// GPUCount is the number of GPUs on the instance.
	GPUCount int `json:"gpuCount,omitempty"`

	// PricePerHour is the actual $/hr cost.
	PricePerHour float64 `json:"pricePerHour,omitempty"`

	// Phase is the current lifecycle phase.
	Phase GPUNodeClaimPhase `json:"phase,omitempty"`

	// Conditions represent the latest available observations.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ProvisionedAt is when the instance was created.
	ProvisionedAt *metav1.Time `json:"provisionedAt,omitempty"`

	// ReadyAt is when the node became ready.
	ReadyAt *metav1.Time `json:"readyAt,omitempty"`

	// IdleSince is when the node last became idle (no GPU workloads).
	IdleSince *metav1.Time `json:"idleSince,omitempty"`

	// RetryCount tracks how many times provisioning has been retried.
	// Used for exponential backoff on failure.
	RetryCount int `json:"retryCount,omitempty"`
}

// +kubebuilder:object:root=true

// GPUNodeClaimList contains a list of GPUNodeClaim.
type GPUNodeClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUNodeClaim `json:"items"`
}
