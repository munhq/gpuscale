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
	// "k3s" = join K3s cluster (requires VM with VPN)
	// "ray-worker" = standalone Ray Serve (works in containers)
	// +kubebuilder:validation:Enum=k3s;ray-worker
	// +kubebuilder:default=k3s
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
}

type ScalingSpec struct {
	// BatchWindow is the duration to wait and batch pending pods before provisioning.
	// +kubebuilder:default="10s"
	BatchWindow metav1.Duration `json:"batchWindow,omitempty"`

	// CooldownPeriod is how long to keep an idle node before destroying it.
	// +kubebuilder:default="10m"
	CooldownPeriod metav1.Duration `json:"cooldownPeriod,omitempty"`

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
	// Image is the Docker image for the bootstrap node container.
	// For k3s nodes: custom image with K3s + Netbird
	// For ray-worker nodes: rayproject/ray:latest-gpu or similar
	Image string `json:"image"`

	// VPNSetupKeySecret references the Secret containing the Netbird setup key.
	// Only used for k3s node type.
	VPNSetupKeySecret SecretReference `json:"vpnSetupKeySecret,omitempty"`

	// K3sTokenSecret references the Secret containing the K3s join token.
	// Only used for k3s node type.
	K3sTokenSecret SecretReference `json:"k3sTokenSecret,omitempty"`

	// K3sURL is the K3s server URL for agent join.
	// Only used for k3s node type.
	K3sURL string `json:"k3sURL,omitempty"`

	// RayConfig contains Ray-specific configuration.
	// Only used for ray-worker node type.
	RayConfig *RayConfig `json:"rayConfig,omitempty"`

	// ModelCacheURL is an optional rclone-compatible URL for pre-caching model weights.
	ModelCacheURL string `json:"modelCacheURL,omitempty"`
}

// RayConfig contains configuration for Ray Serve workers
type RayConfig struct {
	// DashboardPort is the Ray dashboard port (default: 8265)
	// +kubebuilder:default=8265
	DashboardPort int `json:"dashboardPort,omitempty"`

	// ServePort is the Ray Serve HTTP port (default: 8000)
	// +kubebuilder:default=8000
	ServePort int `json:"servePort,omitempty"`

	// HeadAddress is the Ray head node address for workers to join.
	// Empty means run as standalone head node.
	HeadAddress string `json:"headAddress,omitempty"`

	// ServeConfig is an optional YAML/JSON string for Ray Serve deployment config.
	ServeConfig string `json:"serveConfig,omitempty"`
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

	// Requirements describe the GPU resources needed.
	Requirements ClaimRequirements `json:"requirements"`

	// PodRefs are the pending pods that triggered this claim.
	PodRefs []PodReference `json:"podRefs,omitempty"`
}

type ClaimRequirements struct {
	// GPUCount is the number of GPUs needed.
	GPUCount int `json:"gpuCount"`

	// MinVRAM is the minimum VRAM per GPU in GB.
	MinVRAM int `json:"minVRAM,omitempty"`

	// GPUTypes are the acceptable GPU types.
	GPUTypes []string `json:"gpuTypes,omitempty"`

	// MaxPrice is the maximum $/hr.
	MaxPrice float64 `json:"maxPrice,omitempty"`
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
	ClaimPhasePending      GPUNodeClaimPhase = "Pending"
	ClaimPhaseProvisioning GPUNodeClaimPhase = "Provisioning"
	ClaimPhaseBootstrapping GPUNodeClaimPhase = "Bootstrapping"
	ClaimPhaseReady        GPUNodeClaimPhase = "Ready"
	ClaimPhaseDraining     GPUNodeClaimPhase = "Draining"
	ClaimPhaseTerminated   GPUNodeClaimPhase = "Terminated"
)

type GPUNodeClaimStatus struct {
	// Provider is the name of the provider that fulfilled this claim.
	Provider string `json:"provider,omitempty"`

	// InstanceID is the provider-specific instance identifier.
	InstanceID string `json:"instanceID,omitempty"`

	// NodeType indicates if this is a k3s node or ray-worker.
	NodeType string `json:"nodeType,omitempty"`

	// NodeName is the Kubernetes node name once joined.
	// Only set for k3s node type.
	NodeName string `json:"nodeName,omitempty"`

	// Endpoint is the HTTP endpoint for Ray workers.
	// Only set for ray-worker node type.
	// Format: "http://host:port" or "https://host:port"
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
}

// +kubebuilder:object:root=true

// GPUNodeClaimList contains a list of GPUNodeClaim.
type GPUNodeClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUNodeClaim `json:"items"`
}
