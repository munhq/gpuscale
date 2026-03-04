package provider

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoOffersAvailable = errors.New("no offers matching requirements are available")
	ErrInstanceNotFound  = errors.New("instance not found")
	ErrProviderAPI       = errors.New("provider API error")
	ErrOfferExpired      = errors.New("offer expired or no longer available")
)

// GPURequirements describes what GPU resources are needed.
type GPURequirements struct {
	GPUCount        int      // minimum number of GPUs; 0 = any count that covers MinVRAM
	MinVRAM         int      // GB, total VRAM required across all GPUs on the instance
	MaxVRAM         int      // GB, max VRAM per GPU to prevent over-provisioning (0 = no limit)
	GPUTypes        []string // preferred GPU types (empty = any)
	MaxPricePerGPU  float64  // $/hr per GPU; 0 = no limit (total price = MaxPricePerGPU * offer.GPUCount)
	CapacityType    string   // "spot" or "on-demand"
	MinDisk         int      // GB
	MinRAM          int      // GB
	NodeType        string   // "standalone" — used by providers to filter container offers
	MinPCIeGen      int      // minimum PCIe generation (e.g., 4 for PCIe 4.0); 0 = no restriction
	MultiGpu        bool     // allow multi-GPU instances (tensor parallel); false = single GPU only
}

// Offer represents an available GPU instance from a provider.
type Offer struct {
	ProviderName string
	OfferID      string  // provider-specific offer/machine ID
	GPUType      string  // "RTX 4090", "A100 80GB", etc
	GPUCount     int
	VRAM         int     // total VRAM in GB across all GPUs on the instance
	PricePerHour float64 // total $/hr for the instance
	CapacityType string // "spot" or "on-demand"
	Region       string
	Reliability  float64 // 0-1, provider-specific
	DiskGB       int
	RAMGB        int
}

// BootstrapConfig contains the configuration needed to bootstrap a standalone gpu-agent node.
type BootstrapConfig struct {
	NodeType string // always "standalone": gpu-agent + vLLM, outbound WSS tunnel to GPU API
	Image    string // Docker image (e.g., "vllm/vllm-openai:latest")

	// Standalone (gpu-agent) bootstrap fields.
	GPUAPIURL   string // WSS endpoint for gpu-agent tunnel (e.g., "wss://ai.example.com")
	GPUAPIToken string // bearer token for gpu-agent WSS auth (read from Secret by reconciler)
	Models      string // "model_id:port,model_id:port" — one entry per co-located model
	ModelSources string // HuggingFace sources in same order as Models (comma-separated)
	GPUCount    int    // tensor-parallel-size (derived from model config)
	HFToken     string // optional HuggingFace token for gated models

	ModelCacheURL string            // optional rclone URL for model cache
	ModelID       string            // primary model ID (for volume tracking; kept for provider compat)
	InstanceID    string            // unique ID for tracking
	GPUType       string            // GPU type label
	ProviderName  string            // provider name label
	ExtraEnv      map[string]string // additional env vars
	MinDisk       int               // GB, minimum OS volume/disk size

	// Pre-generated bootstrap script and env vars.
	// Set by the coordinator per-offer (since scripts embed ProviderName and GPUType).
	OnStartScript string            // pre-generated startup script
	OnStartEnv    map[string]string // pre-generated environment variables
}

// Instance represents a running instance on a provider.
type Instance struct {
	ProviderName string
	InstanceID   string // provider's instance ID (for destroy)
	NodeType     string // "full-node" or "ray-worker"
	IP           string // public or VPN IP
	SSHPort      int
	Endpoint     string // HTTP endpoint for ray-worker (e.g., "http://host:8000")
	Status       string // "running", "starting", "stopped", "error"
	StatusMsg    string // provider-specific status message (error details, etc.)
	GPUType      string
	GPUCount     int
	PricePerHour float64
	CreatedAt    time.Time
}

// Provider is the interface that GPU cloud providers must implement.
type Provider interface {
	// Name returns the provider identifier (e.g., "vast.ai", "verda", "runpod").
	Name() string

	// SearchOffers returns available GPU offers matching the requirements.
	SearchOffers(ctx context.Context, req GPURequirements) ([]Offer, error)

	// CreateInstance provisions a new instance from the given offer.
	CreateInstance(ctx context.Context, offer Offer, config BootstrapConfig) (*Instance, error)

	// DestroyInstance terminates the given instance.
	DestroyInstance(ctx context.Context, instanceID string) error

	// GetInstance returns the current state of an instance.
	GetInstance(ctx context.Context, instanceID string) (*Instance, error)

	// ListInstances returns all instances managed by this provider.
	ListInstances(ctx context.Context) ([]*Instance, error)
}

// HibernatingProvider is an optional interface for providers that support
// stopping an instance without destroying it (disk preserved) and waking it later.
// This enables fast cold-start: on demand, wake the stopped VM instead of provisioning
// a fresh one — model files are already on disk, skipping the HuggingFace download.
// Currently implemented by Vast.ai full-node VMs.
type HibernatingProvider interface {
	// StopInstance halts the instance without destroying its disk.
	// The instance transitions to a stopped state; storage costs may still apply.
	StopInstance(ctx context.Context, instanceID string) error

	// WakeInstance restarts a previously stopped instance.
	// On success the instance is running again; K3s agent will rejoin the cluster.
	WakeInstance(ctx context.Context, instanceID string) error
}

// VolumeStore tracks volume→model mappings for cloud providers that don't
// support native volume tags. Providers use this to find model-specific
// volumes when reusing boot images.
type VolumeStore interface {
	// RegisterInstanceModel records which model an instance is serving.
	// Called at CreateInstance time so we can later tag its volume.
	RegisterInstanceModel(ctx context.Context, instanceID, modelID string) error

	// GetInstanceModel returns the model ID for a given instance.
	GetInstanceModel(ctx context.Context, instanceID string) string

	// SetInstanceDiskSize records the allocated disk size for an instance.
	// Called at CreateInstance time so DestroyInstance can include it in RegisterVolume.
	SetInstanceDiskSize(ctx context.Context, instanceID string, sizeGB int) error

	// GetInstanceDiskSize returns the disk size recorded for an instance.
	GetInstanceDiskSize(ctx context.Context, instanceID string) int

	// RegisterVolume records a volume→model mapping for later reuse.
	RegisterVolume(ctx context.Context, volumeID, modelID, instanceID string, sizeGB int) error

	// MarkVolumeReady marks a volume's bootstrap as successfully completed.
	// Only volumes marked ready are eligible for reuse.
	MarkVolumeReady(ctx context.Context, instanceID string) error

	// FindVolumeForModel returns the ID of a tracked volume for the given model
	// that was successfully bootstrapped and meets the minimum disk size.
	FindVolumeForModel(ctx context.Context, modelID string, minDisk int) string

	// UnregisterVolume removes a volume tracking entry.
	UnregisterVolume(ctx context.Context, volumeID string) error
}

// Registry holds all configured providers.
type Registry struct {
	providers map[string]Provider
}

// NewRegistry creates a new provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]Provider),
	}
}

// Register adds a provider to the registry.
func (r *Registry) Register(p Provider) {
	r.providers[p.Name()] = p
}

// Get returns a provider by name.
func (r *Registry) Get(name string) (Provider, bool) {
	p, ok := r.providers[name]
	return p, ok
}

// List returns all registered providers.
func (r *Registry) List() []Provider {
	result := make([]Provider, 0, len(r.providers))
	for _, p := range r.providers {
		result = append(result, p)
	}
	return result
}
