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
	GPUCount     int      // nvidia.com/gpu limit
	MinVRAM      int      // GB, from annotation
	MaxVRAM      int      // GB, max VRAM per GPU to prevent over-provisioning (0 = no limit)
	GPUTypes     []string // preferred GPU types (empty = any)
	MaxPrice     float64  // $/hr, 0 = no limit
	CapacityType string   // "spot" or "on-demand"
	MinDisk      int      // GB
	MinRAM       int      // GB
	NodeType     string   // "full-node" or "ray-worker" — used by providers to filter VM vs container offers
}

// Offer represents an available GPU instance from a provider.
type Offer struct {
	ProviderName string
	OfferID      string  // provider-specific offer/machine ID
	GPUType      string  // "RTX 4090", "A100 80GB", etc
	GPUCount     int
	VRAM         int     // per GPU, in GB
	PricePerHour float64
	CapacityType string // "spot" or "on-demand"
	Region       string
	Reliability  float64 // 0-1, provider-specific
	DiskGB       int
	RAMGB        int
}

// BootstrapConfig contains the configuration needed to bootstrap a node.
type BootstrapConfig struct {
	NodeType     string // "full-node" or "ray-worker"
	Image        string // Docker image for the node
	NetbirdKey   string // VPN setup key (full-node only)
	K8sURL       string // Kubernetes API server URL (full-node only)
	K8sToken     string // Kubernetes join token (full-node only)
	RayHeadAddr  string // Ray head address (ray-worker only, empty = run as head)
	RayDashPort  int    // Ray dashboard port (ray-worker only, default 8265)
	RayServePort int    // Ray serve port (ray-worker only, default 8000)

	// Model config (ray-worker only)
	ModelID             string  // HuggingFace model ID (e.g., "THUDM/glm-4-9b-chat")
	ModelSource         string  // HuggingFace model source (defaults to ModelID)
	MaxModelLen         int     // vLLM max_model_len
	DType               string  // "auto", "float16", "bfloat16"
	GPUMemUtil          float64 // gpu_memory_utilization (0.0-1.0)
	TrustRemoteCode     bool    // allow custom model code
	EnablePrefixCaching bool    // vLLM prefix caching for KV cache reuse
	MaxOngoingRequests  int     // max concurrent requests per worker
	TensorParallelSize  int     // tensor_parallel_size for vLLM multi-GPU

	ModelCacheURL string            // optional rclone URL for model cache
	InstanceID    string            // unique ID for tracking
	GPUType       string            // GPU type label
	ProviderName  string            // provider name label
	ExtraEnv      map[string]string // additional env vars

	// Pre-generated bootstrap script and env vars for full-node mode.
	// Callers set these when they have the bootstrap package available.
	// If empty for ray-worker mode, providers generate their own vLLM script.
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
