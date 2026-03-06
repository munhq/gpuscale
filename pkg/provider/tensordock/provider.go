package tensordock

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// Provider implements provider.Provider for TensorDock marketplace.
// TensorDock is an on-demand-only marketplace (no spot pricing).
type Provider struct {
	client *Client
}

// New creates a new TensorDock provider.
func New(apiKey, apiToken string) *Provider {
	return &Provider{client: NewClient(apiKey, apiToken)}
}

func (p *Provider) Name() string { return "tensordock" }

func (p *Provider) Validate(ctx context.Context) error {
	if _, err := p.client.ListHosts(ctx); err != nil {
		return fmt.Errorf("tensordock credential check: %w", err)
	}
	return nil
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	hosts, err := p.client.ListHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("tensordock: list hosts: %w", err)
	}

	var offers []provider.Offer
	for _, h := range hosts {
		gpuType := h.GPU.Type
		gpuCount := h.GPU.Amount
		vramPerGPU := h.GPU.VRAM // GB per GPU
		totalVRAM := gpuCount * vramPerGPU

		if gpuType == "" || gpuCount == 0 {
			continue
		}

		// GPU type filter (soft preference — skip if no match and GPUTypes specified)
		if len(req.GPUTypes) > 0 && !matchesGPUType(gpuType, req.GPUTypes) {
			continue
		}
		// Multi-GPU filter
		if !req.MultiGpu && gpuCount > 1 {
			continue
		}
		// VRAM filter
		if req.MinVRAM > 0 && totalVRAM < req.MinVRAM {
			continue
		}
		if req.MaxVRAM > 0 && vramPerGPU > req.MaxVRAM {
			continue
		}
		// RAM filter
		if req.MinRAM > 0 && h.RAM < req.MinRAM {
			continue
		}
		// Disk filter
		if req.MinDisk > 0 && h.Storage < req.MinDisk {
			continue
		}

		pricePerHour := h.Pricing.GPU.Hourly * float64(gpuCount)
		if req.MaxPricePerGPU > 0 && gpuCount > 0 && h.Pricing.GPU.Hourly > req.MaxPricePerGPU {
			continue
		}

		region := h.Location.ID
		if region == "" {
			region = h.Location.Country
		}

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      h.ID,
			GPUType:      gpuType,
			GPUCount:     gpuCount,
			VRAM:         totalVRAM,
			PricePerHour: pricePerHour,
			CapacityType: "on-demand", // TensorDock has no spot
			Region:       region,
			Reliability:  0.95, // TensorDock is datacenter-grade, no spot interruptions
			DiskGB:       h.Storage,
			RAMGB:        h.RAM,
		})
	}
	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("tensordock: OnStartScript is required")
	}

	diskSize := config.MinDisk
	if diskSize <= 0 {
		diskSize = 100
	}

	// Default compute specs — headroom above bare GPU requirements.
	vcpus := 8
	ram := offer.RAMGB
	if ram <= 0 {
		ram = 32
	}

	hostname := sanitize("gpuapi-" + config.InstanceID)
	if len(hostname) > 24 {
		hostname = hostname[:24]
	}

	req := DeployRequest{
		ServerID:        offer.OfferID,
		GPUModel:        offer.GPUType,
		GPUCount:        offer.GPUCount,
		VCPUs:           vcpus,
		RAM:             ram,
		Storage:         diskSize,
		OperatingSystem: "Ubuntu 22.04 LTS",
		ExternalPorts:   "22",
		Hostname:        hostname,
		StartupScript:   config.OnStartScript,
	}

	resp, err := p.client.DeployVM(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("tensordock: deploy VM: %w", err)
	}

	instanceID := resp.InstanceID
	if instanceID == "" {
		instanceID = resp.ServerID
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		NodeType:     "standalone",
		IP:           resp.IP,
		Status:       "starting",
		GPUType:      offer.GPUType,
		GPUCount:     offer.GPUCount,
		PricePerHour: offer.PricePerHour,
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) DestroyInstance(ctx context.Context, instanceID string) error {
	return p.client.DeleteInstance(ctx, instanceID)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	resp, err := p.client.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	status := normalizeStatus(resp.Status)
	if status == "stopped" {
		return nil, provider.ErrInstanceNotFound
	}
	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		IP:           resp.IP,
		Status:       status,
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	// TensorDock lists deployed instances via the same /client/list/ endpoint
	// with a different filter; return empty for now since we track in our DB.
	return nil, nil
}

func normalizeStatus(s string) string {
	switch strings.ToLower(s) {
	case "running", "online":
		return "running"
	case "deploying", "starting", "pending", "created", "":
		return "starting"
	case "stopped", "offline", "terminated", "deleted":
		return "stopped"
	default:
		return "error"
	}
}

func matchesGPUType(gpuType string, want []string) bool {
	lower := strings.ToLower(gpuType)
	for _, w := range want {
		if strings.Contains(lower, strings.ToLower(w)) {
			return true
		}
	}
	return false
}

func sanitize(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteRune(c)
		case c >= 'A' && c <= 'Z':
			b.WriteRune(c + 32) // to lower
		case c == '_' || c == ' ':
			b.WriteRune('-')
		}
	}
	return b.String()
}
