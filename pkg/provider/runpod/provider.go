package runpod

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// Provider implements the provider.Provider interface for RunPod.
type Provider struct {
	client *Client
}

// New creates a new RunPod provider.
func New(apiKey string) *Provider {
	return &Provider{
		client: NewClient(apiKey),
	}
}

func (p *Provider) Name() string {
	return "runpod"
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	types, err := p.client.ListGPUTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing runpod GPU types: %w", err)
	}

	var offers []provider.Offer
	for _, t := range types {
		// Filter by GPU type
		if len(req.GPUTypes) > 0 && !matchesGPUType(t.DisplayName, req.GPUTypes) {
			continue
		}
		if !req.MultiGpu && req.GPUCount > 1 {
			continue
		}

		gpuCount := 1
		if req.GPUCount > 0 {
			gpuCount = req.GPUCount
		}

		// Total VRAM across all GPUs must cover the requirement.
		totalVRAM := t.MemoryInGB * gpuCount
		if req.MinVRAM > 0 && totalVRAM < req.MinVRAM {
			continue
		}

		// Select pricing based on capacity type
		var price float64
		capacityType := "on-demand"
		if req.CapacityType == "spot" {
			if t.CommunitySpotPrice > 0 {
				price = t.CommunitySpotPrice * float64(gpuCount)
				capacityType = "spot"
			} else if t.SecureSpotPrice > 0 {
				price = t.SecureSpotPrice * float64(gpuCount)
				capacityType = "spot"
			} else {
				price = t.CommunityPrice * float64(gpuCount)
			}
		} else {
			if t.CommunityPrice > 0 {
				price = t.CommunityPrice * float64(gpuCount)
			} else {
				price = t.SecurePrice * float64(gpuCount)
			}
		}

		if price == 0 {
			continue
		}

		// Per-GPU price cap.
		if req.MaxPricePerGPU > 0 && gpuCount > 0 && price/float64(gpuCount) > req.MaxPricePerGPU {
			continue
		}

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      t.ID,
			GPUType:      t.DisplayName,
			GPUCount:     gpuCount,
			VRAM:         totalVRAM,
			PricePerHour: price,
			CapacityType: capacityType,
			Reliability:  0.90, // RunPod doesn't expose reliability
			DiskGB:       50,   // Default
			RAMGB:        0,    // Not specified in GPU types API
		})
	}

	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("OnStartScript is required for standalone nodes")
	}

	image := config.Image
	if image == "" {
		image = "vllm/vllm-openai:latest"
	}

	env := config.OnStartEnv
	for k, v := range config.ExtraEnv {
		if env == nil {
			env = make(map[string]string)
		}
		env[k] = v
	}

	// RunPod doesn't support onstart scripts directly — embed as docker args.
	dockerArgs := fmt.Sprintf("bash -c %q", config.OnStartScript)
	ports := "22/tcp"

	cloudType := "COMMUNITY"
	if offer.CapacityType == "on-demand" {
		cloudType = "SECURE"
	}

	createReq := CreatePodRequest{
		Name:              fmt.Sprintf("gpuscale-%s", config.InstanceID),
		ImageName:         image,
		GPUTypeID:         offer.OfferID,
		GPUCount:          offer.GPUCount,
		CloudType:         cloudType,
		VolumeInGB:        50,
		ContainerDiskInGB: 20,
		Env:               env,
		DockerArgs:        dockerArgs,
		Ports:             ports,
		StartSSH:          true,
	}

	pod, err := p.client.CreatePod(ctx, createReq)
	if err != nil {
		return nil, fmt.Errorf("creating runpod pod: %w", err)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   pod.ID,
		NodeType:     "standalone",
		Status:       normalizeStatus(pod.DesiredStatus),
		GPUType:      offer.GPUType,
		GPUCount:     offer.GPUCount,
		PricePerHour: pod.CostPerHr,
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) DestroyInstance(ctx context.Context, instanceID string) error {
	return p.client.DeletePod(ctx, instanceID)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	pod, err := p.client.GetPod(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, provider.ErrInstanceNotFound
	}

	gpuType := pod.Machine.GPUDisplayName
	gpuCount := 0
	if pod.Runtime != nil {
		gpuCount = len(pod.Runtime.GPUs)
		if gpuCount > 0 && gpuType == "" {
			gpuType = pod.Runtime.GPUs[0].DisplayName
		}
	}

	var ip string
	var endpoint string
	if pod.Runtime != nil {
		for _, port := range pod.Runtime.Ports {
			if port.PrivatePort == 22 && ip == "" {
				ip = port.IP
			}
			if port.PrivatePort == 8000 && port.IP != "" {
				endpoint = fmt.Sprintf("http://%s:%d", port.IP, port.PublicPort)
			}
		}
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   pod.ID,
		IP:           ip,
		Endpoint:     endpoint,
		Status:       normalizeStatus(pod.DesiredStatus),
		GPUType:      gpuType,
		GPUCount:     gpuCount,
		PricePerHour: pod.CostPerHr,
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	pods, err := p.client.ListPods(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*provider.Instance, 0, len(pods))
	for _, pod := range pods {
		gpuType := pod.Machine.GPUDisplayName
		gpuCount := 0
		if pod.Runtime != nil {
			gpuCount = len(pod.Runtime.GPUs)
		}

		result = append(result, &provider.Instance{
			ProviderName: p.Name(),
			InstanceID:   pod.ID,
			Status:       normalizeStatus(pod.DesiredStatus),
			GPUType:      gpuType,
			GPUCount:     gpuCount,
			PricePerHour: pod.CostPerHr,
		})
	}
	return result, nil
}

func matchesGPUType(gpuType string, wanted []string) bool {
	gpuLower := strings.ToLower(gpuType)
	for _, w := range wanted {
		if strings.ToLower(w) == gpuLower || strings.Contains(gpuLower, strings.ToLower(w)) {
			return true
		}
	}
	return false
}

func normalizeStatus(status string) string {
	switch strings.ToLower(status) {
	case "running":
		return "running"
	case "created", "starting", "restarting":
		return "starting"
	case "exited", "stopped", "terminated":
		return "stopped"
	default:
		return "error"
	}
}
