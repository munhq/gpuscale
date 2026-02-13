package verda

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/munhq/gpuscale/internal/bootstrap"
	"github.com/munhq/gpuscale/internal/provider"
)

// Provider implements the provider.Provider interface for Verda.
type Provider struct {
	client *Client
}

// New creates a new Verda provider.
func New(clientID, clientSecret string) *Provider {
	return &Provider{
		client: NewClient(clientID, clientSecret),
	}
}

func (p *Provider) Name() string {
	return "verda"
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	types, err := p.client.ListInstanceTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing verda instance types: %w", err)
	}

	var offers []provider.Offer
	for _, t := range types {
		// Filter by GPU count
		if req.GPUCount > 0 && t.GPUCount < req.GPUCount {
			continue
		}
		// Filter by VRAM
		if req.MinVRAM > 0 && t.VRAMGB < req.MinVRAM {
			continue
		}
		// Filter by RAM
		if req.MinRAM > 0 && t.RAMGB < req.MinRAM {
			continue
		}
		// Filter by GPU type
		if len(req.GPUTypes) > 0 && !matchesGPUType(t.GPUType, req.GPUTypes) {
			continue
		}

		price := t.OnDemandPrice
		capacityType := "on-demand"
		if req.CapacityType == "spot" && t.SpotPrice > 0 {
			price = t.SpotPrice
			capacityType = "spot"
		}

		// Filter by price
		if req.MaxPrice > 0 && price > req.MaxPrice {
			continue
		}

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      t.ID,
			GPUType:      t.GPUType,
			GPUCount:     t.GPUCount,
			VRAM:         t.VRAMGB,
			PricePerHour: price,
			CapacityType: capacityType,
			Reliability:  0.95, // Verda doesn't expose reliability; use a reasonable default
			DiskGB:       t.DiskGB,
			RAMGB:        t.RAMGB,
		})
	}

	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	var script string
	if config.NodeType == "ray-worker" {
		script = generateVLLMBootstrapScript(config)
	} else {
		// Full-node mode — VM joins Kubernetes cluster via VPN
		script = bootstrap.GenerateScript(config)
	}

	scriptResp, err := p.client.CreateStartupScript(ctx, fmt.Sprintf("gpuscale-%s", config.InstanceID), script)
	if err != nil {
		return nil, fmt.Errorf("creating startup script: %w", err)
	}

	createReq := CreateInstanceRequest{
		InstanceType:    offer.OfferID,
		Image:           "ubuntu-24.04-cuda-12.8-open-docker",
		Hostname:        fmt.Sprintf("gpuscale-%s", config.InstanceID),
		StartupScriptID: scriptResp.ID,
		IsSpot:          offer.CapacityType == "spot",
	}

	resp, err := p.client.CreateInstance(ctx, createReq)
	if err != nil {
		return nil, fmt.Errorf("creating verda instance: %w", err)
	}

	// Build endpoint for ray-worker type
	endpoint := ""
	if config.NodeType == "ray-worker" && resp.IP != "" {
		servePort := config.RayServePort
		if servePort == 0 {
			servePort = 8000
		}
		endpoint = fmt.Sprintf("http://%s:%d", resp.IP, servePort)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   resp.ID,
		NodeType:     config.NodeType,
		IP:           resp.IP,
		SSHPort:      22,
		Endpoint:     endpoint,
		Status:       normalizeStatus(resp.Status),
		GPUType:      resp.GPUType,
		GPUCount:     resp.GPUCount,
		PricePerHour: resp.PricePerHour,
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
	if resp == nil {
		return nil, provider.ErrInstanceNotFound
	}

	createdAt, _ := time.Parse(time.RFC3339, resp.CreatedAt)

	// Build endpoint if the instance has an IP
	endpoint := ""
	if resp.IP != "" {
		endpoint = fmt.Sprintf("http://%s:8000", resp.IP)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   resp.ID,
		IP:           resp.IP,
		Endpoint:     endpoint,
		Status:       normalizeStatus(resp.Status),
		GPUType:      resp.GPUType,
		GPUCount:     resp.GPUCount,
		PricePerHour: resp.PricePerHour,
		CreatedAt:    createdAt,
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	instances, err := p.client.ListInstances(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*provider.Instance, 0, len(instances))
	for _, inst := range instances {
		createdAt, _ := time.Parse(time.RFC3339, inst.CreatedAt)
		result = append(result, &provider.Instance{
			ProviderName: p.Name(),
			InstanceID:   inst.ID,
			IP:           inst.IP,
			Status:       normalizeStatus(inst.Status),
			GPUType:      inst.GPUType,
			GPUCount:     inst.GPUCount,
			PricePerHour: inst.PricePerHour,
			CreatedAt:    createdAt,
		})
	}
	return result, nil
}

func generateVLLMBootstrapScript(config provider.BootstrapConfig) string {
	servePort := config.RayServePort
	if servePort == 0 {
		servePort = 8000
	}
	modelID := config.ModelID
	if modelID == "" {
		modelID = "THUDM/glm-4-9b-chat"
	}
	maxModelLen := config.MaxModelLen
	if maxModelLen == 0 {
		maxModelLen = 4096
	}
	dtype := config.DType
	if dtype == "" {
		dtype = "auto"
	}
	gpuMemUtil := config.GPUMemUtil
	if gpuMemUtil <= 0 {
		gpuMemUtil = 0.90
	}

	trustFlag := ""
	if config.TrustRemoteCode {
		trustFlag = " \\\n  --trust-remote-code"
	}

	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail

echo '[gpuscale] Starting vLLM worker on verda'
echo '[gpuscale] Instance: %s, GPU: %s'
echo '[gpuscale] Model: %s'

# Install vLLM
pip install vllm 2>&1 | tail -10

# Start vLLM OpenAI-compatible server
exec python -m vllm.entrypoints.openai.api_server \
  --model '%s' \
  --host 0.0.0.0 \
  --port %d \
  --gpu-memory-utilization %.2f \
  --max-model-len %d \
  --dtype %s%s
`, config.InstanceID, config.GPUType, modelID,
		modelID, servePort, gpuMemUtil, maxModelLen, dtype, trustFlag)
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
	case "running", "active":
		return "running"
	case "starting", "booting", "provisioning":
		return "starting"
	case "stopped", "shutdown", "deleted":
		return "stopped"
	default:
		return "error"
	}
}
