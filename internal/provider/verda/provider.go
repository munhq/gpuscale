package verda

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	// First, create a startup script with the bootstrap configuration
	script := generateBootstrapScript(config)
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

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   resp.ID,
		IP:           resp.IP,
		SSHPort:      22,
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

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   resp.ID,
		IP:           resp.IP,
		SSHPort:      22,
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
			SSHPort:      22,
			Status:       normalizeStatus(inst.Status),
			GPUType:      inst.GPUType,
			GPUCount:     inst.GPUCount,
			PricePerHour: inst.PricePerHour,
			CreatedAt:    createdAt,
		})
	}
	return result, nil
}

func generateBootstrapScript(config provider.BootstrapConfig) string {
	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail

# Install Netbird
curl -fsSL https://pkgs.netbird.io/install.sh | sh

# Join VPN
netbird up --setup-key "%s" --daemon
sleep 5
NETBIRD_IP=$(ip -4 addr show wt0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

# Install and start K3s agent
curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true INSTALL_K3S_EXEC="agent" sh -
k3s agent \
  --server "%s" \
  --token "%s" \
  --node-ip "$NETBIRD_IP" \
  --flannel-iface wt0 \
  --node-label "gpuscale.io/managed=true" \
  --node-label "gpuscale.io/provider=verda" \
  --node-label "gpuscale.io/gpu-type=%s" \
  --node-label "gpuscale.io/instance-id=%s" \
  --node-taint "nvidia.com/gpu:NoSchedule" &

wait
`, config.NetbirdKey, config.K3sURL, config.K3sToken, config.GPUType, config.InstanceID)
	return script
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
