package verda

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
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
		// Extract nested fields
		gpuCount := t.GPU.NumberOfGPUs
		vramGB := t.GPUMemory.SizeInGigabytes
		ramGB := t.Memory.SizeInGigabytes
		gpuType := t.Model

		// Filter by GPU count
		if req.GPUCount > 0 && gpuCount < req.GPUCount {
			continue
		}
		// Filter by VRAM
		if req.MinVRAM > 0 && vramGB < req.MinVRAM {
			continue
		}
		// Filter by RAM
		if req.MinRAM > 0 && ramGB < req.MinRAM {
			continue
		}
		// Filter by GPU type
		if len(req.GPUTypes) > 0 && !matchesGPUType(gpuType, req.GPUTypes) {
			continue
		}

		// Parse string prices to float64
		onDemandPrice, err := parsePrice(t.PricePerHour)
		if err != nil {
			continue // Skip offers with invalid prices
		}
		spotPrice, _ := parsePrice(t.SpotPrice) // Ignore error, spotPrice can be 0

		price := onDemandPrice
		capacityType := "on-demand"
		if req.CapacityType == "spot" && spotPrice > 0 {
			price = spotPrice
			capacityType = "spot"
		}

		// Filter by price
		if req.MaxPrice > 0 && price > req.MaxPrice {
			continue
		}

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      t.InstanceType,
			GPUType:      gpuType,
			GPUCount:     gpuCount,
			VRAM:         vramGB,
			PricePerHour: price,
			CapacityType: capacityType,
			Reliability:  0.95, // Verda doesn't expose reliability; use a reasonable default
			DiskGB:       0,    // Verda API doesn't expose disk in this endpoint
			RAMGB:        ramGB,
		})
	}

	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	var script string
	if config.OnStartScript != "" {
		// Caller provided a bootstrap script — use it as-is.
		script = config.OnStartScript
	} else if config.NodeType == "ray-worker" {
		// Fallback: no script provided, generate standalone vLLM bootstrap.
		script = generateVLLMBootstrapScript(config)
	} else {
		return nil, fmt.Errorf("full-node requires OnStartScript")
	}

	scriptResp, err := p.client.CreateStartupScript(ctx, fmt.Sprintf("gpuscale-%s", config.InstanceID), script)
	if err != nil {
		return nil, fmt.Errorf("creating startup script: %w", err)
	}

	// Try to reuse a detached OS volume — boots faster since packages are
	// already installed (Netbird, K3s, NVIDIA toolkit, etc.).
	// Pass the volume UUID as the "image" field; Verda boots from it directly.
	verdaImage := config.Image
	if verdaImage == "" {
		verdaImage = "ubuntu-24.04-cuda-12.8-open-docker"
	}
	if vol := p.findReusableVolume(ctx); vol != "" {
		verdaImage = vol
	}

	// Fetch SSH keys from account — required for non-OS-volume images.
	// When booting from a reused volume, SSH keys are already baked in,
	// but Verda still requires the field.
	sshKeys, err := p.client.ListSSHKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing ssh keys: %w", err)
	}
	var sshKeyIDs []string
	for _, k := range sshKeys {
		sshKeyIDs = append(sshKeyIDs, k.ID)
	}
	var sshKeyIDsPtr *[]string
	if len(sshKeyIDs) > 0 {
		sshKeyIDsPtr = &sshKeyIDs
	}

	createReq := CreateInstanceRequest{
		InstanceType:    offer.OfferID,
		Image:           verdaImage,
		Description:     fmt.Sprintf("gpuscale %s", config.InstanceID),
		SSHKeyIDs:       sshKeyIDsPtr,
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
	if err := p.client.DeleteInstance(ctx, instanceID); err != nil {
		return err
	}
	// Delete all detached OS volumes to stop $10/mo charges.
	// Verda allows restoring deleted volumes within 96h, so if a new
	// request comes in we can recover one and boot from it.
	p.cleanupDetachedVolumes(ctx)
	return nil
}

func (p *Provider) cleanupDetachedVolumes(ctx context.Context) {
	vols, err := p.client.ListVolumes(ctx)
	if err != nil {
		return
	}
	for _, v := range vols {
		if v.Status == "detached" && v.IsOSVolume {
			_ = p.client.DeleteVolume(ctx, v.ID)
		}
	}
}

// findReusableVolume returns the ID of a detached OS volume that can be
// reused as the boot image for a new instance, or "" if none available.
func (p *Provider) findReusableVolume(ctx context.Context) string {
	vols, err := p.client.ListVolumes(ctx)
	if err != nil {
		return ""
	}
	for _, v := range vols {
		if v.Status == "detached" && v.IsOSVolume {
			return v.ID
		}
	}
	return ""
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
	hfRepo := modelID
	if config.ModelSource != "" {
		hfRepo = strings.TrimPrefix(config.ModelSource, "hf:")
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

	tpFlag := ""
	if config.TensorParallelSize > 1 {
		tpFlag = fmt.Sprintf(" \\\n  --tensor-parallel-size %d", config.TensorParallelSize)
	}

	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail

echo '[gpuscale] Starting vLLM worker on verda'
echo '[gpuscale] Instance: %s, GPU: %s'
echo '[gpuscale] Model: %s (repo: %s)'

# Install vLLM if not present (Verda images may not have it pre-installed)
if ! python -c "import vllm" 2>/dev/null; then
  echo '[gpuscale] Installing vLLM...'
  pip install vllm 2>&1 | tail -10
fi

exec python -m vllm.entrypoints.openai.api_server \
  --model '%s' \
  --host 0.0.0.0 \
  --port %d \
  --gpu-memory-utilization %.2f \
  --max-model-len %d \
  --dtype %s%s%s
`, config.InstanceID, config.GPUType, modelID, hfRepo,
		hfRepo, servePort, gpuMemUtil, maxModelLen, dtype, tpFlag, trustFlag)
}

func parsePrice(priceStr string) (float64, error) {
	if priceStr == "" {
		return 0, nil
	}
	return strconv.ParseFloat(strings.TrimSpace(priceStr), 64)
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
	case "starting", "booting", "provisioning", "creating", "pending", "scheduled", "":
		return "starting"
	case "stopped", "shutdown", "deleted":
		return "stopped"
	default:
		return "error"
	}
}
