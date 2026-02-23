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
	client      *Client
	volumeStore provider.VolumeStore
}

// New creates a new Verda provider.
func New(clientID, clientSecret string) *Provider {
	return &Provider{
		client: NewClient(clientID, clientSecret),
	}
}

// SetVolumeStore configures the external volume→model tracking store.
// Required for model-aware volume reuse since Verda has no native volume tags.
func (p *Provider) SetVolumeStore(vs provider.VolumeStore) {
	p.volumeStore = vs
}

func (p *Provider) Name() string {
	return "verda"
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	types, err := p.client.ListInstanceTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing verda instance types: %w", err)
	}

	isSpot := req.CapacityType == "spot"

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
		// GPUTypes is a soft preference handled by the coordinator's sort order.
		// Verda should not hard-filter by GPU type — the coordinator picks the
		// best matching GPU from whatever offers come back.

		// Parse string prices to float64
		onDemandPrice, err := parsePrice(t.PricePerHour)
		if err != nil {
			continue // Skip offers with invalid prices
		}
		spotPrice, _ := parsePrice(t.SpotPrice) // Ignore error, spotPrice can be 0

		price := onDemandPrice
		capacityType := "on-demand"
		if isSpot && spotPrice > 0 {
			price = spotPrice
			capacityType = "spot"
		}

		// Filter by price
		if req.MaxPrice > 0 && price > req.MaxPrice {
			continue
		}

		// Check real-time availability — ListInstanceTypes returns the catalog,
		// not live capacity. Without this check we get 503 on create for offers
		// that exist but have no available nodes in any location.
		// If the availability check itself fails, include the offer anyway and
		// let the create attempt surface the real error.
		location := ""
		avail, err := p.client.CheckAvailability(ctx, t.InstanceType, isSpot)
		if err == nil {
			available := false
			for _, a := range avail {
				if a.Available {
					available = true
					location = a.Location // may be empty if API returns global availability
					break
				}
			}
			if !available {
				continue // confirmed: no capacity anywhere
			}
		}
		// err != nil: availability API failed — include offer with no pinned location

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      t.InstanceType,
			GPUType:      gpuType,
			GPUCount:     gpuCount,
			VRAM:         vramGB,
			PricePerHour: price,
			CapacityType: capacityType,
			Region:       location,
			Reliability:  0.95,
			DiskGB:       0,
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
	if vol := p.findReusableVolume(ctx, config.ModelID, config.MinDisk); vol != "" {
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
		Description:     fmt.Sprintf("gpuscale %s model=%s", config.InstanceID, config.ModelID),
		SSHKeyIDs:       sshKeyIDsPtr,
		Hostname:        fmt.Sprintf("gpuscale-%s", config.InstanceID),
		StartupScriptID: scriptResp.ID,
		LocationCode:    offer.Region,
		IsSpot:          offer.CapacityType == "spot",
	}
	// Set OS volume name and size from the model's disk requirement.
	// MinDisk = VRAMRequired + 50GB overhead (model weights + OS/K3s/CUDA tools).
	// Verda API requires an os_volume object with {name, size}; plain os_volume_size_gb doesn't exist.
	// on_spot_discontinue=keep_detached means the volume survives spot eviction as a detached
	// volume, which our volume-reuse logic can then recover from trash for the next cold start.
	// Only applicable when image is an OS image type (not a volume UUID reuse).
	if config.NodeType == "full-node" && config.MinDisk > 0 {
		createReq.OsVolume = &OsVolumeRequest{
			Name:              fmt.Sprintf("gpuscale-%s-os", config.InstanceID),
			Size:              config.MinDisk,
			OnSpotDiscontinue: "keep_detached",
		}
	}

	resp, err := p.client.CreateInstance(ctx, createReq)
	if err != nil {
		return nil, fmt.Errorf("creating verda instance: %w", err)
	}

	// Track instance→model and disk size so DestroyInstance can tag the volume correctly.
	if p.volumeStore != nil && config.ModelID != "" {
		_ = p.volumeStore.RegisterInstanceModel(ctx, resp.ID, config.ModelID)
		if config.MinDisk > 0 {
			_ = p.volumeStore.SetInstanceDiskSize(ctx, resp.ID, config.MinDisk)
		}
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
	// Before destroying, discover the attached volume and register it for
	// model-aware reuse. Verda has no volume tags, so we track this in Redis.
	if p.volumeStore != nil {
		if modelID := p.volumeStore.GetInstanceModel(ctx, instanceID); modelID != "" {
			sizeGB := p.volumeStore.GetInstanceDiskSize(ctx, instanceID)
			if vols, err := p.client.ListVolumes(ctx); err == nil {
				for _, v := range vols {
					if v.InstanceID == instanceID && v.IsOSVolume {
						_ = p.volumeStore.RegisterVolume(ctx, v.ID, modelID, instanceID, sizeGB)
					}
				}
			}
		}
	}

	if err := p.client.DeleteInstance(ctx, instanceID); err != nil {
		return err
	}
	// Delete detached OS volumes to stop $10/mo charges.
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

// findReusableVolume returns the ID of a detached or recently deleted (trash)
// OS volume that can be reused as the boot image for a new instance.
// Prioritizes volumes tracked in Redis as serving the same model.
// Falls back to any detached OS volume (still has CUDA/deps installed).
// findReusableVolume returns the ID of a successfully bootstrapped OS volume
// that is suitable for reuse. Only volumes marked BootstrapSucceeded=true in
// the volume store are considered; volumes with SizeGB < minDisk are rejected.
// Falls back to trash volumes within the 96h recovery window.
func (p *Provider) findReusableVolume(ctx context.Context, modelID string, minDisk int) string {
	if p.volumeStore == nil {
		return ""
	}

	// Look up a validated volume for this model from the registry.
	// FindVolumeForModel already filters by BootstrapSucceeded and SizeGB.
	trackedVolumeID := p.volumeStore.FindVolumeForModel(ctx, modelID, minDisk)

	// 1. Check for already detached volumes — no restore needed.
	vols, err := p.client.ListVolumes(ctx)
	if err == nil && trackedVolumeID != "" {
		for _, v := range vols {
			if v.ID == trackedVolumeID && v.Status == "detached" && v.IsOSVolume {
				return v.ID
			}
		}
	}

	// 2. Check the trash for recoverable volumes (96h window).
	trashVols, err := p.client.ListDeletedVolumes(ctx)
	if err != nil {
		return ""
	}

	if trackedVolumeID != "" {
		for _, v := range trashVols {
			if v.ID == trackedVolumeID && v.IsOSVolume {
				if err := p.client.RestoreVolume(ctx, v.ID); err == nil {
					return v.ID
				}
			}
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


// StopInstance implements provider.HibernatingProvider.
// Stops the VM without destroying its OS volume so model weights are preserved on disk.
// On next demand, WakeInstance restarts it — the HuggingFace cache hit is instant.
func (p *Provider) StopInstance(ctx context.Context, instanceID string) error {
	return p.client.StopInstance(ctx, instanceID)
}

// WakeInstance implements provider.HibernatingProvider.
// Restarts a previously stopped instance. K3s agent reconnects automatically on boot.
func (p *Provider) WakeInstance(ctx context.Context, instanceID string) error {
	return p.client.StartInstance(ctx, instanceID)
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
