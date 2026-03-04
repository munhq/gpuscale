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

	// Expand offers across all regions. When a region returns 503 (no capacity),
	// the coordinator's retry-next-offer logic will try the next region naturally.
	locations, err := p.client.ListLocations(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing verda locations: %w", err)
	}
	seen := make(map[string]bool)
	var regionCodes []string
	for _, loc := range locations {
		if !seen[loc.Code] {
			seen[loc.Code] = true
			regionCodes = append(regionCodes, loc.Code)
		}
	}
	if len(regionCodes) == 0 {
		regionCodes = []string{""} // fallback: let Verda pick
	}

	isSpot := req.CapacityType == "spot"

	var offers []provider.Offer
	for _, t := range types {
		gpuCount := t.GPU.NumberOfGPUs
		vramGB := t.GPUMemory.SizeInGigabytes
		ramGB := t.Memory.SizeInGigabytes
		gpuType := t.Model

		if !req.MultiGpu && gpuCount > 1 {
			continue
		}
		totalVRAM := gpuCount * vramGB
		if req.MinVRAM > 0 && totalVRAM < req.MinVRAM {
			continue
		}
		if req.MinRAM > 0 && ramGB < req.MinRAM {
			continue
		}

		onDemandPrice, err := parsePrice(t.PricePerHour)
		if err != nil {
			continue
		}
		spotPrice, _ := parsePrice(t.SpotPrice)

		price := onDemandPrice
		capacityType := "on-demand"
		if isSpot && spotPrice > 0 {
			price = spotPrice
			capacityType = "spot"
		}

		if req.MaxPricePerGPU > 0 && gpuCount > 0 && price/float64(gpuCount) > req.MaxPricePerGPU {
			continue
		}

		// One offer per region — coordinator retries through regions on 503.
		for _, region := range regionCodes {
			offers = append(offers, provider.Offer{
				ProviderName: p.Name(),
				OfferID:      t.InstanceType,
				GPUType:      gpuType,
				GPUCount:     gpuCount,
				VRAM:         totalVRAM,
				PricePerHour: price,
				CapacityType: capacityType,
				Reliability:  0.95,
				DiskGB:       0,
				RAMGB:        ramGB,
				Region:       region,
			})
		}
	}

	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("OnStartScript is required for standalone nodes")
	}
	script := config.OnStartScript

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
	// MinDisk = VRAMRequired + 50GB overhead (model weights + CUDA tools).
	// Verda API requires an os_volume object with {name, size}; plain os_volume_size_gb doesn't exist.
	// on_spot_discontinue=keep_detached means the volume survives spot eviction as a detached
	// volume, which our volume-reuse logic can then recover from trash for the next cold start.
	if config.MinDisk > 0 {
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

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   resp.ID,
		NodeType:     "standalone",
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
