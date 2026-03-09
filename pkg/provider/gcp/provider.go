package gcp

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// machineSpec describes a GCP GPU machine type with approximate 2025 spot pricing.
// For A2/G2/A3 machines the GPU is built-in (no AcceleratorType needed).
// For N1 machines the GPU is attached separately via guestAccelerators.
type machineSpec struct {
	MachineType     string
	AcceleratorType string // empty for A2/G2/A3
	AcceleratorCount int
	GPUType         string
	GPUCount        int
	VRAMPerGPU      int     // GB
	RAMGB           int
	SpotPrice       float64 // $/hr per instance (approximate)
	OnDemandPrice   float64
}

// catalog of GCP GPU machine types. Spot prices ~2025.
var catalog = []machineSpec{
	// T4 — cheapest, 16GB, good for small models
	{MachineType: "n1-standard-4", AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 1,
		GPUType: "Tesla T4", GPUCount: 1, VRAMPerGPU: 16, RAMGB: 15, SpotPrice: 0.11, OnDemandPrice: 0.35},
	// L4 — 24GB, efficient for inference
	{MachineType: "g2-standard-4", AcceleratorType: "", AcceleratorCount: 0,
		GPUType: "NVIDIA L4", GPUCount: 1, VRAMPerGPU: 24, RAMGB: 16, SpotPrice: 0.21, OnDemandPrice: 0.70},
	// V100 16GB
	{MachineType: "n1-standard-8", AcceleratorType: "nvidia-tesla-v100", AcceleratorCount: 1,
		GPUType: "Tesla V100", GPUCount: 1, VRAMPerGPU: 16, RAMGB: 30, SpotPrice: 0.22, OnDemandPrice: 0.74},
	// A100 40GB
	{MachineType: "a2-highgpu-1g", AcceleratorType: "", AcceleratorCount: 0,
		GPUType: "NVIDIA A100 40GB", GPUCount: 1, VRAMPerGPU: 40, RAMGB: 85, SpotPrice: 0.88, OnDemandPrice: 2.93},
	// A100 80GB
	{MachineType: "a2-ultragpu-1g", AcceleratorType: "", AcceleratorCount: 0,
		GPUType: "NVIDIA A100 80GB", GPUCount: 1, VRAMPerGPU: 80, RAMGB: 170, SpotPrice: 1.41, OnDemandPrice: 4.70},
	// H100 80GB
	{MachineType: "a3-highgpu-1g", AcceleratorType: "", AcceleratorCount: 0,
		GPUType: "NVIDIA H100 80GB", GPUCount: 1, VRAMPerGPU: 80, RAMGB: 234, SpotPrice: 3.00, OnDemandPrice: 10.00},
	// 4x T4
	{MachineType: "n1-standard-16", AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 4,
		GPUType: "Tesla T4", GPUCount: 4, VRAMPerGPU: 16, RAMGB: 60, SpotPrice: 0.44, OnDemandPrice: 1.40},
	// 2x L4
	{MachineType: "g2-standard-8", AcceleratorType: "", AcceleratorCount: 0,
		GPUType: "NVIDIA L4", GPUCount: 2, VRAMPerGPU: 24, RAMGB: 32, SpotPrice: 0.42, OnDemandPrice: 1.40},
}

// Provider implements provider.Provider for Google Cloud Compute Engine.
type Provider struct {
	client *Client
	zones  []string
}

// New creates a GCP provider from a service account JSON string, project ID, and
// comma-separated zone list (e.g. "us-central1-a,us-east1-d").
func New(saJSON, projectID, zones string) (*Provider, error) {
	client, err := NewClient(saJSON, projectID)
	if err != nil {
		return nil, err
	}
	zoneList := []string{"us-central1-a", "us-east1-d", "europe-west4-a"}
	if zones != "" {
		zoneList = strings.Split(zones, ",")
	}
	return &Provider{client: client, zones: zoneList}, nil
}

func (p *Provider) Name() string { return "gcp" }

func (p *Provider) Validate(ctx context.Context) error {
	if _, err := p.client.token(ctx); err != nil {
		return fmt.Errorf("gcp credential check: %w", err)
	}
	return nil
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	isSpot := req.CapacityType == "spot"

	var offers []provider.Offer
	for _, spec := range catalog {
		if !req.MultiGpu && spec.GPUCount > 1 {
			continue
		}
		totalVRAM := spec.GPUCount * spec.VRAMPerGPU
		if req.MinVRAM > 0 && totalVRAM < req.MinVRAM {
			continue
		}
		if req.MaxVRAM > 0 && spec.VRAMPerGPU > req.MaxVRAM {
			continue
		}
		if req.MinRAM > 0 && spec.RAMGB < req.MinRAM {
			continue
		}

		price := spec.OnDemandPrice
		capacityType := "on-demand"
		if isSpot {
			price = spec.SpotPrice
			capacityType = "spot"
		}
		if req.MaxPricePerHour > 0 && spec.GPUCount > 0 && price/float64(spec.GPUCount) > req.MaxPricePerHour {
			continue
		}

		// Emit one offer per zone — zone is embedded in OfferID so CreateInstance knows where to deploy.
		for _, zone := range p.zones {
			offers = append(offers, provider.Offer{
				ProviderName: p.Name(),
				OfferID:      spec.MachineType + "@" + zone,
				GPUType:      spec.GPUType,
				GPUCount:     spec.GPUCount,
				VRAM:         totalVRAM,
				PricePerHour: price,
				CapacityType: capacityType,
				Region:       zone,
				Reliability:  0.85, // GCP spot: ~30s preemption notice
				RAMGB:        spec.RAMGB,
			})
		}
	}
	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("gcp provider requires OnStartScript (full-node bootstrap)")
	}

	machineType, zone := splitOfferID(offer.OfferID)
	if zone == "" {
		zone = p.zones[0]
	}

	var spec *machineSpec
	for i := range catalog {
		if catalog[i].MachineType == machineType {
			spec = &catalog[i]
			break
		}
	}
	if spec == nil {
		return nil, fmt.Errorf("unknown GCP machine type: %s", machineType)
	}

	instanceName := sanitizeName("gpuscale-" + config.InstanceID)
	if len(instanceName) > 63 {
		instanceName = instanceName[:63]
	}

	diskSize := config.MinDisk
	if diskSize <= 0 {
		diskSize = 100
	}

	req := createInstanceReq{
		Name:        instanceName,
		MachineType: fmt.Sprintf("zones/%s/machineTypes/%s", zone, machineType),
		Disks: []gcpDisk{
			{
				Boot:       true,
				AutoDelete: true,
				Type:       "PERSISTENT",
				InitializeParams: gcpDiskParams{
					SourceImage: "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts",
					DiskSizeGb:  strconv.Itoa(diskSize),
				},
			},
		},
		NetworkInterfaces: []gcpNIC{
			{
				Network:      "global/networks/default",
				AccessConfigs: []gcpNATCfg{{Type: "ONE_TO_ONE_NAT", Name: "External NAT"}},
			},
		},
		Scheduling: gcpScheduling{
			ProvisioningModel: "SPOT",
			OnHostMaintenance: "TERMINATE",
			AutomaticRestart:  false,
		},
		Labels: map[string]string{
			"managed-by": "gpuscale",
			"model":      sanitizeLabel(config.ModelID),
		},
		Metadata: gcpMetadata{
			Items: []gcpMetaItem{
				{Key: "startup-script", Value: config.OnStartScript},
			},
		},
	}

	if offer.CapacityType != "spot" {
		req.Scheduling.ProvisioningModel = "STANDARD"
		req.Scheduling.OnHostMaintenance = "MIGRATE"
		req.Scheduling.AutomaticRestart = true
	}

	// N1 machines need an explicit GPU attachment; A2/G2/A3 have it built-in.
	if spec.AcceleratorType != "" {
		req.GuestAccelerators = []gcpAccel{
			{
				AcceleratorType:  fmt.Sprintf("zones/%s/acceleratorTypes/%s", zone, spec.AcceleratorType),
				AcceleratorCount: spec.AcceleratorCount,
			},
		}
	}

	if err := p.client.CreateInstance(ctx, zone, req); err != nil {
		return nil, err
	}

	// Instance creation is async — return stub with instanceID; reconciler polls GetInstance.
	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceName + "@" + zone,
		NodeType:     config.NodeType,
		Status:       "starting",
		GPUType:      spec.GPUType,
		GPUCount:     spec.GPUCount,
		PricePerHour: offer.PricePerHour,
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) DestroyInstance(ctx context.Context, instanceID string) error {
	name, zone := splitOfferID(instanceID)
	if zone == "" {
		zone = p.zones[0]
	}
	return p.client.DeleteInstance(ctx, zone, name)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	name, zone := splitOfferID(instanceID)
	if zone == "" {
		zone = p.zones[0]
	}
	inst, err := p.client.GetInstance(ctx, zone, name)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, provider.ErrInstanceNotFound
	}
	ip := ""
	if len(inst.NetworkInterfaces) > 0 && len(inst.NetworkInterfaces[0].AccessConfigs) > 0 {
		ip = inst.NetworkInterfaces[0].AccessConfigs[0].NatIP
	}
	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		IP:           ip,
		Status:       normalizeStatus(inst.Status),
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	var result []*provider.Instance
	for _, zone := range p.zones {
		instances, err := p.client.ListInstances(ctx, zone)
		if err != nil {
			continue
		}
		for _, inst := range instances {
			ip := ""
			if len(inst.NetworkInterfaces) > 0 && len(inst.NetworkInterfaces[0].AccessConfigs) > 0 {
				ip = inst.NetworkInterfaces[0].AccessConfigs[0].NatIP
			}
			result = append(result, &provider.Instance{
				ProviderName: p.Name(),
				InstanceID:   inst.Name + "@" + zone,
				IP:           ip,
				Status:       normalizeStatus(inst.Status),
				CreatedAt:    time.Now(),
			})
		}
	}
	return result, nil
}

func normalizeStatus(s string) string {
	switch strings.ToUpper(s) {
	case "RUNNING":
		return "running"
	case "PROVISIONING", "STAGING":
		return "starting"
	case "STOPPING", "STOPPED", "TERMINATED":
		return "stopped"
	default:
		return "error"
	}
}

func splitOfferID(id string) (name, zone string) {
	parts := strings.SplitN(id, "@", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return id, ""
}

func sanitizeName(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}

func sanitizeLabel(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteRune('-')
		}
	}
	result := b.String()
	if len(result) > 63 {
		return result[:63]
	}
	return result
}
