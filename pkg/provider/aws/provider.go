package aws

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// instanceSpec describes an AWS EC2 GPU instance type.
type instanceSpec struct {
	InstanceType  string
	GPUType       string
	GPUCount      int
	VRAMPerGPU    int     // GB
	RAMGB         int
	OnDemandPrice float64 // $/hr (us-east-1 reference)
}

// catalog of AWS GPU instance types. Spot prices are fetched dynamically.
var catalog = []instanceSpec{
	{InstanceType: "g4dn.xlarge",  GPUType: "Tesla T4",    GPUCount: 1, VRAMPerGPU: 16, RAMGB: 16,   OnDemandPrice: 0.526},
	{InstanceType: "g4dn.2xlarge", GPUType: "Tesla T4",    GPUCount: 1, VRAMPerGPU: 16, RAMGB: 32,   OnDemandPrice: 0.752},
	{InstanceType: "g4dn.4xlarge", GPUType: "Tesla T4",    GPUCount: 1, VRAMPerGPU: 16, RAMGB: 64,   OnDemandPrice: 1.204},
	{InstanceType: "g4dn.12xlarge",GPUType: "Tesla T4",    GPUCount: 4, VRAMPerGPU: 16, RAMGB: 192,  OnDemandPrice: 3.912},
	{InstanceType: "g5.xlarge",    GPUType: "NVIDIA A10G", GPUCount: 1, VRAMPerGPU: 24, RAMGB: 16,   OnDemandPrice: 1.006},
	{InstanceType: "g5.2xlarge",   GPUType: "NVIDIA A10G", GPUCount: 1, VRAMPerGPU: 24, RAMGB: 32,   OnDemandPrice: 1.212},
	{InstanceType: "g5.4xlarge",   GPUType: "NVIDIA A10G", GPUCount: 1, VRAMPerGPU: 24, RAMGB: 64,   OnDemandPrice: 1.624},
	{InstanceType: "g5.12xlarge",  GPUType: "NVIDIA A10G", GPUCount: 4, VRAMPerGPU: 24, RAMGB: 192,  OnDemandPrice: 5.672},
	{InstanceType: "p3.2xlarge",   GPUType: "Tesla V100",  GPUCount: 1, VRAMPerGPU: 16, RAMGB: 61,   OnDemandPrice: 3.060},
	{InstanceType: "p3.8xlarge",   GPUType: "Tesla V100",  GPUCount: 4, VRAMPerGPU: 16, RAMGB: 244,  OnDemandPrice: 12.240},
	{InstanceType: "p3.16xlarge",  GPUType: "Tesla V100",  GPUCount: 8, VRAMPerGPU: 16, RAMGB: 488,  OnDemandPrice: 24.480},
	{InstanceType: "p4d.24xlarge", GPUType: "NVIDIA A100", GPUCount: 8, VRAMPerGPU: 40, RAMGB: 1152, OnDemandPrice: 32.770},
	{InstanceType: "p5.48xlarge",  GPUType: "NVIDIA H100", GPUCount: 8, VRAMPerGPU: 80, RAMGB: 2048, OnDemandPrice: 98.320},
}

// spotPriceCache caches AWS spot prices to avoid hitting the API on every SearchOffers call.
type spotPriceCache struct {
	mu     sync.Mutex
	prices map[string]float64
	expiry time.Time
}

// Provider implements provider.Provider for AWS EC2.
type Provider struct {
	client          *Client
	subnetID        string // optional: blank = default VPC
	securityGroupID string // optional: blank = default security group
	spotCache       spotPriceCache
}

// New creates an AWS provider.
// subnetID and securityGroupID are optional; empty string uses AWS defaults.
func New(accessKeyID, secretAccessKey, region, subnetID, securityGroupID string) *Provider {
	return &Provider{
		client:          NewClient(accessKeyID, secretAccessKey, region),
		subnetID:        subnetID,
		securityGroupID: securityGroupID,
	}
}

func (p *Provider) Name() string { return "aws" }

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	isSpot := req.CapacityType == "spot"

	// Fetch current spot prices (cached for 5 minutes).
	spotPrices := p.getSpotPrices(ctx)

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
			if sp, ok := spotPrices[spec.InstanceType]; ok && sp > 0 {
				price = sp
			} else {
				// Fall back to 70% of on-demand as estimate if live price unavailable.
				price = spec.OnDemandPrice * 0.30
			}
			capacityType = "spot"
		}
		if req.MaxPrice > 0 && price > req.MaxPrice {
			continue
		}

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      spec.InstanceType,
			GPUType:      spec.GPUType,
			GPUCount:     spec.GPUCount,
			VRAM:         spec.VRAMPerGPU,
			PricePerHour: price,
			CapacityType: capacityType,
			Region:       p.client.region,
			Reliability:  0.90, // AWS spot: 2-min interruption notice
			RAMGB:        spec.RAMGB,
		})
	}
	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("aws provider requires OnStartScript (full-node bootstrap)")
	}

	amiID, err := p.client.GetLatestUbuntuAMI(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting Ubuntu AMI: %w", err)
	}

	diskSize := config.MinDisk
	if diskSize <= 0 {
		diskSize = 100
	}

	instanceName := "gpuscale-" + config.InstanceID
	instanceID, err := p.client.RunInstances(ctx,
		offer.OfferID,
		amiID,
		config.OnStartScript,
		p.subnetID,
		p.securityGroupID,
		instanceName,
		diskSize,
	)
	if err != nil {
		return nil, err
	}

	// Find spec for GPU info.
	var spec *instanceSpec
	for i := range catalog {
		if catalog[i].InstanceType == offer.OfferID {
			spec = &catalog[i]
			break
		}
	}
	gpuType := offer.GPUType
	gpuCount := offer.GPUCount
	if spec != nil {
		gpuType = spec.GPUType
		gpuCount = spec.GPUCount
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		NodeType:     config.NodeType,
		Status:       "starting",
		GPUType:      gpuType,
		GPUCount:     gpuCount,
		PricePerHour: offer.PricePerHour,
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) DestroyInstance(ctx context.Context, instanceID string) error {
	return p.client.TerminateInstance(ctx, instanceID)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	inst, err := p.client.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if inst == nil {
		return nil, provider.ErrInstanceNotFound
	}
	if strings.ToLower(inst.State.Name) == "terminated" || strings.ToLower(inst.State.Name) == "shutting-down" {
		return nil, provider.ErrInstanceNotFound
	}
	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		IP:           inst.PublicIpAddress,
		Status:       normalizeStatus(inst.State.Name),
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	instances, err := p.client.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*provider.Instance, 0, len(instances))
	for _, inst := range instances {
		result = append(result, &provider.Instance{
			ProviderName: p.Name(),
			InstanceID:   inst.InstanceId,
			IP:           inst.PublicIpAddress,
			Status:       normalizeStatus(inst.State.Name),
			CreatedAt:    time.Now(),
		})
	}
	return result, nil
}

// getSpotPrices returns cached spot prices, refreshing if stale (5-minute TTL).
func (p *Provider) getSpotPrices(ctx context.Context) map[string]float64 {
	p.spotCache.mu.Lock()
	defer p.spotCache.mu.Unlock()
	if p.spotCache.prices != nil && time.Now().Before(p.spotCache.expiry) {
		return p.spotCache.prices
	}
	instanceTypes := make([]string, len(catalog))
	for i, spec := range catalog {
		instanceTypes[i] = spec.InstanceType
	}
	prices, err := p.client.DescribeSpotPrices(ctx, instanceTypes)
	if err != nil {
		// Return stale cache or empty map on error — SearchOffers falls back to 70% estimate.
		if p.spotCache.prices != nil {
			return p.spotCache.prices
		}
		return map[string]float64{}
	}
	p.spotCache.prices = prices
	p.spotCache.expiry = time.Now().Add(5 * time.Minute)
	return prices
}

func normalizeStatus(s string) string {
	switch strings.ToLower(s) {
	case "running":
		return "running"
	case "pending":
		return "starting"
	case "stopping", "stopped", "terminated", "shutting-down":
		return "stopped"
	default:
		return "error"
	}
}
