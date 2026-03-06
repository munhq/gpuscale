package azure

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// GPU VM catalog for Azure. Prices are on-demand us-east reference; spot is fetched live.
type vmSpec struct {
	VMSize       string
	GPUType      string
	GPUCount     int
	VRAMPerGPU   int     // GB
	RAMGB        int
	OnDemandUSD  float64 // $/hr us-east reference
}

// azureCatalog covers the primary Azure GPU VM families used for LLM inference.
var azureCatalog = []vmSpec{
	// NC T4 v3 family — Tesla T4 (16 GB each)
	{VMSize: "Standard_NC4as_T4_v3",  GPUType: "Tesla T4",    GPUCount: 1, VRAMPerGPU: 16, RAMGB: 28,   OnDemandUSD: 0.526},
	{VMSize: "Standard_NC8as_T4_v3",  GPUType: "Tesla T4",    GPUCount: 1, VRAMPerGPU: 16, RAMGB: 56,   OnDemandUSD: 0.752},
	{VMSize: "Standard_NC16as_T4_v3", GPUType: "Tesla T4",    GPUCount: 4, VRAMPerGPU: 16, RAMGB: 110,  OnDemandUSD: 1.204},
	{VMSize: "Standard_NC64as_T4_v3", GPUType: "Tesla T4",    GPUCount: 4, VRAMPerGPU: 16, RAMGB: 440,  OnDemandUSD: 4.352},
	// NC v3 family — Tesla V100 (16 GB each)
	{VMSize: "Standard_NC6s_v3",      GPUType: "Tesla V100",  GPUCount: 1, VRAMPerGPU: 16, RAMGB: 112,  OnDemandUSD: 3.060},
	{VMSize: "Standard_NC12s_v3",     GPUType: "Tesla V100",  GPUCount: 2, VRAMPerGPU: 16, RAMGB: 224,  OnDemandUSD: 6.120},
	{VMSize: "Standard_NC24s_v3",     GPUType: "Tesla V100",  GPUCount: 4, VRAMPerGPU: 16, RAMGB: 448,  OnDemandUSD: 12.240},
	// ND A100 v4 family — A100 40 GB
	{VMSize: "Standard_ND96asr_v4",   GPUType: "A100 40GB",   GPUCount: 8, VRAMPerGPU: 40, RAMGB: 900,  OnDemandUSD: 32.770},
	// ND A100 v4 family — A100 80 GB
	{VMSize: "Standard_ND96amsr_A100_v4", GPUType: "A100 80GB", GPUCount: 8, VRAMPerGPU: 80, RAMGB: 1900, OnDemandUSD: 38.000},
	// ND H100 v5 family — H100 80 GB
	{VMSize: "Standard_ND96isr_H100_v5",  GPUType: "H100 80GB", GPUCount: 8, VRAMPerGPU: 80, RAMGB: 1900, OnDemandUSD: 98.320},
	// NC A100 v4 — single A100 80 GB
	{VMSize: "Standard_NC24ads_A100_v4",  GPUType: "A100 80GB", GPUCount: 1, VRAMPerGPU: 80, RAMGB: 220,  OnDemandUSD: 3.673},
	{VMSize: "Standard_NC48ads_A100_v4",  GPUType: "A100 80GB", GPUCount: 2, VRAMPerGPU: 80, RAMGB: 440,  OnDemandUSD: 7.346},
	{VMSize: "Standard_NC96ads_A100_v4",  GPUType: "A100 80GB", GPUCount: 4, VRAMPerGPU: 80, RAMGB: 880,  OnDemandUSD: 14.692},
}

// Provider implements provider.Provider for Microsoft Azure.
type Provider struct {
	client *Client

	spotCacheMu  sync.Mutex
	spotCache    map[string]float64
	spotExpiry   time.Time
}

// New creates a new Azure provider.
// resourceGroup and location default if empty (see Client.NewClient).
func New(subscriptionID, tenantID, clientID, clientSecret, resourceGroup, location string) *Provider {
	return &Provider{
		client: NewClient(subscriptionID, tenantID, clientID, clientSecret, resourceGroup, location),
	}
}

func (p *Provider) Name() string { return "azure" }

func (p *Provider) Validate(ctx context.Context) error {
	if _, err := p.client.getToken(ctx); err != nil {
		return fmt.Errorf("azure credential check: %w", err)
	}
	return nil
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	isSpot := req.CapacityType == "spot"
	var spotPrices map[string]float64

	if isSpot {
		spotPrices = p.getSpotPrices(ctx)
	}

	var offers []provider.Offer
	for _, spec := range azureCatalog {
		totalVRAM := spec.GPUCount * spec.VRAMPerGPU

		if !req.MultiGpu && spec.GPUCount > 1 {
			continue
		}
		if req.MinVRAM > 0 && totalVRAM < req.MinVRAM {
			continue
		}
		if req.MaxVRAM > 0 && spec.VRAMPerGPU > req.MaxVRAM {
			continue
		}
		if req.MinRAM > 0 && spec.RAMGB < req.MinRAM {
			continue
		}
		if len(req.GPUTypes) > 0 && !matchesGPUType(spec.GPUType, req.GPUTypes) {
			continue
		}

		price := spec.OnDemandUSD
		capacityType := "on-demand"
		reliability := 0.995

		if isSpot {
			if sp, ok := spotPrices[spec.VMSize]; ok && sp > 0 {
				price = sp
			} else {
				// Estimate: Azure spot is typically 60–85% off on-demand
				price = spec.OnDemandUSD * 0.20
			}
			capacityType = "spot"
			reliability = 0.85
		}

		if req.MaxPricePerGPU > 0 && spec.GPUCount > 0 && price/float64(spec.GPUCount) > req.MaxPricePerGPU {
			continue
		}

		offers = append(offers, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      spec.VMSize,
			GPUType:      spec.GPUType,
			GPUCount:     spec.GPUCount,
			VRAM:         totalVRAM,
			PricePerHour: price,
			CapacityType: capacityType,
			Region:       p.client.location,
			Reliability:  reliability,
			RAMGB:        spec.RAMGB,
		})
	}
	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("azure: OnStartScript is required")
	}

	vmName := sanitize("gpuapi-" + config.InstanceID)
	if len(vmName) > 15 { // Azure VM name limit
		vmName = vmName[:15]
	}

	// 1. Ensure resource group exists.
	if err := p.client.EnsureResourceGroup(ctx); err != nil {
		return nil, fmt.Errorf("azure: ensure resource group: %w", err)
	}

	// 2. Create public IP.
	ipName := vmName + "-ip"
	if err := p.client.CreatePublicIP(ctx, ipName); err != nil {
		return nil, fmt.Errorf("azure: create public IP: %w", err)
	}

	// 3. Create NIC.
	nicName := vmName + "-nic"
	nicID, err := p.client.CreateNIC(ctx, nicName, ipName)
	if err != nil {
		return nil, fmt.Errorf("azure: create NIC: %w", err)
	}

	// 4. Determine disk size.
	diskSize := config.MinDisk
	if diskSize <= 0 {
		diskSize = 128
	}

	// 5. Create the VM. Azure CreateVM is async (202 Accepted); we return
	//    immediately and the provisioner goroutine waits for the agent to connect.
	if err := p.client.CreateVM(ctx, vmName, nicID, offer.OfferID, diskSize, config.OnStartScript); err != nil {
		return nil, fmt.Errorf("azure: create VM: %w", err)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   vmName, // use VM name as our stable ID
		NodeType:     "standalone",
		Status:       "starting",
		GPUType:      offer.GPUType,
		GPUCount:     offer.GPUCount,
		PricePerHour: offer.PricePerHour,
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) DestroyInstance(ctx context.Context, instanceID string) error {
	return p.client.DeleteVM(ctx, instanceID)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	state, err := p.client.GetVMState(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		Status:       normalizeAzureState(state),
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	// Instance tracking is done in our Postgres instances table.
	return nil, nil
}

// getSpotPrices returns cached spot prices, refreshing every 5 minutes.
func (p *Provider) getSpotPrices(ctx context.Context) map[string]float64 {
	p.spotCacheMu.Lock()
	defer p.spotCacheMu.Unlock()

	if p.spotCache != nil && time.Now().Before(p.spotExpiry) {
		return p.spotCache
	}

	vmSizes := make([]string, len(azureCatalog))
	for i, s := range azureCatalog {
		vmSizes[i] = s.VMSize
	}

	prices, err := FetchSpotPrices(ctx, p.client.location, vmSizes)
	if err != nil {
		// Return stale cache or empty map — SearchOffers will fall back to estimate.
		if p.spotCache != nil {
			return p.spotCache
		}
		return map[string]float64{}
	}

	p.spotCache = prices
	p.spotExpiry = time.Now().Add(5 * time.Minute)
	return prices
}

func normalizeAzureState(state string) string {
	switch strings.ToLower(state) {
	case "succeeded", "running":
		return "running"
	case "creating", "updating":
		return "starting"
	case "failed", "canceled":
		return "error"
	case "deleting", "deleted":
		return "stopped"
	default:
		return "starting"
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
	for _, c := range strings.ToLower(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteRune(c)
		}
	}
	return b.String()
}
