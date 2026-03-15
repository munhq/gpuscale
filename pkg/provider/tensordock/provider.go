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

// Validate verifies credentials by making an authenticated list-VMs request.
func (p *Provider) Validate(ctx context.Context) error {
	if _, err := p.client.ListVMs(ctx); err != nil {
		return fmt.Errorf("tensordock credential check: %w", err)
	}
	return nil
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	hosts, err := p.client.ListHostnodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("tensordock: list hostnodes: %w", err)
	}

	var offers []provider.Offer
	for _, h := range hosts {
		// Skip offline or unlisted hosts.
		if !h.Status.Online || !h.Status.Listed {
			continue
		}

		ramGB := h.Specs.RAM.Amount
		storageGB := h.Specs.Storage.Amount

		// Each GPU model on a hostnode becomes a separate offer.
		// The map key is the model name (e.g. "rtx4090", "a100"); VRAM is a direct field.
		for modelKey, gpu := range h.Specs.GPU {
			if gpu.Amount == 0 {
				continue
			}

			gpuType := gpuTypeFromKey(modelKey)
			vramPerGPU := gpu.VRAM
			if vramPerGPU == 0 {
				// Fallback: try to parse VRAM from the key name itself.
				vramPerGPU = ParseVRAMFromSlug(modelKey)
			}
			gpuCount := gpu.Amount
			totalVRAM := gpuCount * vramPerGPU

			if gpuType == "" || vramPerGPU == 0 {
				continue
			}

			// GPU type filter (soft match — skip only if GPUTypes is specified and there's no match).
			if len(req.GPUTypes) > 0 && !matchesGPUType(gpuType, req.GPUTypes) {
				continue
			}
			// Multi-GPU filter.
			if !req.MultiGpu && gpuCount > 1 {
				continue
			}
			// VRAM filter.
			if req.MinVRAM > 0 && totalVRAM < req.MinVRAM {
				continue
			}
			if req.MaxVRAM > 0 && vramPerGPU > req.MaxVRAM {
				continue
			}
			// RAM filter.
			if req.MinRAM > 0 && ramGB < req.MinRAM {
				continue
			}
			// Disk filter.
			if req.MinDisk > 0 && storageGB < req.MinDisk {
				continue
			}

			// Total price: GPU cost (dominant) + RAM + storage at default deploy sizes.
			// We use 8 vCPUs, the host's full RAM and storage as the upper bound.
			// Users see GPU-dominated pricing; small overhead from RAM/storage is included.
			defaultVCPUs := 8
			pricePerHour := gpu.Price*float64(gpuCount) +
				h.Specs.CPU.Price*float64(defaultVCPUs) +
				h.Specs.RAM.Price*float64(ramGB) +
				h.Specs.Storage.Price*float64(storageGB)

			if req.MaxPricePerHour > 0 && pricePerHour > req.MaxPricePerHour {
				continue
			}

			region := h.Location.Region
			if region == "" {
				region = h.Location.Country
			}

			// OfferID encodes both the hostnode UUID and GPU model key,
			// separated by ":", so CreateInstance can reconstruct both.
			offerID := h.UUID + ":" + modelKey

			offers = append(offers, provider.Offer{
				ProviderName: p.Name(),
				OfferID:      offerID,
				GPUType:      gpuType,
				GPUCount:     gpuCount,
				VRAM:         totalVRAM,
				PricePerHour: pricePerHour,
				CapacityType: "on-demand", // TensorDock has no spot
				Region:       region,
				Reliability:  h.Status.Uptime, // real uptime from TensorDock status feed
				DiskGB:       storageGB,
				RAMGB:        ramGB,
			})
		}
	}
	return offers, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	if config.OnStartScript == "" {
		return nil, fmt.Errorf("tensordock: OnStartScript is required")
	}

	// OfferID format: "{hostnode_uuid}:{gpu_model_slug}"
	hostnodeUUID, gpuModelSlug, err := parseOfferID(offer.OfferID)
	if err != nil {
		return nil, fmt.Errorf("tensordock: invalid offer ID %q: %w", offer.OfferID, err)
	}

	diskSize := config.MinDisk
	if diskSize <= 0 {
		diskSize = 100
	}

	ramGB := offer.RAMGB
	if ramGB <= 0 {
		ramGB = 32
	}
	// Cap RAM at a reasonable deployment size (don't allocate the entire host).
	if ramGB > 128 {
		ramGB = 128
	}

	// TensorDock uses cloud-init YAML for startup scripts.
	cloudinit := wrapInCloudInit(config.OnStartScript)

	password := GeneratePassword()

	resp, err := p.client.DeployVM(ctx,
		hostnodeUUID,
		gpuModelSlug,
		offer.GPUCount,
		8, // default vCPUs
		ramGB,
		diskSize,
		password,
		cloudinit,
	)
	if err != nil {
		return nil, fmt.Errorf("tensordock: deploy VM: %w", err)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   resp.Server,
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
	return p.client.DeleteVM(ctx, instanceID)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	resp, err := p.client.GetVM(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, provider.ErrInstanceNotFound
	}
	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   instanceID,
		IP:           resp.IP,
		Status:       normalizeStatus(resp.Status),
		CreatedAt:    time.Now(),
	}, nil
}

func (p *Provider) ListInstances(ctx context.Context) ([]*provider.Instance, error) {
	vms, err := p.client.ListVMs(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*provider.Instance, 0, len(vms))
	for id, vm := range vms {
		result = append(result, &provider.Instance{
			ProviderName: p.Name(),
			InstanceID:   id,
			IP:           vm.IP,
			Status:       normalizeStatus(vm.Status),
			CreatedAt:    time.Now(),
		})
	}
	return result, nil
}

// --- helpers ---

func normalizeStatus(s string) string {
	switch strings.ToLower(s) {
	case "running", "online":
		return "running"
	case "deploying", "starting", "pending", "created", "provisioning", "":
		return "starting"
	case "stopped", "offline", "terminated", "deleted":
		return "stopped"
	case "outbid": // spot preemption (TensorDock spot, if ever added)
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

// gpuTypeFromKey converts a TensorDock GPU model key into a human-readable name.
// Keys are lowercase identifiers like "rtx4090", "a100", "h100", "rtxa4000".
func gpuTypeFromKey(key string) string {
	s := strings.ToLower(key)
	switch {
	case strings.HasPrefix(s, "geforcertx"):
		return "GeForce RTX " + strings.ToUpper(strings.TrimPrefix(s, "geforcertx"))
	case strings.HasPrefix(s, "geforcegt"):
		return "GeForce GT " + strings.ToUpper(strings.TrimPrefix(s, "geforcegt"))
	case strings.HasPrefix(s, "rtxa"):
		return "RTX A" + strings.TrimPrefix(s, "rtxa")
	case strings.HasPrefix(s, "rtx"):
		return "RTX " + strings.ToUpper(strings.TrimPrefix(s, "rtx"))
	case s == "a100" || s == "a30" || s == "a40" || strings.HasPrefix(s, "a100"):
		return strings.ToUpper(s[:1]) + s[1:]
	case strings.HasPrefix(s, "h"):
		return strings.ToUpper(s[:1]) + s[1:] // H100, H200
	case strings.HasPrefix(s, "l"):
		return strings.ToUpper(s[:1]) + s[1:] // L4, L40
	default:
		return strings.ToUpper(s[:1]) + s[1:]
	}
}

// parseOfferID splits an OfferID into hostnode UUID and GPU model slug.
// Format: "{hostnode_uuid}:{gpu_model_slug}"
func parseOfferID(offerID string) (hostnodeUUID, gpuModelSlug string, err error) {
	idx := strings.Index(offerID, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("expected format 'hostnode_uuid:gpu_model_slug'")
	}
	return offerID[:idx], offerID[idx+1:], nil
}

// wrapInCloudInit wraps a bash script in cloud-init YAML so TensorDock can execute it on first boot.
// Uses write_files to place the script and runcmd to execute it asynchronously.
func wrapInCloudInit(script string) string {
	var b strings.Builder
	b.WriteString("#cloud-config\n")
	b.WriteString("write_files:\n")
	b.WriteString("  - path: /root/bootstrap.sh\n")
	b.WriteString("    permissions: '0755'\n")
	b.WriteString("    content: |\n")
	for _, line := range strings.Split(script, "\n") {
		b.WriteString("      ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	// Run async so cloud-init doesn't wait for the long-running bootstrap.
	b.WriteString("runcmd:\n")
	b.WriteString("  - nohup bash /root/bootstrap.sh > /var/log/gpu-bootstrap.log 2>&1 &\n")
	return b.String()
}
