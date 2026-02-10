package vastai

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/munhq/gpuscale/internal/provider"
)

// Provider implements the provider.Provider interface for Vast.ai.
type Provider struct {
	client *Client
}

// New creates a new Vast.ai provider.
func New(apiKey string) *Provider {
	return &Provider{
		client: NewClient(apiKey),
	}
}

func (p *Provider) Name() string {
	return "vast.ai"
}

func (p *Provider) SearchOffers(ctx context.Context, req provider.GPURequirements) ([]provider.Offer, error) {
	params := map[string]string{
		"order":    "dph_total",
		"type":     "ask",
		"verified": "true",
	}

	if req.GPUCount > 0 {
		params["num_gpus"] = strconv.Itoa(req.GPUCount)
	}
	if req.MinVRAM > 0 {
		// Vast.ai uses MB for gpu_totalram
		params["min_gpu_totalram"] = strconv.Itoa(req.MinVRAM * 1024)
	}
	if req.MaxPrice > 0 {
		params["max_dph_total"] = fmt.Sprintf("%.2f", req.MaxPrice)
	}
	if req.MinDisk > 0 {
		params["min_disk_space"] = strconv.Itoa(req.MinDisk)
	}
	if req.MinRAM > 0 {
		// Vast.ai uses MB for cpu_ram
		params["min_cpu_ram"] = strconv.Itoa(req.MinRAM * 1024)
	}
	if len(req.GPUTypes) > 0 {
		params["gpu_name"] = req.GPUTypes[0]
	}
	if req.CapacityType == "spot" {
		params["rentable"] = "true"
	}

	offers, err := p.client.SearchOffers(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("searching vast.ai offers: %w", err)
	}

	result := make([]provider.Offer, 0, len(offers))
	for _, o := range offers {
		vramGB := int(o.GPURAMTotal / 1024) // convert MB to GB
		if vramGB == 0 && o.GPURAMTotal > 0 {
			vramGB = 1
		}

		capacityType := "on-demand"
		if o.Interruptible != nil && *o.Interruptible {
			capacityType = "spot"
		}

		result = append(result, provider.Offer{
			ProviderName: p.Name(),
			OfferID:      strconv.Itoa(o.ID),
			GPUType:      o.GPUName,
			GPUCount:     o.NumGPUs,
			VRAM:         vramGB,
			PricePerHour: o.DPHTotal,
			CapacityType: capacityType,
			Region:       o.Geolocation,
			Reliability:  o.Reliability,
			DiskGB:       int(o.DiskSpace),
			RAMGB:        int(o.RAMTotal / 1024),
		})
	}
	return result, nil
}

func (p *Provider) CreateInstance(ctx context.Context, offer provider.Offer, config provider.BootstrapConfig) (*provider.Instance, error) {
	offerID, err := strconv.Atoi(offer.OfferID)
	if err != nil {
		return nil, fmt.Errorf("invalid offer ID %q: %w", offer.OfferID, err)
	}

	var env map[string]string
	var onstart string
	var image string

	// Configure based on node type
	if config.NodeType == "ray-worker" {
		// Ray worker mode - runs in container
		image = config.Image
		if image == "" {
			image = "rayproject/ray:latest-gpu"
		}

		env = map[string]string{
			"GPU_TYPE":    config.GPUType,
			"PROVIDER":    config.ProviderName,
			"INSTANCE_ID": config.InstanceID,
		}
		if config.ModelCacheURL != "" {
			env["MODEL_CACHE_URL"] = config.ModelCacheURL
		}
		if config.RayHeadAddr != "" {
			env["RAY_HEAD_ADDR"] = config.RayHeadAddr
		}
		for k, v := range config.ExtraEnv {
			env[k] = v
		}

		// Generate Ray bootstrap script
		onstart = generateRayBootstrapScript(config)

	} else {
		// K3s mode - needs VM (but we'll use container for now with warning)
		image = config.Image
		if image == "" {
			image = "ghcr.io/munhq/gpuscale-node:latest"
		}

		env = map[string]string{
			"NETBIRD_SETUP_KEY": config.NetbirdKey,
			"K3S_URL":           config.K3sURL,
			"K3S_TOKEN":         config.K3sToken,
			"GPU_TYPE":          config.GPUType,
			"PROVIDER":          config.ProviderName,
			"INSTANCE_ID":       config.InstanceID,
		}
		if config.ModelCacheURL != "" {
			env["MODEL_CACHE_URL"] = config.ModelCacheURL
		}
		for k, v := range config.ExtraEnv {
			env[k] = v
		}

		onstart = "#!/bin/bash\n/bootstrap.sh"
	}

	createReq := InstanceCreateRequest{
		Image:   image,
		Disk:    50,
		RunType: "args",
		Env:     env,
		Onstart: onstart,
	}

	resp, err := p.client.CreateInstance(ctx, offerID, createReq)
	if err != nil {
		return nil, fmt.Errorf("creating vast.ai instance: %w", err)
	}

	// Build endpoint for ray-worker type
	endpoint := ""
	if config.NodeType == "ray-worker" {
		servePort := config.RayServePort
		if servePort == 0 {
			servePort = 8000
		}
		endpoint = fmt.Sprintf("http://%s:%d", resp.SSHHost, servePort)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   strconv.Itoa(resp.ID),
		NodeType:     config.NodeType,
		IP:           resp.SSHHost,
		SSHPort:      resp.SSHPort,
		Endpoint:     endpoint,
		Status:       normalizeStatus(resp.ActualStatus),
		GPUType:      resp.GPUName,
		GPUCount:     resp.NumGPUs,
		PricePerHour: resp.DPHTotal,
		CreatedAt:    time.Now(),
	}, nil
}

// generateRayBootstrapScript creates an inline bootstrap script for Ray workers
func generateRayBootstrapScript(config provider.BootstrapConfig) string {
	dashPort := config.RayDashPort
	if dashPort == 0 {
		dashPort = 8265
	}
	servePort := config.RayServePort
	if servePort == 0 {
		servePort = 8000
	}

	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail

echo '[gpuscale] Starting Ray worker on %s'
echo '[gpuscale] Instance: %s, GPU: %s'

# Install Ray if needed
if ! command -v ray &> /dev/null; then
  echo '[gpuscale] Installing Ray...'
  pip install -q 'ray[serve]' 2>&1 | tail -5
fi

# Start Ray
`, config.ProviderName, config.InstanceID, config.GPUType)

	if config.RayHeadAddr == "" {
		script += fmt.Sprintf(`echo '[gpuscale] Starting Ray head node...'
ray start --head --port=6379 --dashboard-host=0.0.0.0 --dashboard-port=%d --num-gpus=1
`, dashPort)
	} else {
		script += fmt.Sprintf(`echo '[gpuscale] Joining Ray cluster at %s...'
ray start --address=%s --num-gpus=1
`, config.RayHeadAddr, config.RayHeadAddr)
	}

	script += `
# Wait for Ray to be ready
echo '[gpuscale] Waiting for Ray...'
for i in $(seq 1 30); do
  if ray status 2>/dev/null | grep -q 'Available resources'; then
    echo '[gpuscale] Ray is ready!'
    break
  fi
  sleep 2
done

echo '[gpuscale] Ray worker ready for inference'
tail -f /dev/null
`

	return script
}

func (p *Provider) DestroyInstance(ctx context.Context, instanceID string) error {
	id, err := strconv.Atoi(instanceID)
	if err != nil {
		return fmt.Errorf("invalid instance ID %q: %w", instanceID, err)
	}
	return p.client.DestroyInstance(ctx, id)
}

func (p *Provider) GetInstance(ctx context.Context, instanceID string) (*provider.Instance, error) {
	id, err := strconv.Atoi(instanceID)
	if err != nil {
		return nil, fmt.Errorf("invalid instance ID %q: %w", instanceID, err)
	}

	resp, err := p.client.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, provider.ErrInstanceNotFound
	}

	createdAt := time.Time{}
	if resp.StartDate > 0 {
		createdAt = time.Unix(int64(resp.StartDate), 0)
	}

	return &provider.Instance{
		ProviderName: p.Name(),
		InstanceID:   strconv.Itoa(resp.ID),
		IP:           resp.SSHHost,
		SSHPort:      resp.SSHPort,
		Status:       normalizeStatus(resp.ActualStatus),
		GPUType:      resp.GPUName,
		GPUCount:     resp.NumGPUs,
		PricePerHour: resp.DPHTotal,
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
		createdAt := time.Time{}
		if inst.StartDate > 0 {
			createdAt = time.Unix(int64(inst.StartDate), 0)
		}
		result = append(result, &provider.Instance{
			ProviderName: p.Name(),
			InstanceID:   strconv.Itoa(inst.ID),
			IP:           inst.SSHHost,
			SSHPort:      inst.SSHPort,
			Status:       normalizeStatus(inst.ActualStatus),
			GPUType:      inst.GPUName,
			GPUCount:     inst.NumGPUs,
			PricePerHour: inst.DPHTotal,
			CreatedAt:    createdAt,
		})
	}
	return result, nil
}

func normalizeStatus(vastStatus string) string {
	switch vastStatus {
	case "running":
		return "running"
	case "loading", "creating", "pulling":
		return "starting"
	case "exited", "stopped", "offline":
		return "stopped"
	default:
		return "error"
	}
}
