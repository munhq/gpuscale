package vastai

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

// minCUDAVersion is the minimum CUDA driver version required on Vast.ai hosts.
// The rayproject/ray-llm image uses CUDA 12.8 (cu128). Hosts with older drivers
// fail with OCI runtime errors at container start.
const minCUDAVersion = 12.8

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

	// For full-node (VM) mode, filter to machines that support VMs.
	// Container-only hosts reject vm:true create requests.
	if req.NodeType == "full-node" {
		params["vms_enabled"] = "true"
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
		// Filter out hosts with CUDA drivers too old for our images.
		// rayproject/ray-llm:*-cu128 needs CUDA 12.8+.
		if o.CUDAVersion > 0 && o.CUDAVersion < minCUDAVersion {
			continue
		}

		// Filter by minimum PCIe generation.
		// PCIe 4.0 x16 = 32 GB/s; use 24 GB/s as threshold to allow x8 slots.
		// PCIe 3.0 x16 = 16 GB/s, so any gen-4 machine will exceed 24 GB/s.
		if req.MinPCIeGen >= 4 && o.PcieBW > 0 && o.PcieBW < 24.0 {
			continue
		}

		vramGB := int(o.GPURAMTotal / 1024) // convert MB to GB
		if vramGB == 0 && o.GPURAMTotal > 0 {
			vramGB = 1
		}

		// When searching with rentable=true, the API returns spot-eligible offers.
		// Use the capacity type from the original request since the response
		// no longer includes an interruptible field.
		capacityType := "on-demand"
		if req.CapacityType == "spot" {
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

	image = config.Image

	if config.OnStartScript != "" {
		// Caller provided a bootstrap script — use it as-is.
		// This covers ray-worker (ray start), full-node (VPN+K3s), or any custom script.
		onstart = config.OnStartScript
		env = config.OnStartEnv
		if config.NodeType == "full-node" {
			// Full-node MUST use Vast.ai KVM image — real VM with systemd,
			// WireGuard support, and ability to install K3s.
			// Docker images (like vllm/vllm-openai) don't have these capabilities.
			image = "vastai/kvm:ubuntu_terminal"
		} else if image == "" {
			image = "rayproject/ray-llm:2.53.0-py311-cu128"
		}
	} else if config.NodeType == "ray-worker" {
		// Fallback: no script provided, generate standalone vLLM bootstrap.
		// This is the legacy path — prefer passing OnStartScript from provisioner.
		if image == "" {
			image = "vllm/vllm-openai:latest"
		}
		env = map[string]string{
			"GPU_TYPE":    config.GPUType,
			"PROVIDER":    config.ProviderName,
			"INSTANCE_ID": config.InstanceID,
		}
		if config.ModelID != "" {
			env["MODEL_ID"] = config.ModelID
		}
		if config.ModelCacheURL != "" {
			env["MODEL_CACHE_URL"] = config.ModelCacheURL
		}
		for k, v := range config.ExtraEnv {
			env[k] = v
		}
		onstart = generateVLLMBootstrapScript(config)
	} else {
		return nil, fmt.Errorf("full-node requires OnStartScript")
	}

	// Use ssh_proxy mode when we have an onstart script — "args" mode
	// uses the docker entrypoint and ignores onstart, causing the script
	// content to be treated as a file path (exec error).
	runType := "ssh_proxy"

	// full-node uses Vast.ai VM mode: a real Ubuntu VM with systemd, root access,
	// and the ability to install arbitrary software (Netbird, K3s, NVIDIA toolkit).
	// VMs have outbound internet so Netbird VPN connects without any inbound port hacks.
	isVM := config.NodeType == "full-node"

	createReq := InstanceCreateRequest{
		Image:   image,
		Disk:    50,
		RunType: runType,
		Env:     env,
		Onstart: onstart,
		VM:      isVM,
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
		if resp.SSHHost != "" {
			endpoint = fmt.Sprintf("http://%s:%d", resp.SSHHost, servePort)
		}
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

// generateVLLMBootstrapScript creates an inline bootstrap script for vLLM workers.
// vLLM handles model download internally via huggingface_hub (parallel shard downloads).
func generateVLLMBootstrapScript(config provider.BootstrapConfig) string {
	servePort := config.RayServePort
	if servePort == 0 {
		servePort = 8000
	}

	modelID := config.ModelID
	if modelID == "" {
		modelID = "THUDM/glm-4-9b-chat"
	}
	// Use source if different from ID (e.g., ID="glm-4-7", Source="hf:zai-org/GLM-4.7-Flash")
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
		trustFlag = "\n  --trust-remote-code \\"
	}

	tpFlag := ""
	if config.TensorParallelSize > 1 {
		tpFlag = fmt.Sprintf("\n  --tensor-parallel-size %d \\", config.TensorParallelSize)
	}

	return fmt.Sprintf(`#!/bin/bash
set -euo pipefail

echo '[gpuscale] Starting vLLM worker on %s'
echo '[gpuscale] Instance: %s, GPU: %s'
echo '[gpuscale] Model: %s (repo: %s)'

exec python -m vllm.entrypoints.openai.api_server \
  --model '%s' \
  --host 0.0.0.0 \
  --port %d \
  --gpu-memory-utilization %.2f \
  --max-model-len %d \
  --dtype %s%s%s
`, config.ProviderName, config.InstanceID, config.GPUType, modelID, hfRepo,
		hfRepo, servePort, gpuMemUtil, maxModelLen, dtype, tpFlag, trustFlag)
}

// StopInstance implements HibernatingProvider.
// Sends the instance to "stopped" state — disk is preserved, no compute charge.
// The K3s agent will disconnect from the cluster; the node object stays in K8s.
func (p *Provider) StopInstance(ctx context.Context, instanceID string) error {
	id, err := strconv.Atoi(instanceID)
	if err != nil {
		return fmt.Errorf("invalid instance ID %q: %w", instanceID, err)
	}
	return p.client.SetInstanceState(ctx, id, "stopped")
}

// WakeInstance implements HibernatingProvider.
// Transitions the instance from "stopped" back to "running".
// K3s agent will reconnect automatically (it is a systemd service).
// Model files are already on disk — no HuggingFace download needed.
func (p *Provider) WakeInstance(ctx context.Context, instanceID string) error {
	id, err := strconv.Atoi(instanceID)
	if err != nil {
		return fmt.Errorf("invalid instance ID %q: %w", instanceID, err)
	}
	return p.client.SetInstanceState(ctx, id, "running")
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
		StatusMsg:    resp.StatusMsg,
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
	case "loading", "creating", "pulling", "created", "scheduled", "":
		return "starting"
	case "exited", "offline":
		return "stopped"
	case "stopped":
		return "hibernated"
	default:
		return "error"
	}
}
