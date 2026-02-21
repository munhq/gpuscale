package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	corev1 "k8s.io/api/core/v1"
)

// RayCapacityStore queries Ray cluster for GPU capacity information.
type RayCapacityStore struct {
	k8sClient     client.Client
	namespace     string
	prometheusURL string
	httpClient    *http.Client
}

// NewRayCapacityStore creates a new Ray capacity store.
// Discovers Ray head service dynamically from K8s.
func NewRayCapacityStore(k8sClient client.Client, namespace, prometheusURL string) *RayCapacityStore {
	return &RayCapacityStore{
		k8sClient:     k8sClient,
		namespace:     namespace,
		prometheusURL: prometheusURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GPUNode represents a Ray node with GPU resources.
type GPUNode struct {
	NodeID     string  // Ray raylet ID
	GPUType    string  // GPU model (e.g., "RTX 4090")
	GPUCount   int     // number of GPUs on this node
	VRAMPerGPU int     // VRAM per GPU in GB
	TotalVRAM  int     // GPUCount * VRAMPerGPU
	UsedVRAM   int     // actual VRAM used (from Prometheus)
	FreeVRAM   int     // TotalVRAM - UsedVRAM
}

// ClusterCapacity represents the current Ray cluster GPU capacity.
type ClusterCapacity struct {
	Nodes        []GPUNode // all GPU nodes in cluster
	TotalGPUs    int       // sum of all GPUCount
	TotalVRAM    int       // sum of all TotalVRAM
	LoadedModels []string  // models currently loaded in Ray Serve
	UsedVRAM     int       // sum of all UsedVRAM
	FreeVRAM     int       // sum of all FreeVRAM
}

// rayNodeResponse is the response from Ray Dashboard /api/cluster/nodes
type rayNodeResponse struct {
	Nodes []struct {
		RayletID  string                 `json:"raylet_id"`
		Resources map[string]float64     `json:"resources"`
		Labels    map[string]string      `json:"labels"`
	} `json:"data"`
}

// rayModelsResponse is the response from Ray Serve /v1/models
type rayModelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// prometheusResponse is the response from Prometheus /api/v1/query
type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// GetCapacity queries Ray cluster and returns current GPU capacity.
func (r *RayCapacityStore) GetCapacity(ctx context.Context, demandStore *DemandStore) (*ClusterCapacity, error) {
	// Discover Ray head service
	dashboardURL, serveURL, err := r.discoverRayHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovering Ray head service: %w", err)
	}

	// Query Ray nodes
	nodes, err := r.queryRayNodes(ctx, dashboardURL, demandStore)
	if err != nil {
		return nil, fmt.Errorf("querying Ray nodes: %w", err)
	}

	// Query loaded models
	models, err := r.queryLoadedModels(ctx, serveURL)
	if err != nil {
		return nil, fmt.Errorf("querying loaded models: %w", err)
	}

	// Query Prometheus for actual GPU usage
	if r.prometheusURL != "" {
		if err := r.queryGPUUsage(ctx, nodes); err != nil {
			// Non-fatal: log and continue with estimated usage
			fmt.Printf("WARNING: failed to query Prometheus GPU usage: %v\n", err)
		}
	}

	// Calculate totals
	capacity := &ClusterCapacity{
		Nodes:        nodes,
		LoadedModels: models,
	}
	for _, node := range nodes {
		capacity.TotalGPUs += node.GPUCount
		capacity.TotalVRAM += node.TotalVRAM
		capacity.UsedVRAM += node.UsedVRAM
		capacity.FreeVRAM += node.FreeVRAM
	}

	return capacity, nil
}

// discoverRayHead finds the Ray head service in the namespace.
func (r *RayCapacityStore) discoverRayHead(ctx context.Context) (dashboardURL, serveURL string, err error) {
	var services corev1.ServiceList
	if err := r.k8sClient.List(ctx, &services, client.InNamespace(r.namespace)); err != nil {
		return "", "", fmt.Errorf("listing services: %w", err)
	}

	// Find service matching "rayservice-*-head-svc" or "raycluster-*-head-svc"
	for _, svc := range services.Items {
		if strings.Contains(svc.Name, "-head-svc") {
			dashboardURL = fmt.Sprintf("http://%s.%s.svc.cluster.local:8265", svc.Name, r.namespace)
			serveURL = fmt.Sprintf("http://%s.%s.svc.cluster.local:8000", svc.Name, r.namespace)
			return dashboardURL, serveURL, nil
		}
	}

	return "", "", fmt.Errorf("no Ray head service found in namespace %s", r.namespace)
}

// queryRayNodes queries Ray Dashboard API for node information.
func (r *RayCapacityStore) queryRayNodes(ctx context.Context, dashboardURL string, demandStore *DemandStore) ([]GPUNode, error) {
	url := fmt.Sprintf("%s/api/cluster/nodes", dashboardURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Ray Dashboard returned %d: %s", resp.StatusCode, string(body))
	}

	var rayResp rayNodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&rayResp); err != nil {
		return nil, fmt.Errorf("parsing Ray nodes response: %w", err)
	}

	var nodes []GPUNode
	for _, n := range rayResp.Nodes {
		gpuCount := int(n.Resources["GPU"])
		if gpuCount == 0 {
			continue // skip nodes without GPUs
		}

		// Get GPU type from labels
		gpuType := n.Labels["nvidia.com/gpu.product"]
		if gpuType == "" {
			gpuType = n.Labels["gpu_type"] // fallback
		}
		if gpuType == "" {
			continue // skip if GPU type unknown
		}

		// Look up VRAM per GPU from DemandStore
		vramPerGPU := 0
		if demandStore != nil {
			vramPerGPU, _ = demandStore.GetGPUVRAM(ctx, gpuType)
			if vramPerGPU == 0 {
				// Try without "NVIDIA-" prefix
				vramPerGPU, _ = demandStore.GetGPUVRAM(ctx, strings.TrimPrefix(gpuType, "NVIDIA-"))
			}
		}

		if vramPerGPU == 0 {
			// Unknown GPU type, skip
			continue
		}

		totalVRAM := gpuCount * vramPerGPU
		nodes = append(nodes, GPUNode{
			NodeID:     n.RayletID,
			GPUType:    gpuType,
			GPUCount:   gpuCount,
			VRAMPerGPU: vramPerGPU,
			TotalVRAM:  totalVRAM,
			FreeVRAM:   totalVRAM, // will be updated by queryGPUUsage
		})
	}

	return nodes, nil
}

// queryLoadedModels queries Ray Serve API for currently loaded models.
func (r *RayCapacityStore) queryLoadedModels(ctx context.Context, serveURL string) ([]string, error) {
	url := fmt.Sprintf("%s/v1/models", serveURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Ray Serve returned %d: %s", resp.StatusCode, string(body))
	}

	var modelsResp rayModelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&modelsResp); err != nil {
		return nil, fmt.Errorf("parsing Ray models response: %w", err)
	}

	models := make([]string, 0, len(modelsResp.Data))
	for _, m := range modelsResp.Data {
		models = append(models, m.ID)
	}

	return models, nil
}

// ServeAppStatus represents the status of a Ray Serve application.
type ServeAppStatus struct {
	Name   string // application name (e.g., "qwen3-coder-next")
	Status string // "RUNNING", "DEPLOY_FAILED", "DEPLOYING", "NOT_STARTED", "DELETING"
}

// serveApplicationsResponse is the response from Ray Dashboard GET /api/serve/applications/
type serveApplicationsResponse struct {
	Applications []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"applications"`
}

// GetServeAppStatus queries Ray Dashboard for the status of all Serve applications.
// Returns nil, nil if the dashboard is unreachable (non-fatal).
func (r *RayCapacityStore) GetServeAppStatus(ctx context.Context) ([]ServeAppStatus, error) {
	dashboardURL, _, err := r.discoverRayHead(ctx)
	if err != nil {
		return nil, nil // dashboard not available, not an error
	}

	url := fmt.Sprintf("%s/api/serve/applications/", dashboardURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, nil // connection error is non-fatal
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Ray Serve status returned %d: %s", resp.StatusCode, string(body))
	}

	var serveResp serveApplicationsResponse
	if err := json.NewDecoder(resp.Body).Decode(&serveResp); err != nil {
		return nil, fmt.Errorf("parsing Serve applications response: %w", err)
	}

	statuses := make([]ServeAppStatus, 0, len(serveResp.Applications))
	for _, app := range serveResp.Applications {
		statuses = append(statuses, ServeAppStatus{
			Name:   app.Name,
			Status: app.Status,
		})
	}
	return statuses, nil
}

// ResubmitServeConfig re-submits the current Serve configuration to reset
// DEPLOY_FAILED state. Reads current config, then PUTs it back.
func (r *RayCapacityStore) ResubmitServeConfig(ctx context.Context) error {
	dashboardURL, _, err := r.discoverRayHead(ctx)
	if err != nil {
		return fmt.Errorf("discovering Ray head: %w", err)
	}

	// GET current serve config
	getURL := fmt.Sprintf("%s/api/serve/applications/", dashboardURL)
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return err
	}

	getResp, err := r.httpClient.Do(getReq)
	if err != nil {
		return fmt.Errorf("GET serve config: %w", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		return fmt.Errorf("GET serve config returned %d: %s", getResp.StatusCode, string(body))
	}

	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		return fmt.Errorf("reading serve config: %w", err)
	}

	// PUT the same config back to reset DEPLOY_FAILED
	putURL := fmt.Sprintf("%s/api/serve/applications/", dashboardURL)
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := r.httpClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("PUT serve config: %w", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("PUT serve config returned %d: %s", putResp.StatusCode, string(respBody))
	}

	return nil
}

// queryGPUUsage queries Prometheus for actual GPU memory usage and updates nodes.
func (r *RayCapacityStore) queryGPUUsage(ctx context.Context, nodes []GPUNode) error {
	// Query Prometheus for GPU memory usage
	// nvidia_gpu_memory_used_bytes{namespace="gpu-workloads"} / 1024^3
	query := `sum by (pod) (nvidia_gpu_memory_used_bytes{namespace="` + r.namespace + `"}) / 1024 / 1024 / 1024`
	url := fmt.Sprintf("%s/api/v1/query?query=%s", r.prometheusURL, query)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Prometheus returned %d: %s", resp.StatusCode, string(body))
	}

	var promResp prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&promResp); err != nil {
		return fmt.Errorf("parsing Prometheus response: %w", err)
	}

	if promResp.Status != "success" {
		return fmt.Errorf("Prometheus query failed: %s", promResp.Status)
	}

	// Sum up total used VRAM from all results
	totalUsedVRAM := 0.0
	for _, result := range promResp.Data.Result {
		if len(result.Value) >= 2 {
			if val, ok := result.Value[1].(string); ok {
				var vram float64
				fmt.Sscanf(val, "%f", &vram)
				totalUsedVRAM += vram
			}
		}
	}

	// Distribute used VRAM across nodes (simple: proportional to total VRAM)
	totalCapacity := 0
	for _, node := range nodes {
		totalCapacity += node.TotalVRAM
	}

	if totalCapacity > 0 {
		for i := range nodes {
			proportion := float64(nodes[i].TotalVRAM) / float64(totalCapacity)
			nodes[i].UsedVRAM = int(totalUsedVRAM * proportion)
			nodes[i].FreeVRAM = nodes[i].TotalVRAM - nodes[i].UsedVRAM
			if nodes[i].FreeVRAM < 0 {
				nodes[i].FreeVRAM = 0
			}
		}
	}

	return nil
}
