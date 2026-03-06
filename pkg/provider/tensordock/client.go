package tensordock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const baseURL = "https://marketplace.tensordock.com/api/v0"

// Client is an HTTP client for the TensorDock marketplace API.
// Auth uses api_key + api_token passed as Basic credentials.
type Client struct {
	apiKey     string
	apiToken   string
	httpClient *http.Client
}

// NewClient creates a new TensorDock API client.
func NewClient(apiKey, apiToken string) *Client {
	return &Client{
		apiKey:   apiKey,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// doGET executes a GET request and JSON-decodes the response into dst.
func (c *Client) doGET(ctx context.Context, path string, dst interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.SetBasicAuth(c.apiKey, c.apiToken)
	return c.do(req, dst)
}

// doPOST executes a POST with a JSON body and decodes the response into dst.
func (c *Client) doPOST(ctx context.Context, path string, body, dst interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.apiKey, c.apiToken)
	return c.do(req, dst)
}

func (c *Client) do(req *http.Request, dst interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var apiErr struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &apiErr)
		msg := apiErr.Error
		if msg == "" {
			msg = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return fmt.Errorf("tensordock API error: %s", msg)
	}

	if dst != nil {
		if err := json.Unmarshal(body, dst); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// --- API response types ---

// HostsResponse is returned by GET /client/list/
type HostsResponse struct {
	Success bool                    `json:"success"`
	Hosts   map[string]HostDetails  `json:"servers"`
}

// HostDetails describes one available bare-metal server in the marketplace.
type HostDetails struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	CPU      HostCPU     `json:"cpu"`
	GPU      HostGPU     `json:"gpu"`
	RAM      int         `json:"ram"`      // GB
	Storage  int         `json:"storage"`  // GB
	Location HostLocation `json:"location"`
	Pricing  HostPricing `json:"pricing"`
}

type HostCPU struct {
	Amount int    `json:"amount"`
	Type   string `json:"type"`
}

type HostGPU struct {
	Amount int    `json:"amount"`
	Type   string `json:"type"`
	VRAM   int    `json:"vram"` // GB
}

type HostLocation struct {
	ID      string `json:"id"`
	Country string `json:"country"`
	City    string `json:"city"`
}

type HostPricing struct {
	GPU     HostGPUPricing `json:"gpu"`
	Storage float64        `json:"storage"` // $/GB/hr
}

type HostGPUPricing struct {
	Hourly float64 `json:"hourly"` // $/GPU/hr
}

// DeployRequest is the body for POST /client/deploy/virtualMachine/
type DeployRequest struct {
	// Identifying which host to use
	ServerID string `json:"server_id"`

	// GPU configuration
	GPUModel string `json:"gpu_model"`
	GPUCount int    `json:"gpu_count"`

	// Compute specs (can be overridden per host)
	VCPUs   int `json:"vcpus"`
	RAM     int `json:"ram"`     // GB
	Storage int `json:"storage"` // GB

	// OS
	OperatingSystem string `json:"operating_system"`

	// Network
	ExternalPorts string `json:"external_ports,omitempty"` // e.g. "22,80,443"

	// Hostname
	Hostname string `json:"hostname"`

	// Bootstrap
	StartupScript string `json:"startup_script,omitempty"` // cloud-init / bash script
}

// DeployResponse is the body returned by POST /client/deploy/virtualMachine/
type DeployResponse struct {
	Success    bool   `json:"success"`
	ServerID   string `json:"server"`
	InstanceID string `json:"id"`
	IP         string `json:"ip"`
	Error      string `json:"error,omitempty"`
}

// DeleteResponse is returned by DELETE /client/delete/{id}/
type DeleteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// InstanceStatusResponse is returned by GET /client/single/{id}/
type InstanceStatusResponse struct {
	Success  bool   `json:"success"`
	Status   string `json:"status"`
	IP       string `json:"ip"`
	ServerID string `json:"server_id"`
	Error    string `json:"error,omitempty"`
}

// --- API methods ---

// ListHosts returns all currently available GPU hosts in the marketplace.
func (c *Client) ListHosts(ctx context.Context) ([]HostDetails, error) {
	var resp HostsResponse
	if err := c.doGET(ctx, "/client/list/", &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("tensordock list hosts: API returned success=false")
	}
	hosts := make([]HostDetails, 0, len(resp.Hosts))
	for id, h := range resp.Hosts {
		if h.ID == "" {
			h.ID = id
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

// DeployVM provisions a new VM on the given host.
func (c *Client) DeployVM(ctx context.Context, req DeployRequest) (*DeployResponse, error) {
	var resp DeployResponse
	if err := c.doPOST(ctx, "/client/deploy/virtualMachine/", req, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		msg := resp.Error
		if msg == "" {
			msg = "unknown error"
		}
		return nil, fmt.Errorf("tensordock deploy failed: %s", msg)
	}
	return &resp, nil
}

// GetInstance returns the current status of a deployed instance.
func (c *Client) GetInstance(ctx context.Context, instanceID string) (*InstanceStatusResponse, error) {
	var resp InstanceStatusResponse
	if err := c.doGET(ctx, "/client/single/"+instanceID+"/", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteInstance terminates an instance.
func (c *Client) DeleteInstance(ctx context.Context, instanceID string) error {
	var resp DeleteResponse
	if err := c.doPOST(ctx, "/client/delete/"+instanceID+"/", struct{}{}, &resp); err != nil {
		return err
	}
	if !resp.Success {
		msg := resp.Error
		if msg == "" {
			msg = "unknown error"
		}
		return fmt.Errorf("tensordock delete failed: %s", msg)
	}
	return nil
}
