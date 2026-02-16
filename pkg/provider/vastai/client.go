package vastai

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/munhq/gpuscale/pkg/provider"
)

const baseURL = "https://cloud.vast.ai/api/v0"

// Client is an HTTP client for the Vast.ai API.
type Client struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new Vast.ai API client.
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
	}
}

// BundleSearchResult represents a search response from the Vast.ai bundles API.
type BundleSearchResult struct {
	Offers []BundleOffer `json:"offers"`
}

// BundleOffer represents a single machine offer from Vast.ai.
type BundleOffer struct {
	ID          int     `json:"id"`
	MachineID   int     `json:"machine_id"`
	GPUName     string  `json:"gpu_name"`
	NumGPUs     int     `json:"num_gpus"`
	GPURAMTotal float64 `json:"gpu_total_ram"` // total VRAM in MB
	GPUMemBW    float64 `json:"gpu_mem_bw"`    // memory bandwidth
	DPHTotal    float64 `json:"dph_total"`     // dollars per hour total
	DPHBase     float64 `json:"dph_base"`      // base price
	Reliability float64 `json:"reliability2"`  // reliability score 0-1
	DiskSpace   float64 `json:"disk_space"`    // available disk in GB
	RAMTotal    float64 `json:"cpu_ram"`       // system RAM in MB
	CUDAVersion float64 `json:"cuda_max_good"` // max CUDA version
	Rentable    bool    `json:"rentable"`      // whether this offer is available for renting
	Verified    bool    `json:"verified"`
	Geolocation string  `json:"geolocation"`
}

// InstanceCreateRequest is the body for creating an instance on Vast.ai.
// RunType should be "ssh_proxy" or "ssh_direc" when using Onstart scripts.
// "args" mode uses the docker entrypoint and ignores Onstart.
type InstanceCreateRequest struct {
	Image   string            `json:"image"`
	Disk    float64           `json:"disk"`
	Onstart string            `json:"onstart,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	RunType string            `json:"runtype"`
}

// InstanceResponse represents an instance returned by the Vast.ai API.
type InstanceResponse struct {
	ID             int     `json:"id"`
	MachineID      int     `json:"machine_id"`
	ActualStatus   string  `json:"actual_status"`
	StatusMsg      string  `json:"status_msg"`
	CurState       string  `json:"cur_state"`
	IntendedStatus string  `json:"intended_status"`
	GPUName        string  `json:"gpu_name"`
	NumGPUs        int     `json:"num_gpus"`
	DPHTotal       float64 `json:"dph_total"`
	SSHHost        string  `json:"ssh_host"`
	SSHPort        int     `json:"ssh_port"`
	StartDate      float64 `json:"start_date"` // Unix timestamp
	Label          string  `json:"label"`
}

// SearchOffers queries the Vast.ai bundles API with filters.
func (c *Client) SearchOffers(ctx context.Context, params map[string]string) ([]BundleOffer, error) {
	u, err := url.Parse(c.baseURL + "/bundles/")
	if err != nil {
		return nil, fmt.Errorf("parsing URL: %w", err)
	}

	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vast.ai API returned %d: %s", resp.StatusCode, string(body))
	}

	var result BundleSearchResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return result.Offers, nil
}

// CreateInstance creates a new instance on Vast.ai by renting an offer.
func (c *Client) CreateInstance(ctx context.Context, offerID int, createReq InstanceCreateRequest) (*InstanceResponse, error) {
	body, err := json.Marshal(createReq)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	reqURL := fmt.Sprintf("%s/asks/%d/", c.baseURL, offerID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound ||
		(resp.StatusCode >= 400 && strings.Contains(string(respBody), "no_such_ask")) {
		return nil, fmt.Errorf("vast.ai offer %d expired: %s: %w", offerID, string(respBody), provider.ErrOfferExpired)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("vast.ai create returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Vast.ai create response format: {"success": true, "new_contract": <instance_id>}
	var createResp struct {
		Success     bool   `json:"success"`
		NewContract int    `json:"new_contract"`
		Error       string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(respBody, &createResp); err != nil {
		preview := string(respBody)
		if len(preview) > 500 {
			preview = preview[:500] + "..."
		}
		return nil, fmt.Errorf("decoding create response: %w. Response: %s", err, preview)
	}

	if !createResp.Success || createResp.NewContract == 0 {
		return nil, fmt.Errorf("vast.ai create failed: success=%v, contract=%d, error=%s, raw=%s",
			createResp.Success, createResp.NewContract, createResp.Error, string(respBody))
	}

	// Return the contract ID directly. Don't call GetInstance here — the
	// instance may not be queryable immediately after creation, and any
	// parsing issues could override the valid contract ID with 0.
	// The ClaimReconciler polls GetInstance during the Bootstrapping phase.
	return &InstanceResponse{
		ID:           createResp.NewContract,
		ActualStatus: "created",
	}, nil
}

// GetInstance returns the details of a specific instance.
// Vast.ai wraps the response: {"instances": {<instance fields>}}.
func (c *Client) GetInstance(ctx context.Context, instanceID int) (*InstanceResponse, error) {
	reqURL := fmt.Sprintf("%s/instances/%d/", c.baseURL, instanceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vast.ai get instance returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Vast.ai returns {"instances": {<instance>}} for single-instance GET.
	var wrapped struct {
		Instances InstanceResponse `json:"instances"`
	}
	if err := json.Unmarshal(respBody, &wrapped); err != nil {
		// Fallback: try decoding as flat InstanceResponse
		var instance InstanceResponse
		if err2 := json.Unmarshal(respBody, &instance); err2 != nil {
			preview := string(respBody)
			if len(preview) > 500 {
				preview = preview[:500] + "..."
			}
			return nil, fmt.Errorf("decoding get-instance response: %w. Response: %s", err, preview)
		}
		return &instance, nil
	}
	if wrapped.Instances.ID == 0 {
		// Instance was destroyed or doesn't exist — return nil so the provider
		// maps this to ErrInstanceNotFound.
		return nil, nil
	}
	return &wrapped.Instances, nil
}

// ListInstances returns all instances.
func (c *Client) ListInstances(ctx context.Context) ([]InstanceResponse, error) {
	reqURL := c.baseURL + "/instances/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("vast.ai list instances returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Instances []InstanceResponse `json:"instances"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return result.Instances, nil
}

// DestroyInstance deletes an instance.
func (c *Client) DestroyInstance(ctx context.Context, instanceID int) error {
	reqURL := fmt.Sprintf("%s/instances/%d/", c.baseURL, instanceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vast.ai destroy returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
