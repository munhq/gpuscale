package verda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const baseURL = "https://api.verda.com/v1"

// Client is an HTTP client for the Verda API with OAuth2 auth.
type Client struct {
	clientID     string
	clientSecret string
	httpClient   *http.Client
	baseURL      string

	mu          sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

// NewClient creates a new Verda API client.
func NewClient(clientID, clientSecret string) *Client {
	return &Client{
		clientID:     clientID,
		clientSecret: clientSecret,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
	}
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// authenticate obtains or refreshes the OAuth2 access token.
func (c *Client) authenticate(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.accessToken != "" && time.Now().Before(c.tokenExpiry) {
		return nil
	}

	form := url.Values{}
	form.Set("client_id", c.clientID)
	form.Set("client_secret", c.clientSecret)
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/oauth2/token", bytes.NewBufferString(form.Encode()))
	if err != nil {
		return fmt.Errorf("creating auth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing auth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("verda auth returned %d: %s", resp.StatusCode, string(body))
	}

	var token tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return fmt.Errorf("decoding token: %w", err)
	}

	c.accessToken = token.AccessToken
	// Expire 60s early to avoid edge cases
	c.tokenExpiry = time.Now().Add(time.Duration(token.ExpiresIn-60) * time.Second)
	return nil
}

func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	if err := c.authenticate(ctx); err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshaling body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	c.mu.Lock()
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	c.mu.Unlock()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return c.httpClient.Do(req)
}

// InstanceType represents a Verda instance type with pricing.
// Matches the actual API response format with nested objects.
type InstanceType struct {
	ID           string `json:"id"`
	InstanceType string `json:"instance_type"`
	Model        string `json:"model"` // GPU model name
	Name         string `json:"name"`
	Description  string `json:"description"`

	CPU struct {
		Description   string `json:"description"`
		NumberOfCores int    `json:"number_of_cores"`
	} `json:"cpu"`

	GPU struct {
		Description  string `json:"description"`
		NumberOfGPUs int    `json:"number_of_gpus"`
	} `json:"gpu"`

	GPUMemory struct {
		Description      string `json:"description"`
		SizeInGigabytes int     `json:"size_in_gigabytes"`
	} `json:"gpu_memory"`

	Memory struct {
		Description      string `json:"description"`
		SizeInGigabytes int     `json:"size_in_gigabytes"`
	} `json:"memory"`

	PricePerHour string `json:"price_per_hour"` // String in API, convert to float64
	SpotPrice    string `json:"spot_price"`     // String in API, convert to float64
	P2P          string `json:"p2p,omitempty"`
}

// ListInstanceTypes returns available instance types.
// The Verda API returns either a raw JSON array or {"data": [...]}.
func (c *Client) ListInstanceTypes(ctx context.Context) ([]InstanceType, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/instance-types?currency=usd", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("verda instance-types returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	// Try raw array first (current API format), fall back to wrapped object.
	var types []InstanceType
	if err := json.Unmarshal(body, &types); err == nil {
		return types, nil
	}

	var wrapped struct {
		Data []InstanceType `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		// Log first 500 chars of response for debugging
		preview := string(body)
		if len(preview) > 500 {
			preview = preview[:500] + "..."
		}
		return nil, fmt.Errorf("decoding response (tried both array and wrapped formats): %w. Response preview: %s", err, preview)
	}
	return wrapped.Data, nil
}

// AvailabilityResponse wraps the availability check response.
type AvailabilityResponse struct {
	Available bool   `json:"available"`
	Location  string `json:"location_code"`
}

// CheckAvailability checks if an instance type is available.
func (c *Client) CheckAvailability(ctx context.Context, instanceType string, isSpot bool) ([]AvailabilityResponse, error) {
	spotParam := "false"
	if isSpot {
		spotParam = "true"
	}
	path := fmt.Sprintf("/instance-availability/%s?is_spot=%s", instanceType, spotParam)
	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("verda availability returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var avail []AvailabilityResponse
	if err := json.Unmarshal(body, &avail); err == nil {
		return avail, nil
	}

	var wrapped struct {
		Data []AvailabilityResponse `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return wrapped.Data, nil
}

// CreateInstanceRequest is the body for creating a Verda instance.
type CreateInstanceRequest struct {
	InstanceType    string   `json:"instance_type"`
	Image           string   `json:"image"`
	SSHKeyIDs       []string `json:"ssh_key_ids,omitempty"`
	Hostname        string   `json:"hostname"`
	StartupScriptID string   `json:"startup_script_id,omitempty"`
	LocationCode    string   `json:"location_code,omitempty"`
	IsSpot          bool     `json:"is_spot"`
}

// InstanceResponse represents a Verda instance.
type InstanceResponse struct {
	ID           string  `json:"id"`
	Status       string  `json:"status"` // "running", "starting", "stopped", etc.
	InstanceType string  `json:"instance_type"`
	IP           string  `json:"ip"`
	Hostname     string  `json:"hostname"`
	GPUType      string  `json:"gpu_type"`
	GPUCount     int     `json:"gpu_count"`
	PricePerHour float64 `json:"price_per_hour"`
	CreatedAt    string  `json:"created_at"`
	IsSpot       bool    `json:"is_spot"`
}

// CreateInstance provisions a new instance.
func (c *Client) CreateInstance(ctx context.Context, createReq CreateInstanceRequest) (*InstanceResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, "/instances", createReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("verda create returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data InstanceResponse `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decoding response: %w (body: %s)", err, string(body))
	}
	return &result.Data, nil
}

// GetInstance returns details for a specific instance.
func (c *Client) GetInstance(ctx context.Context, instanceID string) (*InstanceResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/instances/"+instanceID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("verda get instance returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data InstanceResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return &result.Data, nil
}

// ListInstances returns all instances.
func (c *Client) ListInstances(ctx context.Context) ([]InstanceResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/instances", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("verda list instances returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data []InstanceResponse `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return result.Data, nil
}

// InstanceActionRequest represents an action to perform on instances.
type InstanceActionRequest struct {
	Action string   `json:"action"` // "boot", "start", "shutdown", "delete"
	IDs    []string `json:"id"`
}

// DeleteInstance deletes an instance.
func (c *Client) DeleteInstance(ctx context.Context, instanceID string) error {
	actionReq := InstanceActionRequest{
		Action: "delete",
		IDs:    []string{instanceID},
	}
	resp, err := c.doRequest(ctx, http.MethodPut, "/instances", actionReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("verda delete returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// CreateStartupScriptRequest is the body for creating a startup script.
type CreateStartupScriptRequest struct {
	Name   string `json:"name"`
	Script string `json:"script"`
}

// StartupScriptResponse represents a startup script.
type StartupScriptResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// CreateStartupScript creates a startup script that can be referenced by instances.
func (c *Client) CreateStartupScript(ctx context.Context, name, script string) (*StartupScriptResponse, error) {
	req := CreateStartupScriptRequest{Name: name, Script: script}
	resp, err := c.doRequest(ctx, http.MethodPost, "/scripts", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("verda create script returned %d: %s", resp.StatusCode, string(body))
	}

	// Verda API returns the script ID in various formats:
	// 1. Plain text UUID: "7feed3e4-cba3-43a2-b7b2-15a18abb9add"
	// 2. JSON wrapped: {"data": {"id": "...", "name": "..."}}
	// 3. JSON raw value: {"data": "some-id"}
	// Try plain text first (most common), then JSON variants.
	bodyStr := strings.TrimSpace(string(body))

	// Plain text UUID or ID — not valid JSON
	if len(bodyStr) > 0 && bodyStr[0] != '{' && bodyStr[0] != '[' {
		return &StartupScriptResponse{ID: bodyStr, Name: name}, nil
	}

	// JSON wrapped response
	var raw struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		// Last resort: treat entire body as the ID
		return &StartupScriptResponse{ID: bodyStr, Name: name}, nil
	}

	var result StartupScriptResponse
	if err := json.Unmarshal(raw.Data, &result); err == nil && result.ID != "" {
		return &result, nil
	}

	// data is a raw ID (number or string) — use it directly
	var numID json.Number
	if err := json.Unmarshal(raw.Data, &numID); err == nil {
		return &StartupScriptResponse{ID: numID.String(), Name: name}, nil
	}
	var strID string
	if err := json.Unmarshal(raw.Data, &strID); err == nil {
		return &StartupScriptResponse{ID: strID, Name: name}, nil
	}

	return nil, fmt.Errorf("unexpected data format in response: %s", bodyStr)
}
