package tensordock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://marketplace.tensordock.com/api/v0"

// Client is an HTTP client for the TensorDock marketplace API.
// Auth: GET endpoints are public (no auth); POST endpoints use api_key + api_token
// as application/x-www-form-urlencoded body fields.
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

// doGET executes an authenticated GET request, passing api_key and api_token as query params.
func (c *Client) doGET(ctx context.Context, path string, dst interface{}) error {
	u := baseURL + path
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	u += sep + "api_key=" + url.QueryEscape(c.apiKey) + "&api_token=" + url.QueryEscape(c.apiToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	return c.do(req, dst)
}

// doPOST executes an authenticated POST with application/x-www-form-urlencoded body.
// api_key and api_token are injected as form fields.
func (c *Client) doPOST(ctx context.Context, path string, form url.Values, dst interface{}) error {
	if form == nil {
		form = url.Values{}
	}
	form.Set("api_key", c.apiKey)
	form.Set("api_token", c.apiToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
			msg = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return fmt.Errorf("tensordock API error: %s", msg)
	}

	if dst != nil {
		if err := json.Unmarshal(body, dst); err != nil {
			return fmt.Errorf("decode response: %w (body: %.200s)", err, string(body))
		}
	}
	return nil
}

// --- API response types ---

// GPUModel holds the count, price and VRAM for one GPU model on a hostnode.
type GPUModel struct {
	Amount int     `json:"amount"`
	Price  float64 `json:"price"` // $/GPU/hr
	VRAM   int     `json:"vram"`  // GB per GPU
}

// HostnodeSpecs describes available hardware on a TensorDock server.
type HostnodeSpecs struct {
	CPU struct {
		Amount int     `json:"amount"` // max vCPUs
		Type   string  `json:"type"`
		Price  float64 `json:"price"` // $/vCPU/hr
	} `json:"cpu"`
	RAM struct {
		Amount int     `json:"amount"` // GB
		Price  float64 `json:"price"`  // $/GB/hr
	} `json:"ram"`
	Storage struct {
		Amount int     `json:"amount"` // GB
		Price  float64 `json:"price"`  // $/GB/hr
	} `json:"storage"`
	// GPU is a map from model slug (e.g. "geforcertx3080-pcie-10gb") to count+price.
	GPU map[string]GPUModel `json:"gpu"`
}

// HostnodeStatus holds availability info for a TensorDock server.
type HostnodeStatus struct {
	Listed bool    `json:"listed"`
	Online bool    `json:"online"`
	Uptime float64 `json:"uptime"` // 0–1 reliability score
}

// HostnodeLocation holds datacenter location info.
type HostnodeLocation struct {
	City    string `json:"city"`
	Country string `json:"country"`
	Region  string `json:"region"`
	ID      string `json:"id"`
}

// Hostnode represents one TensorDock bare-metal server.
type Hostnode struct {
	UUID     string           // populated from the map key in HostnodesResponse
	Location HostnodeLocation `json:"location"`
	Specs    HostnodeSpecs    `json:"specs"`
	Status   HostnodeStatus   `json:"status"`
}

// HostnodesResponse is returned by GET /client/deploy/hostnodes.
type HostnodesResponse struct {
	Success   bool                `json:"success"`
	Hostnodes map[string]Hostnode `json:"hostnodes"`
}

// DeployResponse is returned by POST /client/deploy/single.
type DeployResponse struct {
	Success      bool              `json:"success"`
	Server       string            `json:"server"` // VM UUID — use as InstanceID
	IP           string            `json:"ip"`
	PortForwards map[string]string `json:"port_forwards"` // external → internal port
	Error        string            `json:"error,omitempty"`
}

// VMStatus is returned by POST /client/get/single.
type VMStatus struct {
	Success      bool              `json:"success"`
	Status       string            `json:"status"` // "Running", "Outbid", etc.
	IP           string            `json:"ip"`
	PortForwards map[string]string `json:"port_forwards"`
	Error        string            `json:"error,omitempty"`
}

// DeleteResponse is returned by POST /client/delete/single.
type DeleteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// VMListEntry is one entry in the POST /client/list response.
type VMListEntry struct {
	Status string `json:"status"`
	IP     string `json:"ip"`
	Name   string `json:"name"`
}

// ListVMsResponse is returned by POST /client/list.
type ListVMsResponse struct {
	Success bool                    `json:"success"`
	Servers map[string]VMListEntry  `json:"servers"`
}

// --- API methods ---

// ListHostnodes returns all available GPU hosts in the marketplace.
// This endpoint is public — no credentials required.
func (c *Client) ListHostnodes(ctx context.Context) ([]Hostnode, error) {
	var resp HostnodesResponse
	if err := c.doGET(ctx, "/client/deploy/hostnodes", &resp); err != nil {
		return nil, err
	}
	nodes := make([]Hostnode, 0, len(resp.Hostnodes))
	for id, node := range resp.Hostnodes {
		node.UUID = id
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// ListVMs returns all VMs deployed under this account.
// Used for credential validation and instance listing.
func (c *Client) ListVMs(ctx context.Context) (map[string]VMListEntry, error) {
	var resp ListVMsResponse
	if err := c.doPOST(ctx, "/client/list", nil, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("tensordock list VMs: API returned success=false")
	}
	return resp.Servers, nil
}

// DeployVM provisions a new VM on the given hostnode.
func (c *Client) DeployVM(ctx context.Context, hostnodeUUID, gpuModel string, gpuCount, vcpus, ramGB, storageGB int, password, cloudinitScript string) (*DeployResponse, error) {
	name := sanitizeHostname("gpuapi-" + hostnodeUUID[:8])

	form := url.Values{
		"hostnode":         {hostnodeUUID},
		"gpu_model":        {gpuModel},
		"gpu_count":        {strconv.Itoa(gpuCount)},
		"vcpus":            {strconv.Itoa(vcpus)},
		"ram":              {strconv.Itoa(ramGB)},
		"storage":          {strconv.Itoa(storageGB)},
		"operating_system": {"Ubuntu 22.04 LTS"},
		"password":         {password},
		"name":             {name},
		"internal_ports":   {"{22}"},
	}
	if cloudinitScript != "" {
		form.Set("cloudinit_script", cloudinitScript)
	}

	var resp DeployResponse
	if err := c.doPOST(ctx, "/client/deploy/single", form, &resp); err != nil {
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

// GetVM returns the current status of a deployed VM.
// Returns (nil, nil) if the server UUID is not found.
func (c *Client) GetVM(ctx context.Context, serverUUID string) (*VMStatus, error) {
	form := url.Values{"server": {serverUUID}}
	var resp VMStatus
	if err := c.doPOST(ctx, "/client/get/single", form, &resp); err != nil {
		// A "not found" error from TensorDock comes as an API error, not HTTP 404.
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "Server not found") {
			return nil, nil
		}
		return nil, err
	}
	if !resp.Success {
		if strings.Contains(strings.ToLower(resp.Error), "not found") {
			return nil, nil
		}
		return nil, fmt.Errorf("tensordock get VM: %s", resp.Error)
	}
	return &resp, nil
}

// DeleteVM terminates a VM.
func (c *Client) DeleteVM(ctx context.Context, serverUUID string) error {
	form := url.Values{"server": {serverUUID}}
	var resp DeleteResponse
	if err := c.doPOST(ctx, "/client/delete/single", form, &resp); err != nil {
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

// GeneratePassword creates a random 24-hex-char password suitable for VM provisioning.
func GeneratePassword() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "Td1!" + hex.EncodeToString(b) // prefix satisfies typical complexity requirements
}

// ParseVRAMFromSlug extracts VRAM in GB from a GPU model slug as a fallback
// when the vram field is absent. E.g. "rtxa4000-pcie-16gb" → 16.
var vramRegex = regexp.MustCompile(`-?(\d+)gb$`)

func ParseVRAMFromSlug(slug string) int {
	m := vramRegex.FindStringSubmatch(strings.ToLower(slug))
	if len(m) < 2 {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// sanitizeHostname returns a lowercase alphanumeric+hyphen string ≤ 24 chars.
func sanitizeHostname(s string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteRune(c)
		}
	}
	if b.Len() > 24 {
		return b.String()[:24]
	}
	return b.String()
}
