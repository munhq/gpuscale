package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	managementBaseURL = "https://management.azure.com"
	loginBaseURL      = "https://login.microsoftonline.com"
	pricingBaseURL    = "https://prices.azure.com/api/retail/prices"
	computeAPIVersion = "2024-03-01"
	networkAPIVersion = "2023-05-01"
	groupAPIVersion   = "2021-04-01"
)

// Client is an HTTP client for the Azure Resource Manager REST API.
// It acquires bearer tokens via client_credentials OAuth2 automatically.
type Client struct {
	subscriptionID string
	tenantID       string
	clientID       string
	clientSecret   string
	resourceGroup  string
	location       string

	httpClient *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
}

// NewClient creates an Azure Resource Manager client.
// resourceGroup and location default to "gpu-api-nodes" and "eastus" if empty.
func NewClient(subscriptionID, tenantID, clientID, clientSecret, resourceGroup, location string) *Client {
	if resourceGroup == "" {
		resourceGroup = "gpu-api-nodes"
	}
	if location == "" {
		location = "eastus"
	}
	return &Client{
		subscriptionID: subscriptionID,
		tenantID:       tenantID,
		clientID:       clientID,
		clientSecret:   clientSecret,
		resourceGroup:  resourceGroup,
		location:       location,
		httpClient:     &http.Client{Timeout: 60 * time.Second},
	}
}

// getToken returns a valid bearer token, refreshing from Azure AD when needed.
func (c *Client) getToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token != "" && time.Now().Before(c.tokenExpiry) {
		return c.token, nil
	}

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
		"scope":         {"https://management.azure.com/.default"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/%s/oauth2/v2.0/token", loginBaseURL, c.tenantID),
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("azure auth HTTP %d: %s", resp.StatusCode, body)
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", fmt.Errorf("parse azure token: %w", err)
	}

	// Expire 2 minutes early for safety margin.
	c.token = tok.AccessToken
	c.tokenExpiry = time.Now().Add(time.Duration(tok.ExpiresIn-120) * time.Second)
	return c.token, nil
}

// arm executes an authenticated request against the ARM API.
func (c *Client) arm(ctx context.Context, method, path string, body, dst interface{}) error {
	token, err := c.getToken(ctx)
	if err != nil {
		return err
	}

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, managementBaseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ARM request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read ARM response: %w", err)
	}

	// 200, 201, 202 are all success for ARM long-running operations.
	if resp.StatusCode >= 400 {
		var azErr struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(respBody, &azErr)
		if azErr.Error.Message != "" {
			return fmt.Errorf("azure %s %s: %s: %s", method, path, azErr.Error.Code, azErr.Error.Message)
		}
		return fmt.Errorf("azure %s %s: HTTP %d", method, path, resp.StatusCode)
	}

	if dst != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, dst); err != nil {
			return fmt.Errorf("decode ARM response: %w", err)
		}
	}
	return nil
}

func (c *Client) rgPath() string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s", c.subscriptionID, c.resourceGroup)
}

// EnsureResourceGroup creates the resource group if it does not exist.
func (c *Client) EnsureResourceGroup(ctx context.Context) error {
	return c.arm(ctx, http.MethodPut,
		fmt.Sprintf("/subscriptions/%s/resourceGroups/%s?api-version=%s",
			c.subscriptionID, c.resourceGroup, groupAPIVersion),
		map[string]string{"location": c.location}, nil)
}

// ensureVNet ensures a VNet with a default subnet exists in the resource group.
func (c *Client) ensureVNet(ctx context.Context, vnetName string) error {
	body := map[string]interface{}{
		"location": c.location,
		"properties": map[string]interface{}{
			"addressSpace": map[string]interface{}{
				"addressPrefixes": []string{"10.10.0.0/16"},
			},
			"subnets": []map[string]interface{}{
				{
					"name": "default",
					"properties": map[string]interface{}{
						"addressPrefix": "10.10.0.0/24",
					},
				},
			},
		},
	}
	return c.arm(ctx, http.MethodPut,
		fmt.Sprintf("%s/providers/Microsoft.Network/virtualNetworks/%s?api-version=%s",
			c.rgPath(), vnetName, networkAPIVersion),
		body, nil)
}

// CreatePublicIP creates a Basic dynamic public IP for a GPU VM.
func (c *Client) CreatePublicIP(ctx context.Context, name string) error {
	body := map[string]interface{}{
		"location": c.location,
		"sku":      map[string]string{"name": "Basic"},
		"properties": map[string]interface{}{
			"publicIPAllocationMethod": "Dynamic",
		},
	}
	return c.arm(ctx, http.MethodPut,
		fmt.Sprintf("%s/providers/Microsoft.Network/publicIPAddresses/%s?api-version=%s",
			c.rgPath(), name, networkAPIVersion),
		body, nil)
}

// CreateNIC creates a NIC and attaches it to the given public IP.
// Returns the NIC resource ID.
func (c *Client) CreateNIC(ctx context.Context, nicName, publicIPName string) (string, error) {
	vnetName := "gpu-api-vnet"
	if err := c.ensureVNet(ctx, vnetName); err != nil {
		return "", fmt.Errorf("ensure vnet: %w", err)
	}

	publicIPID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPAddresses/%s",
		c.subscriptionID, c.resourceGroup, publicIPName)
	subnetID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/virtualNetworks/%s/subnets/default",
		c.subscriptionID, c.resourceGroup, vnetName)

	body := map[string]interface{}{
		"location": c.location,
		"properties": map[string]interface{}{
			"ipConfigurations": []map[string]interface{}{
				{
					"name": "ipconfig1",
					"properties": map[string]interface{}{
						"privateIPAllocationMethod": "Dynamic",
						"publicIPAddress":           map[string]string{"id": publicIPID},
						"subnet":                    map[string]string{"id": subnetID},
					},
				},
			},
		},
	}

	var resp struct {
		ID string `json:"id"`
	}
	if err := c.arm(ctx, http.MethodPut,
		fmt.Sprintf("%s/providers/Microsoft.Network/networkInterfaces/%s?api-version=%s",
			c.rgPath(), nicName, networkAPIVersion),
		body, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// CreateVM provisions a GPU VM with cloud-init user data for the bootstrap script.
// Returns the provider VM name (used as instanceID).
func (c *Client) CreateVM(ctx context.Context, vmName, nicID, vmSize string, diskSizeGB int, bootstrapScript string) error {
	// Wrap the bootstrap script in minimal cloud-init YAML.
	cloudInit := "#cloud-config\nruncmd:\n  - bash -c '" + strings.ReplaceAll(bootstrapScript, "'", "'\"'\"'") + "'\n"
	customData := base64.StdEncoding.EncodeToString([]byte(cloudInit))

	body := map[string]interface{}{
		"location": c.location,
		"properties": map[string]interface{}{
			"hardwareProfile": map[string]string{
				"vmSize": vmSize,
			},
			"storageProfile": map[string]interface{}{
				"imageReference": map[string]string{
					"publisher": "Canonical",
					"offer":     "0001-com-ubuntu-server-jammy",
					"sku":       "22_04-lts-gen2",
					"version":   "latest",
				},
				"osDisk": map[string]interface{}{
					"createOption": "FromImage",
					"diskSizeGB":   diskSizeGB,
					"managedDisk": map[string]string{
						"storageAccountType": "Premium_LRS",
					},
				},
			},
			"osProfile": map[string]interface{}{
				"computerName":  vmName,
				"adminUsername": "ubuntu",
				"linuxConfiguration": map[string]interface{}{
					"disablePasswordAuthentication": true,
					"ssh": map[string]interface{}{
						// No SSH keys needed — gpu-agent connects outbound only.
						"publicKeys": []interface{}{},
					},
				},
				"customData": customData,
			},
			"networkProfile": map[string]interface{}{
				"networkInterfaces": []map[string]interface{}{
					{"id": nicID, "properties": map[string]bool{"primary": true}},
				},
			},
		},
	}

	return c.arm(ctx, http.MethodPut,
		fmt.Sprintf("%s/providers/Microsoft.Compute/virtualMachines/%s?api-version=%s",
			c.rgPath(), vmName, computeAPIVersion),
		body, nil)
}

// DeleteVM terminates a VM and best-effort deletes its NIC and public IP.
func (c *Client) DeleteVM(ctx context.Context, vmName string) error {
	if err := c.arm(ctx, http.MethodDelete,
		fmt.Sprintf("%s/providers/Microsoft.Compute/virtualMachines/%s?api-version=%s",
			c.rgPath(), vmName, computeAPIVersion),
		nil, nil); err != nil {
		return err
	}
	// Best-effort cleanup.
	_ = c.arm(ctx, http.MethodDelete,
		fmt.Sprintf("%s/providers/Microsoft.Network/networkInterfaces/%s?api-version=%s",
			c.rgPath(), vmName+"-nic", networkAPIVersion),
		nil, nil)
	_ = c.arm(ctx, http.MethodDelete,
		fmt.Sprintf("%s/providers/Microsoft.Network/publicIPAddresses/%s?api-version=%s",
			c.rgPath(), vmName+"-ip", networkAPIVersion),
		nil, nil)
	return nil
}

// GetVMState returns the provisioning state of a VM: "Succeeded", "Failed", "Creating", etc.
func (c *Client) GetVMState(ctx context.Context, vmName string) (string, error) {
	var resp struct {
		Properties struct {
			ProvisioningState string `json:"provisioningState"`
		} `json:"properties"`
	}
	if err := c.arm(ctx, http.MethodGet,
		fmt.Sprintf("%s/providers/Microsoft.Compute/virtualMachines/%s?api-version=%s",
			c.rgPath(), vmName, computeAPIVersion),
		nil, &resp); err != nil {
		return "", err
	}
	return resp.Properties.ProvisioningState, nil
}

// SpotPriceResult holds a spot price for one VM size.
type SpotPriceResult struct {
	VMSize string
	Price  float64
}

// FetchSpotPrices returns current spot prices for the given VM sizes in the configured location.
// Uses the public Azure Retail Prices API (no auth required).
func FetchSpotPrices(ctx context.Context, location string, vmSizes []string) (map[string]float64, error) {
	if location == "" {
		location = "eastus"
	}

	sizeFilters := make([]string, len(vmSizes))
	for i, s := range vmSizes {
		sizeFilters[i] = fmt.Sprintf("armSkuName eq '%s'", s)
	}
	filter := fmt.Sprintf(
		"serviceName eq 'Virtual Machines' and armRegionName eq '%s' and priceType eq 'Consumption' and (%s)",
		location, strings.Join(sizeFilters, " or "))

	u, err := url.Parse(pricingBaseURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("api-version", "2023-01-01")
	q.Set("$filter", filter)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Items []struct {
			ArmSKUName  string  `json:"armSkuName"`
			RetailPrice float64 `json:"retailPrice"`
			SKUName     string  `json:"skuName"`
		} `json:"Items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	prices := make(map[string]float64)
	for _, item := range result.Items {
		// Spot prices have " Spot" in their SKU name; on-demand do not.
		if strings.HasSuffix(item.SKUName, " Spot") && item.RetailPrice > 0 {
			if _, ok := prices[item.ArmSKUName]; !ok {
				prices[item.ArmSKUName] = item.RetailPrice
			}
		}
	}
	return prices, nil
}
