package runpod

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const baseURL = "https://rest.runpod.io/v1"

// Client is an HTTP client for the RunPod API.
type Client struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new RunPod API client.
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: baseURL,
	}
}

func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
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
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return c.httpClient.Do(req)
}

// GPUType represents an available RunPod GPU type.
type GPUType struct {
	ID               string  `json:"id"`
	DisplayName      string  `json:"displayName"`
	MemoryInGB       int     `json:"memoryInGb"`
	SecurePrice      float64 `json:"securePrice"`
	CommunityPrice   float64 `json:"communityPrice"`
	SecureSpotPrice  float64 `json:"secureSpotPrice"`
	CommunitySpotPrice float64 `json:"communitySpotPrice"`
}

// ListGPUTypes returns available GPU types.
func (c *Client) ListGPUTypes(ctx context.Context) ([]GPUType, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/gpu-types", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("runpod gpu-types returned %d: %s", resp.StatusCode, string(body))
	}

	var result []GPUType
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return result, nil
}

// CreatePodRequest is the body for creating a RunPod pod.
type CreatePodRequest struct {
	Name          string            `json:"name"`
	ImageName     string            `json:"imageName"`
	GPUTypeID     string            `json:"gpuTypeId"`
	GPUCount      int               `json:"gpuCount"`
	CloudType     string            `json:"cloudType"` // "SECURE", "COMMUNITY", "ALL"
	VolumeInGB    int               `json:"volumeInGb"`
	ContainerDiskInGB int           `json:"containerDiskInGb"`
	Env           map[string]string `json:"env,omitempty"`
	DockerArgs    string            `json:"dockerArgs,omitempty"`
	Ports         string            `json:"ports,omitempty"`
	StartSSH      bool              `json:"startSsh"`
}

// PodResponse represents a RunPod pod.
type PodResponse struct {
	ID           string  `json:"id"`
	Name         string  `json:"name"`
	DesiredStatus string `json:"desiredStatus"`
	Runtime      *struct {
		Uptime int    `json:"uptimeInSeconds"`
		GPUs   []struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
			MemoryInGB  int    `json:"memoryInGb"`
		} `json:"gpus"`
		Ports []struct {
			IP         string `json:"ip"`
			PublicPort int    `json:"publicPort"`
			PrivatePort int   `json:"privatePort"`
			Type       string `json:"type"`
		} `json:"ports"`
	} `json:"runtime"`
	Machine struct {
		GPUDisplayName string `json:"gpuDisplayName"`
	} `json:"machine"`
	CostPerHr float64 `json:"costPerHr"`
}

// CreatePod creates a new pod on RunPod.
func (c *Client) CreatePod(ctx context.Context, req CreatePodRequest) (*PodResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, "/pods", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("runpod create pod returned %d: %s", resp.StatusCode, string(body))
	}

	var pod PodResponse
	if err := json.NewDecoder(resp.Body).Decode(&pod); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return &pod, nil
}

// GetPod returns details for a specific pod.
func (c *Client) GetPod(ctx context.Context, podID string) (*PodResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/pods/"+podID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("runpod get pod returned %d: %s", resp.StatusCode, string(body))
	}

	var pod PodResponse
	if err := json.NewDecoder(resp.Body).Decode(&pod); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return &pod, nil
}

// ListPods returns all pods.
func (c *Client) ListPods(ctx context.Context) ([]PodResponse, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/pods", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("runpod list pods returned %d: %s", resp.StatusCode, string(body))
	}

	var pods []PodResponse
	if err := json.NewDecoder(resp.Body).Decode(&pods); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return pods, nil
}

// DeletePod terminates a pod.
func (c *Client) DeletePod(ctx context.Context, podID string) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, "/pods/"+podID, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("runpod delete pod returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
