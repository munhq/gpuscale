package gcp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const computeBaseURL = "https://compute.googleapis.com/compute/v1"

type serviceAccountJSON struct {
	PrivateKey  string `json:"private_key"`
	ClientEmail string `json:"client_email"`
	TokenURI    string `json:"token_uri"`
	ProjectID   string `json:"project_id"`
}

// Client is an HTTP client for the GCP Compute Engine API using service account auth.
type Client struct {
	sa         serviceAccountJSON
	projectID  string
	httpClient *http.Client

	mu          sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

// NewClient parses the service account JSON and returns a Client.
func NewClient(saJSON, projectID string) (*Client, error) {
	var sa serviceAccountJSON
	if err := json.Unmarshal([]byte(saJSON), &sa); err != nil {
		return nil, fmt.Errorf("parsing service account JSON: %w", err)
	}
	if sa.ClientEmail == "" || sa.PrivateKey == "" {
		return nil, fmt.Errorf("service account JSON missing client_email or private_key")
	}
	if projectID == "" {
		projectID = sa.ProjectID
	}
	return &Client{
		sa:        sa,
		projectID: projectID,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (c *Client) token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Now().Before(c.tokenExpiry.Add(-60 * time.Second)) {
		return c.accessToken, nil
	}

	now := time.Now()
	tokenURI := c.sa.TokenURI
	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}

	// Build JWT: header.claims.signature
	header := base64urlEncode([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claimsJSON, _ := json.Marshal(map[string]interface{}{
		"iss":   c.sa.ClientEmail,
		"scope": "https://www.googleapis.com/auth/compute",
		"aud":   tokenURI,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	claims := base64urlEncode(claimsJSON)
	sigInput := header + "." + claims

	pk, err := parseRSAKey(c.sa.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("parsing private key: %w", err)
	}
	h := sha256.New()
	h.Write([]byte(sigInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, pk, crypto.SHA256, h.Sum(nil))
	if err != nil {
		return "", fmt.Errorf("signing JWT: %w", err)
	}
	jwt := sigInput + "." + base64urlEncode(sig)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, tokenURI,
		strings.NewReader("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion="+jwt))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var tr struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
		ErrorDesc   string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &tr); err != nil || tr.AccessToken == "" {
		return "", fmt.Errorf("gcp token error: %s %s", tr.Error, tr.ErrorDesc)
	}
	c.accessToken = tr.AccessToken
	c.tokenExpiry = now.Add(time.Duration(tr.ExpiresIn) * time.Second)
	return c.accessToken, nil
}

func (c *Client) do(ctx context.Context, method, path string, body interface{}) ([]byte, int, error) {
	tok, err := c.token(ctx)
	if err != nil {
		return nil, 0, err
	}
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, computeBaseURL+path, bodyReader)
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("gcp request: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

// GCPInstance is the Compute Engine instance resource.
type GCPInstance struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	NetworkInterfaces []struct {
		AccessConfigs []struct {
			NatIP string `json:"natIP"`
		} `json:"accessConfigs"`
	} `json:"networkInterfaces"`
	Labels map[string]string `json:"labels"`
}

type createInstanceReq struct {
	Name              string            `json:"name"`
	MachineType       string            `json:"machineType"`
	Disks             []gcpDisk         `json:"disks"`
	NetworkInterfaces []gcpNIC          `json:"networkInterfaces"`
	Scheduling        gcpScheduling     `json:"scheduling"`
	Labels            map[string]string `json:"labels"`
	Metadata          gcpMetadata       `json:"metadata"`
	GuestAccelerators []gcpAccel        `json:"guestAccelerators,omitempty"`
}

type gcpDisk struct {
	Boot             bool           `json:"boot"`
	AutoDelete       bool           `json:"autoDelete"`
	Type             string         `json:"type"`
	InitializeParams gcpDiskParams  `json:"initializeParams"`
}

type gcpDiskParams struct {
	SourceImage string `json:"sourceImage"`
	DiskSizeGb  string `json:"diskSizeGb,omitempty"`
}

type gcpNIC struct {
	Network      string      `json:"network"`
	AccessConfigs []gcpNATCfg `json:"accessConfigs"`
}

type gcpNATCfg struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type gcpScheduling struct {
	ProvisioningModel string `json:"provisioningModel"`
	OnHostMaintenance string `json:"onHostMaintenance"`
	AutomaticRestart  bool   `json:"automaticRestart"`
}

type gcpMetadata struct {
	Items []gcpMetaItem `json:"items"`
}

type gcpMetaItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type gcpAccel struct {
	AcceleratorType  string `json:"acceleratorType"`
	AcceleratorCount int    `json:"acceleratorCount"`
}

func (c *Client) CreateInstance(ctx context.Context, zone string, req createInstanceReq) error {
	body, status, err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("/projects/%s/zones/%s/instances", c.projectID, zone), req)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		var op struct {
			Error *struct {
				Errors []struct{ Message string `json:"message"` } `json:"errors"`
			} `json:"error"`
		}
		_ = json.Unmarshal(body, &op)
		if op.Error != nil && len(op.Error.Errors) > 0 {
			return fmt.Errorf("gcp create error: %s", op.Error.Errors[0].Message)
		}
		return fmt.Errorf("gcp create returned %d: %s", status, string(body))
	}
	return nil
}

func (c *Client) GetInstance(ctx context.Context, zone, name string) (*GCPInstance, error) {
	body, status, err := c.do(ctx, http.MethodGet,
		fmt.Sprintf("/projects/%s/zones/%s/instances/%s", c.projectID, zone, name), nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("gcp get returned %d: %s", status, string(body))
	}
	var inst GCPInstance
	if err := json.Unmarshal(body, &inst); err != nil {
		return nil, fmt.Errorf("decoding instance: %w", err)
	}
	return &inst, nil
}

func (c *Client) DeleteInstance(ctx context.Context, zone, name string) error {
	_, status, err := c.do(ctx, http.MethodDelete,
		fmt.Sprintf("/projects/%s/zones/%s/instances/%s", c.projectID, zone, name), nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil // already gone
	}
	if status != http.StatusOK {
		return fmt.Errorf("gcp delete returned %d", status)
	}
	return nil
}

func (c *Client) ListInstances(ctx context.Context, zone string) ([]GCPInstance, error) {
	body, status, err := c.do(ctx, http.MethodGet,
		fmt.Sprintf("/projects/%s/zones/%s/instances?filter=labels.managed-by%%3Dgpuscale", c.projectID, zone), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("gcp list returned %d", status)
	}
	var result struct {
		Items []GCPInstance `json:"items"`
	}
	_ = json.Unmarshal(body, &result)
	return result.Items, nil
}

func base64urlEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func parseRSAKey(pemStr string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing PKCS8 key: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("expected RSA private key")
	}
	return rsaKey, nil
}
