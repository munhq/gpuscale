package aws

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Client is an HTTP client for the AWS EC2 API using SigV4 authentication.
type Client struct {
	accessKeyID     string
	secretAccessKey string
	region          string
	httpClient      *http.Client

	amiMu      sync.Mutex
	cachedAMI  string
	amiExpiry  time.Time
}

// NewClient creates a new AWS EC2 client.
func NewClient(accessKeyID, secretAccessKey, region string) *Client {
	if region == "" {
		region = "us-east-1"
	}
	return &Client{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		region:          region,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) endpoint() string {
	return fmt.Sprintf("https://ec2.%s.amazonaws.com/", c.region)
}

// ec2Action calls the EC2 API with SigV4-signed POST and returns the response body.
func (c *Client) ec2Action(ctx context.Context, params map[string]string) ([]byte, error) {
	params["Version"] = "2016-11-15"

	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	body := []byte(form.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(), strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c.sign(req, body)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("aws ec2 request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Errors struct {
				Error struct {
					Code    string `xml:"Code"`
					Message string `xml:"Message"`
				} `xml:"Error"`
			} `xml:"Errors"`
		}
		_ = xml.Unmarshal(respBody, &errResp)
		code := errResp.Errors.Error.Code
		msg := errResp.Errors.Error.Message
		if code != "" {
			return nil, fmt.Errorf("aws %s: %s", code, msg)
		}
		return nil, fmt.Errorf("aws ec2 returned %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

// sign adds AWS SigV4 Authorization header to the request.
func (c *Client) sign(req *http.Request, body []byte) {
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	timeStamp := now.Format("20060102T150405Z")

	req.Header.Set("X-Amz-Date", timeStamp)
	req.Header.Set("Host", req.URL.Host)

	// Canonical headers must be sorted alphabetically.
	// We use: content-type, host, x-amz-date.
	contentType := req.Header.Get("Content-Type")
	canonicalHeaders := "content-type:" + contentType + "\n" +
		"host:" + req.URL.Host + "\n" +
		"x-amz-date:" + timeStamp + "\n"
	signedHeaders := "content-type;host;x-amz-date"

	bodyHash := sha256hex(body)
	canonicalRequest := strings.Join([]string{
		req.Method,
		"/",                 // canonical URI
		"",                  // canonical query string (POST uses body)
		canonicalHeaders,
		signedHeaders,
		bodyHash,
	}, "\n")

	scope := dateStamp + "/" + c.region + "/ec2/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + timeStamp + "\n" + scope + "\n" + sha256hex([]byte(canonicalRequest))

	kDate := hmacSHA256([]byte("AWS4"+c.secretAccessKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(c.region))
	kService := hmacSHA256(kRegion, []byte("ec2"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKeyID, scope, signedHeaders, sig,
	))
}

// XML response types

type runInstancesResponse struct {
	XMLName      xml.Name `xml:"RunInstancesResponse"`
	InstancesSet struct {
		Items []ec2InstanceItem `xml:"item"`
	} `xml:"instancesSet"`
}

type ec2InstanceItem struct {
	InstanceId string `xml:"instanceId"`
	State      struct {
		Name string `xml:"name"`
	} `xml:"instanceState"`
	PublicIpAddress  string `xml:"ipAddress"`
	PrivateIpAddress string `xml:"privateIpAddress"`
	Tags             struct {
		Items []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"item"`
	} `xml:"tagSet"`
}

type describeInstancesResponse struct {
	XMLName      xml.Name `xml:"DescribeInstancesResponse"`
	Reservations struct {
		Items []struct {
			InstancesSet struct {
				Items []ec2InstanceItem `xml:"item"`
			} `xml:"instancesSet"`
		} `xml:"item"`
	} `xml:"reservationSet"`
}

type describeSpotPriceResponse struct {
	XMLName             xml.Name `xml:"DescribeSpotPriceHistoryResponse"`
	SpotPriceHistorySet struct {
		Items []struct {
			InstanceType string `xml:"instanceType"`
			SpotPrice    string `xml:"spotPrice"`
		} `xml:"item"`
	} `xml:"spotPriceHistorySet"`
}

type describeImagesResponse struct {
	XMLName   xml.Name `xml:"DescribeImagesResponse"`
	ImagesSet struct {
		Items []struct {
			ImageId      string `xml:"imageId"`
			CreationDate string `xml:"creationDate"`
		} `xml:"item"`
	} `xml:"imagesSet"`
}

// RunInstances creates a spot EC2 instance with the given parameters.
func (c *Client) RunInstances(ctx context.Context, instanceType, amiID, userData, subnetID, securityGroupID, instanceName string, diskSizeGB int) (string, error) {
	params := map[string]string{
		"Action":       "RunInstances",
		"ImageId":      amiID,
		"InstanceType": instanceType,
		"MinCount":     "1",
		"MaxCount":     "1",
		"UserData":     base64.StdEncoding.EncodeToString([]byte(userData)),
		// Spot market options
		"InstanceMarketOptions.MarketType":                    "spot",
		"InstanceMarketOptions.SpotOptions.SpotInstanceType":  "one-time",
		// Tags
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "managed-by",
		"TagSpecification.1.Tag.1.Value":  "gpuscale",
		"TagSpecification.1.Tag.2.Key":    "Name",
		"TagSpecification.1.Tag.2.Value":  instanceName,
		// EBS root volume
		"BlockDeviceMapping.1.DeviceName":              "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.VolumeSize":          fmt.Sprintf("%d", diskSizeGB),
		"BlockDeviceMapping.1.Ebs.VolumeType":          "gp3",
		"BlockDeviceMapping.1.Ebs.DeleteOnTermination": "true",
	}
	if subnetID != "" {
		params["SubnetId"] = subnetID
	}
	if securityGroupID != "" {
		params["SecurityGroupId.1"] = securityGroupID
	}

	body, err := c.ec2Action(ctx, params)
	if err != nil {
		return "", err
	}
	var result runInstancesResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decoding RunInstances response: %w", err)
	}
	if len(result.InstancesSet.Items) == 0 {
		return "", fmt.Errorf("RunInstances returned no instances")
	}
	return result.InstancesSet.Items[0].InstanceId, nil
}

// TerminateInstance terminates an EC2 instance by ID.
func (c *Client) TerminateInstance(ctx context.Context, instanceID string) error {
	_, err := c.ec2Action(ctx, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": instanceID,
	})
	return err
}

// GetInstance returns the current state of an EC2 instance.
func (c *Client) GetInstance(ctx context.Context, instanceID string) (*ec2InstanceItem, error) {
	body, err := c.ec2Action(ctx, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instanceID,
	})
	if err != nil {
		return nil, err
	}
	var result describeInstancesResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decoding DescribeInstances: %w", err)
	}
	for _, r := range result.Reservations.Items {
		for i := range r.InstancesSet.Items {
			return &r.InstancesSet.Items[i], nil
		}
	}
	return nil, nil
}

// ListInstances returns all EC2 instances tagged managed-by=gpuscale.
func (c *Client) ListInstances(ctx context.Context) ([]ec2InstanceItem, error) {
	body, err := c.ec2Action(ctx, map[string]string{
		"Action":              "DescribeInstances",
		"Filter.1.Name":       "tag:managed-by",
		"Filter.1.Value.1":    "gpuscale",
		"Filter.2.Name":       "instance-state-name",
		"Filter.2.Value.1":    "running",
		"Filter.2.Value.2":    "pending",
	})
	if err != nil {
		return nil, err
	}
	var result describeInstancesResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decoding DescribeInstances: %w", err)
	}
	var instances []ec2InstanceItem
	for _, r := range result.Reservations.Items {
		instances = append(instances, r.InstancesSet.Items...)
	}
	return instances, nil
}

// DescribeSpotPrices returns the latest spot price for each instance type.
func (c *Client) DescribeSpotPrices(ctx context.Context, instanceTypes []string) (map[string]float64, error) {
	params := map[string]string{
		"Action":                "DescribeSpotPriceHistory",
		"ProductDescription.1":  "Linux/UNIX",
		"MaxResults":            "100",
	}
	for i, t := range instanceTypes {
		params[fmt.Sprintf("InstanceType.%d", i+1)] = t
	}
	body, err := c.ec2Action(ctx, params)
	if err != nil {
		return nil, err
	}
	var result describeSpotPriceResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decoding DescribeSpotPriceHistory: %w", err)
	}

	// Take the lowest price seen per instance type (most recent/best AZ).
	prices := map[string]float64{}
	for _, item := range result.SpotPriceHistorySet.Items {
		var p float64
		fmt.Sscanf(item.SpotPrice, "%f", &p)
		if p > 0 {
			if existing, ok := prices[item.InstanceType]; !ok || p < existing {
				prices[item.InstanceType] = p
			}
		}
	}
	return prices, nil
}

// GetLatestUbuntuAMI returns the latest Ubuntu 22.04 LTS x86_64 AMI ID for the region,
// cached for 6 hours.
func (c *Client) GetLatestUbuntuAMI(ctx context.Context) (string, error) {
	c.amiMu.Lock()
	defer c.amiMu.Unlock()
	if c.cachedAMI != "" && time.Now().Before(c.amiExpiry) {
		return c.cachedAMI, nil
	}

	params := map[string]string{
		"Action":            "DescribeImages",
		"Owner.1":           "099720109477", // Canonical
		"Filter.1.Name":     "name",
		"Filter.1.Value.1":  "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*",
		"Filter.2.Name":     "virtualization-type",
		"Filter.2.Value.1":  "hvm",
		"Filter.3.Name":     "root-device-type",
		"Filter.3.Value.1":  "ebs",
		"Filter.4.Name":     "architecture",
		"Filter.4.Value.1":  "x86_64",
		"Filter.5.Name":     "state",
		"Filter.5.Value.1":  "available",
	}
	body, err := c.ec2Action(ctx, params)
	if err != nil {
		return "", fmt.Errorf("DescribeImages: %w", err)
	}
	var result describeImagesResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decoding DescribeImages: %w", err)
	}
	if len(result.ImagesSet.Items) == 0 {
		return "", fmt.Errorf("no Ubuntu 22.04 AMI found in %s", c.region)
	}

	// Sort by creation date descending, pick the newest.
	items := result.ImagesSet.Items
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreationDate > items[j].CreationDate
	})
	c.cachedAMI = items[0].ImageId
	c.amiExpiry = time.Now().Add(6 * time.Hour)
	return c.cachedAMI, nil
}

// DescribeAvailabilityZones is the lightest authenticated EC2 call available.
// It validates that the access key, secret, and region are all accepted by AWS.
func (c *Client) DescribeAvailabilityZones(ctx context.Context) error {
	_, err := c.ec2Action(ctx, map[string]string{"Action": "DescribeAvailabilityZones"})
	return err
}

func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
