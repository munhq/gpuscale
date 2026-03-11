package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/munhq/gpuscale/api/v1alpha1"
	"github.com/redis/go-redis/v9"
)

const (
	// workersKey is the Redis hash where worker status is tracked.
	// GPU API and other consumers read this for observability.
	workersKey = "gpuscale:workers"
)

// WorkerInfo is the JSON value stored in Dragonfly for each worker.
type WorkerInfo struct {
	ClaimName  string `json:"claimName"`
	Provider   string `json:"provider"`
	InstanceID string `json:"instanceId"`
	NodeType   string `json:"nodeType"`
	GPUType    string `json:"gpuType"`
	GPUCount   int    `json:"gpuCount"`
	Phase      string `json:"phase"`
	Endpoint   string `json:"endpoint,omitempty"`
	NodeName   string `json:"nodeName,omitempty"`
	ReadyAt    string `json:"readyAt,omitempty"`
}

// WorkerStore writes GPUNodeClaim status to Dragonfly for observability.
// This is NOT used for routing. This is for GPU API
// to show users provisioning state ("spinning up", "ready", etc).
type WorkerStore struct {
	rdb *redis.Client
}

// NewWorkerStore creates a worker store connected to Dragonfly.
// Returns nil if redisURL is empty (Dragonfly integration disabled).
func NewWorkerStore(redisURL string) *WorkerStore {
	if redisURL == "" {
		return nil
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		// Fall back to host:port format (no scheme)
		opts = &redis.Options{
			Addr: redisURL,
			DB:   0,
		}
	}

	return &WorkerStore{
		rdb: redis.NewClient(opts),
	}
}

// SetWorker writes or updates a worker entry in Dragonfly.
func (s *WorkerStore) SetWorker(ctx context.Context, claim *v1alpha1.GPUNodeClaim) error {
	if s == nil || s.rdb == nil {
		return nil
	}

	info := WorkerInfo{
		ClaimName:  claim.Name,
		Provider:   claim.Status.Provider,
		InstanceID: claim.Status.InstanceID,
		NodeType:   claim.Status.NodeType,
		GPUType:    claim.Status.GPUType,
		GPUCount:   claim.Status.GPUCount,
		Phase:      string(claim.Status.Phase),
		Endpoint:   claim.Status.Endpoint,
		NodeName:   claim.Status.NodeName,
	}
	if claim.Status.ReadyAt != nil {
		info.ReadyAt = claim.Status.ReadyAt.Time.Format(time.RFC3339)
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling worker info: %w", err)
	}

	return s.rdb.HSet(ctx, workersKey, claim.Name, string(data)).Err()
}

// RemoveWorker deletes a worker entry from Dragonfly.
func (s *WorkerStore) RemoveWorker(ctx context.Context, claimName string) error {
	if s == nil || s.rdb == nil {
		return nil
	}
	return s.rdb.HDel(ctx, workersKey, claimName).Err()
}
