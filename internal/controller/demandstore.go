package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DemandStore reads demand counters and model config from Dragonfly DB 3.
// GPU API maintains these counters (INCR on enqueue, DECR on complete/fail).
// The DisruptionController reads them to decide if a worker is idle.
type DemandStore struct {
	rdb *redis.Client
}

// NewDemandStore creates a demand store connected to Dragonfly DB 3.
// Returns nil if redisURL is empty.
func NewDemandStore(redisURL string) *DemandStore {
	if redisURL == "" {
		return nil
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisURL,
		DB:           3, // DB 3 = request queue + demand counters
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		DialTimeout:  5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil
	}

	return &DemandStore{rdb: rdb}
}

// demandKey returns the Dragonfly key for a model's demand counter.
func demandKey(model string) string { return "demand:" + model }

// GetDemand returns the current demand count for a model.
// Returns 0 if the key doesn't exist.
func (s *DemandStore) GetDemand(ctx context.Context, model string) (int64, error) {
	if s == nil || s.rdb == nil {
		return 0, nil
	}
	val, err := s.rdb.Get(ctx, demandKey(model)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// modelConfigKey returns the key for the model config stored in Dragonfly.
const modelConfigHashKey = "gpu_api:model_config"

// IsAlwaysActive checks if a model is marked as always-active.
// GPU API publishes model config to Dragonfly at startup.
func (s *DemandStore) IsAlwaysActive(ctx context.Context, model string) (bool, error) {
	if s == nil || s.rdb == nil {
		return false, nil
	}

	data, err := s.rdb.HGet(ctx, modelConfigHashKey, model).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading model config for %s: %w", model, err)
	}

	var cfg struct {
		AlwaysActive bool `json:"alwaysActive"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return false, fmt.Errorf("parsing model config for %s: %w", model, err)
	}
	return cfg.AlwaysActive, nil
}

// Close shuts down the Redis connection.
func (s *DemandStore) Close() {
	if s != nil && s.rdb != nil {
		s.rdb.Close()
	}
}
