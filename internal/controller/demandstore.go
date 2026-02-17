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

// ModelConfig represents the full model configuration from GPU API.
type ModelConfig struct {
	ID             string   `json:"id"`
	Source         string   `json:"source"`          // HuggingFace source (e.g., "hf:zai-org/GLM-4.7-Flash")
	VRAMRequired   int      `json:"vramRequired"`    // GB total VRAM needed
	AlwaysActive   bool     `json:"alwaysActive"`
	MinReplicas    int      `json:"minReplicas"`
	MaxReplicas    int      `json:"maxReplicas"`
	NodeType       string   `json:"nodeType"`        // "ray-worker" or "full-node"
	MaxPricePerGPU float64  `json:"maxPricePerGPU"`
	MaxVRAMPerGPU  int      `json:"maxVramPerGpu"`   // max VRAM per GPU in GB (0 = no limit)
	PreferredGPUs  []string `json:"preferredGpus"`   // preferred GPU types (tried first)
}

// GetModelConfig retrieves a model configuration from Dragonfly by config ID
// or by model source path. The config hash is keyed by config ID (e.g.,
// "glm-4-7b-flash"), but callers may pass either the config ID or the
// HuggingFace model path (e.g., "zai-org/GLM-4.7-Flash"). If a direct key
// lookup fails, we scan all configs for a matching source field.
func (s *DemandStore) GetModelConfig(ctx context.Context, model string) (*ModelConfig, error) {
	if s == nil || s.rdb == nil {
		return nil, nil
	}

	// Try direct key lookup first (fast path for config IDs).
	data, err := s.rdb.HGet(ctx, modelConfigHashKey, model).Bytes()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("reading model config for %s: %w", model, err)
	}
	if err == nil {
		var cfg ModelConfig
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parsing model config for %s: %w", model, err)
		}
		return &cfg, nil
	}

	// Key not found — scan all configs for a matching source field.
	// The source field is "hf:<org>/<model>" and the caller may pass
	// just "<org>/<model>" (from the request body model field).
	all, err := s.rdb.HGetAll(ctx, modelConfigHashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("scanning model configs for %s: %w", model, err)
	}
	hfSource := "hf:" + model
	for _, raw := range all {
		var cfg ModelConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			continue
		}
		if cfg.Source == model || cfg.Source == hfSource {
			return &cfg, nil
		}
	}

	return nil, nil
}

// GetAllModelConfigs retrieves all model configurations from Dragonfly.
func (s *DemandStore) GetAllModelConfigs(ctx context.Context) (map[string]*ModelConfig, error) {
	if s == nil || s.rdb == nil {
		return nil, nil
	}

	data, err := s.rdb.HGetAll(ctx, modelConfigHashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("reading all model configs: %w", err)
	}

	configs := make(map[string]*ModelConfig, len(data))
	for model, jsonStr := range data {
		var cfg ModelConfig
		if err := json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
			continue // skip invalid entries
		}
		configs[model] = &cfg
	}
	return configs, nil
}

// queueKey returns the Dragonfly key for a model's request queue.
func queueKey(model string) string { return "chatqueue:" + model }

// GetQueueDepth returns the number of queued requests for a model.
// Returns 0 if the queue doesn't exist.
func (s *DemandStore) GetQueueDepth(ctx context.Context, model string) (int64, error) {
	if s == nil || s.rdb == nil {
		return 0, nil
	}
	length, err := s.rdb.LLen(ctx, queueKey(model)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return length, err
}

// ModelDemand aggregates all demand signals for a model.
type ModelDemand struct {
	Model        string
	QueueDepth   int64 // pending requests in chatqueue:{model}
	ActiveDemand int64 // active requests from demand:{model}
	VRAMRequired int   // VRAM needed to load the model (GB)
	AlwaysActive bool  // should stay warm
}

// GetModelDemand retrieves all demand data for a specific model.
func (s *DemandStore) GetModelDemand(ctx context.Context, model string) (*ModelDemand, error) {
	if s == nil || s.rdb == nil {
		return nil, nil
	}

	cfg, err := s.GetModelConfig(ctx, model)
	if err != nil {
		return nil, fmt.Errorf("getting config for %s: %w", model, err)
	}
	if cfg == nil {
		return nil, nil // model not configured
	}

	queueDepth, err := s.GetQueueDepth(ctx, model)
	if err != nil {
		return nil, fmt.Errorf("getting queue depth for %s: %w", model, err)
	}

	activeDemand, err := s.GetDemand(ctx, model)
	if err != nil {
		return nil, fmt.Errorf("getting active demand for %s: %w", model, err)
	}

	return &ModelDemand{
		Model:        model,
		QueueDepth:   queueDepth,
		ActiveDemand: activeDemand,
		VRAMRequired: cfg.VRAMRequired,
		AlwaysActive: cfg.AlwaysActive,
	}, nil
}

// GetAllDemands retrieves demand data for all configured models.
func (s *DemandStore) GetAllDemands(ctx context.Context) ([]*ModelDemand, error) {
	if s == nil || s.rdb == nil {
		return nil, nil
	}

	configs, err := s.GetAllModelConfigs(ctx)
	if err != nil {
		return nil, err
	}

	demands := make([]*ModelDemand, 0, len(configs))
	for model, cfg := range configs {
		queueDepth, _ := s.GetQueueDepth(ctx, model)
		activeDemand, _ := s.GetDemand(ctx, model)

		demands = append(demands, &ModelDemand{
			Model:        model,
			QueueDepth:   queueDepth,
			ActiveDemand: activeDemand,
			VRAMRequired: cfg.VRAMRequired,
			AlwaysActive: cfg.AlwaysActive,
		})
	}
	return demands, nil
}

// GetGPUVRAM looks up VRAM capacity for a GPU type from gpu_api:gpu_specs.
// Returns 0 if not found (caller should handle fallback).
func (s *DemandStore) GetGPUVRAM(ctx context.Context, gpuType string) (int, error) {
	if s == nil || s.rdb == nil {
		return 0, nil
	}

	data, err := s.rdb.HGet(ctx, "gpu_api:gpu_specs", gpuType).Bytes()
	if err == redis.Nil {
		return 0, nil // not found
	}
	if err != nil {
		return 0, fmt.Errorf("reading GPU spec for %s: %w", gpuType, err)
	}

	var spec struct {
		VRAMGB int `json:"vram_gb"`
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return 0, fmt.Errorf("parsing GPU spec for %s: %w", gpuType, err)
	}
	return spec.VRAMGB, nil
}

// --- Model Registry (loaded_models) ---

const (
	loadedModelPrefix  = "loaded_models:"
	modelLoadedChannel = "gpuscale:model_loaded"
	provisionChannel   = "gpuscale:provision"
)

// LoadedModelInfo is the JSON value stored in loaded_models:{model}.
type LoadedModelInfo struct {
	ClaimName  string `json:"claimName"`
	Provider   string `json:"provider"`
	GPUType    string `json:"gpuType"`
	GPUCount   int    `json:"gpuCount"`
	InstanceID string `json:"instanceId"`
	ReadyAt    string `json:"readyAt"`
}

// SetModelLoaded marks a model as loaded in Dragonfly and notifies GPU API.
func (s *DemandStore) SetModelLoaded(ctx context.Context, model string, info LoadedModelInfo) error {
	if s == nil || s.rdb == nil || model == "" {
		return nil
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling loaded model info: %w", err)
	}
	if err := s.rdb.Set(ctx, loadedModelPrefix+model, data, 0).Err(); err != nil {
		return fmt.Errorf("setting loaded_models:%s: %w", model, err)
	}
	// Notify GPU API queue processor to drain queued requests for this model.
	s.rdb.Publish(ctx, modelLoadedChannel, model)
	return nil
}

// RemoveModelLoaded deletes the loaded_models entry for a model.
// Only deletes if no other Ready claims serve this model.
func (s *DemandStore) RemoveModelLoaded(ctx context.Context, model string) error {
	if s == nil || s.rdb == nil || model == "" {
		return nil
	}
	return s.rdb.Del(ctx, loadedModelPrefix+model).Err()
}

// IsModelLoaded checks if a model has a loaded_models key.
func (s *DemandStore) IsModelLoaded(ctx context.Context, model string) bool {
	if s == nil || s.rdb == nil || model == "" {
		return false
	}
	val, err := s.rdb.Exists(ctx, loadedModelPrefix+model).Result()
	return err == nil && val > 0
}

// SubscribeProvisionTrigger subscribes to cold-start provision requests from GPU API.
// Returns a channel that receives model IDs. Caller must cancel ctx to stop.
func (s *DemandStore) SubscribeProvisionTrigger(ctx context.Context) <-chan string {
	ch := make(chan string, 10)
	if s == nil || s.rdb == nil {
		close(ch)
		return ch
	}

	pubsub := s.rdb.Subscribe(ctx, provisionChannel)
	go func() {
		defer close(ch)
		defer pubsub.Close()
		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(1 * time.Second)
				continue
			}
			select {
			case ch <- msg.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// PublishProvisionTrigger publishes a provision request (used by InterruptionController for auto-replace).
func (s *DemandStore) PublishProvisionTrigger(ctx context.Context, model string) error {
	if s == nil || s.rdb == nil {
		return nil
	}
	return s.rdb.Publish(ctx, provisionChannel, model).Err()
}

// Client returns the underlying Redis client (for ProvisionTrigger to reuse).
func (s *DemandStore) Client() *redis.Client {
	if s == nil {
		return nil
	}
	return s.rdb
}

// Close shuts down the Redis connection.
func (s *DemandStore) Close() {
	if s != nil && s.rdb != nil {
		s.rdb.Close()
	}
}
