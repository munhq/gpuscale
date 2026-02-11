package bootstrap

import (
	"strings"
	"testing"

	"github.com/munhq/gpuscale/internal/provider"
)

func TestGenerateRayWorkerScriptDefaults(t *testing.T) {
	config := provider.BootstrapConfig{
		ProviderName: "vast.ai",
		InstanceID:   "inst-123",
		GPUType:      "RTX 4090",
		RayHeadAddr:  "10.0.40.1",
	}

	script := GenerateRayWorkerScript(config)

	// Should join Ray cluster at head address
	if !strings.Contains(script, "ray start --address='10.0.40.1:6379'") {
		t.Error("expected ray start --address with head address")
	}
	if !strings.Contains(script, "--num-gpus=1") {
		t.Error("expected default 1 GPU")
	}
	if !strings.Contains(script, "--block") {
		t.Error("expected --block flag to keep worker running")
	}
	if !strings.Contains(script, "gpuscale.io/provider") {
		t.Error("expected provider label")
	}
	if !strings.Contains(script, "inst-123") {
		t.Error("expected instance ID in labels")
	}
}

func TestGenerateRayWorkerScriptCustomGPUs(t *testing.T) {
	config := provider.BootstrapConfig{
		ProviderName: "verda",
		InstanceID:   "inst-456",
		GPUType:      "A100",
		RayHeadAddr:  "10.0.40.1",
		ExtraEnv: map[string]string{
			"NUM_GPUS": "4",
		},
	}

	script := GenerateRayWorkerScript(config)

	if !strings.Contains(script, "--num-gpus=4") {
		t.Error("expected 4 GPUs from ExtraEnv")
	}
	if !strings.Contains(script, "A100") {
		t.Error("expected GPU type in labels")
	}
}

func TestGenerateRayWorkerScriptFallbackHead(t *testing.T) {
	// When RayHeadAddr is empty, falls back to localhost
	config := provider.BootstrapConfig{
		ProviderName: "vast.ai",
		InstanceID:   "inst-789",
		GPUType:      "RTX 3090",
	}

	script := GenerateRayWorkerScript(config)

	if !strings.Contains(script, "ray start --address='localhost:6379'") {
		t.Error("expected fallback to localhost when RayHeadAddr is empty")
	}
}

func TestGenerateRayWorkerScriptModelCache(t *testing.T) {
	config := provider.BootstrapConfig{
		RayHeadAddr:   "10.0.40.1",
		ModelCacheURL: "s3:my-bucket/models",
	}
	script := GenerateRayWorkerScript(config)
	if !strings.Contains(script, "rclone sync") {
		t.Error("expected rclone sync for model cache")
	}
	if !strings.Contains(script, "s3:my-bucket/models") {
		t.Error("expected model cache URL in script")
	}
}

func TestGenerateRayWorkerScriptNoCacheWithoutURL(t *testing.T) {
	config := provider.BootstrapConfig{
		RayHeadAddr: "10.0.40.1",
	}
	script := GenerateRayWorkerScript(config)
	if strings.Contains(script, "rclone") {
		t.Error("should not contain rclone when no cache URL set")
	}
}

func TestGenerateRayWorkerScriptWaitsForHead(t *testing.T) {
	config := provider.BootstrapConfig{
		RayHeadAddr: "10.0.40.1",
	}
	script := GenerateRayWorkerScript(config)
	if !strings.Contains(script, "Waiting for Ray head") {
		t.Error("expected connectivity check before joining cluster")
	}
}
