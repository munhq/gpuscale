package bootstrap

import (
	"testing"

	"github.com/munhq/gpuscale/pkg/provider"
)

func TestGenerateEnvVars(t *testing.T) {
	config := provider.BootstrapConfig{
		GPUType:       "RTX 4090",
		ProviderName:  "vast.ai",
		InstanceID:    "inst-abc",
		ModelID:       "test-model",
		ModelCacheURL: "s3:bucket/models",
	}

	env := GenerateEnvVars(config)

	expected := map[string]string{
		"GPU_TYPE":        "RTX 4090",
		"PROVIDER":        "vast.ai",
		"INSTANCE_ID":     "inst-abc",
		"MODEL_ID":        "test-model",
		"MODEL_CACHE_URL": "s3:bucket/models",
		"NODE_TYPE":       "standalone",
	}

	for k, v := range expected {
		if env[k] != v {
			t.Errorf("env[%q] = %q, want %q", k, env[k], v)
		}
	}
}

func TestGenerateEnvVarsNoOptional(t *testing.T) {
	config := provider.BootstrapConfig{
		GPUType:      "A100",
		ProviderName: "verda",
		InstanceID:   "inst-xyz",
	}

	env := GenerateEnvVars(config)

	if _, ok := env["MODEL_ID"]; ok {
		t.Error("MODEL_ID should not be set when empty")
	}
	if _, ok := env["MODEL_CACHE_URL"]; ok {
		t.Error("MODEL_CACHE_URL should not be set when empty")
	}
}

func TestGenerateEnvVarsExtraEnv(t *testing.T) {
	config := provider.BootstrapConfig{
		GPUType:      "RTX 4090",
		ProviderName: "vast.ai",
		InstanceID:   "inst-1",
		ExtraEnv: map[string]string{
			"CUSTOM_VAR": "custom_value",
		},
	}

	env := GenerateEnvVars(config)
	if env["CUSTOM_VAR"] != "custom_value" {
		t.Errorf("expected CUSTOM_VAR=custom_value, got %q", env["CUSTOM_VAR"])
	}
}

func TestGenerateScriptContainsGPUAgent(t *testing.T) {
	config := provider.BootstrapConfig{
		NodeType:     "standalone",
		InstanceID:   "claim-abc123",
		GPUAPIURL:    "wss://ai.example.com",
		GPUAPIToken:  "tok-test",
		Models:       "qwen3-7b:8000",
		ModelSources: "Qwen/Qwen3-7B",
		GPUCount:     1,
		GPUType:      "RTX 4090",
		ProviderName: "vast.ai",
	}

	script := GenerateScript(config)

	checks := []string{
		"gpu-agent",
		"GPU_API_URL=",
		"NODE_ID=",
		"AUTH_TOKEN=",
		"MODELS=",
		"claim-abc123",
		"wss://ai.example.com",
		"nvidia-smi",
	}
	for _, want := range checks {
		if !containsStr(script, want) {
			t.Errorf("script missing %q", want)
		}
	}
}

func TestGenerateScriptWSSToHTTPS(t *testing.T) {
	config := provider.BootstrapConfig{
		InstanceID:   "claim-xyz",
		GPUAPIURL:    "wss://ai.example.com",
		GPUAPIToken:  "tok",
		Models:       "model-a:8000",
		ModelSources: "org/model-a",
		GPUCount:     1,
		GPUType:      "A100",
		ProviderName: "verda",
	}
	script := GenerateScript(config)
	// Bootstrap-event URL should use https, not wss
	if containsStr(script, "wss://ai.example.com/internal/bootstrap-event") {
		t.Error("script should use https:// for bootstrap-event, not wss://")
	}
	if !containsStr(script, "https://ai.example.com/internal/bootstrap-event") {
		t.Error("script should contain https:// bootstrap-event URL")
	}
}

func TestSanitizeLabel(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"RTX 4090", "RTX-4090"},
		{"A100 80GB", "A100-80GB"},
		{"simple", "simple"},
		{"with.dots", "with.dots"},
		{"with_underscores", "with_underscores"},
		{"special!@#chars", "specialchars"},
	}

	for _, tt := range tests {
		got := SanitizeLabel(tt.input)
		if got != tt.expected {
			t.Errorf("SanitizeLabel(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestSanitizeLabelMaxLength(t *testing.T) {
	long := "abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"
	got := SanitizeLabel(long)
	if len(got) > 63 {
		t.Errorf("label too long: %d chars", len(got))
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}
