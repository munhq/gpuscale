package v1alpha1

import "testing"

func TestClaimPhaseConstants(t *testing.T) {
	phases := map[GPUNodeClaimPhase]string{
		ClaimPhasePending:       "Pending",
		ClaimPhaseProvisioning:  "Provisioning",
		ClaimPhaseBootstrapping: "Bootstrapping",
		ClaimPhaseReady:         "Ready",
		ClaimPhaseDraining:      "Draining",
		ClaimPhaseTerminated:    "Terminated",
	}

	for phase, expected := range phases {
		if string(phase) != expected {
			t.Errorf("phase %v = %q, want %q", phase, string(phase), expected)
		}
	}
}

func TestModelConfigDefaults(t *testing.T) {
	mc := ModelConfig{}
	if mc.DType != "" {
		t.Errorf("expected empty dtype default, got %q", mc.DType)
	}
	if mc.GPUMemoryUtilization != 0 {
		t.Errorf("expected zero GPUMemoryUtilization, got %f", mc.GPUMemoryUtilization)
	}
	if mc.EnablePrefixCaching != false {
		t.Error("expected false EnablePrefixCaching by default")
	}
}

func TestProviderConfigDefaultNodeType(t *testing.T) {
	pc := ProviderConfig{}
	// NodeType zero value is empty string; code defaults to "ray-worker" at runtime
	if pc.NodeType != "" {
		t.Errorf("expected empty NodeType zero value, got %q", pc.NodeType)
	}
}
