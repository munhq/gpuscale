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

func TestProviderConfigDefaultNodeType(t *testing.T) {
	pc := ProviderConfig{}
	// NodeType zero value is empty string; code defaults to "standalone" at runtime
	if pc.NodeType != "" {
		t.Errorf("expected empty NodeType zero value, got %q", pc.NodeType)
	}
}
