package bootstrap

import (
	"strings"
	"testing"

	"github.com/munhq/gpuscale/pkg/provider"
)

func TestAgentURLRendersFromAPIBase(t *testing.T) {
	s := GenerateScript(provider.BootstrapConfig{
		InstanceID: "n1", GPUType: "A100", ProviderName: "runpod",
		GPUAPIURL: "wss://ai.hotmun.com", GPUAPIToken: "tok123", GPUCount: 1,
	})
	if !strings.Contains(s, `AGENT_URL="${GPU_API%/internal/bootstrap-event}/internal/agent/linux-amd64"`) {
		t.Fatalf("agent URL not derived from GPU_API:\n%s", s)
	}
	if strings.Contains(s, "github.com") {
		t.Fatalf("script still references github.com")
	}
}
