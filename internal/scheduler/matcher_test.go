package scheduler

import (
	"testing"

	"github.com/munhq/gpuscale/pkg/provider"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func gpuPod(gpuCount string) *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse(gpuCount),
						},
					},
				},
			},
		},
	}
}

func TestIsGPUPod_WithGPULimit(t *testing.T) {
	if !IsGPUPod(gpuPod("1")) {
		t.Error("pod with nvidia.com/gpu=1 should be a GPU pod")
	}
}

func TestIsGPUPod_MultiGPU(t *testing.T) {
	if !IsGPUPod(gpuPod("4")) {
		t.Error("pod with nvidia.com/gpu=4 should be a GPU pod")
	}
}

func TestIsGPUPod_ZeroGPU(t *testing.T) {
	p := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("0"),
						},
					},
				},
			},
		},
	}
	if IsGPUPod(p) {
		t.Error("pod with nvidia.com/gpu=0 should NOT be a GPU pod")
	}
}

func TestIsGPUPod_CPUOnly(t *testing.T) {
	p := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
				},
			},
		},
	}
	if IsGPUPod(p) {
		t.Error("CPU-only pod should not be a GPU pod")
	}
}

func TestIsGPUPod_InitContainer(t *testing.T) {
	p := &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
	if !IsGPUPod(p) {
		t.Error("pod with GPU in init container should be a GPU pod")
	}
}

func TestIsUnschedulable_True(t *testing.T) {
	p := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionFalse,
					Reason: "Unschedulable",
				},
			},
		},
	}
	if !IsUnschedulable(p) {
		t.Error("pod with Unschedulable condition should return true")
	}
}

func TestIsUnschedulable_Scheduled(t *testing.T) {
	p := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if IsUnschedulable(p) {
		t.Error("successfully scheduled pod should return false")
	}
}

func TestIsUnschedulable_NoConditions(t *testing.T) {
	if IsUnschedulable(&corev1.Pod{}) {
		t.Error("pod with no conditions should return false")
	}
}

func TestExtractGPURequirements_Basic(t *testing.T) {
	p := gpuPod("2")
	p.Annotations = map[string]string{
		AnnotationGPUVRAM:  "24",
		AnnotationGPUType:  "A100",
		AnnotationMaxPrice: "3.50",
	}
	req := ExtractGPURequirements(p)

	if req.GPUCount != 2 {
		t.Errorf("GPUCount: want 2, got %d", req.GPUCount)
	}
	if req.MinVRAM != 24 {
		t.Errorf("MinVRAM: want 24, got %d", req.MinVRAM)
	}
	if len(req.GPUTypes) != 1 || req.GPUTypes[0] != "A100" {
		t.Errorf("GPUTypes: want [A100], got %v", req.GPUTypes)
	}
	if req.MaxPricePerHour != 3.50 {
		t.Errorf("MaxPricePerHour: want 3.50, got %f", req.MaxPricePerHour)
	}
	if req.CapacityType != "spot" {
		t.Errorf("default CapacityType: want spot, got %s", req.CapacityType)
	}
}

func TestExtractGPURequirements_OnDemandPriority(t *testing.T) {
	p := gpuPod("1")
	p.Annotations = map[string]string{
		AnnotationPriority: "on-demand",
	}
	req := ExtractGPURequirements(p)
	if req.CapacityType != "on-demand" {
		t.Errorf("on-demand annotation: want on-demand, got %s", req.CapacityType)
	}
}

func TestExtractGPURequirements_NoAnnotations(t *testing.T) {
	req := ExtractGPURequirements(gpuPod("1"))
	if req.GPUCount != 1 {
		t.Errorf("GPUCount with no annotations: want 1, got %d", req.GPUCount)
	}
	if req.CapacityType != "spot" {
		t.Errorf("default CapacityType: want spot, got %s", req.CapacityType)
	}
}

func TestExtractGPURequirements_MultipleContainers(t *testing.T) {
	p := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")},
				}},
				{Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")},
				}},
			},
		},
	}
	req := ExtractGPURequirements(p)
	if req.GPUCount != 4 {
		t.Errorf("multi-container GPU sum: want 4, got %d", req.GPUCount)
	}
}

func TestMergeRequirements_Empty(t *testing.T) {
	req := MergeRequirements(nil)
	if req.GPUCount != 0 {
		t.Errorf("empty merge: want GPUCount=0, got %d", req.GPUCount)
	}
}

func TestMergeRequirements_Single(t *testing.T) {
	in := provider.GPURequirements{GPUCount: 2, MinVRAM: 24, CapacityType: "spot"}
	got := MergeRequirements([]provider.GPURequirements{in})
	if got.GPUCount != 2 || got.MinVRAM != 24 {
		t.Errorf("single passthrough: want {2,24}, got {%d,%d}", got.GPUCount, got.MinVRAM)
	}
}

func TestMergeRequirements_SumsGPUCount(t *testing.T) {
	reqs := []provider.GPURequirements{
		{GPUCount: 2, MinVRAM: 24},
		{GPUCount: 2, MinVRAM: 48},
	}
	got := MergeRequirements(reqs)
	if got.GPUCount != 4 {
		t.Errorf("GPUCount sum: want 4, got %d", got.GPUCount)
	}
	if got.MinVRAM != 48 {
		t.Errorf("MinVRAM max: want 48, got %d", got.MinVRAM)
	}
}

func TestMergeRequirements_OnDemandPropagates(t *testing.T) {
	reqs := []provider.GPURequirements{
		{GPUCount: 1, CapacityType: "spot"},
		{GPUCount: 1, CapacityType: "on-demand"},
	}
	got := MergeRequirements(reqs)
	if got.CapacityType != "on-demand" {
		t.Errorf("on-demand should propagate: got %s", got.CapacityType)
	}
}

func TestMergeRequirements_MaxPriceUsesMinimum(t *testing.T) {
	reqs := []provider.GPURequirements{
		{GPUCount: 1, MaxPricePerHour: 5.0},
		{GPUCount: 1, MaxPricePerHour: 3.0},
	}
	got := MergeRequirements(reqs)
	if got.MaxPricePerHour != 3.0 {
		t.Errorf("MaxPricePerHour should be minimum: want 3.0, got %f", got.MaxPricePerHour)
	}
}

func TestMergeRequirements_GPUTypesDeduped(t *testing.T) {
	reqs := []provider.GPURequirements{
		{GPUTypes: []string{"A100", "H100"}},
		{GPUTypes: []string{"H100", "A100"}},
	}
	got := MergeRequirements(reqs)
	seen := make(map[string]int)
	for _, t := range got.GPUTypes {
		seen[t]++
	}
	for k, v := range seen {
		if v > 1 {
			t.Errorf("GPU type %q appears %d times, want 1 (deduped)", k, v)
		}
	}
}
