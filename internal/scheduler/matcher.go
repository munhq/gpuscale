package scheduler

import (
	"strconv"

	"github.com/munhq/gpuscale/internal/provider"
	corev1 "k8s.io/api/core/v1"
)

const (
	// Annotation keys for GPU requirements.
	AnnotationGPUVRAM    = "gpuscale.io/gpu-vram"
	AnnotationGPUType    = "gpuscale.io/gpu-type"
	AnnotationMaxPrice   = "gpuscale.io/max-price"
	AnnotationProvider   = "gpuscale.io/provider"
	AnnotationPriority   = "gpuscale.io/priority"

	// Label keys for managed nodes.
	LabelManaged    = "gpuscale.io/managed"
	LabelProvider   = "gpuscale.io/provider"
	LabelGPUType    = "gpuscale.io/gpu-type"
	LabelInstanceID = "gpuscale.io/instance-id"
)

// IsGPUPod returns true if the pod requests nvidia.com/gpu resources.
func IsGPUPod(pod *corev1.Pod) bool {
	for i := range pod.Spec.Containers {
		if q, ok := pod.Spec.Containers[i].Resources.Limits["nvidia.com/gpu"]; ok && !q.IsZero() {
			return true
		}
	}
	for i := range pod.Spec.InitContainers {
		if q, ok := pod.Spec.InitContainers[i].Resources.Limits["nvidia.com/gpu"]; ok && !q.IsZero() {
			return true
		}
	}
	return false
}

// IsUnschedulable returns true if the pod has a PodScheduled=False condition with reason Unschedulable.
func IsUnschedulable(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled &&
			cond.Status == corev1.ConditionFalse &&
			cond.Reason == "Unschedulable" {
			return true
		}
	}
	return false
}

// ExtractGPURequirements extracts GPU requirements from a pod spec and annotations.
func ExtractGPURequirements(pod *corev1.Pod) provider.GPURequirements {
	req := provider.GPURequirements{
		CapacityType: "spot", // default to spot
	}

	// Extract GPU count from resource limits
	for i := range pod.Spec.Containers {
		if q, ok := pod.Spec.Containers[i].Resources.Limits["nvidia.com/gpu"]; ok {
			req.GPUCount += int(q.Value())
		}
	}

	annotations := pod.Annotations
	if annotations == nil {
		return req
	}

	// Parse annotations
	if vram, ok := annotations[AnnotationGPUVRAM]; ok {
		if v, err := strconv.Atoi(vram); err == nil {
			req.MinVRAM = v
		}
	}
	if gpuType, ok := annotations[AnnotationGPUType]; ok {
		req.GPUTypes = []string{gpuType}
	}
	if maxPrice, ok := annotations[AnnotationMaxPrice]; ok {
		if p, err := strconv.ParseFloat(maxPrice, 64); err == nil {
			req.MaxPrice = p
		}
	}
	if priority, ok := annotations[AnnotationPriority]; ok {
		if priority == "on-demand" {
			req.CapacityType = "on-demand"
		}
	}

	return req
}

// MergeRequirements merges multiple GPU requirements into one that satisfies all.
// Used when batching multiple pending pods into a single node provision.
func MergeRequirements(reqs []provider.GPURequirements) provider.GPURequirements {
	if len(reqs) == 0 {
		return provider.GPURequirements{}
	}
	if len(reqs) == 1 {
		return reqs[0]
	}

	merged := provider.GPURequirements{
		CapacityType: "spot",
	}

	for _, r := range reqs {
		merged.GPUCount += r.GPUCount
		if r.MinVRAM > merged.MinVRAM {
			merged.MinVRAM = r.MinVRAM
		}
		if r.MinDisk > merged.MinDisk {
			merged.MinDisk = r.MinDisk
		}
		if r.MinRAM > merged.MinRAM {
			merged.MinRAM = r.MinRAM
		}
		// If any pod requires on-demand, use on-demand
		if r.CapacityType == "on-demand" {
			merged.CapacityType = "on-demand"
		}
		// Use the lowest max price (most restrictive)
		if r.MaxPrice > 0 {
			if merged.MaxPrice == 0 || r.MaxPrice < merged.MaxPrice {
				merged.MaxPrice = r.MaxPrice
			}
		}
		// Collect all GPU types
		merged.GPUTypes = append(merged.GPUTypes, r.GPUTypes...)
	}

	// Deduplicate GPU types
	if len(merged.GPUTypes) > 0 {
		seen := make(map[string]bool)
		unique := make([]string, 0, len(merged.GPUTypes))
		for _, t := range merged.GPUTypes {
			if !seen[t] {
				seen[t] = true
				unique = append(unique, t)
			}
		}
		merged.GPUTypes = unique
	}

	return merged
}
