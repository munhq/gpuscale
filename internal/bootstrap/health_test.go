package bootstrap

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIsNodeReady(t *testing.T) {
	tests := []struct {
		name     string
		node     *corev1.Node
		expected bool
	}{
		{
			name: "node is ready",
			node: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: true,
		},
		{
			name: "node is not ready",
			node: &corev1.Node{
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name: "node has no conditions",
			node: &corev1.Node{
				Status: corev1.NodeStatus{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNodeReady(tt.node); got != tt.expected {
				t.Errorf("IsNodeReady() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHasGPUCapacity(t *testing.T) {
	tests := []struct {
		name     string
		node     *corev1.Node
		expected bool
	}{
		{
			name: "node has GPU",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "gpu-node"},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						"nvidia.com/gpu": *mustParseQuantity("1"),
					},
				},
			},
			expected: true,
		},
		{
			name: "node has no GPU key",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "cpu-node"},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasGPUCapacity(tt.node); got != tt.expected {
				t.Errorf("HasGPUCapacity() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func mustParseQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}
