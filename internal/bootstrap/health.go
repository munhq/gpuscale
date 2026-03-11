package bootstrap

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeHealthChecker checks whether a node has joined the cluster and is ready.
// Used by the full-node bootstrap path.
type NodeHealthChecker struct {
	client client.Client
}

// NewNodeHealthChecker creates a new health checker.
func NewNodeHealthChecker(c client.Client) *NodeHealthChecker {
	return &NodeHealthChecker{client: c}
}

// WaitForNodeReady polls the K8s API until a node with the given instance ID label
// appears and is in Ready condition, or until the timeout expires.
func (h *NodeHealthChecker) WaitForNodeReady(ctx context.Context, instanceID string, timeout time.Duration) (*corev1.Node, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		node, err := h.findNodeByInstanceID(ctx, instanceID)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		if node == nil {
			time.Sleep(5 * time.Second)
			continue
		}

		if isNodeReady(node) {
			return node, nil
		}
		time.Sleep(5 * time.Second)
	}

	return nil, fmt.Errorf("node with instance-id %s did not become ready within %s", instanceID, timeout)
}

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (h *NodeHealthChecker) findNodeByInstanceID(ctx context.Context, instanceID string) (*corev1.Node, error) {
	var nodeList corev1.NodeList
	if err := h.client.List(ctx, &nodeList, client.MatchingLabels{
		"gpuscale.io/instance-id": instanceID,
	}); err != nil {
		return nil, err
	}
	if len(nodeList.Items) == 0 {
		return nil, nil
	}
	return &nodeList.Items[0], nil
}

