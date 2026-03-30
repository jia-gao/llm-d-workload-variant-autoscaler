package fixtures

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EnsureEndpointPickerConfig creates or updates the EndpointPickerConfig for the benchmark
func EnsureEndpointPickerConfig(ctx context.Context, crClient client.Client, namespace, name string) error {
	eppConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			"default-plugins.yaml": `apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
featureGates:
- flowControl
plugins:
- type: queue-scorer
- type: kv-cache-utilization-scorer
- type: prefix-cache-scorer
schedulingProfiles:
- name: default
  plugins:
  - pluginRef: queue-scorer
    weight: 2
  - pluginRef: kv-cache-utilization-scorer
    weight: 2
  - pluginRef: prefix-cache-scorer
    weight: 3`,
		},
	}

	// Try to get existing
	existing := &corev1.ConfigMap{}
	err := crClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if err == nil {
		// Update existing
		eppConfig.SetResourceVersion(existing.GetResourceVersion())
		if err := crClient.Update(ctx, eppConfig); err != nil {
			return fmt.Errorf("failed to update EndpointPickerConfig ConfigMap: %w", err)
		}
		return nil
	}

	// Create new
	if err := crClient.Create(ctx, eppConfig); err != nil {
		return fmt.Errorf("failed to create EndpointPickerConfig ConfigMap: %w", err)
	}

	return nil
}
