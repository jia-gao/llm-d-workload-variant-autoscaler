package fixtures

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EnsureEndpointPickerConfig creates or updates the EndpointPickerConfig for the benchmark
func EnsureEndpointPickerConfig(ctx context.Context, crClient client.Client, namespace, name string) error {
	eppConfig := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "inference.networking.x-k8s.io/v1alpha1",
			"kind":       "EndpointPickerConfig",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"featureGates": []interface{}{
					"flowControl",
				},
				"plugins": []interface{}{
					map[string]interface{}{"type": "queue-scorer"},
					map[string]interface{}{"type": "kv-cache-utilization-scorer"},
					map[string]interface{}{"type": "prefix-cache-scorer"},
				},
				"schedulingProfiles": []interface{}{
					map[string]interface{}{
						"name": "default",
						"plugins": []interface{}{
							map[string]interface{}{
								"pluginRef": "queue-scorer",
								"weight":    2,
							},
							map[string]interface{}{
								"pluginRef": "kv-cache-utilization-scorer",
								"weight":    2,
							},
							map[string]interface{}{
								"pluginRef": "prefix-cache-scorer",
								"weight":    3,
							},
						},
					},
				},
			},
		},
	}

	// Try to get existing
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "inference.networking.x-k8s.io",
		Version: "v1alpha1",
		Kind:    "EndpointPickerConfig",
	})
	
	err := crClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if err == nil {
		// Update existing
		eppConfig.SetResourceVersion(existing.GetResourceVersion())
		if err := crClient.Update(ctx, eppConfig); err != nil {
			return fmt.Errorf("failed to update EndpointPickerConfig: %w", err)
		}
		return nil
	}

	// Create new
	if err := crClient.Create(ctx, eppConfig); err != nil {
		return fmt.Errorf("failed to create EndpointPickerConfig: %w", err)
	}

	return nil
}
