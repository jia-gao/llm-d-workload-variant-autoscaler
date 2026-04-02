package fixtures

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// EndpointPickerConfigYAML is the full config text passed via --config-text.
// It enables flowControl and sets scorer weights: queue=2, kv-cache=2, prefix-cache=3.
const EndpointPickerConfigYAML = `apiVersion: inference.networking.x-k8s.io/v1alpha1
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
    weight: 3`

// PatchEPPWithConfigText patches the EPP deployment to use --config-text with
// the EndpointPickerConfig YAML inline. It also removes the deprecated
// ENABLE_EXPERIMENTAL_FLOW_CONTROL_LAYER env var to avoid conflicts (the
// config-text featureGates supersede it). This approach avoids ConfigMap
// volume mounts which simplifies the patch. Waits for the rollout to complete.
func PatchEPPWithConfigText(ctx context.Context, k8sClient *kubernetes.Clientset, namespace, eppDeploymentName string) error {
	dep, err := k8sClient.AppsV1().Deployments(namespace).Get(ctx, eppDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get EPP deployment %s: %w", eppDeploymentName, err)
	}

	c := &dep.Spec.Template.Spec.Containers[0]

	// Check if already patched
	for _, a := range c.Args {
		if strings.HasPrefix(a, "--config-text=") {
			return nil
		}
	}

	// Remove the ENABLE_EXPERIMENTAL_FLOW_CONTROL_LAYER env var — the config
	// text's featureGates: [flowControl] supersedes it and having both causes
	// the EPP to malfunction on v0.5.0-rc.1.
	filtered := make([]corev1.EnvVar, 0, len(c.Env))
	for _, e := range c.Env {
		if e.Name != "ENABLE_EXPERIMENTAL_FLOW_CONTROL_LAYER" {
			filtered = append(filtered, e)
		}
	}
	c.Env = filtered

	// Append --config-text with inline YAML (preserves all existing args)
	c.Args = append(c.Args, "--config-text="+EndpointPickerConfigYAML)

	_, err = k8sClient.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update EPP deployment with config-text: %w", err)
	}

	// Wait for rollout: new pods ready
	deadline := time.After(5 * time.Minute)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timed out waiting for EPP deployment %s to roll out", eppDeploymentName)
		case <-tick.C:
			d, getErr := k8sClient.AppsV1().Deployments(namespace).Get(ctx, eppDeploymentName, metav1.GetOptions{})
			if getErr != nil {
				continue
			}
			if d.Status.UpdatedReplicas > 0 && d.Status.ReadyReplicas == d.Status.UpdatedReplicas &&
				d.Status.UnavailableReplicas == 0 {
				return nil
			}
		}
	}
}

// FindEPPDeployment discovers the EPP deployment by looking for deployments
// containing "epp" in their name.
func FindEPPDeployment(ctx context.Context, k8sClient *kubernetes.Clientset, namespace string) (string, error) {
	deps, err := k8sClient.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list deployments: %w", err)
	}
	for i := range deps.Items {
		name := deps.Items[i].Name
		if containsAny(name, "epp", "inference-scheduler") {
			return name, nil
		}
	}
	return "", fmt.Errorf("no EPP deployment found in namespace %s", namespace)
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

