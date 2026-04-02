package fixtures

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const eppConfigKey = "config.yaml"

// EnsureEndpointPickerConfig creates or updates the EndpointPickerConfig for the benchmark
func EnsureEndpointPickerConfig(ctx context.Context, crClient client.Client, namespace, name string) error {
	eppConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			eppConfigKey: `apiVersion: inference.networking.x-k8s.io/v1alpha1
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

	existing := &corev1.ConfigMap{}
	err := crClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if err == nil {
		eppConfig.SetResourceVersion(existing.GetResourceVersion())
		if err := crClient.Update(ctx, eppConfig); err != nil {
			return fmt.Errorf("failed to update EndpointPickerConfig ConfigMap: %w", err)
		}
		return nil
	}

	if err := crClient.Create(ctx, eppConfig); err != nil {
		return fmt.Errorf("failed to create EndpointPickerConfig ConfigMap: %w", err)
	}

	return nil
}

// PatchEPPWithConfigFile patches the EPP deployment to mount the EndpointPickerConfig
// ConfigMap and pass --config-file to the container. It reads the existing container
// args and appends the flag so that all original flags (--pool-name, --grpc-port, etc.)
// are preserved. It then waits for the rollout to complete.
func PatchEPPWithConfigFile(ctx context.Context, k8sClient *kubernetes.Clientset, namespace, eppDeploymentName, configMapName string) error {
	dep, err := k8sClient.AppsV1().Deployments(namespace).Get(ctx, eppDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get EPP deployment %s: %w", eppDeploymentName, err)
	}

	const volumeName = "epp-config"
	const mountPath = "/etc/epp"
	configFilePath := mountPath + "/" + eppConfigKey
	configFileArg := "--config-file=" + configFilePath

	// Check if already patched (volume already exists)
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if v.Name == volumeName {
			return nil
		}
	}

	// Add volume
	dep.Spec.Template.Spec.Volumes = append(dep.Spec.Template.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
			},
		},
	})

	// Add volumeMount and --config-file arg to the first container, preserving existing args
	c := &dep.Spec.Template.Spec.Containers[0]
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: mountPath,
		ReadOnly:  true,
	})

	hasArg := false
	for _, a := range c.Args {
		if strings.HasPrefix(a, "--config-file=") {
			hasArg = true
			break
		}
	}
	if !hasArg {
		c.Args = append(c.Args, configFileArg)
	}

	_, err = k8sClient.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update EPP deployment with config-file: %w", err)
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

