package fixtures

import (
	"context"
	"fmt"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

// EnsureHPA creates or replaces the standard WVA HPA (idempotent for test setup).
func EnsureHPA(
	ctx context.Context,
	k8sClient *kubernetes.Clientset,
	namespace, name, deploymentName, vaName string,
	minReplicas, maxReplicas int32,
	behavior *autoscalingv2.HorizontalPodAutoscalerBehavior,
) error {
	hpa := buildHPA(namespace, name, deploymentName, vaName, minReplicas, maxReplicas)
	if behavior != nil {
		hpa.Spec.Behavior = behavior
	}
	return applyHPA(ctx, k8sClient, namespace, hpa)
}

// EnsureStandardHPA creates a standard CPU-based HPA for baseline comparison
func EnsureStandardHPA(
	ctx context.Context,
	k8sClient *kubernetes.Clientset,
	namespace, name, deploymentName string,
	minReplicas, maxReplicas int32,
	scaleUpStabilization, scaleDownStabilization int32,
	scaleUpPolicies, scaleDownPolicies []autoscalingv2.HPAScalingPolicy,
) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-standard-hpa",
			Namespace: namespace,
			Labels:    map[string]string{"test-resource": "true"},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			},
			MinReplicas: ptr.To(minReplicas),
			MaxReplicas: maxReplicas,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr.To(int32(80)), // Standard 80% CPU target
						},
					},
				},
			},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(scaleUpStabilization),
					Policies:                   scaleUpPolicies,
				},
				ScaleDown: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(scaleDownStabilization),
					Policies:                   scaleDownPolicies,
				},
			},
		},
	}
	return applyHPA(ctx, k8sClient, namespace, hpa)
}

func applyHPA(ctx context.Context, k8sClient *kubernetes.Clientset, namespace string, hpa *autoscalingv2.HorizontalPodAutoscaler) error {
	existing, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, hpa.Name, metav1.GetOptions{})
	if err == nil && existing != nil {
		deleteErr := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).Delete(ctx, hpa.Name, metav1.DeleteOptions{})
		if deleteErr != nil && !errors.IsNotFound(deleteErr) {
			return fmt.Errorf("delete existing HPA %s: %w", hpa.Name, deleteErr)
		}
		waitErr := wait.PollUntilContextTimeout(ctx, 1*time.Second, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, hpa.Name, metav1.GetOptions{})
			return errors.IsNotFound(err), nil
		})
		if waitErr != nil {
			return fmt.Errorf("timeout waiting for HPA %s deletion: %w", hpa.Name, waitErr)
		}
	} else if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("check existing HPA %s: %w", hpa.Name, err)
	}
	_, err = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(namespace).Create(ctx, hpa, metav1.CreateOptions{})
	return err
}

func buildHPA(namespace, name, deploymentName, vaName string, minReplicas, maxReplicas int32) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-hpa",
			Namespace: namespace,
			Labels:    map[string]string{"test-resource": "true"},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			},
			MinReplicas: ptr.To(minReplicas),
			MaxReplicas: maxReplicas,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ExternalMetricSourceType,
					External: &autoscalingv2.ExternalMetricSource{
						Metric: autoscalingv2.MetricIdentifier{
							Name: "wva_desired_replicas",
							Selector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"variant_name": vaName},
							},
						},
						Target: autoscalingv2.MetricTarget{
							Type:         autoscalingv2.AverageValueMetricType,
							AverageValue: resource.NewQuantity(1, resource.DecimalSI),
						},
					},
				},
			},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(int32(0)),
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 10, PeriodSeconds: 15}},
				},
				ScaleDown: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(int32(60)),
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 1, PeriodSeconds: 60}},
				},
			},
		},
	}
	if minReplicas == 0 {
		hpa.Annotations = map[string]string{"autoscaling.alpha.kubernetes.io/feature-gates": "HPAScaleToZero=true"}
	}
	return hpa
}
