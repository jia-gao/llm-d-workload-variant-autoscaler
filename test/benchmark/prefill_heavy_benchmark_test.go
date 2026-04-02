package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/common/model"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	variantautoscalingv1alpha1 "github.com/llm-d/llm-d-workload-variant-autoscaler/api/v1alpha1"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/test/e2e/fixtures"
)

// PrefillResult holds results for one prefill benchmark run (HPA or WVA).
type PrefillResult struct {
	AutoscalerType   string          `json:"autoscaler_type"`
	ReplicaTimeline  []ReplicaSnap   `json:"replica_timeline"`
	MetricsTimeline  []MetricSnap    `json:"metrics_timeline"`
	AvgReplicas      float64         `json:"avg_replicas"`
	MaxReplicas      int32           `json:"max_replicas"`
	AvgQueueDepth    float64         `json:"avg_queue_depth"`
	AvgEPPQueueDepth float64         `json:"avg_epp_queue_depth"`
	AvgKVCache       float64         `json:"avg_kv_cache"`
	TTFT             json.RawMessage `json:"ttft,omitempty"`
	ITL              json.RawMessage `json:"itl,omitempty"`
	Throughput       json.RawMessage `json:"throughput,omitempty"`
	GuideLLMRaw      json.RawMessage `json:"guidellm_raw,omitempty"`
	DurationSec      float64         `json:"duration_sec"`
}

// ReplicaSnap records replica count at a point in time.
type ReplicaSnap struct {
	ElapsedSec    float64 `json:"elapsed_sec"`
	SpecReplicas  int32   `json:"spec_replicas"`
	ReadyReplicas int32   `json:"ready_replicas"`
}

// MetricSnap records KV cache and queue depth at a point in time.
type MetricSnap struct {
	ElapsedSec    float64 `json:"elapsed_sec"`
	QueueDepth    float64 `json:"queue_depth"`
	EPPQueueDepth float64 `json:"epp_queue_depth"`
	KVCache       float64 `json:"kv_cache"`
}

var prefillResults []PrefillResult

const prefillResultsFile = "/tmp/prefill-benchmark-results.json"

var _ = Describe("Prefill Heavy Workload Benchmark", Label("benchmark", "phase4"), func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		res    ScenarioResources
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		res = ScenarioResources{
			PoolName:     benchCfg.PoolName,
			ModelService: "prefill-ms",
			VAName:       "prefill-va",
			HPAName:      "prefill-hpa",
			JobBaseName:  "prefill-ms",
		}
	})

	AfterEach(func() {
		cancel()
	})

	// cleanupAutoscalers removes leftover HPAs and VAs from previous tests to avoid conflicts.
	cleanupAutoscalers := func() {
		GinkgoWriter.Println("Cleaning up existing autoscalers...")
		_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(benchCfg.LLMDNamespace).Delete(ctx, res.HPAName+"-standard-hpa", metav1.DeleteOptions{})
		_ = k8sClient.AutoscalingV2().HorizontalPodAutoscalers(benchCfg.LLMDNamespace).Delete(ctx, res.HPAName+"-hpa", metav1.DeleteOptions{})
		_ = fixtures.DeleteVariantAutoscaling(ctx, crClient, benchCfg.LLMDNamespace, res.VAName)
		time.Sleep(3 * time.Second)
	}

	// findInfraDecodeDeployment discovers the Helm-deployed decode deployment.
	// We reuse this deployment instead of creating a new one because the Gateway/EPP
	// routing is configured to match its labels (InferencePool selector).
	findInfraDecodeDeployment := func() string {
		By("Finding Helm-deployed decode deployment for Gateway-compatible routing")
		deployments, err := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred(), "Failed to list deployments")
		for i := range deployments.Items {
			d := &deployments.Items[i]
			if strings.HasSuffix(d.Name, "-decode") && strings.Contains(d.Name, "modelservice") {
				GinkgoWriter.Printf("  Found infra decode deployment: %s\n", d.Name)
				return d.Name
			}
		}
		Fail("No Helm-deployed decode deployment found in namespace " + benchCfg.LLMDNamespace)
		return ""
	}

	// ensureInfraDeploymentReady scales the Helm-deployed model service to 1 replica and waits for readiness.
	ensureInfraDeploymentReady := func() {
		By("Ensuring infra decode deployment is scaled to 1 and ready")
		one := int32(1)
		deployment, err := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 {
			deployment.Spec.Replicas = &one
			_, err = k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Update(ctx, deployment, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred(), "Failed to scale infra deployment to 1")
			GinkgoWriter.Printf("  Scaled %s to 1 replica\n", res.DeploymentName)
		}

		By("Waiting for infra deployment to have at least 1 ready replica")
		Eventually(func(g Gomega) {
			d, getErr := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
			g.Expect(getErr).NotTo(HaveOccurred())
			spec := int32(0)
			if d.Spec.Replicas != nil {
				spec = *d.Spec.Replicas
			}
			GinkgoWriter.Printf("  %s: spec=%d, ready=%d\n", res.DeploymentName, spec, d.Status.ReadyReplicas)
			g.Expect(d.Status.ReadyReplicas).To(BeNumerically(">=", 1), "Deployment should have at least 1 ready replica")
		}, 15*time.Minute, 10*time.Second).Should(Succeed())
	}

	// waitForVAAndMetrics waits for the VA to stabilize, external metrics to be available,
	// and Prometheus to scrape vLLM metrics. This is essential for WVA to be able to scale.
	waitForVAAndMetrics := func() {
		By("Waiting for VA to stabilize (NumReplicas set)")
		Eventually(func(g Gomega) {
			currentVA := &variantautoscalingv1alpha1.VariantAutoscaling{}
			err := crClient.Get(ctx, client.ObjectKey{
				Namespace: benchCfg.LLMDNamespace,
				Name:      res.VAName,
			}, currentVA)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(currentVA.Status.DesiredOptimizedAlloc.NumReplicas).NotTo(BeNil(), "NumReplicas should be set")
			g.Expect(*currentVA.Status.DesiredOptimizedAlloc.NumReplicas).To(BeNumerically(">=", 1), "VA should have optimized >= 1")
			GinkgoWriter.Printf("VA status: desired replicas = %d\n", *currentVA.Status.DesiredOptimizedAlloc.NumReplicas)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("Verifying external metrics API serves wva_desired_replicas")
		Eventually(func(g Gomega) {
			result, err := k8sClient.RESTClient().
				Get().
				AbsPath("/apis/external.metrics.k8s.io/v1beta1/namespaces/" + benchCfg.LLMDNamespace + "/wva_desired_replicas").
				DoRaw(ctx)
			g.Expect(err).NotTo(HaveOccurred(), "External metrics API should be accessible")
			g.Expect(string(result)).To(ContainSubstring("wva_desired_replicas"), "Metric should be available")
			g.Expect(string(result)).To(ContainSubstring(res.VAName), "Metric should reference the benchmark VA")
			GinkgoWriter.Printf("External metrics API confirmed: wva_desired_replicas available for %s\n", res.VAName)
		}, 5*time.Minute, 10*time.Second).Should(Succeed())

		By("Waiting for Prometheus to scrape vLLM metrics")
		Eventually(func(g Gomega) {
			_, err := promClient.QueryWithRetry(ctx, `vllm:kv_cache_usage_perc`)
			g.Expect(err).NotTo(HaveOccurred(), "Prometheus should have KV cache metrics from vLLM")
			GinkgoWriter.Println("Prometheus confirmed: vllm:kv_cache_usage_perc is available")
		}, 5*time.Minute, 15*time.Second).Should(Succeed())
	}

	// dumpInfrastructureDiagnostics captures EPP, InferencePool, InferenceModel, HTTPRoute
	// state for debugging Gateway 500 errors.
	dumpInfrastructureDiagnostics := func() {
		By("Dumping infrastructure diagnostics for Gateway debugging")

		GinkgoWriter.Println("--- EPP Pod Status ---")
		pods, err := k8sClient.CoreV1().Pods(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		if err == nil {
			for i := range pods.Items {
				p := &pods.Items[i]
				if strings.Contains(p.Name, "epp") || strings.Contains(p.Name, "inference-scheduler") {
					phase := string(p.Status.Phase)
					ready := false
					for _, c := range p.Status.ContainerStatuses {
						if c.Ready {
							ready = true
						}
					}
					GinkgoWriter.Printf("  %s: phase=%s ready=%v restarts=%d\n", p.Name, phase, ready, func() int32 {
						for _, c := range p.Status.ContainerStatuses {
							return c.RestartCount
						}
						return 0
					}())
				}
			}
		}

		GinkgoWriter.Println("--- All Services (port 8000 or gateway) ---")
		svcs, svcErr := k8sClient.CoreV1().Services(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		if svcErr == nil {
			for i := range svcs.Items {
				s := &svcs.Items[i]
				for _, port := range s.Spec.Ports {
					if port.Port == 8000 || port.Port == 80 || strings.Contains(s.Name, "gateway") || strings.Contains(s.Name, "epp") {
						GinkgoWriter.Printf("  svc/%s  type=%s  ports=%d→%s  selector=%v\n",
							s.Name, s.Spec.Type, port.Port, port.TargetPort.String(), s.Spec.Selector)
						break
					}
				}
			}
		}

		GinkgoWriter.Println("--- All Deployments ---")
		deps, depErr := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		if depErr == nil {
			for i := range deps.Items {
				d := &deps.Items[i]
				spec := int32(0)
				if d.Spec.Replicas != nil {
					spec = *d.Spec.Replicas
				}
				GinkgoWriter.Printf("  deploy/%s  spec=%d  ready=%d  selector=%v\n",
					d.Name, spec, d.Status.ReadyReplicas, d.Spec.Selector.MatchLabels)
			}
		}
		GinkgoWriter.Println("--- End Diagnostics ---")
	}

	// ensureEPPConfig is available but NOT called during benchmarks.
	// The deploy script already enables flow control via env var. Patching
	// the EPP with --config-file on v0.5.0-rc.1 causes Gateway routing to
	// break (HTTP 500). Uncomment the call in runPrefillBenchmark when the
	// EPP image supports --config-file alongside the env var.
	_ = func() {
		const configMapName = "benchmark-epp-config"

		By("Creating EndpointPickerConfig ConfigMap")
		err := fixtures.EnsureEndpointPickerConfig(ctx, crClient, benchCfg.LLMDNamespace, configMapName)
		Expect(err).NotTo(HaveOccurred(), "Failed to create EndpointPickerConfig ConfigMap")

		By("Discovering EPP deployment")
		eppDeployName, findErr := fixtures.FindEPPDeployment(ctx, k8sClient, benchCfg.LLMDNamespace)
		Expect(findErr).NotTo(HaveOccurred(), "Failed to find EPP deployment")
		GinkgoWriter.Printf("  Found EPP deployment: %s\n", eppDeployName)

		By("Patching EPP deployment with --config-file volume mount")
		patchErr := fixtures.PatchEPPWithConfigFile(ctx, k8sClient, benchCfg.LLMDNamespace, eppDeployName, configMapName)
		Expect(patchErr).NotTo(HaveOccurred(), "Failed to patch EPP deployment with config-file")
		GinkgoWriter.Println("  EPP deployment patched and rolled out successfully")
	}

	// ensureDirectModelService creates a ClusterIP service that targets the
	// Helm-deployed model server pods directly on port 8000, bypassing the Gateway/EPP.
	ensureDirectModelService := func() string {
		svcName := "prefill-direct-vllm"
		By("Ensuring direct model server service for Gateway bypass")

		deployment, dErr := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
		Expect(dErr).NotTo(HaveOccurred(), "Failed to get deployment for label discovery")
		selector := deployment.Spec.Selector.MatchLabels
		GinkgoWriter.Printf("  Using selector from deployment: %v\n", selector)

		_ = k8sClient.CoreV1().Services(benchCfg.LLMDNamespace).Delete(ctx, svcName, metav1.DeleteOptions{})
		time.Sleep(time.Second)

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: benchCfg.LLMDNamespace,
				Labels:    map[string]string{"test-resource": "true"},
			},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: selector,
				Ports: []corev1.ServicePort{{
					Name:     "http",
					Port:     8000,
					Protocol: corev1.ProtocolTCP,
				}},
			},
		}
		_, createErr := k8sClient.CoreV1().Services(benchCfg.LLMDNamespace).Create(ctx, svc, metav1.CreateOptions{})
		Expect(createErr).NotTo(HaveOccurred(), "Failed to create direct model server service")

		directURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:8000", svcName, benchCfg.LLMDNamespace)
		GinkgoWriter.Printf("  Direct model server URL: %s\n", directURL)
		return directURL
	}

	runPrefillBenchmark := func(autoscalerType string) {
		ensureInfraDeploymentReady()
		// NOTE: We intentionally do NOT call ensureEPPConfig() here.
		// The deploy script (install.sh) already enables flow control via
		// ENABLE_EXPERIMENTAL_FLOW_CONTROL_LAYER=true env var on the EPP.
		// Patching the EPP with --config-file causes it to restart and break
		// Gateway routing (HTTP 500). The scorer weights use defaults.
		dumpInfrastructureDiagnostics()

		gatewayURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d",
			benchCfg.GatewayServiceName, benchCfg.LLMDNamespace, benchCfg.GatewayServicePort)

		By("Verifying Gateway connectivity (prefer Gateway for EPP queue metrics)")
		gwErr := fixtures.VerifyGatewayConnectivity(ctx, k8sClient, benchCfg.LLMDNamespace, gatewayURL, benchCfg.ModelID)

		var targetURL string
		if gwErr != nil {
			GinkgoWriter.Printf("WARNING: Gateway connectivity check failed: %v\n", gwErr)
			GinkgoWriter.Println("WARNING: Falling back to direct model server connection (bypassing Gateway/EPP)")
			GinkgoWriter.Println("WARNING: EPP queue depth metrics will show 0 since traffic does not flow through EPP")
			targetURL = ensureDirectModelService()

			By("Verifying direct model server connectivity (with retries)")
			Eventually(func(g Gomega) {
				directErr := fixtures.VerifyGatewayConnectivity(ctx, k8sClient, benchCfg.LLMDNamespace, targetURL, benchCfg.ModelID)
				g.Expect(directErr).NotTo(HaveOccurred(), "Direct model server not yet reachable")
			}, 3*time.Minute, 20*time.Second).Should(Succeed(), "Direct model server connectivity check failed after retries — backend is truly unreachable")
		} else {
			GinkgoWriter.Println("Gateway connectivity check passed — using Gateway URL (EPP queue metrics will be captured)")
			targetURL = gatewayURL
		}
		GinkgoWriter.Printf("  Using target URL: %s\n", targetURL)

		By("Checking Prometheus metric availability before load")
		for _, q := range []string{
			fmt.Sprintf(`vllm:kv_cache_usage_perc{namespace="%s"}`, benchCfg.LLMDNamespace),
			fmt.Sprintf(`vllm:num_requests_waiting{namespace="%s"}`, benchCfg.LLMDNamespace),
			fmt.Sprintf(`inference_extension_flow_control_queue_size{namespace="%s"}`, benchCfg.LLMDNamespace),
			fmt.Sprintf(`kube_deployment_status_replicas{deployment="%s",namespace="%s"}`, res.DeploymentName, benchCfg.LLMDNamespace),
		} {
			val, err := QueryRangeAvg(promClient.API(), q, time.Now().Add(-2*time.Minute), time.Now(), 30*time.Second)
			if err != nil {
				GinkgoWriter.Printf("  Metric check: %s → NOT FOUND (%v)\n", q, err)
			} else {
				GinkgoWriter.Printf("  Metric check: %s → %.4f\n", q, val)
			}
		}

		By("Checking HPA status before load")
		hpaList, err := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		if err == nil {
			for i := range hpaList.Items {
				hpa := &hpaList.Items[i]
				GinkgoWriter.Printf("  HPA %s: currentReplicas=%d desiredReplicas=%d\n", hpa.Name, hpa.Status.CurrentReplicas, hpa.Status.DesiredReplicas)
				for _, cond := range hpa.Status.Conditions {
					GinkgoWriter.Printf("    condition %s: %s (%s)\n", cond.Type, cond.Status, cond.Message)
				}
			}
		}

		By("Launching GuideLLM Load Generator")

		err = fixtures.CreateGuideLLMJobWithArgs(
			ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService,
			targetURL, benchCfg.ModelID,
		)
		Expect(err).NotTo(HaveOccurred(), "Failed to create GuideLLM load job")

		loadStart := time.Now()
		jobName := res.ModelService + "-load"

		By("Monitoring replicas and HPA status while GuideLLM runs (~10 min)")
		var timeline []ReplicaSnap
		var metricsTimeline []MetricSnap
		var maxReplicas int32 = 1
		done := make(chan error, 1)

		go func() {
			done <- fixtures.WaitForJobCompletion(ctx, k8sClient, benchCfg.LLMDNamespace, jobName, 15*time.Minute)
		}()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

	monitorLoop:
		for {
			select {
			case jobErr := <-done:
				if jobErr != nil {
					logs, logErr := fixtures.GetJobPodLogs(ctx, k8sClient, benchCfg.LLMDNamespace, jobName)
					if logErr == nil {
						GinkgoWriter.Printf("\n--- GuideLLM Job Failed. Pod Logs ---\n%s\n---------------------------\n", logs)
					}
				}
				Expect(jobErr).NotTo(HaveOccurred(), "GuideLLM job failed or timed out")
				break monitorLoop
			case <-ticker.C:
				elapsed := time.Since(loadStart).Seconds()
				deployment, depErr := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
				if depErr == nil {
					spec := *deployment.Spec.Replicas
					ready := deployment.Status.ReadyReplicas
					if spec > maxReplicas {
						maxReplicas = spec
					}
					timeline = append(timeline, ReplicaSnap{ElapsedSec: elapsed, SpecReplicas: spec, ReadyReplicas: ready})
					GinkgoWriter.Printf("  [%.0fs] replicas: spec=%d ready=%d\n", elapsed, spec, ready)
				}

				// Sample KV cache, vLLM queue depth, and EPP queue depth from Prometheus
				qdQuery := fmt.Sprintf(`avg(vllm:num_requests_waiting{namespace="%s"})`, benchCfg.LLMDNamespace)
				kvQuery := fmt.Sprintf(`avg(vllm:kv_cache_usage_perc{namespace="%s"})`, benchCfg.LLMDNamespace)
				eppQDQuery := fmt.Sprintf(`sum(inference_extension_flow_control_queue_size{namespace="%s"})`, benchCfg.LLMDNamespace)
				snap := MetricSnap{ElapsedSec: elapsed}
				if qdResult, _, qdErr := promClient.API().Query(ctx, qdQuery, time.Now()); qdErr == nil {
					if vec, ok := qdResult.(model.Vector); ok && len(vec) > 0 {
						snap.QueueDepth = float64(vec[0].Value)
					}
				}
				if kvResult, _, kvErr := promClient.API().Query(ctx, kvQuery, time.Now()); kvErr == nil {
					if vec, ok := kvResult.(model.Vector); ok && len(vec) > 0 {
						snap.KVCache = float64(vec[0].Value)
					}
				}
				if eppResult, _, eppErr := promClient.API().Query(ctx, eppQDQuery, time.Now()); eppErr == nil {
					if vec, ok := eppResult.(model.Vector); ok && len(vec) > 0 {
						snap.EPPQueueDepth = float64(vec[0].Value)
					}
				}
				metricsTimeline = append(metricsTimeline, snap)
				GinkgoWriter.Printf("  [%.0fs] queue_depth=%.1f epp_queue=%.1f kv_cache=%.3f\n", elapsed, snap.QueueDepth, snap.EPPQueueDepth, snap.KVCache)

				// Pod-level health check for crash detection
				pods, podErr := k8sClient.CoreV1().Pods(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{
					LabelSelector: fmt.Sprintf("app=%s", res.DeploymentName),
				})
				if podErr == nil {
					for i := range pods.Items {
						p := &pods.Items[i]
						for _, cs := range p.Status.ContainerStatuses {
							if cs.RestartCount > 0 {
								reason := "running"
								if cs.State.Waiting != nil {
									reason = cs.State.Waiting.Reason
								} else if cs.State.Terminated != nil {
									reason = fmt.Sprintf("terminated(%s,exit=%d)", cs.State.Terminated.Reason, cs.State.Terminated.ExitCode)
								}
								GinkgoWriter.Printf("  [%.0fs] Pod %s: restarts=%d state=%s\n", elapsed, p.Name, cs.RestartCount, reason)
							}
						}
					}
				}

				hpaList, hpaErr := k8sClient.AutoscalingV2().HorizontalPodAutoscalers(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
				if hpaErr == nil {
					for i := range hpaList.Items {
						hpa := &hpaList.Items[i]
						GinkgoWriter.Printf("  [%.0fs] HPA %s: current=%d desired=%d\n", elapsed, hpa.Name, hpa.Status.CurrentReplicas, hpa.Status.DesiredReplicas)
					}
				}
			}
		}
		loadEnd := time.Now()
		loadDuration := loadEnd.Sub(loadStart).Seconds()

		By("Extracting GuideLLM results from pod logs")
		logs, err := fixtures.GetJobPodLogs(ctx, k8sClient, benchCfg.LLMDNamespace, jobName)
		Expect(err).NotTo(HaveOccurred(), "Failed to get GuideLLM pod logs")

		var guidellmRaw json.RawMessage
		var ttftJSON, itlJSON, throughputJSON json.RawMessage

		if idx := strings.Index(logs, "=== BENCHMARK JSON ==="); idx != -1 {
			jsonStr := strings.TrimSpace(logs[idx+len("=== BENCHMARK JSON ==="):])
			guidellmRaw = json.RawMessage(jsonStr)

			var parsed map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(jsonStr), &parsed); jsonErr == nil {
				// GuideLLM stores metrics at benchmarks[0].metrics.<metric_name>.successful
				extractGuideLLMMetric(&parsed, "time_to_first_token_ms", &ttftJSON)
				extractGuideLLMMetric(&parsed, "inter_token_latency_ms", &itlJSON)
				extractGuideLLMMetric(&parsed, "output_tokens_per_second", &throughputJSON)

				// Also extract request totals for diagnostics
				var requestTotals json.RawMessage
				extractGuideLLMMetric(&parsed, "request_totals", &requestTotals)
				if requestTotals != nil {
					GinkgoWriter.Printf("  Request Totals: %s\n", string(requestTotals))
				}
			} else {
				GinkgoWriter.Printf("Warning: failed to parse GuideLLM JSON: %v\n", jsonErr)
				GinkgoWriter.Printf("Raw JSON (first 1000 chars): %s\n", truncateHead(jsonStr, 1000))
			}
		} else {
			GinkgoWriter.Println("Warning: '=== BENCHMARK JSON ===' marker not found in pod logs")
			GinkgoWriter.Printf("Pod log tail (last 500 chars): %s\n", truncateTail(logs, 500))
		}

		By("Querying Prometheus for Replicas, Queue Depth, and KV Cache")
		replicaAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`avg(kube_deployment_status_replicas{deployment="%s", namespace="%s"})`, res.DeploymentName, benchCfg.LLMDNamespace),
			loadStart, loadEnd, 30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query replica avg: %v\n", err)
		}

		qdAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`avg(vllm:num_requests_waiting{namespace="%s"})`, benchCfg.LLMDNamespace),
			loadStart, loadEnd, 30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query queue depth avg: %v\n", err)
		}

		kvAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`avg(vllm:kv_cache_usage_perc{namespace="%s"})`, benchCfg.LLMDNamespace),
			loadStart, loadEnd, 30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query KV cache avg: %v\n", err)
		}

		eppQDAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`sum(inference_extension_flow_control_queue_size{namespace="%s"})`, benchCfg.LLMDNamespace),
			loadStart, loadEnd, 30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query EPP queue depth avg: %v\n", err)
		}

		result := PrefillResult{
			AutoscalerType:   autoscalerType,
			ReplicaTimeline:  timeline,
			MetricsTimeline:  metricsTimeline,
			AvgReplicas:      replicaAvg,
			MaxReplicas:      maxReplicas,
			AvgQueueDepth:    qdAvg,
			AvgEPPQueueDepth: eppQDAvg,
			AvgKVCache:       kvAvg,
			TTFT:            ttftJSON,
			ITL:             itlJSON,
			Throughput:      throughputJSON,
			GuideLLMRaw:     guidellmRaw,
			DurationSec:     loadDuration,
		}
		prefillResults = append(prefillResults, result)

		GinkgoWriter.Printf("\n========================================\n")
		GinkgoWriter.Printf("  %s PREFILL BENCHMARK RESULTS\n", autoscalerType)
		GinkgoWriter.Printf("========================================\n")
		GinkgoWriter.Printf("  Duration:        %.0fs\n", loadDuration)
		GinkgoWriter.Printf("  Max Replicas:    %d\n", maxReplicas)
		GinkgoWriter.Printf("  Avg Replicas:    %.2f\n", replicaAvg)
		GinkgoWriter.Printf("  Avg Queue Depth: %.2f\n", qdAvg)
		GinkgoWriter.Printf("  Avg EPP Queue:   %.2f\n", eppQDAvg)
		GinkgoWriter.Printf("  Avg KV Cache:    %.3f\n", kvAvg)
		if ttftJSON != nil {
			GinkgoWriter.Printf("  TTFT:            %s\n", string(ttftJSON))
		}
		if itlJSON != nil {
			GinkgoWriter.Printf("  ITL:             %s\n", string(itlJSON))
		}
		if throughputJSON != nil {
			GinkgoWriter.Printf("  Throughput:      %s\n", string(throughputJSON))
		}
		GinkgoWriter.Printf("  Replica Timeline (%d snapshots):\n", len(timeline))
		for _, s := range timeline {
			GinkgoWriter.Printf("    t=%.0fs  spec=%d  ready=%d\n", s.ElapsedSec, s.SpecReplicas, s.ReadyReplicas)
		}
		GinkgoWriter.Printf("========================================\n\n")

		By("Saving prefill benchmark results to file")
		data, _ := json.MarshalIndent(prefillResults, "", "  ")
		_ = os.WriteFile(prefillResultsFile, data, 0644)
	}

	Context("HPA Baseline", func() {
		It("should run the prefill heavy workload against standard HPA", func() {
			cleanupAutoscalers()
			res.DeploymentName = findInfraDecodeDeployment()

			By("Creating standard HPA (CPU-based, Scale Up: 0s, Scale Down: 240s)")
			scaleUpPolicies := []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}
			scaleDownPolicies := []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}

			err := fixtures.EnsureStandardHPA(
				ctx, k8sClient, benchCfg.LLMDNamespace, res.HPAName, res.DeploymentName,
				1, 10,
				0, 240,
				scaleUpPolicies, scaleDownPolicies,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create standard HPA")

			runPrefillBenchmark("HPA")
		})
	})

	Context("WVA", func() {
		It("should run the prefill heavy workload against WVA", func() {
			cleanupAutoscalers()
			res.DeploymentName = findInfraDecodeDeployment()

			By("Waiting for model server to recover after previous test")
			time.Sleep(30 * time.Second)
			ensureInfraDeploymentReady()

			By("Scaling deployment to 1 replica before WVA test (clean baseline)")
			scale, err := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).GetScale(ctx, res.DeploymentName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			if scale.Spec.Replicas != 1 {
				scale.Spec.Replicas = 1
				_, err = k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).UpdateScale(ctx, res.DeploymentName, scale, metav1.UpdateOptions{})
				Expect(err).NotTo(HaveOccurred())
				GinkgoWriter.Printf("Scaled deployment %s to 1 replica\n", res.DeploymentName)
			}
			Eventually(func(g Gomega) {
				dep, depErr := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
				g.Expect(depErr).NotTo(HaveOccurred())
				g.Expect(dep.Status.ReadyReplicas).To(Equal(int32(1)), "Deployment should have exactly 1 ready replica")
			}, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("Waiting for stale Prometheus metrics to settle (60s)")
			time.Sleep(60 * time.Second)

			By("Creating VariantAutoscaling resource (Scale Up: 0s, Scale Down: 240s)")
			err = fixtures.EnsureVariantAutoscaling(
				ctx, crClient, benchCfg.LLMDNamespace, res.VAName, res.DeploymentName,
				benchCfg.ModelID, benchCfg.AcceleratorType, 30.0, benchCfg.ControllerInstance,
				fixtures.WithMinReplicas(1),
				fixtures.WithMaxReplicas(10),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create VA")

			By("Creating HPA for the deployment (WVA-driven external metric)")
			behavior := &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(int32(0)),
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 10, PeriodSeconds: 150}},
				},
				ScaleDown: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(int32(240)),
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 10, PeriodSeconds: 150}},
				},
			}

			err = fixtures.EnsureHPA(ctx, k8sClient, benchCfg.LLMDNamespace, res.HPAName, res.DeploymentName, res.VAName, 1, 10, behavior)
			Expect(err).NotTo(HaveOccurred(), "Failed to create HPA")

			waitForVAAndMetrics()

			runPrefillBenchmark("WVA")
		})
	})
})

// extractGuideLLMMetric extracts a metric from the GuideLLM JSON structure.
// The structure is: benchmarks[0].metrics.<key>.successful (for successful request stats).
// Falls back to benchmarks[0].metrics.<key> if "successful" sub-key doesn't exist.
func extractGuideLLMMetric(parsed *map[string]interface{}, key string, out *json.RawMessage) {
	benchmarks, ok := (*parsed)["benchmarks"].([]interface{})
	if !ok || len(benchmarks) == 0 {
		return
	}
	bm, ok := benchmarks[0].(map[string]interface{})
	if !ok {
		return
	}
	metrics, ok := bm["metrics"].(map[string]interface{})
	if !ok {
		return
	}
	metricVal, ok := metrics[key]
	if !ok {
		return
	}
	metricMap, ok := metricVal.(map[string]interface{})
	if ok {
		if successful, ok := metricMap["successful"]; ok {
			raw, _ := json.Marshal(successful)
			*out = raw
			return
		}
	}
	raw, _ := json.Marshal(metricVal)
	*out = raw
}

func truncateTail(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return "..." + s[len(s)-maxLen:]
}

func truncateHead(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
