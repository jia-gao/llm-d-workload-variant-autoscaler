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
	AutoscalerType  string          `json:"autoscaler_type"`
	ReplicaTimeline []ReplicaSnap   `json:"replica_timeline"`
	AvgReplicas     float64         `json:"avg_replicas"`
	MaxReplicas     int32           `json:"max_replicas"`
	AvgQueueDepth   float64         `json:"avg_queue_depth"`
	AvgKVCache      float64         `json:"avg_kv_cache"`
	TTFT            json.RawMessage `json:"ttft,omitempty"`
	ITL             json.RawMessage `json:"itl,omitempty"`
	Throughput      json.RawMessage `json:"throughput,omitempty"`
	GuideLLMRaw     json.RawMessage `json:"guidellm_raw,omitempty"`
	DurationSec     float64         `json:"duration_sec"`
}

// ReplicaSnap records replica count at a point in time.
type ReplicaSnap struct {
	ElapsedSec    float64 `json:"elapsed_sec"`
	SpecReplicas  int32   `json:"spec_replicas"`
	ReadyReplicas int32   `json:"ready_replicas"`
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
			PoolName:       benchCfg.PoolName,
			ModelService:   "prefill-ms",
			DeploymentName: "prefill-ms-decode",
			ServiceName:    "prefill-ms-service",
			VAName:         "prefill-va",
			HPAName:        "prefill-hpa",
			JobBaseName:    "prefill-ms",
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

	// scaleDownOtherDecodeDeployments scales any decode deployments that aren't ours to 0 replicas.
	// This ensures the EPP only routes to our prefill-ms-decode pods with the correct max-model-len.
	scaleDownOtherDecodeDeployments := func() {
		By("Scaling down other decode deployments in the pool")
		deployments, err := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			GinkgoWriter.Printf("Warning: could not list deployments: %v\n", err)
			return
		}
		zero := int32(0)
		for i := range deployments.Items {
			d := &deployments.Items[i]
			if d.Name == res.DeploymentName {
				continue
			}
			if !strings.HasSuffix(d.Name, "-decode") {
				continue
			}
			if d.Spec.Replicas != nil && *d.Spec.Replicas == 0 {
				continue
			}
			GinkgoWriter.Printf("  Scaling down %s from %d to 0 replicas\n", d.Name, *d.Spec.Replicas)
			d.Spec.Replicas = &zero
			_, updateErr := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Update(ctx, d, metav1.UpdateOptions{})
			if updateErr != nil {
				GinkgoWriter.Printf("  Warning: failed to scale down %s: %v\n", d.Name, updateErr)
			}
		}
		time.Sleep(5 * time.Second)
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

	runPrefillBenchmark := func(autoscalerType string) {
		By("Waiting for deployment to be ready (with pod health diagnostics)")
		var lastLogDump int32
		Eventually(func(g Gomega) {
			deployment, err := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())

			pods, podErr := k8sClient.CoreV1().Pods(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", res.DeploymentName),
			})
			if podErr == nil {
				for i := range pods.Items {
					p := &pods.Items[i]
					for _, cs := range p.Status.ContainerStatuses {
						if cs.State.Waiting != nil {
							GinkgoWriter.Printf("  Pod %s: WAITING reason=%s message=%s restarts=%d\n",
								p.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message, cs.RestartCount)
						} else if cs.State.Terminated != nil {
							GinkgoWriter.Printf("  Pod %s: TERMINATED reason=%s exitCode=%d restarts=%d\n",
								p.Name, cs.State.Terminated.Reason, cs.State.Terminated.ExitCode, cs.RestartCount)
						} else if cs.State.Running != nil {
							GinkgoWriter.Printf("  Pod %s: RUNNING ready=%v restarts=%d\n",
								p.Name, cs.Ready, cs.RestartCount)
						}
						// Dump previous container logs once after first crash to see why it died
						if cs.RestartCount > 0 && cs.RestartCount > lastLogDump {
							lastLogDump = cs.RestartCount
							logOpts := &corev1.PodLogOptions{Container: cs.Name, Previous: true, TailLines: ptr.To(int64(50))}
							req := k8sClient.CoreV1().Pods(benchCfg.LLMDNamespace).GetLogs(p.Name, logOpts)
							logStream, logErr := req.Stream(ctx)
							if logErr == nil {
								buf := make([]byte, 8192)
								n, _ := logStream.Read(buf)
								logStream.Close()
								if n > 0 {
									GinkgoWriter.Printf("\n--- CRASHED CONTAINER LOGS (previous, tail 50) ---\n%s\n--- END LOGS ---\n\n", string(buf[:n]))
								}
							}
						}
					}
				}
			}

			g.Expect(deployment.Status.ReadyReplicas).To(BeNumerically(">=", 1), "Deployment should have at least 1 ready replica")
		}, 15*time.Minute, 10*time.Second).Should(Succeed())

		By("Listing all decode deployments in namespace (diagnostics)")
		deployments, _ := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).List(ctx, metav1.ListOptions{})
		if deployments != nil {
			for i := range deployments.Items {
				d := &deployments.Items[i]
				if strings.HasSuffix(d.Name, "-decode") || strings.Contains(d.Name, "decode") {
					ready := d.Status.ReadyReplicas
					spec := int32(0)
					if d.Spec.Replicas != nil {
						spec = *d.Spec.Replicas
					}
					GinkgoWriter.Printf("  Decode deployment: %s (spec=%d, ready=%d)\n", d.Name, spec, ready)
				}
			}
		}

		By("Verifying Gateway connectivity with a small test request")
		targetURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d",
			benchCfg.GatewayServiceName, benchCfg.LLMDNamespace, benchCfg.GatewayServicePort)
		err := fixtures.VerifyGatewayConnectivity(ctx, k8sClient, benchCfg.LLMDNamespace, targetURL, benchCfg.ModelID)
		Expect(err).NotTo(HaveOccurred(), "Gateway connectivity check failed — backend is not reachable, aborting benchmark")

		By("Checking Prometheus metric availability before load")
		for _, q := range []string{
			fmt.Sprintf(`vllm:kv_cache_usage_perc{namespace="%s"}`, benchCfg.LLMDNamespace),
			fmt.Sprintf(`vllm:num_requests_waiting{namespace="%s"}`, benchCfg.LLMDNamespace),
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

		result := PrefillResult{
			AutoscalerType:  autoscalerType,
			ReplicaTimeline: timeline,
			AvgReplicas:     replicaAvg,
			MaxReplicas:     maxReplicas,
			AvgQueueDepth:   qdAvg,
			AvgKVCache:      kvAvg,
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
			scaleDownOtherDecodeDeployments()

			By("Setting up EPP Configuration with Flow Control")
			err := fixtures.EnsureEndpointPickerConfig(ctx, crClient, benchCfg.LLMDNamespace, benchCfg.EPPServiceName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create EndpointPickerConfig")

			By("Creating model service deployment")
			err = fixtures.EnsureModelService(ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService, res.PoolName, benchCfg.ModelID, benchCfg.UseSimulator, benchCfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create model service")

			By("Creating service to expose model server")
			err = fixtures.EnsureService(ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService, res.DeploymentName, 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create service")

			By("Creating ServiceMonitor for metrics scraping")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, benchCfg.MonitoringNS, benchCfg.LLMDNamespace, res.ModelService, res.DeploymentName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ServiceMonitor")

			By("Creating standard HPA (CPU-based, Scale Up: 0s, Scale Down: 240s)")
			scaleUpPolicies := []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}
			scaleDownPolicies := []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}

			err = fixtures.EnsureStandardHPA(
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
			scaleDownOtherDecodeDeployments()

			By("Setting up EPP Configuration with Flow Control")
			err := fixtures.EnsureEndpointPickerConfig(ctx, crClient, benchCfg.LLMDNamespace, benchCfg.EPPServiceName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create EndpointPickerConfig")

			By("Creating model service deployment")
			err = fixtures.EnsureModelService(ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService, res.PoolName, benchCfg.ModelID, benchCfg.UseSimulator, benchCfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create model service")

			By("Creating service to expose model server")
			err = fixtures.EnsureService(ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService, res.DeploymentName, 8000)
			Expect(err).NotTo(HaveOccurred(), "Failed to create service")

			By("Creating ServiceMonitor for metrics scraping")
			err = fixtures.EnsureServiceMonitor(ctx, crClient, benchCfg.MonitoringNS, benchCfg.LLMDNamespace, res.ModelService, res.DeploymentName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ServiceMonitor")

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
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 1, PeriodSeconds: 60}},
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
