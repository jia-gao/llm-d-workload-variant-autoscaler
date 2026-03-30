package benchmark

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/test/e2e/fixtures"
)

var _ = Describe("Prefill Heavy Workload Benchmark", Label("benchmark", "phase4"), func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		res    ScenarioResources
	)

	const (
		// The exact YAML profile provided for the prefill heavy workload
		prefillHeavyProfileYAML = `
profile: poisson
request_type: text_completions
rate: 20
max_seconds: 600
data:
  prompt_tokens: 4000
  output_tokens: 1000
`
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		res = ScenarioResources{
			PoolName:       "prefill-pool",
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

	runPrefillBenchmark := func(autoscalerType string) {
		By("Waiting for deployment to be ready")
		Eventually(func(g Gomega) {
			deployment, err := k8sClient.AppsV1().Deployments(benchCfg.LLMDNamespace).Get(ctx, res.DeploymentName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(deployment.Status.ReadyReplicas).To(BeNumerically(">=", 1), "Deployment should have at least 1 ready replica")
		}, 15*time.Minute, 5*time.Second).Should(Succeed())

		By("Launching GuideLLM Load Generator")
		targetURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/v1/completions",
			benchCfg.GatewayServiceName, benchCfg.LLMDNamespace, benchCfg.GatewayServicePort)

		err := fixtures.CreateGuideLLMJobWithProfile(
			ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService,
			targetURL, benchCfg.ModelID, prefillHeavyProfileYAML,
		)
		Expect(err).NotTo(HaveOccurred(), "Failed to create GuideLLM load job")

		loadStart := time.Now()
		jobName := res.ModelService + "-load"

		By("Waiting for GuideLLM job to complete (this will take ~10 minutes)")
		err = fixtures.WaitForJobCompletion(ctx, k8sClient, benchCfg.LLMDNamespace, jobName, 15*time.Minute)
		Expect(err).NotTo(HaveOccurred(), "GuideLLM job failed or timed out")
		loadEnd := time.Now()

		By("Extracting GuideLLM results from pod logs")
		logs, err := fixtures.GetJobPodLogs(ctx, k8sClient, benchCfg.LLMDNamespace, jobName)
		Expect(err).NotTo(HaveOccurred(), "Failed to get GuideLLM pod logs")

		// Extract the JSON part from the logs
		jsonStr := ""
		if idx := strings.Index(logs, "=== BENCHMARK JSON ==="); idx != -1 {
			jsonStr = logs[idx+len("=== BENCHMARK JSON ==="):]
		}

		GinkgoWriter.Printf("\n--- %s GuideLLM Results ---\n%s\n---------------------------\n", autoscalerType, jsonStr)

		By("Querying Prometheus for Replicas, Queue Depth, and KV Cache")

		// 1. Average Replicas
		replicaAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`avg(kube_deployment_status_replicas{deployment="%s", namespace="%s"})`, res.DeploymentName, benchCfg.LLMDNamespace),
			loadStart, loadEnd,
			30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query replica avg: %v\n", err)
		}

		// 2. Average EPP Queue Depth (Waiting Requests)
		// EPP exposes metrics, but we can also check the vllm metric directly
		qdAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`avg(vllm:num_requests_waiting{namespace="%s"})`, benchCfg.LLMDNamespace),
			loadStart, loadEnd,
			30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query queue depth avg: %v\n", err)
		}

		// 3. Average KV Cache Utilization
		kvAvg, err := QueryRangeAvg(
			promClient.API(),
			fmt.Sprintf(`avg(vllm:kv_cache_usage_perc{namespace="%s"})`, benchCfg.LLMDNamespace),
			loadStart, loadEnd,
			30*time.Second,
		)
		if err != nil {
			GinkgoWriter.Printf("Warning: failed to query KV cache avg: %v\n", err)
		}

		GinkgoWriter.Printf("\n=== %s Prometheus Metrics ===\n", autoscalerType)
		GinkgoWriter.Printf("Avg Replicas: %.2f\n", replicaAvg)
		GinkgoWriter.Printf("Avg Queue Depth: %.2f\n", qdAvg)
		GinkgoWriter.Printf("Avg KV Cache Usage: %.3f\n", kvAvg)
		GinkgoWriter.Println("=================================")
	}

	Context("HPA Baseline", func() {
		It("should run the prefill heavy workload against standard HPA", func() {
			By("Setting up EPP Configuration with Flow Control")
			err := fixtures.EnsureEndpointPickerConfig(ctx, crClient, benchCfg.LLMDNamespace, res.PoolName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create EndpointPickerConfig")

			By("Creating model service deployment")
			err = fixtures.EnsureModelService(ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService, res.PoolName, benchCfg.ModelID, benchCfg.UseSimulator, benchCfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create model service")

			By("Creating standard HPA (Scale Up: 0, Scale Down: 240)")
			scaleUpPolicies := []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}
			scaleDownPolicies := []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}} // Default fallback

			err = fixtures.EnsureStandardHPA(
				ctx, k8sClient, benchCfg.LLMDNamespace, res.HPAName, res.DeploymentName,
				1, 10, // min/max replicas
				0, 240, // scale up/down stabilization window
				scaleUpPolicies, scaleDownPolicies,
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create standard HPA")

			runPrefillBenchmark("HPA")
		})
	})

	Context("WVA", func() {
		It("should run the prefill heavy workload against WVA", func() {
			By("Setting up EPP Configuration with Flow Control")
			err := fixtures.EnsureEndpointPickerConfig(ctx, crClient, benchCfg.LLMDNamespace, res.PoolName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create EndpointPickerConfig")

			By("Creating model service deployment")
			err = fixtures.EnsureModelService(ctx, k8sClient, benchCfg.LLMDNamespace, res.ModelService, res.PoolName, benchCfg.ModelID, benchCfg.UseSimulator, benchCfg.MaxNumSeqs)
			Expect(err).NotTo(HaveOccurred(), "Failed to create model service")

			By("Creating VariantAutoscaling resource (Scale Up: 0, Scale Down: 240)")
			behavior := &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(int32(0)),
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 10, PeriodSeconds: 150}},
				},
				ScaleDown: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To(int32(240)),
					Policies:                   []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 1, PeriodSeconds: 60}}, // Default fallback
				},
			}

			err = fixtures.EnsureVariantAutoscaling(
				ctx, crClient, benchCfg.LLMDNamespace, res.VAName, res.DeploymentName,
				benchCfg.ModelID, benchCfg.AcceleratorType, 30.0, benchCfg.ControllerInstance,
				fixtures.WithMinReplicas(1),
				fixtures.WithMaxReplicas(10),
			)
			Expect(err).NotTo(HaveOccurred(), "Failed to create VA")

			By("Creating HPA for the deployment")
			err = fixtures.EnsureHPA(ctx, k8sClient, benchCfg.LLMDNamespace, res.HPAName, res.DeploymentName, res.VAName, 1, 10, behavior)
			Expect(err).NotTo(HaveOccurred(), "Failed to create HPA")

			runPrefillBenchmark("WVA")
		})
	})
})
