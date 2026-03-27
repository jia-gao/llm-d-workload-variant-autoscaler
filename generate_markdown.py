import json

with open('/tmp/benchmark-results.json', 'r') as f:
    data = json.load(f)

def fmt_time(v):
    return 'N/A' if v < 0 else f"{v:.1f}s"

results_table = f"""| Metric | Value |
|--------|-------|
| Scale-up time | {fmt_time(data['scaleUpTimeSec'])} |
| Scale-down time | {fmt_time(data['scaleDownTimeSec'])} |
| Max replicas | {data['maxReplicas']} |
| Avg KV cache usage | {data['avgKVCacheUsage']:.3f} |
| Avg queue depth | {data['avgQueueDepth']:.1f} |
| Replica oscillation (σ) | {data['replicaOscillation']:.2f} |
| Total duration | {data['totalDurationSec']:.0f}s |"""

print("### Benchmark: scale-up-latency (Kind)\n")
print(results_table)
