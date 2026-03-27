const fs = require('fs');

const data = JSON.parse(fs.readFileSync('/tmp/benchmark-results.json', 'utf8'));
const fmtTime = (v) => v < 0 ? 'N/A' : `${v.toFixed(1)}s`;

let resultsTable = `| Metric | Value |
|--------|-------|
| Scale-up time | ${fmtTime(data.scaleUpTimeSec)} |
| Scale-down time | ${fmtTime(data.scaleDownTimeSec)} |
| Max replicas | ${data.maxReplicas} |
| Avg KV cache usage | ${data.avgKVCacheUsage.toFixed(3)} |
| Avg queue depth | ${data.avgQueueDepth.toFixed(1)} |
| Replica oscillation (σ) | ${data.replicaOscillation.toFixed(2)} |
| Total duration | ${data.totalDurationSec.toFixed(0)}s |`;

console.log("### Benchmark: scale-up-latency (Kind)\n");
console.log(resultsTable);
