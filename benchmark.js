// benchmark.js
import autocannon from 'autocannon';

async function runBenchmark(name, url) {
  console.log(`\n========================================`);
  console.log(`🚀 Running Benchmark: ${name}`);
  console.log(`========================================\n`);

  const instance = autocannon({
    url,
    connections: 200000, // 20 concurrent clients
    duration: 10,    // 10 seconds test
    pipelining: 1,
  });

  autocannon.track(instance, { renderProgressBar: true });

  const result = await instance;

  console.log(`\nResults for ${name}:`);
  console.log(`- Total Requests:  ${result.requests.total}`);
  console.log(`- Throughput:      ${result.requests.average} req/sec`);
  console.log(`- 2xx (Allowed):   ${result['2xx']}`);
  console.log(`- 4xx (Blocked):   ${result['4xx']}`);
  console.log(`- Latency Avg:     ${result.latency.average} ms`);
  console.log(`- Latency p97.5:     ${result.latency.p97_5} ms`);
  console.log(`- Latency p99:     ${result.latency.p99} ms`);
}

async function start() {
  await runBenchmark('Distributed Rate Limiter', 'http://localhost:5000/rate-check');
}

start();