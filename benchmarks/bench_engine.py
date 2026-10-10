import time
import statistics
import sys

# Mocking swim-gossip-core components for demonstration of microbenchmark structure.
# In a real scenario, import the actual compiled extension or library:
# import swim_gossip_core

class MockSwimGossipCore:
    """Mock implementation representing the swim-gossip-core operations."""
    def __init__(self):
        self._members = {f"node-{i}": {"state": "alive", "incarnation": 0} for i in range(100)}

    def update_member(self, node_id: str, state: str, incarnation: int) -> None:
        self._members[node_id] = {"state": state, "incarnation": incarnation}

    def get_membership(self) -> dict:
        return self._members

def run_benchmark(iterations: int = 100000):
    core = MockSwimGossipCore()
    
    # Pre-generate payload to avoid measuring test overhead
    node_id = "node-42"
    state = "alive"
    incarnation = 1
    
    latencies = []
    
    # Warmup phase
    for _ in range(1000):
        core.update_member(node_id, state, incarnation)
        
    # Measurement phase
    start_time = time.perf_counter()
    
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        core.update_member(node_id, state, incarnation)
        t1 = time.perf_counter_ns()
        latencies.append(t1 - t0)
        
    end_time = time.perf_counter()
    total_duration = end_time - start_time
    
    ops_per_sec = iterations / total_duration
    
    # Calculate latency percentiles (convert ns to microseconds for readability)
    latencies.sort()
    p50 = statistics.median(latencies) / 1000.0
    p99_idx = int(iterations * 0.99)
    p99 = latencies[p99_idx] / 1000.0
    p95 = latencies[int(iterations * 0.95)] / 1000.0
    min_lat = latencies[0] / 1000.0
    max_lat = latencies[-1] / 1000.0
    
    # Output formatting
    print("=" * 60)
    print(f"{'SWIM-GOSSIP-CORE MICROBENCHMARK RESULTS':^60}")
    print("=" * 60)
    print(f"Total Iterations:   {iterations:,}")
    print(f"Total Duration:     {total_duration:.4f} seconds")
    print("-" * 60)
    print(f"{'Metric':<30} | {'Value':<25}")
    print("-" * 60)
    print(f"{'Throughput (ops/sec)':<30} | {ops_per_sec:,.2f}")
    print(f"{'Min Latency (µs)':<30} | {min_lat:.2f}")
    print(f"{'p50 Latency (µs)':<30} | {p50:.2f}")
    print(f"{'p95 Latency (µs)':<30} | {p95:.2f}")
    print(f"{'p99 Latency (µs)':<30} | {p99:.2f}")
    print(f"{'Max Latency (µs)':<30} | {max_lat:.2f}")
    print("=" * 60)

if __name__ == "__main__":
    iterations = 200000
    if len(sys.argv) > 1:
        try:
            iterations = int(sys.argv[1])
        except ValueError:
            pass
    run_benchmark(iterations)