# 1. Start a fresh 3-node cluster
python scripts/run_nodes.py start --fresh

# 2. Add demo bucket + object (use whichever port is leader)
curl -X PUT http://127.0.0.1:8001/demo
curl -X PUT http://127.0.0.1:8001/demo/hello.txt -d "hello"

# 3. Kill node 1
python scripts/run_nodes.py kill --port 8001

# 4. Wipe node 1's data but keep its identity
python scripts/run_nodes.py wipe --port 8001

# 5. Bring the same node 1 back up (uses saved identity + peers)
python scripts/run_nodes.py start --port 8001

# 6. Wipe all nodes' state (keeps identities)
python scripts/run_nodes.py wipe --all

# --- Performance testing ---

# 7. Basic perf run (2k steady, 10x burst)
python scripts/run_nodes.py run --performance

# 8. Find max sustainable throughput ceiling
python scripts/run_nodes.py run --performance --perf-find-ceiling

# 9. Simulate 10ms latency with 3ms jitter between nodes (requires sudo)
python scripts/run_nodes.py run --performance --perf-latency-ms 10 --perf-jitter-ms 3

# 10. Find ceiling under realistic network conditions
python scripts/run_nodes.py run --performance --perf-find-ceiling --perf-latency-ms 10 --perf-jitter-ms 3
