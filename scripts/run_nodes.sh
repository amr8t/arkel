#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

LOG_DIR="./logs"
DATA_DIR="./"

mkdir -p "$LOG_DIR"

# --- Node configuration ---
NODE1_ADDR="127.0.0.1:8001"
NODE2_ADDR="127.0.0.1:8002"
NODE3_ADDR="127.0.0.1:8003"

NODE1_LOG="$LOG_DIR/node1.log"
NODE2_LOG="$LOG_DIR/node2.log"
NODE3_LOG="$LOG_DIR/node3.log"

# --- Helper: wait for a string to appear in a log ---
wait_for_log() {
    local log="$1"
    local pattern="$2"
    local timeout="${3:-8}"
    for ((i=0; i<timeout*5; i++)); do
        if grep -q "$pattern" "$log" 2>/dev/null; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# --- Helper: find the first pubkey in a log ---
extract_pubkey() {
    local log="$1"
    grep -m1 "Full Address" "$log" 2>/dev/null | sed 's/.*Full Address  : \(.*\)@.*/\1/' || true
}

# --- Helper: find the local node id in a log ---
extract_node_id() {
    local log="$1"
    grep -m1 "Local Node ID" "$log" 2>/dev/null | sed 's/.*Local Node ID : \(.*\)/\1/' || true
}

echo "== Arkel 3-Node Cluster Runner =="
echo "Logs: $LOG_DIR/"
echo ""

# --- Clean state ---
echo "[clean] Removing old data dirs and logs..."
rm -rf "$DATA_DIR"/.arkel_index_800{1,2,3}_data
rm -f "$LOG_DIR"/node{1,2,3}.log
export RUST_LOG="${RUST_LOG:-info}"

# --- Identity generation ---
# Generate identities first, then extract pubkeys, then restart all nodes with
# the full cluster configuration. Raft state is in-memory, so restarting works.
echo "[identity] Generating node identities..."

start_for_identity() {
    local addr="$1"
    local log="$2"
    # Use RUST_LOG=info so the "Full Address" INFO line is captured.
    RUST_LOG=info cargo run --quiet -- index --http-addr "$addr" > "$log" 2>&1 &
    echo $!
}

PID1=$(start_for_identity "$NODE1_ADDR" "$NODE1_LOG")
PID2=$(start_for_identity "$NODE2_ADDR" "$NODE2_LOG")
PID3=$(start_for_identity "$NODE3_ADDR" "$NODE3_LOG")

if ! wait_for_log "$NODE1_LOG" "Full Address" 10; then
    echo "ERROR: Node 1 did not print its identity in time"
    kill $PID1 $PID2 $PID3 2>/dev/null || true
    exit 1
fi
# Give nodes 2 and 3 a moment to print their identities too
sleep 2

echo "[identity] Extracting pubkeys..."
PUB1=$(extract_pubkey "$NODE1_LOG")
PUB2=$(extract_pubkey "$NODE2_LOG")
PUB3=$(extract_pubkey "$NODE3_LOG")

if [[ -z "$PUB1" || -z "$PUB2" || -z "$PUB3" ]]; then
    echo "ERROR: Failed to extract one or more pubkeys"
    kill $PID1 $PID2 $PID3 2>/dev/null || true
    exit 1
fi

# Save mapping for debugging
cat > "$LOG_DIR/pubkey_map.txt" <<EOF
$PUB1@$NODE1_ADDR
$PUB2@$NODE2_ADDR
$PUB3@$NODE3_ADDR
EOF

echo "[identity] Killing temporary instances..."
kill $PID1 $PID2 $PID3 2>/dev/null || true
wait 2>/dev/null || true

# Clear raft logs (state is in-memory; keep identity.key files)
rm -f "$NODE1_LOG" "$NODE2_LOG" "$NODE3_LOG"

FULL_PEERS="${PUB1}@${NODE1_ADDR},${PUB2}@${NODE2_ADDR},${PUB3}@${NODE3_ADDR}"

echo ""
echo "[cluster] Starting 3-node cluster with shared membership..."
echo "        Node 1: ${PUB1:0:16}... @$NODE1_ADDR"
echo "        Node 2: ${PUB2:0:16}... @$NODE2_ADDR"
echo "        Node 3: ${PUB3:0:16}... @$NODE3_ADDR"

start_node() {
    local addr="$1"
    local log="$2"
    RUST_LOG="$RUST_LOG" \
        cargo run --quiet -- index --http-addr "$addr" --peer-addresses "$FULL_PEERS" \
        > "$log" 2>&1 &
    echo $!
}

PID1=$(start_node "$NODE1_ADDR" "$NODE1_LOG")
sleep 1
PID2=$(start_node "$NODE2_ADDR" "$NODE2_LOG")
sleep 1
PID3=$(start_node "$NODE3_ADDR" "$NODE3_LOG")

cleanup() {
    echo ""
    echo "Stopping nodes..."
    kill $PID1 $PID2 $PID3 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo ""
echo "[wait] Waiting for leader election..."
WAIT_START=$(date +%s)
while true; do
    if grep -q "become leader" "$NODE1_LOG" "$NODE2_LOG" "$NODE3_LOG" 2>/dev/null; then
        break
    fi
    if (( $(date +%s) - WAIT_START > 15 )); then
        echo "WARN: No leader elected within 15s, proceeding anyway..."
        break
    fi
    sleep 0.5
done
sleep 2

# --- Detect leader ---
echo "[leader] Detecting leader..."
LEADER_PORT=""
for port in 8001 8002 8003; do
    log="$LOG_DIR/node"$((port - 8000))".log"
    if grep -q "become leader" "$log" 2>/dev/null; then
        LEADER_PORT="$port"
        break
    fi
done

if [[ -z "$LEADER_PORT" ]]; then
    echo "WARN: Could not detect leader. Writing to node 1 (port 8001) and hoping..."
    LEADER_PORT=8001
fi

echo "        Leader detected on port: $LEADER_PORT"
echo ""

# --- Test commands ---
echo "[test] Writing bucket and object through leader (port $LEADER_PORT)..."
curl -sS -w "\n" -X PUT "http://127.0.0.1:${LEADER_PORT}/photos"
curl -sS -w "\n" -X PUT "http://127.0.0.1:${LEADER_PORT}/photos/avatar.png" \
    -H "Content-Type: image/png" \
    -d "fake-blob-data"

# Small wait for replication
sleep 2

echo ""
echo "[test] Read-back from all 3 nodes (expecting identical metadata)..."
for port in 8001 8002 8003; do
    echo "--- Node $((port - 8000)) (port $port) ---"
    curl -sS "http://127.0.0.1:${port}/photos/avatar.png"
    echo ""
    echo ""
done

echo "[test] List buckets from all 3 nodes..."
for port in 8001 8002 8003; do
    echo "--- Node $((port - 8000)) (port $port) ---"
    curl -sS "http://127.0.0.1:${port}/"
    echo ""
    echo ""
done

# Verify consistency
NODE1_OBJ=$(curl -sS "http://127.0.0.1:8001/photos/avatar.png")
NODE2_OBJ=$(curl -sS "http://127.0.0.1:8002/photos/avatar.png")
NODE3_OBJ=$(curl -sS "http://127.0.0.1:8003/photos/avatar.png")

NODE1_BUCKETS=$(curl -sS "http://127.0.0.1:8001/")
NODE2_BUCKETS=$(curl -sS "http://127.0.0.1:8002/")
NODE3_BUCKETS=$(curl -sS "http://127.0.0.1:8003/")

echo ""
echo "[verify] Consistency check..."
if [[ "$NODE1_OBJ" == "$NODE2_OBJ" && "$NODE2_OBJ" == "$NODE3_OBJ" ]]; then
    echo "  OBJECT METADATA: CONSISTENT across all 3 nodes"
else
    echo "  OBJECT METADATA: INCONSISTENT"
    echo "    node1: $NODE1_OBJ"
    echo "    node2: $NODE2_OBJ"
    echo "    node3: "$NODE3_OBJ"
    exit 1
fi

if [[ "$NODE1_BUCKETS" == "$NODE2_BUCKETS" && "$NODE2_BUCKETS" == "$NODE3_BUCKETS" ]]; then
    echo "  BUCKET LIST:     CONSISTENT across all 3 nodes"
else
    echo "  BUCKET LIST:     INCONSISTENT"
    echo "    node1: $NODE1_BUCKETS"
    echo "    node2: $NODE2_BUCKETS"
    echo "    node3: "$NODE3_BUCKETS"
    exit 1
fi

echo ""
echo "[done] Raft consensus is working end-to-end. Logs are in $LOG_DIR/"
echo "Tail them with: tail -f $LOG_DIR/node*.log"
echo "Press Ctrl+C to stop all nodes..."

# Pause until interrupted; the trap handles cleanup.
while true; do
    sleep 1
done
