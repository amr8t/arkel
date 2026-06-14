#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

LOG_DIR="./logs"
DATA_DIR="./"

mkdir -p "$LOG_DIR"

echo "== Arkel 3-Node Cluster Runner =="
echo "Logs will be written to: $LOG_DIR/"
echo ""

# --- Clean up old data and logs ---
echo "Cleaning old data dirs and logs..."
rm -rf "$DATA_DIR"/.arkel_index_800{1,2,3}_data
rm -f "$LOG_DIR"/node{1,2,3}.log

export RUST_LOG="${RUST_LOG:-info}"

# --- Node 1 ---
echo "[1/3] Starting Node 1 on 127.0.0.1:8001..."
RUST_LOG="$RUST_LOG" \
    cargo run --quiet -- index --http-addr 127.0.0.1:8001 \
    > "$LOG_DIR/node1.log" 2>&1 &
PID1=$!
sleep 3

# Extract Node 1 public key for peer-address seeding
PUB1=$(grep -m1 "Full Address" "$LOG_DIR/node1.log" | sed 's/.*Full Address  : \(.*\)@.*/\1/' || true)
if [[ -z "$PUB1" ]]; then
    echo "ERROR: Could not extract Node 1 pubkey. Check $LOG_DIR/node1.log"
    kill $PID1 2>/dev/null || true
    exit 1
fi
echo "        Node 1 pubkey: ${PUB1:0:16}..."

# --- Node 2 ---
echo "[2/3] Starting Node 2 on 127.0.0.1:8002..."
RUST_LOG="$RUST_LOG" \
    cargo run --quiet -- index --http-addr 127.0.0.1:8002 \
        --peer-addresses "${PUB1}@127.0.0.1:8001" \
    > "$LOG_DIR/node2.log" 2>&1 &
PID2=$!
sleep 3

PUB2=$(grep -m1 "Full Address" "$LOG_DIR/node2.log" | sed 's/.*Full Address  : \(.*\)@.*/\1/' || true)
if [[ -z "$PUB2" ]]; then
    echo "ERROR: Could not extract Node 2 pubkey. Check $LOG_DIR/node2.log"
    kill $PID1 $PID2 2>/dev/null || true
    exit 1
fi
echo "        Node 2 pubkey: ${PUB2:0:16}..."

# --- Node 3 ---
echo "[3/3] Starting Node 3 on 127.0.0.1:8003..."
RUST_LOG="$RUST_LOG" \
    cargo run --quiet -- index --http-addr 127.0.0.1:8003 \
        --peer-addresses "${PUB1}@127.0.0.1:8001,${PUB2}@127.0.0.1:8002" \
    > "$LOG_DIR/node3.log" 2>&1 &
PID3=$!

echo ""
echo "All 3 nodes running."
echo "  PIDs: $PID1, $PID2, $PID3"
echo ""
echo "Tail individual logs:"
echo "  tail -f $LOG_DIR/node1.log"
echo "  tail -f $LOG_DIR/node2.log"
echo "  tail -f $LOG_DIR/node3.log"
echo ""
echo "Tail all logs (with file headers):"
echo "  tail -f $LOG_DIR/node*.log"
echo ""
echo "Press Ctrl+C to stop all nodes..."

# Wait for interrupt, then clean up
cleanup() {
    echo ""
    echo "Stopping nodes..."
    kill $PID1 $PID2 $PID3 2>/dev/null || true
    wait 2>/dev/null || true
    echo "Done."
    exit 0
}
trap cleanup INT TERM EXIT

wait $PID1 $PID2 $PID3
