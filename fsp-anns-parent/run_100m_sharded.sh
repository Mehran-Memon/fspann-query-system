#!/usr/bin/env bash
# ============================================================
# FSP-ANN 100M "AGGRESSIVE SHARDED" RUN
# Goal: 100M Insertions without stalling.
# Allocation: 300GB Heap | 16 Metadata Shards
# ============================================================

# Detect Paths
PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG_DIR="$PROJECT_ROOT/config/src/main/resources"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_ROOT="/mnt/data/mehran/SCALABILITY_RUNS"
LARGE_TMP="/mnt/data/mehran/tmp"

# Inputs
CONFIG_FILE="$CONFIG_DIR/config_sift1b_10m.json"
DATA_FILE="$BASE_DIR/bigann_learn_100m.bvecs"
QUERY_FILE="$BASE_DIR/bigann_query_1k.bvecs"
GT_FILE="$BASE_DIR/bigann_query_groundtruth.ivecs"

# JVM Configuration (G1GC + 300GB)
# -Xms and -Xmx are set high to prevent resizing overhead
JVM_ARGS=(
  "-server"
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=500"
  "-XX:+AlwaysPreTouch"
  "-Xms300g"
  "-Xmx300g"
  "-Dfile.encoding=UTF-8"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dreenc.mode=end"
  "-Dmetadata.sharded=true"
  "-Dmetadata.shards=16"
)

# Experiment Config
DIMENSION=128
BATCH_SIZE=1000000

echo "=========================================="
echo "   STARTING 100M AGGRESSIVE RUN           "
echo "   Heap: 300GB | GC: G1 | Shards: 16      "
echo "   Start Time: $(date)                    "
echo "=========================================="

mkdir -p "$OUTPUT_ROOT"
mkdir -p "$LARGE_TMP"

RUN_DIR="$OUTPUT_ROOT/SIFT1B-100M-AGGRESSIVE"
LOG_FILE="$RUN_DIR/run.log"

# Clean previous run artifacts
echo "Cleaning artifacts..."
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR/results"

echo "Executing..."
nohup java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG_FILE" "$DATA_FILE" "$QUERY_FILE" \
  "$RUN_DIR/keys.blob" "$DIMENSION" "$RUN_DIR" \
  "$GT_FILE" "$BATCH_SIZE" > "$LOG_FILE" 2>&1 &

PID=$!
echo "Aggressive Process started with PID: $PID"
echo "   Monitor with: tail -f $LOG_FILE"