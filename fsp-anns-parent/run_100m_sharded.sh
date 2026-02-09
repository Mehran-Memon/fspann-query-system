#!/usr/bin/env bash
# Hardware: 384GB RAM | SIFT100M | 16 Shards
# Fix: Batch file format (1000 points/file)
# Strategy: 315GB Heap + G1GC

PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_DIR="/mnt/data/mehran/SCALABILITY_RUNS/SIFT1B-100M-BATCH-FIX"
LARGE_TMP="/mnt/data/mehran/tmp"

CONFIG="$PROJECT_ROOT/config/src/main/resources/config_sift1b_10m.json"
DATA="$BASE_DIR/bigann_learn_100m.bvecs"
QUERY="$BASE_DIR/bigann_query_1k.bvecs"
GT="$BASE_DIR/bigann_query_groundtruth.ivecs"

# CRITICAL: Clean start
sudo pkill -9 java
sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
rm -rf "$OUTPUT_DIR"
rm -rf "$LARGE_TMP"/*

mkdir -p "$OUTPUT_DIR"
mkdir -p "$LARGE_TMP"

# JVM Args: 315GB Heap (Maximum safe allocation)
JVM_ARGS=(
  "-server"
  "-XX:+UseG1GC"
  "-Xms315g"
  "-Xmx315g"
  "-XX:+AlwaysPreTouch"
  "-XX:G1HeapRegionSize=32M"
  "-XX:MaxGCPauseMillis=500"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dmetadata.sharded=true"
  "-Dmetadata.shards=16"
)

# Batch Size: 100k (optimal for G1GC)
BATCH_SIZE=100000

nohup java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG" "$DATA" "$QUERY" \
  "$OUTPUT_DIR/keys.blob" 128 "$OUTPUT_DIR" \
  "$GT" "$BATCH_SIZE" > "$OUTPUT_DIR/run.log" 2>&1 &

echo "100M Batch-File-Fixed Run Started: PID=$!"
echo "Monitor: tail -f $OUTPUT_DIR/run.log"
echo "Expected time: ~6-8 hours (based on SIFT1M benchmarks)"
