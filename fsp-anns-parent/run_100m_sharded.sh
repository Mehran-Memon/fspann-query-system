#!/usr/bin/env bash
# Hardware: 384GB RAM | SIFT100M | 16 Shards
# JVM Target: 220G Heap | Native Target: 164G (OS/RocksDB)

PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_DIR="/mnt/data/mehran/SCALABILITY_RUNS/SIFT1B-100M-FINAL"
LARGE_TMP="/mnt/data/mehran/tmp"

# Correct file names based on your previous logs
CONFIG="$PROJECT_ROOT/config/src/main/resources/config_sift1b_10m.json"
DATA="$BASE_DIR/bigann_learn_100m.bvecs"
QUERY="$BASE_DIR/bigann_query_1k.bvecs"
GT="$BASE_DIR/bigann_query_groundtruth.ivecs"

mkdir -p "$OUTPUT_DIR"

# JVM_ARGS: Using G1GC with 220G.
# Leaving ~160G for 16 Shards to handle background compaction.
JVM_ARGS=(
  "-server"
  "-XX:+UseG1GC"
  "-Xms220g"
  "-Xmx220g"
  "-XX:+AlwaysPreTouch"
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dmetadata.sharded=true"
  "-Dmetadata.shards=16"
)

# SMALL BATCH SIZE (100k) is the key to preventing the 83M stall.
nohup java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG" "$DATA" "$QUERY" \
  "$OUTPUT_DIR/keys.blob" 128 "$OUTPUT_DIR" \
  "$GT" 100000 > "$OUTPUT_DIR/run.log" 2>&1 &

echo "100M Run Initiated. PID: $!"