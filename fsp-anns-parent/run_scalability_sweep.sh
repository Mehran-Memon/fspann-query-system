#!/usr/bin/env bash
# ============================================================
# FSP-ANN 100M BREAKTHROUGH SCRIPT
# Strategy: Generational ZGC + 355GB Heap
# ============================================================

PROJECT_ROOT="$(pwd)"
JAR="$PROJECT_ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
BASE_DIR="/mnt/data/mehran/Datasets/SIFT1B"
OUTPUT_DIR="/mnt/data/mehran/SCALABILITY_RUNS/SIFT1B-100M-FINAL"

# JVM ARGS for JDK 21 ZGC
JVM_ARGS=(
  "-server"
  "-XX:+UseZGC"
  "-XX:+ZGenerational"
  "-Xms355g"
  "-Xmx355g"
  "-XX:+AlwaysPreTouch"
  "-Dmetadata.sharded=true"
  "-Dmetadata.shards=16"
)

# Use a very small batch to keep memory stable
BATCH_SIZE=20000

nohup java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$PROJECT_ROOT/config/src/main/resources/config_sift1b_10m.json" \
  "$BASE_DIR/bigann_learn_100m.bvecs" \
  "$BASE_DIR/bigann_query_1k.bvecs" \
  "$OUTPUT_DIR/keys.blob" 128 "$OUTPUT_DIR" \
  "$BASE_DIR/bigann_query_groundtruth.ivecs" \
  "$BATCH_SIZE" > "$OUTPUT_DIR/run.log" 2>&1 &