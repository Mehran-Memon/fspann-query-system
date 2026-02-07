#!/usr/bin/env bash
# Hardware: 384GB RAM | SIFT100M | 16 Shards
# JVM Target: 350G Heap | Native Target: 164G (OS/RocksDB)

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

# JVM_ARGS: Increased heap size and tuned GC for better performance.
JVM_ARGS=(
  "-server"
  "-XX:+UseG1GC"
  "-Xms350g"                       # Increased heap size
  "-Xmx350g"                       # Increased max heap size
  "-XX:+AlwaysPreTouch"            # Pre-touch memory for faster GC
  "-XX:+PrintGCDetails"            # GC details logging
  "-XX:+PrintGCDateStamps"         # GC timestamps
  "-Xloggc:/mnt/data/mehran/gc.log" # GC logs location
  "-Djava.io.tmpdir=$LARGE_TMP"
  "-Dmetadata.sharded=true"
  "-Dmetadata.shards=16"
  "-XX:MaxGCPauseMillis=200"       # Set max GC pause time (tune as needed)
  "-XX:G1HeapRegionSize=32m"       # G1 GC region size
)

# SMALL BATCH SIZE (100k) is the key to preventing the 83M stall.
# Experiment with batch size (adjust based on system performance)
BATCH_SIZE=50000

# Start run
nohup java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG" "$DATA" "$QUERY" \
  "$OUTPUT_DIR/keys.blob" 128 "$OUTPUT_DIR" \
  "$GT" "$BATCH_SIZE" > "$OUTPUT_DIR/run.log" 2>&1 &

echo "100M Run Initiated. PID: $!"
