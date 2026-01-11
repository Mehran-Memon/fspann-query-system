#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

echo "SIFT1B 50M Scalability (50× baseline)"
echo "Started: $(date)"

JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
CONFIG="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1b_50m.json"
BASE="/mnt/data/mehran/Datasets/SIFT1B/bigann_learn_50m.bvecs"
QUERY="/mnt/data/mehran/Datasets/SIFT1B/bigann_query_1k.bvecs"
GT="/mnt/data/mehran/Datasets/SIFT1B/bigann_query_groundtruth.ivecs"
OUTPUT="/mnt/data/mehran/SIFT1B-50M"
DIMENSION=128
BATCH_SIZE=100000

JVM_ARGS=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xms320g"
  "-Xmx354g"
  "-Xmn128g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

rm -rf "$OUTPUT"
mkdir -p "$OUTPUT/results"

START_TIME=$(date +%s)

java "${JVM_ARGS[@]}" -jar "$JAR" \
  "$CONFIG" "$BASE" "$QUERY" "$OUTPUT/keys.blob" \
  "$DIMENSION" "$OUTPUT" "$GT" "$BATCH_SIZE" \
  > "$OUTPUT/run.log" 2>&1

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINS=$(((DURATION % 3600) / 60))

echo "COMPLETED: ${HOURS}h ${MINS}m"
echo "Finished: $(date)"


