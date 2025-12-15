#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# FSP-ANN 3-Dataset Runner (Linux)
# Fixes: Direct execution (no background), proper error handling, metrics extraction

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
ConfigPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config.json"
OutRoot="/mnt/data/mehran"

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx16g"
  "-Ddisable.exit=true"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  "-Dreenc.minTouched=5000"
  "-Dreenc.batchSize=2000"
  "-Dlog.progress.everyN=100000"
  "-Dpaper.buildThreshold=2000000"
)

Batch="100000"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

ensure_file() {
  [[ -f "$1" ]] || die "File not found: $1"
}

clean_run_metadata() {
  local r="$1"
  for p in metadata points results; do
    [[ -d "$r/$p" ]] && rm -rf "$r/$p"
    mkdir -p "$r/$p"
  done
}

extract_metrics() {
  local log="$1"
  [[ ! -f "$log" ]] && { echo "N/A|N/A|N/A|N/A"; return 0; }

  local server=$(grep -oE 'Server[^\d]*([0-9]+\.?[0-9]*)' "$log" 2>/dev/null | grep -oE '[0-9]+\.?[0-9]*' | tail -1 || echo "N/A")
  local client=$(grep -oE 'Client[^\d]*([0-9]+\.?[0-9]*)' "$log" 2>/dev/null | grep -oE '[0-9]+\.?[0-9]*' | tail -1 || echo "N/A")
  local ratio=$(grep -oE 'Ratio[^\d]*([0-9]+\.?[0-9]*)' "$log" 2>/dev/null | grep -oE '[0-9]+\.?[0-9]*' | tail -1 || echo "N/A")
  local precision=$(grep -oE 'Precision[^\d]*([0-9]+\.?[0-9]*)' "$log" 2>/dev/null | grep -oE '[0-9]+\.?[0-9]*' | tail -1 || echo "N/A")

  echo "$server|$client|$ratio|$precision"
}

echo "FSP-ANN 3-Dataset Runner (Linux)"
echo "Verifying system..."

command -v java >/dev/null 2>&1 || die "Java not found in PATH"
java -version 2>&1 | head -1
ensure_file "$JarPath"
ensure_file "$ConfigPath"
command -v jq >/dev/null 2>&1 || die "jq not found"

mkdir -p "$OutRoot"
echo "All requirements satisfied"
echo ""

# Datasets
declare -a DATASET_NAMES=("SIFT1M" "glove-100" "RedCaps")
declare -a DATASET_DIMS=("128" "100" "512")
declare -a DATASET_BASE=(
  "/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
  "/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
  "/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
)
declare -a DATASET_QUERY=(
  "/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
  "/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
  "/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
)
declare -a DATASET_GT=(
  "/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
  "/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
  "/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
)

declare -a all_results=()

for idx in "${!DATASET_NAMES[@]}"; do
  dataset_name="${DATASET_NAMES[$idx]}"
  dim="${DATASET_DIMS[$idx]}"
  base="${DATASET_BASE[$idx]}"
  query="${DATASET_QUERY[$idx]}"
  gt="${DATASET_GT[$idx]}"

  [[ ! -f "$base" ]] && { echo "Skipping $dataset_name (missing base)"; continue; }
  [[ ! -f "$query" ]] && { echo "Skipping $dataset_name (missing query)"; continue; }
  [[ ! -f "$gt" ]] && { echo "Skipping $dataset_name (missing GT)"; continue; }

  dataset_root="$OutRoot/$dataset_name"
  mkdir -p "$dataset_root"

  echo "Dataset: $dataset_name (Dim=$dim)"
  echo ""

  cfg_json="$(cat "$ConfigPath")"
  base_json=$(jq -c 'del(.profiles)' <<<"$cfg_json")
  profiles_count=$(jq '.profiles | length' <<<"$cfg_json")

  [[ "$profiles_count" -eq 0 ]] && { echo "No profiles found"; continue; }

  echo "Profile                   | Status  | Server(ms) | Client(ms) | ART(ms) | Ratio  | Precision"
  echo "----------------------------------------------------------------------------------------------------"

  for ((p=0; p<profiles_count; p++)); do
    profile=$(jq -c ".profiles[$p]" <<<"$cfg_json")
    label=$(jq -r '.name' <<<"$profile")
    [[ -z "$label" ]] && continue

    run_dir="$dataset_root/$label"
    mkdir -p "$run_dir"
    clean_run_metadata "$run_dir"

    ovr=$(jq -c '.overrides // {}' <<<"$profile")
    final=$(jq -n \
      --argjson base "$base_json" \
      --argjson ovr "$ovr" \
      --arg resdir "$run_dir/results" \
      --arg gtpath "$gt" \
      '$base as $b | ($b + $ovr) | .output.resultsDir=$resdir | .ratio.source="gt" | .ratio.gtPath=$gtpath | .ratio.gtSample=10000 | .ratio.autoComputeGT=false | .ratio.allowComputeIfMissing=false')

    echo "$final" > "$run_dir/config.json"

    run_log="$run_dir/run.out.log"
    start_ts=$(date +%s)

    # DIRECT EXECUTION (not background)
    java \
      "${JvmArgs[@]}" \
      "-Dcli.dataset=$dataset_name" \
      "-Dcli.profile=$label" \
      "-jar" "$JarPath" \
      "$run_dir/config.json" \
      "$base" \
      "$query" \
      "$run_dir/keys.blob" \
      "$dim" \
      "$run_dir" \
      "$gt" \
      "$Batch" \
      > "$run_log" 2>&1

    exit_code=$?
    end_ts=$(date +%s)
    elapsed=$((end_ts - start_ts))

    metrics=$(extract_metrics "$run_log")
    IFS='|' read -r server_ms client_ms ratio precision <<<"$metrics"

    art_ms="N/A"
    if [[ "$server_ms" != "N/A" && "$client_ms" != "N/A" ]]; then
      art_ms=$(awk -v s="$server_ms" -v c="$client_ms" 'BEGIN {printf "%.1f", s+c}')
    fi

    status="OK"
    [[ $exit_code -ne 0 ]] && status="FAILED"

    printf "%-25s | %-7s | %10s | %10s | %7s | %6s | %9s\n" \
      "$label" "$status" "$server_ms" "$client_ms" "$art_ms" "$ratio" "$precision"

    all_results+=("$dataset_name|$label|$exit_code|$elapsed|$server_ms|$client_ms|$ratio|$precision")
  done

  echo ""
done

echo "RUN SUMMARY"
echo "======================================================================================================"
echo ""
echo "Dataset        | Profile               | Elapsed(s) | ART(ms) | Ratio | Status"
echo "----------------------------------------------------------------------------------------------------"

for result in "${all_results[@]}"; do
  IFS='|' read -r ds prof exit_code elapsed srv cli ratio prec <<<"$result"

  status="OK"
  [[ $exit_code -ne 0 ]] && status="FAILED"

  art="N/A"
  if [[ "$srv" != "N/A" && "$cli" != "N/A" ]]; then
    art=$(awk -v s="$srv" -v c="$cli" 'BEGIN {printf "%.1f", s+c}')
  fi

  printf "%-14s | %-21s | %10d | %7s | %5s | %s\n" \
    "$ds" "$prof" "$elapsed" "$art" "$ratio" "$status"
done

echo ""
echo "Results: $OutRoot"
echo ""