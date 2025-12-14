#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ============================
# FSP-ANN 3-Dataset Runner (Linux) - FIXED
# - Proper sequential execution (no associative arrays)
# - Better error handling
# - Timeout protection
# - Real-time metrics display
# ============================

# ---- Required paths ----
JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
ConfigPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config.json"
OutRoot="/mnt/data/mehran"

# ---- JVM Arguments ----
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
CleanPerRun="true"
TimeoutSec=3600  # 1 hour per run

# ---- Helpers ----
die() {
  echo "❌ ERROR: $*" >&2
  exit 1
}

warn() {
  echo "⚠️  WARNING: $*" >&2
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
  [[ ! -f "$log" ]] && { echo "N/A|N/A|N/A|N/A"; return; }

  # Look for metrics in log
  local server=$(grep -oE 'Server[^\d]*([0-9]+\.[0-9]+)' "$log" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | tail -1 || echo "N/A")
  local client=$(grep -oE 'Client[^\d]*([0-9]+\.[0-9]+)' "$log" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | tail -1 || echo "N/A")
  local ratio=$(grep -oE 'Ratio[^\d]*([0-9]+\.[0-9]+)' "$log" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | tail -1 || echo "N/A")
  local precision=$(grep -oE 'Precision[^\d]*([0-9]+\.[0-9]+)' "$log" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+' | tail -1 || echo "N/A")

  echo "$server|$client|$ratio|$precision"
}

combine_csv_files() {
  local out="$1" name="$2"
  shift 2
  local files=("$@")

  [[ ${#files[@]} -eq 0 ]] && return
  mkdir -p "$(dirname "$out")"
  rm -f "$out"

  local header_done=false
  for f in "${files[@]}"; do
    [[ -f "$f" ]] || continue
    local prof=$(basename $(dirname $(dirname "$f")))

    if [[ "$header_done" == "false" ]]; then
      { echo "${name},$(head -n1 "$f" 2>/dev/null || true)"
        tail -n +2 "$f" 2>/dev/null | awk -v p="$prof" 'NF {print p","$0}' || true
      } > "$out"
      header_done="true"
    else
      tail -n +2 "$f" 2>/dev/null | awk -v p="$prof" 'NF {print p","$0}' >> "$out" || true
    fi
  done
}

# ---- Verification ----
echo "Verifying system..."
command -v java >/dev/null 2>&1 || die "Java not found in PATH"
java -version 2>&1 | head -2
ensure_file "$JarPath"
ensure_file "$ConfigPath"
command -v jq >/dev/null 2>&1 || die "jq not found. Install: sudo apt-get install -y jq"
mkdir -p "$OutRoot"
echo "✅ All requirements satisfied"
echo ""

# ---- Sequential Datasets (NO associative arrays) ----
declare -a DATASET_NAMES=(
  "SIFT1M"
  "glove-100"
  "RedCaps"
)

declare -a DATASET_DIMS=(
  "128"
  "100"
  "512"
)

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

# ---- Main Loop ----
declare -a all_results=()

for idx in "${!DATASET_NAMES[@]}"; do
  dataset_name="${DATASET_NAMES[$idx]}"
  dim="${DATASET_DIMS[$idx]}"
  base="${DATASET_BASE[$idx]}"
  query="${DATASET_QUERY[$idx]}"
  gt="${DATASET_GT[$idx]}"

  # Verify files exist
  if [[ ! -f "$base" ]]; then
    warn "Skipping $dataset_name (missing base: $base)"
    continue
  fi
  if [[ ! -f "$query" ]]; then
    warn "Skipping $dataset_name (missing query: $query)"
    continue
  fi
  if [[ ! -f "$gt" ]]; then
    warn "Skipping $dataset_name (missing GT: $gt)"
    continue
  fi

  dataset_root="$OutRoot/$dataset_name"
  mkdir -p "$dataset_root"

  echo ""
  echo "  Dataset: $dataset_name (Dim=$dim)"
  echo ""

  # Read config and profiles
  cfg_json="$(cat "$ConfigPath")"
  base_json=$(jq -c 'del(.profiles)' <<<"$cfg_json")
  profiles_count=$(jq '.profiles | length' <<<"$cfg_json")

  if [[ "$profiles_count" -eq 0 ]]; then
    warn "No profiles in config.json"
    continue
  fi

  echo "Profile                   | Status      | Server(ms) | Client(ms) | ART(ms) | Ratio  | Precision"
  echo "──────────────────────────────────────────────────────────────────────────────────────────────────"

  # Iterate profiles
  for ((p=0; p<profiles_count; p++)); do
    profile=$(jq -c ".profiles[$p]" <<<"$cfg_json")
    label=$(jq -r '.name' <<<"$profile")
    [[ -z "$label" ]] && continue

    run_dir="$dataset_root/$label"
    mkdir -p "$run_dir"
    [[ "$CleanPerRun" == "true" ]] && clean_run_metadata "$run_dir"

    # Merge config
    ovr=$(jq -c '.overrides // {}' <<<"$profile")
    final=$(jq -n \
      --argjson base "$base_json" \
      --argjson ovr "$ovr" \
      --arg resdir "$run_dir/results" \
      --arg gtpath "$gt" \
      '$base as $b | ($b + $ovr) |
       .output.resultsDir = $resdir |
       .ratio.source = "gt" |
       .ratio.gtPath = $gtpath |
       .ratio.gtSample = 10000 |
       .ratio.autoComputeGT = false |
       .ratio.allowComputeIfMissing = false')

    echo "$final" > "$run_dir/config.json"

    # Save command
    {
      echo "java \\"
      for arg in "${JvmArgs[@]}"; do
        echo "  '$arg' \\"
      done
      echo "  '-Dcli.dataset=$dataset_name' \\"
      echo "  '-Dcli.profile=$label' \\"
      echo "  '-jar' '$JarPath' \\"
      echo "  '$run_dir/config.json' \\"
      echo "  '$base' \\"
      echo "  '$query' \\"
      echo "  '$run_dir/keys.blob' \\"
      echo "  '$dim' \\"
      echo "  '$run_dir' \\"
      echo "  '$gt' \\"
      echo "  '$Batch'"
    } > "$run_dir/cmdline.sh"
    chmod +x "$run_dir/cmdline.sh"

    # Execute with timeout
    run_log="$run_dir/run.out.log"
    start_ts=$(date +%s%N)

    timeout "$TimeoutSec" java \
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
      > "$run_log" 2>&1 &

    java_pid=$!

    # Wait with progress
    wait $java_pid
    exit_code=$?

    end_ts=$(date +%s%N)
    elapsed_sec=$(( (end_ts - start_ts) / 1000000000 ))

    # Handle exit codes
    if [[ $exit_code -eq 124 ]]; then
      status="❌ TIMEOUT"
      exit_code=124
    elif [[ $exit_code -eq 130 ]]; then
      status="❌ INTERRUPTED"
    elif [[ $exit_code -eq 0 ]]; then
      status="✅ OK"
    else
      status="❌ EXIT $exit_code"
    fi

    # Extract metrics
    metrics=$(extract_metrics "$run_log")
    IFS='|' read -r server_ms client_ms ratio precision <<<"$metrics"

    # Calculate total ART
    art_ms="N/A"
    if [[ "$server_ms" != "N/A" && "$client_ms" != "N/A" ]]; then
      art_ms=$(awk -v s="$server_ms" -v c="$client_ms" 'BEGIN {printf "%.1f", s+c}')
    fi

    # Display result
    printf "%-25s | %-11s | %10s | %10s | %7s | %6s | %9s  [%ds]\n" \
      "$label" "$status" "$server_ms" "$client_ms" "$art_ms" "$ratio" "$precision" "$elapsed_sec"

    # Save result
    all_results+=("${dataset_name}|${label}|${exit_code}|${elapsed_sec}|${server_ms}|${client_ms}|${ratio}|${precision}")

    # If interrupted, stop this dataset
    if [[ $exit_code -eq 130 ]]; then
      echo ""
      echo "⚠️  Skipping remaining profiles for $dataset_name (interrupted)"
      break
    fi
  done

  # Combine results per dataset
  mapfile -t results_files < <(find "$dataset_root" -type f -path "*/results/results_table.csv" 2>/dev/null || true)
  mapfile -t prec_files < <(find "$dataset_root" -type f -path "*/results/global_precision.csv" 2>/dev/null || true)
  mapfile -t topk_files < <(find "$dataset_root" -type f -path "*/results/topk_evaluation.csv" 2>/dev/null || true)

  [[ ${#results_files[@]} -gt 0 ]] && combine_csv_files "$dataset_root/combined_results.csv" "profile" "${results_files[@]}"
  [[ ${#prec_files[@]} -gt 0 ]] && combine_csv_files "$dataset_root/combined_precision.csv" "profile" "${prec_files[@]}"
  [[ ${#topk_files[@]} -gt 0 ]] && combine_csv_files "$dataset_root/combined_evaluation.csv" "profile" "${topk_files[@]}"

done

# ---- Final Summary ----
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║                      RUN SUMMARY                                  ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Dataset        | Profile               | Elapsed(s) | ART(ms) | Ratio | Status"
echo "──────────────────────────────────────────────────────────────────────────────"

for result in "${all_results[@]}"; do
  IFS='|' read -r ds prof exit_code elapsed srv cli ratio prec <<<"$result"

  if [[ $exit_code -eq 0 ]]; then
    status="✅"
  elif [[ $exit_code -eq 124 ]]; then
    status="⏱️ TIMEOUT"
  elif [[ $exit_code -eq 130 ]]; then
    status="INTERRUPTED"
  else
    status="Failed"
  fi

  art="N/A"
  if [[ "$srv" != "N/A" && "$cli" != "N/A" ]]; then
    art=$(awk -v s="$srv" -v c="$cli" 'BEGIN {printf "%.1f", s+c}')
  fi

  printf "%-14s | %-21s | %10d | %7s | %5s | %s\n" \
    "$ds" "$prof" "$elapsed" "$art" "$ratio" "$status"
done

echo ""
echo "Results: $OutRoot"
echo "Combined CSVs created per dataset"
echo ""