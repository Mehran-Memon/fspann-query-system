#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# FSP-ANN COMPLETE SWEEP RUNNER (PROFILE-AWARE QUERY BUDGET)
# ============================================================

export TMPDIR="/mnt/data/mehran/tmp"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$TMPDIR"

mkdir -p "$TMPDIR"
chmod 777 "$TMPDIR"

echo "=========================================="
echo "TMPDIR: $TMPDIR ($(df -h "$TMPDIR" | tail -1 | awk '{print $4}') free)"
echo "Root:   /tmp ($(df -h / | tail -1 | awk '{print $4}') free)"
echo "=========================================="
echo ""

JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OUT_ROOT="/mnt/data/mehran"
BATCH_SIZE=100000

# ========== QUERY BUDGETS ==========
FULL_QUERIES=10000
ABLATION_QUERIES=1000

JVM_ARGS=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xms300g"
  "-Xmx325g"
  "-Xmn128g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  "-Djava.io.tmpdir=$TMPDIR"
  "-XX:+HeapDumpOnOutOfMemoryError"
  "-XX:HeapDumpPath=$TMPDIR/heap_dumps"
)

die() { echo "ERROR: $*" >&2; exit 1; }
need_file() { [[ -f "$1" ]] || die "Missing: $1"; }
need_cmd() { command -v "$1" >/dev/null || die "Missing command: $1"; }

need_cmd java
need_cmd jq
need_cmd od
need_cmd dd
need_file "$JAR"
mkdir -p "$OUT_ROOT"

# ========== DISK SPACE CHECK ==========
AVAILABLE_GB=$(df -BG "$OUT_ROOT" | tail -1 | awk '{print $4}' | sed 's/G//')
REQUIRED_GB=80
if [ "$AVAILABLE_GB" -lt "$REQUIRED_GB" ]; then
  die "Insufficient disk: ${AVAILABLE_GB}GB available, need ${REQUIRED_GB}GB"
fi
echo "✓ Disk space: ${AVAILABLE_GB}GB available"

# ========== DATASET CONFIGURATION ==========
declare -A CFG=(
  ["SIFT1M"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
  ["glove-100"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_glove100.json"
  ["RedCaps"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_redcaps.json"
)

declare -A DIM=(
  ["SIFT1M"]=128
  ["glove-100"]=100
  ["RedCaps"]=512
)

declare -A BASE=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
)

declare -A QUERY=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
)

declare -A GT=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
)

for ds in SIFT1M glove-100 RedCaps; do
  need_file "${CFG[$ds]}"
  need_file "${BASE[$ds]}"
  need_file "${QUERY[$ds]}"
  need_file "${GT[$ds]}"
done

ONLY_DATASET=""
ONLY_PROFILE=""

FAILED_PROFILES=()
SWEEP_START=$(date +%s)

echo "FSP-ANNS SWEEP"
echo "Started: $(date)"
echo ""

GLOBAL_SUMMARY="$OUT_ROOT/global_summary.csv"
echo "dataset,profile,queries,ART_ms,AvgRatio,recall_at_100" > "$GLOBAL_SUMMARY"

# ============================================================
# Query budget logic (FIXED)
#   - Treat any explicit ablation token as ABLATION (even if name ends with _HIGH)
#   - Only then treat FAST/BALANCED/HIGH/BASE suffixes as FULL
# ============================================================
query_budget_for_profile() {
  local name="$1"

  # Explicit ablation markers first (strong rule)
  if [[ "$name" =~ (_M[0-9]+|_LAMBDA[0-9]+|_DIV[0-9]+|_TABLES?[0-9]+) ]]; then
    echo "$ABLATION_QUERIES"
    return
  fi

  # Full evaluation profiles by suffix
  if [[ "$name" =~ (_FAST|_BALANCED|_HIGH|_BASE)$ ]]; then
    echo "$FULL_QUERIES"
    return
  fi

  # Default safe
  echo "$ABLATION_QUERIES"
}

# ============================================================
# Binary-safe .fvecs truncation (first N vectors)
# ============================================================
extract_fvecs() {
  local src="$1"
  local dst="$2"
  local n="$3"

  [[ "$n" -gt 0 ]] || die "extract_fvecs: n must be > 0"

  # First 4 bytes is dimension (int32 little-endian). od outputs decimal.
  local dim
  dim=$(od -An -t u4 -N4 "$src" | tr -d ' ')
  [[ -n "$dim" ]] || die "extract_fvecs: failed to read dim from $src"

  local bytes_per_vec=$((4 + 4 * dim))
  local total_bytes=$((bytes_per_vec * n))

  dd if="$src" of="$dst" bs=1 count="$total_bytes" status=none
}

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  echo ""
  echo "=========================================="
  echo "DATASET: $ds"
  echo "=========================================="

  cfg="${CFG[$ds]}"
  dim="${DIM[$ds]}"
  base="${BASE[$ds]}"
  query="${QUERY[$ds]}"
  gt="${GT[$ds]}"

  PROFILE_COUNT=$(jq '.profiles | length' "$cfg")
  [[ "$PROFILE_COUNT" -gt 0 ]] || die "No profiles in $cfg"
  echo "Profiles: $PROFILE_COUNT"

  BASE_JSON=$(jq -c 'del(.profiles)' "$cfg")
  ds_root="$OUT_ROOT/$ds"
  mkdir -p "$ds_root"
  echo "dataset,profile,queries,ART_ms,AvgRatio,recall_at_100" > "$ds_root/dataset_summary.csv"

  for ((i=0;i<PROFILE_COUNT;i++)); do
    prof=$(jq -c ".profiles[$i]" "$cfg")
    name=$(jq -r '.name' <<<"$prof")

    # Skip early (no side effects)
    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue

    echo ""
    echo "[$ds] $((i+1))/$PROFILE_COUNT: $name"
    echo "  Started: $(date +%H:%M:%S)"

    overrides=$(jq -c '.overrides // {}' <<<"$prof")

    run_dir="$ds_root/$name"
    rm -rf "$run_dir"
    mkdir -p "$run_dir/results"

    # Decide query budget & create truncated query file if needed
    QUERY_LIMIT=$(query_budget_for_profile "$name")
    if [[ "$QUERY_LIMIT" -eq "$FULL_QUERIES" ]]; then
      PROFILE_QUERY="$query"
      echo "  Queries: FULL ($QUERY_LIMIT)"
    else
      PROFILE_QUERY="$run_dir/query_${QUERY_LIMIT}.fvecs"
      echo "  Queries: ABLATION ($QUERY_LIMIT)"
      extract_fvecs "$query" "$PROFILE_QUERY" "$QUERY_LIMIT"
    fi

    # Merge config
    final_cfg=$(jq -n '
      def deepmerge(a;b):
        reduce (b|keys_unsorted[]) as $k (a;
          .[$k]=(if (a[$k]|type)=="object" and (b[$k]|type)=="object"
                 then deepmerge(a[$k];b[$k]) else b[$k] end));
      deepmerge($base;$ovr)
      | .ratio.gtPath=$gt
      | .output.resultsDir=$res
    ' --argjson base "$BASE_JSON" \
      --argjson ovr "$overrides" \
      --arg gt "$gt" \
      --arg res "$run_dir/results")

    echo "$final_cfg" > "$run_dir/config.json"

    start_ts=$(date +%s)

    if ! java "${JVM_ARGS[@]}" -jar "$JAR" \
      "$run_dir/config.json" "$base" "$PROFILE_QUERY" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$BATCH_SIZE" \
      >"$run_dir/run.log" 2>&1; then
        echo "  ✗ FAILED"
        FAILED_PROFILES+=("$ds/$name")
        continue
    fi

    end_ts=$(date +%s)
    duration=$((end_ts - start_ts))
    mins=$((duration / 60))
    echo "  ✓ DONE in ${mins}m"

    acc="$run_dir/results/summary.csv"
    if [[ ! -f "$acc" ]]; then
      echo "  ⚠️  No summary.csv"
      FAILED_PROFILES+=("$ds/$name (no summary)")
      continue
    fi

    DATA_ROW=$(tail -1 "$acc")

    avg_art=$(echo "$DATA_ROW" | awk -F',' '{print $13}')      # avg_art_ms
    avg_ratio=$(echo "$DATA_ROW" | awk -F',' '{print $7}')     # avg_distance_ratio
    recall_100=$(echo "$DATA_ROW" | awk -F',' '{print $27}')   # recall_at_100

    if [[ -z "$avg_art" || -z "$avg_ratio" || -z "$recall_100" ]]; then
      echo "  ⚠️  Failed to extract metrics"
      FAILED_PROFILES+=("$ds/$name (extraction failed)")
      continue
    fi

    echo "$ds,$name,$QUERY_LIMIT,$avg_art,$avg_ratio,$recall_100" >> "$ds_root/dataset_summary.csv"
    echo "$ds,$name,$QUERY_LIMIT,$avg_art,$avg_ratio,$recall_100" >> "$GLOBAL_SUMMARY"

    echo "  Recall@100: $recall_100 | ART: ${avg_art}ms | Ratio: $avg_ratio"
  done

  echo ""
  echo "✓ $ds completed"
done

SWEEP_END=$(date +%s)
SWEEP_DURATION=$((SWEEP_END - SWEEP_START))
SWEEP_HOURS=$((SWEEP_DURATION / 3600))
SWEEP_MINS=$(((SWEEP_DURATION % 3600) / 60))

echo ""
echo "SWEEP COMPLETE"
echo "Finished: $(date)"
echo "Duration: ${SWEEP_HOURS}h ${SWEEP_MINS}m"
echo ""

if [ ${#FAILED_PROFILES[@]} -gt 0 ]; then
  echo "⚠️  FAILED (${#FAILED_PROFILES[@]}):"
  for fail in "${FAILED_PROFILES[@]}"; do
    echo "  - $fail"
  done
else
  echo "✅ ALL PROFILES SUCCESS!"
fi

echo ""
echo "Results:"
echo "  Global: $GLOBAL_SUMMARY"
echo "  SIFT1M: $OUT_ROOT/SIFT1M/dataset_summary.csv"
echo "  Glove:  $OUT_ROOT/glove-100/dataset_summary.csv"
echo "  RedCaps:$OUT_ROOT/RedCaps/dataset_summary.csv"
echo "=========================================="
