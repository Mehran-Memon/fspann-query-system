#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# FSP-ANN MULTI-DATASET SWEEP RUNNER (PAPER-GRADE)
# ============================================================

# -------------------- PATHS --------------------
JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran"
Batch=100000
QUERY_LIMIT=200

# -------------------- JVM ----------------------
JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx16g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

# -------------------- SAFETY -------------------
die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }
ensure_cmd() { command -v "$1" >/dev/null || die "$1 not found"; }

ensure_cmd java
ensure_cmd jq
ensure_cmd python3
ensure_file "$JarPath"
mkdir -p "$OutRoot"

# ================= DATASETS ====================

declare -A DATASET_CONFIG=(
  ["SIFT1M"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_sift1m.json"
  ["glove-100"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_glove100.json"
  ["RedCaps"]="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config_redcaps.json"
)

declare -A DATASET_DIM=(
  ["SIFT1M"]=128
  ["glove-100"]=100
  ["RedCaps"]=512
)

declare -A DATASET_BASE=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_base.fvecs"
)

declare -A DATASET_QUERY=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query.fvecs"
)

declare -A DATASET_GT=(
  ["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
  ["glove-100"]="/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs"
  ["RedCaps"]="/mnt/data/mehran/Datasets/redcaps/redcaps_query_groundtruth.ivecs"
)

# ================= DEBUG SWITCHES =================
ONLY_DATASET=""
ONLY_PROFILE=""

# ================= METRICS ========================

extract_metrics() {
  local csv="$1/profiler_metrics.csv"
  [[ -f "$csv" ]] || { echo "NA,NA"; return; }

  python3 <<EOF
import pandas as pd
df = pd.read_csv("$csv")
art = df["clientMs"].mean()
ratio = df["ratio"].mean()
print(f"{art:.2f},{ratio:.4f}")
EOF
}

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OutRoot/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio" > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP ======================

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  cfg="${DATASET_CONFIG[$ds]}"
  dim="${DATASET_DIM[$ds]}"
  base="${DATASET_BASE[$ds]}"
  query="${DATASET_QUERY[$ds]}"
  gt="${DATASET_GT[$ds]}"

  ensure_file "$cfg" "$base" "$query" "$gt"

  CFG_JSON="$(cat "$cfg")"
  BASE_JSON="$(jq -c 'del(.profiles)' <<<"$CFG_JSON")"

  ds_root="$OutRoot/$ds"
  mkdir -p "$ds_root"
  echo "profile,ART_ms,AvgRatio" > "$ds_root/dataset_summary.csv"

  PROFILE_COUNT="$(jq '.profiles | length' <<<"$CFG_JSON")"
  ran_any=false

  for ((i=0; i<PROFILE_COUNT; i++)); do
    profile="$(jq -c ".profiles[$i]" <<<"$CFG_JSON")"
    name="$(jq -r '.name' <<<"$profile")"

    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue

    ran_any=true
    overrides="$(jq -c '.overrides // {}' <<<"$profile")"

    run_dir="$ds_root/$name"
    mkdir -p "$run_dir/results"
    rm -f "$run_dir/keys.blob"
    rm -rf "$run_dir/metadata"

    # -------- FIXED jq MERGE (portable) --------
    final_cfg="$(jq -n '
      def deepmerge(a; b):
        reduce (b | keys_unsorted[]) as $k
          (a;
           .[$k] =
             if (a[$k] | type) == "object" and (b[$k] | type) == "object"
             then deepmerge(a[$k]; b[$k])
             else b[$k]
             end
          );

      deepmerge($base; $ovr)
      | .output.resultsDir = $resdir
      | .ratio.source = "gt"
      | .ratio.gtPath = $gtpath
      | .ratio.allowComputeIfMissing = false
    ' \
      --argjson base "$BASE_JSON" \
      --argjson ovr "$overrides" \
      --arg resdir "$run_dir/results" \
      --arg gtpath "$gt"
    )"

    echo "$final_cfg" > "$run_dir/config.json"

    log="$run_dir/run.log"

    java "${JvmArgs[@]}" \
      -Dcli.dataset="$ds" \
      -Dcli.profile="$name" \
      -Dquery.limit="$QUERY_LIMIT" \
      -jar "$JarPath" \
      "$run_dir/config.json" \
      "$base" "$query" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$Batch" \
      >"$log" 2>&1 || {
        echo "FAILED: $ds / $name"
        tail -60 "$log"
        exit 1
      }

    metrics="$(extract_metrics "$run_dir/results")"
    IFS=',' read -r art ratio <<<"$metrics"

    echo "$name,$art,$ratio" >> "$ds_root/dataset_summary.csv"
    echo "$ds,$name,$art,$ratio" >> "$GLOBAL_SUMMARY"

    printf "%-10s | %-12s | ART=%7sms | Ratio=%s\n" "$ds" "$name" "$art" "$ratio"
  done

  [[ "$ran_any" == true ]] || die "No profile executed for dataset $ds"
done
