#!/usr/bin/env bash
set -Eeuo pipefail

# =========================
# FSP-ANN MULTI-DATASET RUNNER (FINAL)
# =========================

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran"
Batch=100000

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx16g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
ensure_file "$JarPath"
mkdir -p "$OutRoot"

# ================= DATASETS =================

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

# ================= METRICS =================

extract_metrics() {
  local results_dir="$1"
  local art="NA"
  local ratio="NA"

  if [[ -f "$results_dir/metrics_summary.txt" ]]; then
    art=$(grep -oP 'ART\(ms\)=\K[0-9.]+' "$results_dir/metrics_summary.txt" || echo "NA")
    ratio=$(grep -oP 'AvgRatio=\K[0-9.]+' "$results_dir/metrics_summary.txt" || echo "NA")
  fi

  echo "$art,$ratio"
}

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OutRoot/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio" > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP =================

for ds in SIFT1M glove-100 RedCaps; do
  cfg="${DATASET_CONFIG[$ds]}"
  dim="${DATASET_DIM[$ds]}"
  base="${DATASET_BASE[$ds]}"
  query="${DATASET_QUERY[$ds]}"
  gt="${DATASET_GT[$ds]}"

  ensure_file "$cfg"
  ensure_file "$base"
  ensure_file "$query"
  ensure_file "$gt"

  ds_root="$OutRoot/$ds"
  mkdir -p "$ds_root"

  DATASET_SUMMARY="$ds_root/dataset_summary.csv"
  echo "profile,ART_ms,AvgRatio" > "$DATASET_SUMMARY"

  CFG_JSON="$(cat "$cfg")"
  BASE_JSON="$(jq -c 'del(.profiles)' <<<"$CFG_JSON")"
  PROFILE_COUNT="$(jq '.profiles | length' <<<"$CFG_JSON")"

  for ((i=0; i<PROFILE_COUNT; i++)); do
    profile="$(jq -c ".profiles[$i]" <<<"$CFG_JSON")"
    name="$(jq -r '.name' <<<"$profile")"
    overrides="$(jq -c '.overrides // {}' <<<"$profile")"

    run_dir="$ds_root/$name"
    mkdir -p "$run_dir/results"
    rm -f "$run_dir/keys.blob"

    final_cfg="$(jq -n '
      def deepmerge(a; b):
        reduce (b | keys[]) as $k
          (a;
           if (a[$k] | type) == "object" and (b[$k] | type) == "object"
           then .[$k] = deepmerge(a[$k]; b[$k])
           else .[$k] = b[$k]
           end);

      deepmerge($base; $ovr)
      | .output.resultsDir = $resdir
      | .ratio.source = "gt"
      | .ratio.gtPath = $gtpath
      | .ratio.autoComputeGT = false
    ' \
    --argjson base "$BASE_JSON" \
    --argjson ovr  "$overrides" \
    --arg resdir "$run_dir/results" \
    --arg gtpath "$gt"
    )"

    echo "$final_cfg" > "$run_dir/config.json"

    # Guard: ensure overrides applied
  expected_div="$(jq -r '.paper.divisions' "$run_dir/config.json")"
  [[ "$expected_div" != "8" ]] \
  || die "paper.divisions still default(8) for $ds / $name"


    log="$run_dir/run.log"

    if ! java "${JvmArgs[@]}" \
        -Dcli.dataset="$ds" \
        -Dcli.profile="$name" \
        -jar "$JarPath" \
        "$run_dir/config.json" \
        "$base" \
        "$query" \
        "$run_dir/keys.blob" \
        "$dim" \
        "$run_dir" \
        "$gt" \
        "$Batch" \
        >"$log" 2>&1; then
      echo "FAILED: $ds / $name" >&2
      continue
    fi

    metrics=$(extract_metrics "$run_dir/results")
    IFS=',' read -r art ratio <<<"$metrics"

    echo "$name,$art,$ratio" >> "$DATASET_SUMMARY"
    echo "$ds,$name,$art,$ratio" >> "$GLOBAL_SUMMARY"

    printf "%-10s | ART=%8s ms | Ratio=%s\n" "$name" "$art" "$ratio"
  done
done

echo "DONE"
echo "Per-dataset CSVs: $OutRoot/*/dataset_summary.csv"
echo "Global CSV:       $GLOBAL_SUMMARY"
