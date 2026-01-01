#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============================================================
# FSP-ANN MULTI-DATASET SWEEP RUNNER (PAPER-GRADE, JAVA-ONLY)
# ============================================================

# -------------------- PATHS --------------------
JAR="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OUT_ROOT="/mnt/data/mehran"
BATCH_SIZE=100000

# -------------------- JVM ----------------------
JVM_ARGS=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx24g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  #"-Dreenc.fullMigration=true"

)

# -------------------- SAFETY -------------------
die() { echo "ERROR: $*" >&2; exit 1; }
need_file() { [[ -f "$1" ]] || die "Missing file: $1"; }
need_cmd() { command -v "$1" >/dev/null || die "Missing command: $1"; }

need_cmd java
need_cmd jq
need_cmd jshell
need_file "$JAR"

mkdir -p "$OUT_ROOT"

# ================= DATASETS ====================

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

# ================= FILTERS =====================
ONLY_DATASET=""
ONLY_PROFILE=""

# ================= K VARIANTS ==================
KLIST="20,40,60,80,100"

# ================= GLOBAL SUMMARY =================

GLOBAL_SUMMARY="$OUT_ROOT/global_summary.csv"
echo "dataset,profile,ART_ms,AvgRatio,ratio@20,ratio@40,ratio@60,ratio@80,ratio@100" \
  > "$GLOBAL_SUMMARY"

# ================= MAIN LOOP ======================

DATASETS=(SIFT1M glove-100 RedCaps)
[[ -n "$ONLY_DATASET" ]] && DATASETS=("$ONLY_DATASET")

for ds in "${DATASETS[@]}"; do
  echo "DATASET START: $ds"
  cfg="${CFG[$ds]}"
  dim="${DIM[$ds]}"
  base="${BASE[$ds]}"
  query="${QUERY[$ds]}"
  gt="${GT[$ds]}"

  BASE_JSON="$(jq -c 'del(.profiles)' "$cfg")"

  ds_root="$OUT_ROOT/$ds"
  mkdir -p "$ds_root"
  echo "profile,ART_ms,AvgRatio,ratio@20,ratio@40,ratio@60,ratio@80,ratio@100" \
    > "$ds_root/dataset_summary.csv"

  PROFILE_COUNT="$(jq '.profiles | length' "$cfg")"
  ran_any=false

  for ((i=0;i<PROFILE_COUNT;i++)); do
    prof="$(jq -c ".profiles[$i]" "$cfg")"
    name="$(jq -r '.name' <<<"$prof")"
    [[ -n "$ONLY_PROFILE" && "$name" != "$ONLY_PROFILE" ]] && continue
    ran_any=true

    overrides="$(jq -c '.overrides // {}' <<<"$prof")"
    run_dir="$ds_root/$name"
    rm -rf "$run_dir"; mkdir -p "$run_dir/results"

    final_cfg="$(jq -n '
      def deepmerge(a;b):
        reduce (b|keys_unsorted[]) as $k (a;
          .[$k]=(if (a[$k]|type)=="object" and (b[$k]|type)=="object"
                 then deepmerge(a[$k];b[$k]) else b[$k] end));
      deepmerge($base;$ovr)
      | .output.resultsDir=$res
      | .ratio.source="gt"
      | .ratio.gtPath=$gt
      | .ratio.allowComputeIfMissing=false
    ' --argjson base "$BASE_JSON" --argjson ovr "$overrides" \
       --arg res "$run_dir/results" --arg gt "$gt")"

    echo "$final_cfg" > "$run_dir/config.json"
    sha256sum "$run_dir/config.json" > "$run_dir/config.sha256"

    echo "RUNNING PROFILE: $ds | $name"

    start_ts=$(date +%s)

    if ! java "${JVM_ARGS[@]}" -jar "$JAR" \
      "$run_dir/config.json" "$base" "$query" "$run_dir/keys.blob" \
      "$dim" "$run_dir" "$gt" "$BATCH_SIZE" \
      >"$run_dir/run.log" 2>&1; then
        echo "FAILED: $ds | $name (see run.log)" >&2
        continue
    fi

    end_ts=$(date +%s)
    echo "COMPLETED: $ds | $name | runtime=$((end_ts - start_ts))s"

    acc="$run_dir/results/summary.csv"
    [[ -f "$acc" ]] || die "Missing summary.csv"

    row="$(tail -n 1 "$acc")"
    IFS=',' read -r \
      dataset profile m lambda divisions index_ms \
      avg_ratio avg_precision avg_recall \
      avg_server avg_client avg_art avg_decrypt \
      p20 p40 p60 p80 p100 \
      r20 r40 r60 r80 r100 \
    <<<"$row"

    echo "$name,$avg_art,$avg_ratio,$p20,$p40,$p60,$p80,$p100,$((end_ts - start_ts))" \
    >> "$ds_root/dataset_summary.csv"
    echo "$ds,$name,$avg_art,$avg_ratio,$p20,$p40,$p60,$p80,$p100" >> "$GLOBAL_SUMMARY"
  done

  [[ "$ran_any" == true ]] || die "No profile executed for $ds"
done
