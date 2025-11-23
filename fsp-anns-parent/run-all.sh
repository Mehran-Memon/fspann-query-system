#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ============================
# FSP-ANN (multi-dataset, multi-profile batch) — Linux bash
# - Disables GT recompute; uses explicit GT path
# - Integrates EvaluationSummaryPrinter (expects Java side in place)
# - Consolidates per-profile results into per-dataset combined files
# ============================

# ---- required paths ----
JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
ConfigPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/config/src/main/resources/config.json"
OutRoot="/mnt/data/mehran/fsp-ann"

# ---- alpha and JVM system props ----
Alpha="0.1"
JvmArgs=(
  "-XX:+UseG1GC" "-XX:MaxGCPauseMillis=200" "-XX:+AlwaysPreTouch"
  "-Ddisable.exit=true"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
  "-Dreenc.minTouched=5000"
  "-Dreenc.batchSize=2000"
  "-Dlog.progress.everyN=0"
  "-Dpaper.buildThreshold=2000000"
  "-Djava.security.egd=file:/dev/./urandom"
  "-Dpaper.alpha=${Alpha}"
)

# ---- app batch size arg ----
Batch="100000"

# ---- profile filter ----
# - "" => all
# - exact name => that profile only
# - wildcard => pattern (e.g., "*ell3*")
OnlyProfile=""

# ---- toggles ----
CleanPerRun="true"
QueryOnly="false"
RestoreVersion=""

# ---------- helpers ----------
die() { echo "Error: $*" >&2; exit 1; }

have_java() { command -v java >/dev/null 2>&1; }

safe_resolve() {
  # usage: safe_resolve <path> [allowMissing=true|false]
  local p="${1:-}" allow="${2:-false}"
  if [[ "$allow" == "true" && ! -e "$p" ]]; then
    printf "%s" "$p"; return 0
  fi
  if command -v realpath >/dev/null 2>&1; then
    realpath "$p" 2>/dev/null || printf "%s" "$p"
  else
    printf "%s" "$p"
  fi
}

ensure_files() {
  local base="$1" query="$2" gt="$3" ok=0
  [[ -f "$base" ]] || { echo "Missing base:  $base" >&2; ok=1; }
  [[ -f "$query" ]] || { echo "Missing query: $query" >&2; ok=1; }
  [[ -f "$gt"    ]] || { echo "Missing GT:    $gt" >&2; ok=1; }
  return $ok
}

fast_delete_dir() {
  local target="$1"
  [[ -d "$target" ]] || { rm -f "$target" >/dev/null 2>&1 || true; return 0; }
  local empty="/tmp/empty_$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
  mkdir -p "$empty"
  rsync -a --delete "${empty}/" "${target}/" >/dev/null 2>&1 || true
  rm -rf "$target" "$empty" 2>/dev/null || true
}

clean_run_metadata() {
  local run_dir="$1"
  local paths=("$run_dir/metadata" "$run_dir/points" "$run_dir/results")
  for p in "${paths[@]}"; do
    [[ -e "$p" ]] && { echo "Cleaning $p ..."; fast_delete_dir "$p"; }
    mkdir -p "$p"
  done
}

sha256_of() {
  local p="$1"
  [[ -f "$p" ]] || { printf ""; return 0; }
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$p" | awk '{print $1}'
  else
    openssl dgst -sha256 "$p" | awk '{print $2}'
  fi
}

detect_fvecs_dim() {
  # Reads first 4 bytes (int32 little-endian) => dim
  local path="$1"
  python - "$path" <<'PY'
import sys,struct
p=sys.argv[1]
with open(p,'rb') as f:
    b=f.read(4)
    if len(b)!=4:
        print("", end="")
        sys.exit(0)
    print(struct.unpack('<i',b)[0], end="")
PY
}

json_merge_with_overrides() {
  # jq-merge base JSON with overrides JSON (top-level keys; merge objects at keys)
  local base_json="$1" ovr_json="$2"
  jq -c --argjson ovr "${ovr_json}" '
    def merge_at_top(a;b):
      reduce (b|keys[]) as $k (a;
        if (a[$k]|type=="object") and (b[$k]|type=="object")
          then .[$k] = (a[$k] + b[$k])
        else .[$k] = b[$k]
        end);
    merge_at_top(.; $ovr)
  ' <<< "$base_json"
}

# --- helpers for combining results ---
profile_from_path() {
  # input: /.../<dataset>/<profile>/results/<file>
  local p="$1"
  local parent="$(dirname "$p")"  # .../<profile>/results
  basename "$(dirname "$parent")" # <profile>
}

combine_csv_with_profile() {
  # combine_csv_with_profile <out_csv> <colname> <file1> <file2> ...
  local out="$1"; shift
  local colname="$1"; shift
  local header_done="false"
  mkdir -p "$(dirname "$out")"
  rm -f "$out"
  for f in "$@"; do
    [[ -f "$f" ]] || continue
    local prof; prof="$(profile_from_path "$f")"
    if [[ "$header_done" == "false" ]]; then
      { echo "${colname},$(head -n1 "$f")"
        tail -n +2 "$f" | awk -v p="$prof" 'NF{print p","$0}'
      } > "$out"
      header_done="true"
    else
      tail -n +2 "$f" | awk -v p="$prof" 'NF{print p","$0}' >> "$out"
    fi
  done
}

concat_txt_with_profile() {
  # concat_txt_with_profile <out_txt> <file1> <file2> ...
  local out="$1"; shift
  mkdir -p "$(dirname "$out")"
  rm -f "$out"
  for f in "$@"; do
    [[ -f "$f" ]] || continue
    local prof; prof="$(profile_from_path "$f")"
    {
      echo "===== PROFILE: ${prof} ====="
      cat "$f"
      echo
    } >> "$out"
  done
}

# ---------- sanity ----------
command -v jq >/dev/null 2>&1 || die "jq not found. Install: sudo apt-get install -y jq"
have_java || die "Java not found in PATH. Install JDK or add 'java' to PATH."
[[ -f "$JarPath"    ]] || die "Jar not found: $JarPath"
[[ -f "$ConfigPath" ]] || die "Config not found: $ConfigPath"
mkdir -p "$OutRoot"

# ---------- read config ----------
cfg_json="$(cat "$ConfigPath")"
profiles_count="$(jq '.profiles|length' <<<"$cfg_json")"
[[ "$profiles_count" -gt 0 ]] || die "config.json must contain a non-empty 'profiles' array."

# Build base payload (back-compat w/ optional 'base' node)
if jq -e '.base' >/dev/null 2>&1 <<<"$cfg_json"; then
  base_json="$(jq -c '.base' <<<"$cfg_json")"
else
  base_json="$(jq -c 'del(.profiles)' <<<"$cfg_json")"
fi

# ---------- dataset matrix ----------
# If Dim is "", it will be auto-detected from base file header.
# Format: Name::Base::Query::GT::Dim
DATASETS=(
#  "Enron::/mnt/data/mehran/Datasets/Enron/enron_base.fvecs::/mnt/data/mehran/Datasets/Enron/enron_query.fvecs::/mnt/data/mehran/Datasets/Enron/enron_groundtruth.ivecs::1369"
#  "audio::/mnt/data/mehran/Datasets/audio/audio_base.fvecs::/mnt/data/mehran/Datasets/audio/audio_query.fvecs::/mnt/data/mehran/Datasets/audio/audio_groundtruth.ivecs::192"
  "SIFT1M::/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs::/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs::/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs::128"
  "synthetic_128::/mnt/data/mehran/Datasets/synthetic_128/base.fvecs::/mnt/data/mehran/Datasets/synthetic_128/query.fvecs::/mnt/data/mehran/Datasets/synthetic_128/groundtruth.ivecs::128"
  "synthetic_256::/mnt/data/mehran/Datasets/synthetic_256/base.fvecs::/mnt/data/mehran/Datasets/synthetic_256/query.fvecs::/mnt/data/mehran/Datasets/synthetic_256/groundtruth.ivecs::256"
  "synthetic_512::/mnt/data/mehran/Datasets/synthetic_512/base.fvecs::/mnt/data/mehran/Datasets/synthetic_512/query.fvecs::/mnt/data/mehran/Datasets/synthetic_512/groundtruth.ivecs::512"
  "synthetic_1024::/mnt/data/mehran/Datasets/synthetic_1024/base.fvecs::/mnt/data/mehran/Datasets/synthetic_1024/query.fvecs::/mnt/data/mehran/Datasets/synthetic_1024/groundtruth.ivecs::1024"
  "glove-100::/mnt/data/mehran/Datasets/glove-100/glove-100_base.fvecs::/mnt/data/mehran/Datasets/glove-100/glove-100_query.fvecs::/mnt/data/mehran/Datasets/glove-100/glove-100_groundtruth.ivecs::100"
)

# ---------- MAIN LOOP: datasets x profiles ----------
for ds in "${DATASETS[@]}"; do
# robust split on "::<exactly>"
Name="$(awk -F '::' '{print $1}' <<<"$ds")"
Base="$(awk -F '::' '{print $2}' <<<"$ds")"
Query="$(awk -F '::' '{print $3}' <<<"$ds")"
GT="$(awk -F '::' '{print $4}' <<<"$ds")"
Dim="$(awk -F '::' '{print $5}' <<<"$ds")"

  if [[ -z "$Dim" ]]; then
    [[ -f "$Base" ]] || { echo "Skipping $Name (missing base)"; continue; }
    Dim="$(detect_fvecs_dim "$Base")"
    [[ -n "$Dim" ]] || die "Could not detect dimension for $Name from $Base. Set Dim explicitly."
  fi

  if ! ensure_files "$Base" "$Query" "$GT"; then
    echo "Skipping $Name due to missing files."
    continue
  fi

  datasetRoot="${OutRoot}/${Name}"
  mkdir -p "$datasetRoot"

  # Iterate profiles from config.json
  while IFS= read -r profile; do
    label="$(jq -r '.name // empty' <<<"$profile")"
    [[ -n "$label" ]] || continue

    # Apply filter if provided
    if [[ -n "$OnlyProfile" ]]; then
      if [[ "$OnlyProfile" == *"*"* || "$OnlyProfile" == *"?"* ]]; then
        [[ "$label" == $OnlyProfile ]] || continue
      else
        [[ "$label" == "$OnlyProfile" ]] || continue
      fi
    fi

    runDir="${datasetRoot}/${label}"
    mkdir -p "$runDir"
    if [[ "$CleanPerRun" == "true" ]]; then
      clean_run_metadata "$runDir"
    fi

    # Merge base + overrides
    ovr_json="$(jq -c '.overrides // {}' <<<"$profile")"
    final_json="$(json_merge_with_overrides "$base_json" "$ovr_json")"

    # Ensure output/eval/cloak exist and set fields
    final_json="$(jq -c \
      --arg resultsDir "${runDir}/results" \
      '.output |= (. // {})
       | .output.resultsDir = $resultsDir
       | .output.exportArtifacts = true
       | .output.suppressLegacyMetrics = true
       | .eval |= (. // {})
       | .eval.computePrecision = true
       | .eval.writeGlobalPrecisionCsv = true
       | .cloak |= (. // {})
       | .cloak.noise = 0.0
      ' <<<"$final_json")"

    # ratio: enforce GT path & disable recompute
    final_json="$(jq -c \
      --arg gtPath "$(safe_resolve "$GT")" \
      '.ratio |= (. // {})
       | .ratio.source = "gt"
       | .ratio.gtPath = $gtPath
       | .ratio.gtSample = ( .ratio.gtSample // 10000 )
       | .ratio.gtMismatchTolerance = 0.0
       | .ratio.autoComputeGT = false
       | .ratio.allowComputeIfMissing = false
      ' <<<"$final_json")"

    # Paper vs LSH knobs (preserve your logic)
    final_json="$(jq -c '
      .paper |= (. // {})
      | if (.paper.enabled // false) then
          (.paper.seed |= (. // 13))
          | .lsh |= (. // {})
          | .lsh.numTables = 0
          | .lsh.rowsPerBand = 0
          | .lsh.probeShards = 0
        else
          .
        end
    ' <<<"$final_json")"

    # Write run-specific config
    tmpConf="${runDir}/config.json"
    echo "$final_json" > "$tmpConf"

# args
keysFile="${runDir}/keystore.blob"
gtArg="$(safe_resolve "$GT")" # ignored when gtPath provided & compute disabled
dataArg="$Base"
queryArg="$Query"

declare -a restoreFlags=()   # <— add declare to ensure array exists

if [[ "$QueryOnly" == "true" ]]; then
  dataArg="POINTS_ONLY"
  restoreFlags+=("-Dquery.only=true")
  if [[ -n "${RestoreVersion}" ]]; then
    restoreFlags+=("-Drestore.version=${RestoreVersion}")
  fi
fi

# Build java arg list (order preserved)
argList=()
argList+=("${JvmArgs[@]}")

# add restoreFlags only if non-empty (avoids unbound expansion with set -u)
if ((${#restoreFlags[@]})); then
  argList+=("${restoreFlags[@]}")
fi

argList+=("-Dbase.path=$(safe_resolve "$Base" true)")
argList+=("-jar" "$(safe_resolve "$JarPath")")
argList+=("$(safe_resolve "$tmpConf")")
argList+=("$(safe_resolve "$dataArg" true)")
argList+=("$(safe_resolve "$queryArg" true)")
argList+=("$(safe_resolve "$keysFile" true)")
argList+=("${Dim}")
argList+=("$(safe_resolve "$runDir")")
argList+=("$(safe_resolve "$gtArg")")
argList+=("${Batch}")

    # Manifest
    manifest_json="$(jq -n -c \
      --arg dataset "$Name" \
      --arg dimension "$Dim" \
      --arg profile "$label" \
      --arg queryOnly "$QueryOnly" \
      --arg batchSize "$Batch" \
      --arg jarPath "$(safe_resolve "$JarPath")" \
      --arg jarSha256 "$(sha256_of "$(safe_resolve "$JarPath")")" \
      --arg configPath "$(safe_resolve "$tmpConf")" \
      --arg configSha256 "$(sha256_of "$(safe_resolve "$tmpConf")")" \
      --arg baseVectors "$(safe_resolve "$Base" true)" \
      --arg queryVectors "$(safe_resolve "$Query" true)" \
      --arg gtPath "$(safe_resolve "$GT" true)" \
      --arg alpha "$Alpha" \
      --arg timestampUtc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      --argjson jvmArgs "$(printf '%s\n' "${JvmArgs[@]}" | jq -R . | jq -s .)" '
      {
        dataset: $dataset,
        dimension: ($dimension|tonumber),
        profile: $profile,
        queryOnly: ($queryOnly=="true"),
        batchSize: ($batchSize|tonumber),
        jarPath: $jarPath,
        jarSha256: $jarSha256,
        configPath: $configPath,
        configSha256: $configSha256,
        baseVectors: $baseVectors,
        queryVectors: $queryVectors,
        gtPath: $gtPath,
        jvmArgs: $jvmArgs,
        alpha: ($alpha|tonumber),
        timestampUtc: $timestampUtc
      }')"
    echo "$manifest_json" > "${runDir}/manifest.json"

    # Log commandline
    printf "%s\n" "java ${argList[*]}" > "${runDir}/cmdline.txt"

    # Run (filter GT progress from console; full log kept)
    combinedLog="${runDir}/run.out.log"
    start_ts="$(date +%s)"
    set +e
    java "${argList[@]}" 2>&1 | tee "$combinedLog" | grep -Ev '^\[[0-9]+/[0-9]+\]\s+ queries processed \(GT\)' || true
    exit_code=${PIPESTATUS[0]}
    set -e
    end_ts="$(date +%s)"
    elapsed="$((end_ts - start_ts))"
    printf "ElapsedSec=%.1f\n" "$elapsed" > "${runDir}/elapsed.txt"

    if [[ "$exit_code" -ne 0 ]]; then
      echo "Run failed: dataset=${Name}, profile=${label}, exit=${exit_code}" >&2
    else
      echo "Completed: ${Name} (${label})"
    fi

  done < <(jq -c '.profiles[]' <<<"$cfg_json")

  # ---- per-dataset merges ----
  mapfile -t g_prec        < <(find "${datasetRoot}" -type f -path "*/results/global_precision.csv"        2>/dev/null)
  mapfile -t g_results     < <(find "${datasetRoot}" -type f -path "*/results/results_table.csv"           2>/dev/null)
  mapfile -t g_topk        < <(find "${datasetRoot}" -type f -path "*/results/topk_evaluation.csv"         2>/dev/null)
  mapfile -t g_reenc       < <(find "${datasetRoot}" -type f -path "*/results/reencrypt_metrics.csv"       2>/dev/null)
  mapfile -t g_samples     < <(find "${datasetRoot}" -type f -path "*/results/retrieved_samples.csv"       2>/dev/null)
  mapfile -t g_worst       < <(find "${datasetRoot}" -type f -path "*/results/retrieved_worst.csv"         2>/dev/null)
  mapfile -t g_stor_sum    < <(find "${datasetRoot}" -type f -path "*/results/storage_summary.csv"         2>/dev/null)
  mapfile -t g_metrics_txt < <(find "${datasetRoot}" -type f -path "*/results/metrics_summary.txt"         2>/dev/null)
  mapfile -t g_break_txt   < <(find "${datasetRoot}" -type f -path "*/results/storage_breakdown.txt"       2>/dev/null)
  mapfile -t g_readme      < <(find "${datasetRoot}" -type f -path "*/results/README_results_columns.txt"  2>/dev/null)

  combinedResults="${datasetRoot}/combined_results.csv"
  combinedPrecision="${datasetRoot}/combined_precision.csv"
  combinedTopk="${datasetRoot}/combined_evaluation.csv"
  combinedReenc="${datasetRoot}/combined_reencrypt_metrics.csv"
  combinedSamples="${datasetRoot}/combined_retrieved_samples.csv"
  combinedWorst="${datasetRoot}/combined_retrieved_worst.csv"
  combinedStorSum="${datasetRoot}/combined_storage_summary.csv"
  combinedMetricsTxt="${datasetRoot}/combined_metrics_summary.txt"
  combinedStorBreakTxt="${datasetRoot}/combined_storage_breakdown.txt"

  [[ "${#g_results[@]}"  -gt 0 ]] && combine_csv_with_profile "$combinedResults"   "profile" "${g_results[@]}"
  [[ "${#g_prec[@]}"     -gt 0 ]] && combine_csv_with_profile "$combinedPrecision" "profile" "${g_prec[@]}"
  [[ "${#g_topk[@]}"     -gt 0 ]] && combine_csv_with_profile "$combinedTopk"      "profile" "${g_topk[@]}"
  [[ "${#g_reenc[@]}"    -gt 0 ]] && combine_csv_with_profile "$combinedReenc"     "profile" "${g_reenc[@]}"
  [[ "${#g_samples[@]}"  -gt 0 ]] && combine_csv_with_profile "$combinedSamples"   "profile" "${g_samples[@]}"
  [[ "${#g_worst[@]}"    -gt 0 ]] && combine_csv_with_profile "$combinedWorst"     "profile" "${g_worst[@]}"
  [[ "${#g_stor_sum[@]}" -gt 0 ]] && combine_csv_with_profile "$combinedStorSum"   "profile" "${g_stor_sum[@]}"

  [[ "${#g_metrics_txt[@]}" -gt 0 ]] && concat_txt_with_profile "$combinedMetricsTxt"   "${g_metrics_txt[@]}"
  [[ "${#g_break_txt[@]}"   -gt 0 ]] && concat_txt_with_profile "$combinedStorBreakTxt" "${g_break_txt[@]}"

  if [[ "${#g_readme[@]}" -gt 0 ]]; then
    mkdir -p "${datasetRoot}/results_readme"
    cp -n "${g_readme[0]}" "${datasetRoot}/results_readme/README_results_columns.txt" || true
  fi

  if command -v tar >/dev/null 2>&1; then
    # archive all per-profile results/ folders (shallow)
    mapfile -t results_dirs < <(find "${datasetRoot}" -maxdepth 2 -type d -name "results" -printf '%P\n' 2>/dev/null)
    [[ "${#results_dirs[@]}" -gt 0 ]] && tar -czf "${datasetRoot}/all_results_tarball.tgz" -C "${datasetRoot}" "${results_dirs[@]}" 2>/dev/null || true
  fi

done
