#!/usr/bin/env bash
set -Eeuo pipefail

# =========================
# FSP-ANN SMOKE TEST RUNNER
# =========================
# Tests ONE profile on ONE dataset with LIMITED queries (200)
# Use this before running full evaluation sweep

echo "============================================"
echo "  FSP-ANN Smoke Test"
echo "============================================"

# ================= CONFIGURATION =================

JarPath="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran/SMOKE_TEST"
Batch=100000

# Test parameters
TEST_DATASET="SIFT1M"
TEST_PROFILE="BASE"  # Use "BASE" for no profile override, or "M24_lambda3" etc.
QUERY_LIMIT=200      # Number of queries to test

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx8g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
ensure_file "$JarPath"

# ================= DATASET CONFIG =================

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

# ================= VALIDATE FILES =================

cfg="${DATASET_CONFIG[$TEST_DATASET]}"
dim="${DATASET_DIM[$TEST_DATASET]}"
base="${DATASET_BASE[$TEST_DATASET]}"
query="${DATASET_QUERY[$TEST_DATASET]}"
gt="${DATASET_GT[$TEST_DATASET]}"

ensure_file "$cfg"
ensure_file "$base"
ensure_file "$query"
ensure_file "$gt"

echo ""
echo "Dataset:  $TEST_DATASET"
echo "Profile:  $TEST_PROFILE"
echo "Queries:  $QUERY_LIMIT"
echo "Dim:      $dim"
echo ""

# ================= SETUP OUTPUT DIR =================

run_dir="$OutRoot/${TEST_DATASET}_${TEST_PROFILE}"
mkdir -p "$run_dir/results"
rm -rf "$run_dir"/*  # Clean previous run
mkdir -p "$run_dir/results"

echo "Output:   $run_dir"
echo ""

# ================= BUILD CONFIG =================

CFG_JSON="$(cat "$cfg")"

if [[ "$TEST_PROFILE" == "BASE" ]]; then
  # Use base config without profile override
  final_cfg="$(jq -c 'del(.profiles)' <<<"$CFG_JSON")"
else
  # Find and apply specific profile
  BASE_JSON="$(jq -c 'del(.profiles)' <<<"$CFG_JSON")"

  profile="$(jq -c ".profiles[] | select(.name == \"$TEST_PROFILE\")" <<<"$CFG_JSON")"
  [[ -n "$profile" ]] || die "Profile not found: $TEST_PROFILE"

  overrides="$(jq -c '.overrides // {}' <<<"$profile")"

  final_cfg="$(jq -n '
    def deepmerge(a; b):
      reduce (b | keys[]) as $k
        (a;
         if (a[$k] | type) == "object" and (b[$k] | type) == "object"
         then .[$k] = deepmerge(a[$k]; b[$k])
         else .[$k] = b[$k]
         end);
    deepmerge($base; $ovr)
  ' \
    --argjson base "$BASE_JSON" \
    --argjson ovr  "$overrides"
  )"
fi

# Set output paths and GT config
final_cfg="$(jq -c '
  .output.resultsDir = $resdir |
  .ratio.source = "gt" |
  .ratio.gtPath = $gtpath |
  .ratio.autoComputeGT = false
' \
  --arg resdir "$run_dir/results" \
  --arg gtpath "$gt" \
  <<<"$final_cfg"
)"

echo "$final_cfg" > "$run_dir/config.json"

# ================= DISPLAY CONFIG =================

echo "Configuration:"
echo "  m:         $(jq -r '.base.paper.m // .paper.m' "$run_dir/config.json")"
echo "  lambda:    $(jq -r '.base.paper.lambda // .paper.lambda' "$run_dir/config.json")"
echo "  divisions: $(jq -r '.base.paper.divisions // .paper.divisions' "$run_dir/config.json")"
echo "  alpha:     $(jq -r '.base.stabilization.alpha // .stabilization.alpha' "$run_dir/config.json")"
echo "  minCand:   $(jq -r '.base.stabilization.minCandidates // .stabilization.minCandidates' "$run_dir/config.json")"
echo ""

# ================= RUN TEST =================

log="$run_dir/run.log"

echo "Starting smoke test..."
echo "Log: $log"
echo ""

start_time=$(date +%s)

java "${JvmArgs[@]}" \
  -Dcli.dataset="$TEST_DATASET" \
  -Dcli.profile="$TEST_PROFILE" \
  -Dquery.limit="$QUERY_LIMIT" \
  -jar "$JarPath" \
  "$run_dir/config.json" \
  "$base" \
  "$query" \
  "$run_dir/keys.blob" \
  "$dim" \
  "$run_dir" \
  "$gt" \
  "$Batch" \
  >"$log" 2>&1

exit_code=$?
end_time=$(date +%s)
elapsed=$((end_time - start_time))

echo ""
echo "============================================"

if [[ $exit_code -ne 0 ]]; then
  echo "SMOKE TEST FAILED (exit code: $exit_code)"
  echo "============================================"
  echo ""
  echo "Last 50 lines of log:"
  tail -50 "$log"
  exit $exit_code
fi

echo "SMOKE TEST COMPLETED"
echo "============================================"
echo ""
echo "Runtime: ${elapsed}s"
echo ""

# ================= EXTRACT RESULTS =================

profiler_csv="$run_dir/results/profiler_metrics.csv"
summary_txt="$run_dir/results/metrics_summary.txt"

if [[ ! -f "$profiler_csv" ]]; then
  echo "WARNING: profiler_metrics.csv not found"
  exit 1
fi

echo "Results:"
echo "--------"

# Extract metrics using awk (more portable than Python)
if command -v python3 >/dev/null; then
  # Use Python if available
  python3 <<EOF
import pandas as pd

try:
    df = pd.read_csv("$profiler_csv")

    print(f"Queries:      {len(df)}")
    print(f"")
    print(f"Ratio:")
    print(f"  Mean:       {df['ratio'].mean():.3f}")
    print(f"  Median:     {df['ratio'].median():.3f}")
    print(f"  Min:        {df['ratio'].min():.3f}")
    print(f"  Max:        {df['ratio'].max():.3f}")
    print(f"  Std:        {df['ratio'].std():.3f}")
    print(f"")
    print(f"Recall:")
    print(f"  Mean:       {df['recall'].mean():.3f}")
    print(f"  Min:        {df['recall'].min():.3f}")
    print(f"")
    print(f"Latency (ms):")
    print(f"  Server:     {df['serverMs'].mean():.1f}")
    print(f"  Client:     {df['clientMs'].mean():.1f}")
    print(f"  Total:      {(df['serverMs'] + df['clientMs']).mean():.1f}")
    print(f"")

    # Check targets
    avg_ratio = df['ratio'].mean()
    avg_recall = df['recall'].mean()

    print("Status:")
    if avg_ratio <= 1.30:
        print(f"  ✓ Ratio {avg_ratio:.3f} <= 1.30 (PASS)")
    else:
        print(f"  ✗ Ratio {avg_ratio:.3f} > 1.30 (FAIL)")

    if avg_recall >= 0.85:
        print(f"  ✓ Recall {avg_recall:.3f} >= 0.85 (PASS)")
    else:
        print(f"  ✗ Recall {avg_recall:.3f} < 0.85 (FAIL)")

except Exception as e:
    print(f"Error analyzing results: {e}")
    exit(1)
EOF
else
  # Fallback: basic awk analysis
  echo "Queries:      $(tail -n +2 "$profiler_csv" | wc -l)"
  echo ""
  echo "Ratio:"
  tail -n +2 "$profiler_csv" | awk -F',' '{sum+=$7; if(NR==1||$7<min)min=$7; if(NR==1||$7>max)max=$7} END {printf "  Mean:       %.3f\n  Min:        %.3f\n  Max:        %.3f\n", sum/NR, min, max}'
  echo ""
  echo "Recall:"
  tail -n +2 "$profiler_csv" | awk -F',' '{sum+=$8; if(NR==1||$8<min)min=$8} END {printf "  Mean:       %.3f\n  Min:        %.3f\n", sum/NR, min}'
fi

echo ""
echo "Files:"
echo "  Config:    $run_dir/config.json"
echo "  Results:   $profiler_csv"
echo "  Log:       $log"
echo ""

# ================= CHECK GT VALIDATION =================

if grep -q "GT VALIDATION PASSED" "$log"; then
  echo "✓ GT Validation: PASSED"
elif grep -q "GT VALIDATION FAILED" "$log"; then
  echo "✗ GT Validation: FAILED"
  echo ""
  grep -A 5 "GT VALIDATION FAILED" "$log"
else
  echo "? GT Validation: Not found in log"
fi

echo ""
echo "============================================"
echo "Smoke test complete!"
echo "Review results above before full sweep."
echo "============================================"