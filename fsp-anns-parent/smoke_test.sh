#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================
# FSP-ANN FULL SMOKE TEST (Linux)
# ============================================
# Pure bash/awk implementation - NO Python required
# End-to-end validation:
# 1. Verify config sanity (guards against known bad values)
# 2. Build merged config (base + profile)
# 3. Clean workspace
# 4. Run full system (index + query) with live progress
# 5. Analyze profiler CSV with awk
#
# USE THIS BEFORE ANY FULL SWEEP OR PAPER RESULTS

echo "============================================"
echo "  FSP-ANN Smoke Test (Linux)"
echo "============================================"
echo ""

# ================= USER PARAMETERS =================

TEST_DATASET="SIFT1M"
TEST_PROFILE="M24"          # BASE or profile name
QUERY_LIMIT=200             # Smoke test queries only

# ================= PATHS =================

ROOT="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent"
JarPath="$ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran/SMOKE_TEST"
Batch=100000

# ================= JVM SETTINGS =================

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xmx8g"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

# ================= HELPERS =================

die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
command -v awk  >/dev/null || die "awk not found"
command -v bc   >/dev/null || die "bc not found"
ensure_file "$JarPath"

# ================= DATASET MAP =================

declare -A CFG BASE QUERY GT DIM

CFG["SIFT1M"]="$ROOT/config/src/main/resources/config_sift1m.json"
BASE["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_base.fvecs"
QUERY["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query.fvecs"
GT["SIFT1M"]="/mnt/data/mehran/Datasets/SIFT1M/sift_query_groundtruth.ivecs"
DIM["SIFT1M"]=128

cfg="${CFG[$TEST_DATASET]}"
base="${BASE[$TEST_DATASET]}"
query="${QUERY[$TEST_DATASET]}"
gt="${GT[$TEST_DATASET]}"
dim="${DIM[$TEST_DATASET]}"

ensure_file "$cfg"
ensure_file "$base"
ensure_file "$query"
ensure_file "$gt"

echo "Dataset : $TEST_DATASET"
echo "Profile : $TEST_PROFILE"
echo "Queries : $QUERY_LIMIT"
echo "Dim     : $dim"
echo ""

# ================= STEP 1: CONFIG SANITY =================

echo "[STEP 1/5] Verifying configuration..."

maxCand=$(jq '.base.runtime.maxCandidateFactor' "$cfg")
gtSample=$(jq '.base.ratio.gtSample' "$cfg")
alpha=$(jq '.base.stabilization.alpha' "$cfg")

echo "  maxCandidateFactor = $maxCand"
echo "  gtSample           = $gtSample"
echo "  alpha              = $alpha"

if (( $(echo "$maxCand > 3" | bc -l) )); then
  echo "WARNING: maxCandidateFactor > 3 → ratio inflation risk"
  echo "         Recommended: 3"
  read -p "Press ENTER to continue anyway, Ctrl+C to abort..."
fi

if (( gtSample > 20 )); then
  echo "WARNING: gtSample too high → long hangs"
  echo "         Recommended: 10"
  read -p "Press ENTER to continue anyway, Ctrl+C to abort..."
fi

if (( $(echo "$alpha > 0.05" | bc -l) )); then
  echo "WARNING: alpha too aggressive → recall risk"
  echo "         Recommended: 0.02"
  read -p "Press ENTER to continue anyway, Ctrl+C to abort..."
fi

echo "  Config sanity OK"
echo ""

# ================= STEP 2: BUILD MERGED CONFIG =================

echo "[STEP 2/5] Building final config..."

CFG_JSON="$(cat "$cfg")"

if [[ "$TEST_PROFILE" == "BASE" ]]; then
  final_cfg="$(jq 'del(.profiles)' <<<"$CFG_JSON")"
else
  base_json="$(jq 'del(.profiles)' <<<"$CFG_JSON")"
  profile="$(jq ".profiles[] | select(.name == \"$TEST_PROFILE\")" <<<"$CFG_JSON")"
  [[ -n "$profile" ]] || die "Profile not found: $TEST_PROFILE"

  overrides="$(jq '.overrides' <<<"$profile")"

  final_cfg="$(jq -n '
    def deepmerge(a; b):
      reduce (b | keys[]) as $k
        (a;
         if (a[$k] | type) == "object" and (b[$k] | type) == "object"
         then .[$k] = deepmerge(a[$k]; b[$k])
         else .[$k] = b[$k]
         end);
    deepmerge($base; $ovr)
  ' --argjson base "$base_json" --argjson ovr "$overrides")"
fi

run_dir="$OutRoot/${TEST_DATASET}_${TEST_PROFILE}"
rm -rf "$run_dir"
mkdir -p "$run_dir/results"

final_cfg="$(jq '
  .output.resultsDir = $res |
  .ratio.source = "gt" |
  .ratio.gtPath = $gt |
  .ratio.gtSample = 10
' --arg res "$run_dir/results" --arg gt "$gt" <<<"$final_cfg")"

echo "$final_cfg" > "$run_dir/config.json"

echo "  Config written: $run_dir/config.json"
echo ""

# ================= STEP 3: RUN SYSTEM =================

echo "[STEP 3/5] Running smoke test..."
echo "  (Live progress below - this may take 5-10 minutes)"
echo ""
log="$run_dir/run.log"
start=$(date +%s)

# Run with live progress feedback
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
  2>&1 | tee "$log"

# Capture Java exit code
java_exit=$?
end=$(date +%s)
elapsed=$((end - start))

echo ""
echo "  Runtime: ${elapsed}s ($(($elapsed / 60))m $(($elapsed % 60))s)"

# Check if Java succeeded
if [ $java_exit -ne 0 ]; then
  echo ""
  echo "============================================"
  echo " ERROR: Java process failed with exit code $java_exit"
  echo "============================================"
  echo ""
  echo "Last 30 lines of log:"
  tail -30 "$log"
  echo ""
  echo "Full log available at: $log"
  exit $java_exit
fi

echo ""

# ================= STEP 4: ANALYZE RESULTS =================

echo "[STEP 4/5] Analyzing results..."

csv="$run_dir/results/profiler_metrics.csv"

# Check if CSV exists before analyzing
if [[ ! -f "$csv" ]]; then
  echo ""
  echo "============================================"
  echo " ERROR: Results CSV not found"
  echo "============================================"
  echo ""
  echo "Expected file: $csv"
  echo ""
  echo "This usually means:"
  echo "  - Queries didn't run (check for errors above)"
  echo "  - System crashed during query execution"
  echo "  - Results directory path is incorrect"
  echo ""
  echo "Last 50 lines of log:"
  tail -50 "$log"
  echo ""
  echo "Full log available at: $log"
  exit 1
fi

# Count lines to verify queries ran
line_count=$(wc -l < "$csv")
expected_lines=$((QUERY_LIMIT + 1))  # +1 for header

if [ $line_count -lt 2 ]; then
  echo ""
  echo "============================================"
  echo " ERROR: CSV exists but is empty"
  echo "============================================"
  echo ""
  echo "CSV has only $line_count lines (expected at least 2: header + 1 query)"
  echo "File: $csv"
  echo ""
  exit 1
fi

if [ $line_count -lt $expected_lines ]; then
  echo ""
  echo "WARNING: CSV has fewer lines than expected"
  echo "  Expected: $expected_lines (header + $QUERY_LIMIT queries)"
  echo "  Found:    $line_count"
  echo "  This may indicate some queries failed"
  echo ""
fi

echo "  Found CSV with $line_count lines"
echo ""

# Parse CSV header to find column indices
header=$(head -1 "$csv")
IFS=',' read -ra cols <<< "$header"

ratio_col=-1
precision_col=-1
serverMs_col=-1
clientMs_col=-1

for i in "${!cols[@]}"; do
  case "${cols[$i]}" in
    ratio) ratio_col=$((i+1)) ;;
    precision) precision_col=$((i+1)) ;;
    serverMs) serverMs_col=$((i+1)) ;;
    clientMs) clientMs_col=$((i+1)) ;;
  esac
done

# Check if required columns exist
if [ $ratio_col -eq -1 ] || [ $precision_col -eq -1 ] || [ $serverMs_col -eq -1 ] || [ $clientMs_col -eq -1 ]; then
  echo "ERROR: CSV missing required columns"
  echo "Available columns: $header"
  echo "Required: ratio, precision, serverMs, clientMs"
  exit 1
fi

# Calculate statistics using awk
stats=$(awk -F',' -v rc="$ratio_col" -v pc="$precision_col" -v sc="$serverMs_col" -v cc="$clientMs_col" '
BEGIN {
    count = 0
    ratio_sum = 0; ratio_min = 999999; ratio_max = 0
    prec_sum = 0; prec_min = 999999; prec_max = 0
    server_sum = 0; client_sum = 0
}
NR > 1 {
    # Skip header
    count++

    # Ratio stats
    r = $rc + 0
    ratio_sum += r
    if (r < ratio_min) ratio_min = r
    if (r > ratio_max) ratio_max = r
    ratio_vals[count] = r

    # Precision stats
    p = $pc + 0
    prec_sum += p
    if (p < prec_min) prec_min = p
    if (p > prec_max) prec_max = p

    # Latency stats
    server_sum += $sc + 0
    client_sum += $cc + 0
}
END {
    if (count == 0) {
        print "ERROR: No data rows found"
        exit 1
    }

    ratio_mean = ratio_sum / count
    prec_mean = prec_sum / count
    server_mean = server_sum / count
    client_mean = client_sum / count

    # Calculate ratio standard deviation
    ratio_var_sum = 0
    for (i = 1; i <= count; i++) {
        diff = ratio_vals[i] - ratio_mean
        ratio_var_sum += diff * diff
    }
    ratio_std = sqrt(ratio_var_sum / count)

    # Sort ratio values for median and P95
    n = asort(ratio_vals, sorted_ratios)
    ratio_median = sorted_ratios[int(n/2)]
    ratio_p95 = sorted_ratios[int(n * 0.95)]

    # Output format: count|ratio_mean|ratio_median|ratio_std|ratio_min|ratio_max|ratio_p95|prec_mean|prec_min|prec_max|server_mean|client_mean
    printf "%d|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.2f|%.2f\n",
           count, ratio_mean, ratio_median, ratio_std, ratio_min, ratio_max, ratio_p95,
           prec_mean, prec_min, prec_max, server_mean, client_mean
}
' "$csv")

# Check if awk succeeded
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to parse CSV"
  exit 1
fi

# Parse awk output
IFS='|' read -r count ratio_mean ratio_median ratio_std ratio_min ratio_max ratio_p95 \
                   prec_mean prec_min prec_max server_mean client_mean <<< "$stats"

# Check if parsing succeeded
if [ -z "$count" ] || [ "$count" -eq 0 ]; then
  echo "ERROR: No data found in CSV"
  exit 1
fi

# Calculate total latency
total_mean=$(echo "$server_mean + $client_mean" | bc)

# Display results
echo "Queries: $count"
echo ""
echo "Ratio:"
echo "  Mean   : $ratio_mean"
echo "  Median : $ratio_median"
echo "  Std    : $ratio_std"
echo "  Min    : $ratio_min"
echo "  Max    : $ratio_max"
echo "  P95    : $ratio_p95"
echo ""
echo "Precision:"
echo "  Mean   : $prec_mean"
echo "  Min    : $prec_min"
echo "  Max    : $prec_max"
echo ""
echo "Latency (ms):"
echo "  Server : $server_mean"
echo "  Client : $client_mean"
echo "  Total  : $total_mean"
echo ""

# Validate results
ok_ratio=false
ok_prec=false

if (( $(echo "$ratio_mean <= 1.30" | bc -l) )); then
  ok_ratio=true
fi

if (( $(echo "$prec_mean >= 0.85" | bc -l) )); then
  ok_prec=true
fi

echo "=================================================="
echo "VALIDATION STATUS:"
echo "=================================================="
if [ "$ok_ratio" = true ]; then
  echo "  Ratio     : ✓ PASS (mean=$ratio_mean, target≤1.30)"
else
  echo "  Ratio     : ✗ FAIL (mean=$ratio_mean, target≤1.30)"
fi

if [ "$ok_prec" = true ]; then
  echo "  Precision : ✓ PASS (mean=$prec_mean, target≥0.85)"
else
  echo "  Precision : ✗ FAIL (mean=$prec_mean, target≥0.85)"
fi
echo ""

analysis_exit=0
if [ "$ok_ratio" = true ] && [ "$ok_prec" = true ]; then
  echo "✓ Smoke test PASSED - Safe to proceed to full run"
else
  echo "✗ Smoke test FAILED - Review config before full run"
  if [ "$ok_ratio" = false ]; then
    echo "  → Ratio too high ($ratio_mean). Try:"
    echo "     - Decrease alpha"
    echo "     - Decrease maxCandidateFactor"
  fi
  if [ "$ok_prec" = false ]; then
    echo "  → Precision too low ($prec_mean). Try:"
    echo "     - Increase alpha"
    echo "     - Increase maxCandidateFactor"
  fi
  analysis_exit=1
fi

# ================= STEP 5: GT CHECK =================

echo ""
echo "[STEP 5/5] GT Validation..."

if grep -q "GT VALIDATION PASSED" "$log"; then
  echo "✓ GT validation PASSED"
elif grep -q "GT VALIDATION FAILED" "$log"; then
  echo "✗ GT validation FAILED"
  echo ""
  grep -A 5 "GT VALIDATION FAILED" "$log"
  exit 1
else
  echo "? GT validation not found in log"
  echo "  (This may be normal if GT validation wasn't enabled)"
fi

echo ""
echo "============================================"
echo " Smoke test COMPLETE"
echo "============================================"
echo ""
echo "Results location:"
echo "  CSV:    $csv"
echo "  Log:    $log"
echo "  Config: $run_dir/config.json"
echo ""

if [ $analysis_exit -eq 0 ]; then
  echo "✓ All checks PASSED - Ready for full evaluation"
  echo ""
  echo "Next steps:"
  echo "  1. Run full SIFT1M: Edit QUERY_LIMIT=10000 and rerun"
  echo "  2. Run all datasets: Use multi_dataset_runner.sh"
else
  echo "✗ Some checks FAILED - Review results and config"
  echo ""
  echo "Next steps:"
  echo "  1. Adjust config parameters based on feedback above"
  echo "  2. Rerun smoke test to validate changes"
fi

exit $analysis_exit