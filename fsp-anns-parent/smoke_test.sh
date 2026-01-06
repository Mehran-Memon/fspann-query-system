#!/usr/bin/env bash
set -Eeuo pipefail

# ============================================
# FSP-ANN FULL SMOKE TEST (Linux)
# ============================================
# Pure bash/awk implementation - NO Python required
# Handles missing precision column gracefully
# End-to-end validation with parameter tuning guidance
# FIXED: Deep merge for profile overrides + 16GB heap

echo "============================================"
echo "  FSP-ANN Smoke Test (Linux)"
echo "============================================"
echo ""

# ================= USER PARAMETERS =================

TEST_DATASET="SIFT1M"
TEST_PROFILE="M24"          # BASE or profile name
QUERY_LIMIT=20             # Smoke test queries only

# ================= PATHS =================

ROOT="/home/jeco/IdeaProjects/fspann-query-system/fsp-anns-parent"
JarPath="$ROOT/api/target/api-0.0.1-SNAPSHOT-shaded.jar"
OutRoot="/mnt/data/mehran/SMOKE_TEST"
Batch=100000

# ================= JVM SETTINGS  =================

JvmArgs=(
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=200"
  "-XX:+AlwaysPreTouch"
  "-Xms128g"
  "-Xmx256g"
  "-XX:G1HeapRegionSize=32m"
  "-XX:+UseStringDeduplication"
  "-XX:ConcGCThreads=8"
  "-XX:ParallelGCThreads=16"
  "-Dfile.encoding=UTF-8"
  "-Dreenc.mode=end"
)

# ================= HELPERS =================

die() { echo "ERROR: $*" >&2; exit 1; }
ensure_file() { [[ -f "$1" ]] || die "File not found: $1"; }

command -v java >/dev/null || die "java not found"
command -v jq   >/dev/null || die "jq not found"
command -v gawk >/dev/null || die "gawk not found (required for asort)"
AWK=gawk
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

maxCand=$(jq -r '.runtime.maxCandidateFactor // empty' "$cfg")
gtSample=$(jq -r '.ratio.gtSample // empty' "$cfg")
alpha=$(jq -r '.stabilization.alpha // empty' "$cfg")

: "${maxCand:=3}"
: "${gtSample:=10}"
: "${alpha:=0.02}"

echo "  maxCandidateFactor = $maxCand"
echo "  gtSample           = $gtSample"
echo "  alpha              = $alpha"

if (( $(echo "$maxCand > 3" | bc -l) )); then
  echo "WARNING: maxCandidateFactor > 3 → ratio inflation risk"
  echo "         Recommended: 3"
fi

if (( gtSample > 20 )); then
  echo "WARNING: gtSample too high → long hangs"
  echo "         Recommended: 10"
fi

if (( $(echo "$alpha > 0.05" | bc -l) )); then
  echo "WARNING: alpha too aggressive → recall risk"
  echo "         Recommended: 0.02"
fi

echo "  Config sanity OK"
echo ""

# ================= STEP 2: BUILD MERGED CONFIG (FIXED) =================

echo "[STEP 2/5] Building final config..."

# Read base config
base_config=$(jq 'del(.profiles)' "$cfg")

# Apply profile if not BASE
if [ "$TEST_PROFILE" != "BASE" ]; then
  echo "  Applying profile: $TEST_PROFILE"

  # Extract profile overrides
  profile_json=$(jq ".profiles[] | select(.name == \"$TEST_PROFILE\") | .overrides" "$cfg")

  if [ -z "$profile_json" ] || [ "$profile_json" = "null" ]; then
    echo "  ERROR: Profile '$TEST_PROFILE' not found in $cfg"
    exit 1
  fi

  # FIXED: Deep merge profile overrides into base config
  merged_config=$(echo "$base_config" | jq --argjson overrides "$profile_json" '
  .paper         = (.paper         + ($overrides.paper         // {})) |
  .stabilization = (.stabilization + ($overrides.stabilization // {})) |
  .runtime       = (.runtime       + ($overrides.runtime       // {})) |
  .ratio         = (.ratio         + ($overrides.ratio         // {}))
  ')
else
  echo "  Using BASE profile (no overrides)"
  merged_config="$base_config"
fi

# Create test directory
run_dir="$OutRoot/${TEST_DATASET}_${TEST_PROFILE}"
rm -rf "$run_dir"
mkdir -p "$run_dir/results"

# Override paths for smoke test
final_config=$(echo "$merged_config" | jq \
  --arg resultsDir "$run_dir/results" \
  --arg gtPath "$gt" \
  '
  .output.resultsDir = $resultsDir |
  .ratio.source = "gt" |
  .ratio.gtPath = $gtPath |
  .ratio.gtSample = 10 |
  .runtime.precisionMode = false |
  .runtime.refinementLimit = 3000 |
  .runtime.maxCandidateFactor = 3
  ')

# Write final config
final_config_path="$run_dir/config.json"
echo "$final_config" > "$final_config_path"
echo "  Config written: $final_config_path"

# ADDED: Verify applied parameters
m_value=$(echo "$final_config" | jq -r '.paper.m')
lambda_value=$(echo "$final_config" | jq -r '.paper.lambda')
divisions_value=$(echo "$final_config" | jq -r '.paper.divisions')
echo "  Applied params: m=$m_value λ=$lambda_value divisions=$divisions_value"
echo ""

# ================= STEP 3: RUN SYSTEM =================

echo "[STEP 3/5] Running smoke test..."
echo "  (Live progress below - this may take 5-10 minutes)"
echo ""
log="$run_dir/run.log"
start=$(date +%s)

java "${JvmArgs[@]}" \
  -Dcli.dataset="$TEST_DATASET" \
  -Dcli.profile="$TEST_PROFILE" \
  -DqueryLimit="$QUERY_LIMIT" \
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

  java_exit=$?
  end=$(date +%s)
  elapsed=$((end - start))

  echo ""
  echo "  Runtime: ${elapsed}s ($(($elapsed / 60))m $(($elapsed % 60))s)"

  if [ $java_exit -ne 0 ]; then
    echo ""
    echo "============================================"
    echo " ERROR: Java process failed with exit code $java_exit"
    echo "============================================"
    echo ""
    echo "Last 30 lines of log:"
    tail -30 "$log"
    echo ""
    exit $java_exit
  fi

  echo ""

# ================= STEP 4: ANALYZE RESULTS =================

  echo "[STEP 4/5] Analyzing results..."

csv="$run_dir/results/profiler_metrics.csv"

if [[ ! -f "$csv" ]]; then
  echo ""
  echo "============================================"
  echo " ERROR: Results CSV not found"
  echo "============================================"
  echo ""
  echo "Expected file: $csv"
  echo ""
  echo "Last 50 lines of log:"
  tail -50 "$log"
  echo ""
  exit 1
fi

line_count=$(wc -l < "$csv")

if [ $line_count -lt 2 ]; then
  echo ""
  echo "============================================"
  echo " ERROR: CSV exists but is empty"
  echo "============================================"
  echo ""
  exit 1
fi

echo "  Found CSV with $line_count lines"
echo ""

# Parse CSV header to find column indices dynamically
header=$(head -1 "$csv")
IFS=',' read -ra cols <<< "$header"

distance_ratio_col=-1
recall_col=-1
serverMs_col=-1
clientMs_col=-1

for i in "${!cols[@]}"; do
  case "${cols[$i]}" in
    distance_ratio) distance_ratio_col=$((i+1)) ;;
    recall) recall_col=$((i+1)) ;;
    serverMs) serverMs_col=$((i+1)) ;;
    clientMs) clientMs_col=$((i+1)) ;;
  esac
done

# Check critical columns
missing_cols=()
[ $distance_ratio_col -eq -1 ] && missing_cols+=("ratio")
[ $serverMs_col -eq -1 ] && missing_cols+=("serverMs")
[ $clientMs_col -eq -1 ] && missing_cols+=("clientMs")

if [ ${#missing_cols[@]} -gt 0 ]; then
  echo "ERROR: CSV missing critical columns: ${missing_cols[*]}"
  echo "Available columns: $header"
  exit 1
fi

has_precision=false
if [ $recall_col -ne -1 ]; then
  has_recall=true
fi

# Calculate statistics using awk with dynamic column handling
stats=$(gawk -F',' -v rc="$distance_ratio_col" -v pc="$recall_col" -v sc="$serverMs_col" -v cc="$clientMs_col" -v has_recall="$has_recall" '
BEGIN {
    count = 0
    ratio_sum = 0; ratio_min = 999999; ratio_max = 0
    recall_sum = 0; recall_min = 999999; recall_max = 0
    server_sum = 0; client_sum = 0
}
NR > 1 {
    count++

    # Ratio stats
    r = $rc + 0
    ratio_sum += r
    if (r < ratio_min) ratio_min = r
    if (r > ratio_max) ratio_max = r
    ratio_vals[count] = r

    # Recall stats (if column exists)
    if (has_recall == "true" && rec > 0) {
        p = $pc + 0
        recall_sum += rec
        if (rec < recall_min) recall_min = p
        if (rec > recall_max) recall_max = p
    }

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
    recall_mean = (has_recall == "true" && pc > 0) ? recall_sum / count : -1
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

    # Use -1 for missing precision stats
    if (recall_mean < 0) {
        recall_min = -1
        recall_max = -1
    }

    printf "%d|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.4f|%.2f|%.2f\n",
           count, ratio_mean, ratio_median, ratio_std, ratio_min, ratio_max, ratio_p95,
           recall_mean, recall_min, recall_max, server_mean, client_mean
}
' "$csv")

if [ $? -ne 0 ]; then
  echo "ERROR: Failed to parse CSV"
  exit 1
fi

# Parse awk output
IFS='|' read -r count ratio_mean ratio_median ratio_std ratio_min ratio_max ratio_p95 \
                   recall_mean recall_min recall_max server_mean client_mean <<< "$stats"

if [ -z "$count" ] || [ "$count" -eq 0 ]; then
  echo "ERROR: No data found in CSV"
  exit 1
fi

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

if $has_recall && (( $(echo "$recall_mean >= 0" | bc -l) )); then
  echo "Recall:"
  echo "  Mean   : $recall_mean"
  echo "  Min    : $recall_min"
  echo "  Max    : $recall_max"
  echo ""
else
  echo "Recall: ⚠ NOT AVAILABLE"
  echo ""
fi

echo "Latency (ms):"
echo "  Server : $server_mean"
echo "  Client : $client_mean"
echo "  Total  : $total_mean"
echo ""

# Validate results
ok_ratio=false
ok_recall=false

# Ratio validation (critical)
if (( $(echo "$ratio_mean <= 1.30" | bc -l) )); then
  ok_ratio=true
fi

if $has_recall && (( $(echo "$recall_mean >= 0" | bc -l) )); then
  if (( $(echo "$recall_mean >= 0.85" | bc -l) )); then
    ok_recall=true
  else
    ok_recall=false
  fi
else
  # Skip precision validation if not available
  ok_recall=true
fi

echo "=================================================="
echo "VALIDATION STATUS:"
echo "=================================================="

if [ "$ok_ratio" = true ]; then
  echo "  Ratio     : ✓ PASS (mean=$ratio_mean, target≤1.30)"
else
  echo "  Ratio     : ✗ FAIL (mean=$ratio_mean, target≤1.30)"
fi

analysis_exit=0
if [ "$ok_ratio" = true ] && [ "$ok_recall" = true ]; then
  echo "✓ Smoke test PASSED"
  echo ""
else
  echo "✗ Smoke test FAILED - Review config"
  echo ""

  if [ "$ok_ratio" = false ]; then
    echo "  → RATIO TOO HIGH ($ratio_mean > 1.30)"
    echo ""
    echo "    Parameter tuning suggestions:"
    echo "    1. Increase m (bits per token): M24 → M28 → M32"
    echo "       Higher m = more buckets = fewer false positives"
    echo ""
    echo "    2. Decrease lambda (token repeats): λ=2 → λ=1"
    echo "       Fewer tokens = tighter filtering"
    echo ""
    echo "    3. Decrease alpha (stabilization): α=0.02 → α=0.01"
    echo "       Less aggressive candidate expansion"
    echo ""
    echo "    4. Try profile 'M32' for lowest ratio (tradeoff: slower indexing)"
    echo ""
    echo "    Example: Edit line 19 in smoke_test.sh:"
    echo "      TEST_PROFILE=\"M32\"  # or \"M28\" for moderate improvement"
    echo ""
  fi

  if $has_precision && [ "$ok_recall" = false ]; then
    echo "  → PRECISION TOO LOW ($recall_mean < 0.85)"
    echo ""
    echo "    Parameter tuning suggestions:"
    echo "    1. Increase lambda: λ=2 → λ=3"
    echo "       More tokens = better recall"
    echo ""
    echo "    2. Increase alpha: α=0.02 → α=0.03"
    echo "       More aggressive candidate expansion"
    echo ""
    echo "    3. Increase divisions: d=3 → d=4"
    echo "       Finer-grained partitioning"
    echo ""
  fi

  analysis_exit=1
fi

# ================= STEP 5: GT CHECK =================

echo "[STEP 5/5] GT Validation..."

if grep -q "GT VALIDATION PASSED" "$log"; then
  echo "✓ GT validation PASSED"
elif grep -q "GT VALIDATION FAILED" "$log"; then
  echo "✗ GT validation FAILED"
  echo ""
  grep -A 5 "GT VALIDATION FAILED" "$log"
  exit 1
else
  echo "⚠ GT validation not found in log"
fi

echo ""
echo "============================================"
echo " Smoke test COMPLETE"
echo "============================================"
echo ""
echo "Results:"
echo "  CSV:    $csv"
echo "  Log:    $log"
echo "  Config: $run_dir/config.json"
echo ""

if [ $analysis_exit -eq 0 ]; then
  echo "✓ All checks PASSED - Ready for full evaluation"
  echo ""
  echo "Next steps:"
  echo "  1. Full SIFT1M: Edit QUERY_LIMIT=10000 and rerun"
  echo "  2. All datasets: Use multi_dataset_runner.sh"
  echo "  3. Paper results: Collect ratio/precision across all profiles"
else
  echo "✗ Some checks FAILED"
  echo ""
  echo "Next steps:"
  echo "  1. Adjust config parameters (see suggestions above)"
  echo "  2. Try different profile: M28, M32, or M28_L1"
  echo "  3. Rerun smoke test to validate changes"
fi

exit $analysis_exit