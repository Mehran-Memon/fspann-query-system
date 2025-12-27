# ============================================
# FSP-ANN FULL RUN - Windows PowerShell
# ============================================
# Complete end-to-end test:
# 1. Verifies/fixes config (IMPROVED)
# 2. Rebuilds JAR
# 3. Builds fresh index
# 4. Runs queries
# 5. Reports results

param(
    [string]$Dataset = "SIFT1M",
    [string]$Profile = "M20",
    [int]$QueryLimit = 50
)

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  FSP-ANN FULL RUN (Build + Query)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "WARNING: This will take ~90 minutes!" -ForegroundColor Yellow
Write-Host "Press Ctrl+C in next 5 seconds to cancel..." -ForegroundColor Yellow
Start-Sleep -Seconds 5
Write-Host ""

$startTimeTotal = Get-Date

# ================= CONFIGURATION =================

$JarPath = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_sift1m.json"
$OutRoot = "G:\SMOKE_TEST"
$Batch = 100000

# JVM Arguments
$JvmArgs = @(
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:+AlwaysPreTouch",
    "-Xmx12g",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end"
)

# ================= DATASET CONFIG =================

$DatasetConfig = @{
    "SIFT1M" = @{
        Config = $ConfigPath
        Dim = 128
        Base = "E:\Research Work\Datasets\SIFT1M\sift_base.fvecs"
        Query = "E:\Research Work\Datasets\SIFT1M\sift_query.fvecs"
        GT = "E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"
    }
}

if (-not $DatasetConfig.ContainsKey($Dataset)) {
    Write-Host "ERROR: Unknown dataset: $Dataset" -ForegroundColor Red
    exit 1
}

$ds = $DatasetConfig[$Dataset]

# ================= STEP 1: VERIFY CONFIG =================

Write-Host "[STEP 1/5] Verifying configuration..." -ForegroundColor Cyan

if (-not (Test-Path $ConfigPath)) {
    Write-Host "ERROR: Config not found: $ConfigPath" -ForegroundColor Red
    exit 1
}

$configJson = Get-Content $ConfigPath -Raw | ConvertFrom-Json

# Check CRITICAL configuration values
$maxCandFactor = $configJson.base.runtime.maxCandidateFactor
$maxRelaxDepth = $configJson.base.runtime.maxRelaxationDepth
$gtSample = $configJson.base.ratio.gtSample

Write-Host "  Current config:" -ForegroundColor Gray
Write-Host "    maxCandidateFactor:  $maxCandFactor" -ForegroundColor Gray
Write-Host "    maxRelaxationDepth:  $maxRelaxDepth" -ForegroundColor Gray
Write-Host "    gtSample:            $gtSample" -ForegroundColor Gray
Write-Host ""

# CRITICAL VALIDATION
$configErrors = @()

# Check maxCandidateFactor
if ($maxCandFactor -gt 10) {
    $configErrors += "maxCandidateFactor=$maxCandFactor (should be 3)"
}

# Check maxRelaxationDepth (CRITICAL!)
if ($maxRelaxDepth -eq $null -or $maxRelaxDepth -lt 1) {
    $configErrors += "maxRelaxationDepth=$maxRelaxDepth (MUST be >= 1, recommended 2)"
}

# Check gtSample
if ($gtSample -gt 50 -or $gtSample -eq $null) {
    $configErrors += "gtSample=$gtSample (should be 10 to avoid 2+ hour hangs)"
}

# Check profile-specific lambda
$profileObj = $configJson.profiles | Where-Object { $_.name -eq $Profile }
if ($profileObj) {
    $lambda = $profileObj.overrides.base.paper.lambda
    if ($lambda -eq $null) {
        $lambda = $configJson.base.paper.lambda
    }

    Write-Host "  Profile '$Profile':" -ForegroundColor Gray
    Write-Host "    lambda: $lambda" -ForegroundColor Gray

    # CRITICAL: lambda must be >= 2
    if ($lambda -lt 2) {
        $configErrors += "lambda=$lambda (MUST be >= 2, system breaks with lambda=1!)"
    }
}

if ($configErrors.Count -gt 0) {
    Write-Host ""
    Write-Host "ERROR: Critical configuration issues found!" -ForegroundColor Red
    Write-Host ""
    foreach ($err in $configErrors) {
        Write-Host "  ❌ $err" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "Fix these in: $ConfigPath" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Example fixes:" -ForegroundColor Yellow
    Write-Host '  "runtime": {' -ForegroundColor Gray
    Write-Host '    "maxCandidateFactor": 3,' -ForegroundColor Gray
    Write-Host '    "maxRelaxationDepth": 2,' -ForegroundColor Gray
    Write-Host '    ...' -ForegroundColor Gray
    Write-Host '  }' -ForegroundColor Gray
    Write-Host ""
    Write-Host "Press Enter to continue ANYWAY (not recommended), or Ctrl+C to cancel..." -ForegroundColor Yellow
    Read-Host
}

Write-Host "  Config validation complete" -ForegroundColor Green
Write-Host ""

# ================= STEP 2: REBUILD JAR =================

Write-Host "[STEP 2/5] Rebuilding JAR..." -ForegroundColor Cyan

Push-Location "F:\fspann-query-system\fsp-anns-parent"

try {
    Write-Host "  Running: mvn clean install -DskipTests" -ForegroundColor Gray
    $mvnOutput = & mvn clean install -DskipTests 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Maven build failed" -ForegroundColor Red
        Write-Host $mvnOutput
        exit 1
    }

    if (-not (Test-Path $JarPath)) {
        Write-Host "ERROR: JAR not found after build: $JarPath" -ForegroundColor Red
        exit 1
    }

    $jarTime = (Get-Item $JarPath).LastWriteTime
    $jarSize = (Get-Item $JarPath).Length / 1MB
    Write-Host "  JAR built: $jarTime ($([math]::Round($jarSize, 1)) MB)" -ForegroundColor Green
} finally {
    Pop-Location
}

Write-Host ""

# ================= STEP 3: CLEAN OLD INDEX =================

Write-Host "[STEP 3/5] Cleaning old index..." -ForegroundColor Cyan

$runDir = Join-Path $OutRoot "$($Dataset)_$($Profile)"

if (Test-Path $runDir) {
    Write-Host "  Deleting: $runDir" -ForegroundColor Gray
    Remove-Item -Path $runDir -Recurse -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1  # Give filesystem time to release handles
}

New-Item -ItemType Directory -Path $runDir -Force | Out-Null
Write-Host "  Clean workspace ready: $runDir" -ForegroundColor Green
Write-Host ""

# ================= STEP 4: BUILD INDEX & RUN QUERIES =================

Write-Host "[STEP 4/5] Building index and running queries..." -ForegroundColor Cyan
Write-Host "  Expected time: ~83 minutes for 1M vectors" -ForegroundColor Gray
Write-Host ""

$resultsDir = Join-Path $runDir "results"
New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

# Load and merge config
$configJson = Get-Content $ds.Config -Raw | ConvertFrom-Json
$profileObj = $configJson.profiles | Where-Object { $_.name -eq $Profile }

if (-not $profileObj) {
    Write-Host "ERROR: Profile not found: $Profile" -ForegroundColor Red
    exit 1
}

function Merge-Objects($base, $override) {
    $result = $base.PSObject.Copy()
    foreach ($prop in $override.PSObject.Properties) {
        if ($result.PSObject.Properties[$prop.Name] -and
                $result.($prop.Name) -is [PSCustomObject] -and
                $prop.Value -is [PSCustomObject]) {
            $result.($prop.Name) = Merge-Objects $result.($prop.Name) $prop.Value
        } else {
            $result | Add-Member -MemberType NoteProperty -Name $prop.Name -Value $prop.Value -Force
        }
    }
    return $result
}

$baseConfig = $configJson.PSObject.Copy()
$baseConfig.PSObject.Properties.Remove('profiles')
$finalConfig = Merge-Objects $baseConfig $profileObj.

# ================= FINAL CONFIG VALIDATION (AUTHORITATIVE) =================

$lambda = $finalConfig.base.paper.lambda
$maxRelaxDepth = $finalConfig.base.runtime.maxRelaxationDepth
$maxCandFactor = $finalConfig.base.runtime.maxCandidateFactor

if ($lambda -lt 2) {
    throw "FINAL CONFIG INVALID: lambda=$lambda < 2 (ANN precision undefined)"
}
if ($maxRelaxDepth -lt 1) {
    throw "FINAL CONFIG INVALID: maxRelaxationDepth < 1"
}
if ($maxCandFactor -gt 5) {
    throw "FINAL CONFIG INVALID: maxCandidateFactor=$maxCandFactor too large"
}

# Set output paths
if (-not $finalConfig.output) {
    $finalConfig | Add-Member -MemberType NoteProperty -Name output -Value ([PSCustomObject]@{})
}
$finalConfig.output | Add-Member -MemberType NoteProperty -Name resultsDir -Value $resultsDir -Force

# Set ratio config
if (-not $finalConfig.ratio) {
    $finalConfig | Add-Member -MemberType NoteProperty -Name ratio -Value ([PSCustomObject]@{})
}
$finalConfig.ratio | Add-Member -MemberType NoteProperty -Name source -Value "gt" -Force
$finalConfig.ratio | Add-Member -MemberType NoteProperty -Name gtPath -Value $ds.GT -Force
$finalConfig.ratio | Add-Member -MemberType NoteProperty -Name gtSample -Value 10 -Force
$finalConfig.ratio | Add-Member -MemberType NoteProperty -Name gtMismatchTolerance -Value 0.05 -Force

# Save merged config
$configPath = Join-Path $runDir "config.json"
$finalConfig | ConvertTo-Json -Depth 10 | Set-Content $configPath

# Display final config
Write-Host "  Final Configuration:" -ForegroundColor Yellow
Write-Host "    Profile:       $Profile" -ForegroundColor White
Write-Host "    m:             $($finalConfig.base.paper.m)" -ForegroundColor White
Write-Host "    lambda:        $($finalConfig.base.paper.lambda)" -ForegroundColor White
Write-Host "    divisions:     $($finalConfig.base.paper.divisions)" -ForegroundColor White
Write-Host "    tables:        $($finalConfig.base.paper.tables)" -ForegroundColor White
Write-Host "    alpha:         $($finalConfig.base.stabilization.alpha)" -ForegroundColor White
Write-Host "    maxRelaxDepth: $($finalConfig.base.runtime.maxRelaxationDepth)" -ForegroundColor White
Write-Host ""

# Verify lambda >= 2
if ($finalConfig.base.paper.lambda -lt 2) {
    Write-Host "  ⚠️  WARNING: lambda=$($finalConfig.base.paper.lambda) < 2" -ForegroundColor Red
    Write-Host "  System will have 0% precision with lambda=1!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Press Enter to continue anyway, or Ctrl+C to cancel..." -ForegroundColor Yellow
    Read-Host
}

# Run Java (FULL MODE)
$logPath = Join-Path $runDir "run.log"
$errorLogPath = Join-Path $runDir "error.log"
$startTimeBuild = Get-Date

Write-Host "  Starting at $(Get-Date -Format 'HH:mm:ss')..." -ForegroundColor Gray
Write-Host "  Output log: $logPath" -ForegroundColor Gray
Write-Host "  Error log:  $errorLogPath" -ForegroundColor Gray
Write-Host ""

$javaCmd = "java"
$javaArgs = $JvmArgs + @(
    "-Dcli.dataset=$Dataset",
    "-Dcli.profile=$Profile",
    "-Dquery.limit=$QueryLimit",
    "-Dbase.path=`"$($ds.Base)`"",
    "-jar",
    "`"$JarPath`"",
    "`"$configPath`"",
    "`"$($ds.Base)`"",      # BASE VECTORS (full indexing)
    "`"$($ds.Query)`"",
    "`"$(Join-Path $runDir 'keys.blob')`"",
    $ds.Dim.ToString(),
    "`"$runDir`"",
    "`"$($ds.GT)`"",
    $Batch.ToString()
)

Write-Host "  Command: java $($javaArgs -join ' ')" -ForegroundColor DarkGray
Write-Host ""

# Run Java process
$process = Start-Process -FilePath $javaCmd -ArgumentList $javaArgs `
    -NoNewWindow -Wait -PassThru `
    -RedirectStandardOutput $logPath `
    -RedirectStandardError $errorLogPath

$exitCode = $process.ExitCode
$buildDuration = (Get-Date) - $startTimeBuild

if ($exitCode -ne 0) {
    Write-Host ""
    Write-Host "ERROR: Process failed (exit code: $exitCode)" -ForegroundColor Red
    Write-Host ""
    Write-Host "Last 50 lines of stdout:" -ForegroundColor Yellow
    Get-Content $logPath -Tail 50
    Write-Host ""
    Write-Host "Last 50 lines of stderr:" -ForegroundColor Yellow
    Get-Content $errorLogPath -Tail 50
    exit $exitCode
}

Write-Host "  Completed in: $([math]::Round($buildDuration.TotalMinutes, 1)) minutes" -ForegroundColor Green
Write-Host ""

# ================= STEP 5: ANALYZE RESULTS =================

Write-Host "[STEP 5/5] Analyzing results..." -ForegroundColor Cyan

$profilerCsv = Join-Path $resultsDir "profiler_metrics.csv"

if (-not (Test-Path $profilerCsv)) {
    Write-Host "ERROR: Results CSV not found: $profilerCsv" -ForegroundColor Red
    Write-Host ""
    Write-Host "Available files in results directory:" -ForegroundColor Yellow
    if (Test-Path $resultsDir) {
        Get-ChildItem $resultsDir | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    }
    Write-Host ""
    Write-Host "Last 100 lines of log:" -ForegroundColor Yellow
    Get-Content $logPath -Tail 100
    exit 1
}

# Parse CSV
$data = Import-Csv $profilerCsv

if ($data.Count -eq 0) {
    Write-Host "ERROR: No data in results CSV" -ForegroundColor Red
    exit 1
}

$queries = $data.Count
$ratios = $data | ForEach-Object { [double]$_.ratio }
$precisions = $data | ForEach-Object { [double]$_.precision }
$serverMs = $data | ForEach-Object { [double]$_.serverMs }
$clientMs = $data | ForEach-Object { [double]$_.clientMs }

# Calculate statistics
function Get-Stats($values) {
    $sorted = $values | Sort-Object
    return @{
        Mean = ($values | Measure-Object -Average).Average
        Median = $sorted[[math]::Floor($sorted.Count / 2)]
        Min = ($values | Measure-Object -Minimum).Minimum
        Max = ($values | Measure-Object -Maximum).Maximum
        Std = [math]::Sqrt((($values | ForEach-Object { [math]::Pow($_ - ($values | Measure-Object -Average).Average, 2) } | Measure-Object -Sum).Sum / $values.Count))
    }
}

$ratioStats = Get-Stats $ratios
$precisionStats = Get-Stats $precisions

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "           FINAL RESULTS" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Dataset:      $Dataset" -ForegroundColor White
Write-Host "Profile:      $Profile" -ForegroundColor White
Write-Host "Queries:      $queries" -ForegroundColor White
Write-Host ""
Write-Host "Ratio:" -ForegroundColor Yellow
Write-Host "  Mean:       $("{0:F3}" -f $ratioStats.Mean)" -ForegroundColor White
Write-Host "  Median:     $("{0:F3}" -f $ratioStats.Median)" -ForegroundColor White
Write-Host "  Min:        $("{0:F3}" -f $ratioStats.Min)" -ForegroundColor White
Write-Host "  Max:        $("{0:F3}" -f $ratioStats.Max)" -ForegroundColor White
Write-Host "  Std:        $("{0:F3}" -f $ratioStats.Std)" -ForegroundColor White
Write-Host ""
Write-Host "Precision:" -ForegroundColor Yellow
Write-Host "  Mean:       $("{0:F3}" -f $precisionStats.Mean)" -ForegroundColor White
Write-Host "  Median:     $("{0:F3}" -f $precisionStats.Median)" -ForegroundColor White
Write-Host "  Min:        $("{0:F3}" -f $precisionStats.Min)" -ForegroundColor White
Write-Host "  Max:        $("{0:F3}" -f $precisionStats.Max)" -ForegroundColor White
Write-Host ""
Write-Host "Latency (ms):" -ForegroundColor Yellow
Write-Host "  Server:     $("{0:F1}" -f ($serverMs | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host "  Client:     $("{0:F1}" -f ($clientMs | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host "  Total:      $("{0:F1}" -f (($serverMs + $clientMs) | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host ""

# ================= STATUS CHECK =================

$passRatio = $ratioStats.Mean -le 1.40
$passPrecision = $precisionStats.Mean -ge 0.25

Write-Host "Status:" -ForegroundColor Cyan
Write-Host ""

$ratioMsg = "Ratio {0:F3}" -f $ratioStats.Mean
if ($passRatio) {
    Write-Host "  ✅ [PASS] $ratioMsg ≤ 1.40" -ForegroundColor Green
} else {
    Write-Host "  ❌ [FAIL] $ratioMsg > 1.30" -ForegroundColor Red
}

$precisionMsg = "Precision {0:F3}" -f $precisionStats.Mean
if ($passPrecision) {
    Write-Host "  ✅ [PASS] $precisionMsg ≥ 0.25" -ForegroundColor Green
} else {
    Write-Host "  ❌ [FAIL] $precisionMsg < 0.25" -ForegroundColor Red
}

Write-Host ""

# Check GT validation
$logContent = Get-Content $logPath -Raw
if ($logContent -match "GT Validation PASSED") {
    Write-Host "  ✅ [PASS] GT Validation" -ForegroundColor Green
} elseif ($logContent -match "GT VALIDATION FAILED") {
    Write-Host "  ❌ [FAIL] GT Validation" -ForegroundColor Red
} else {
    Write-Host "  ⚠️  [WARN] GT Validation not found in log" -ForegroundColor Yellow
}

# Check for critical errors
if (($precisions | Measure-Object -Maximum).Maximum -eq 0) {
    Write-Host "❌ [FAIL] All queries have zero precision" -ForegroundColor Red
}


Write-Host ""

# ================= SUMMARY =================

$totalDuration = (Get-Date) - $startTimeTotal

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Files:" -ForegroundColor Cyan
Write-Host "  Config:    $configPath" -ForegroundColor Gray
Write-Host "  Results:   $profilerCsv" -ForegroundColor Gray
Write-Host "  Log:       $logPath" -ForegroundColor Gray
Write-Host "  Errors:    $errorLogPath" -ForegroundColor Gray
Write-Host ""
Write-Host "Total runtime: $([math]::Round($totalDuration.TotalMinutes, 1)) minutes" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

if ($passRatio -and $passPrecision) {
    Write-Host "✅ FULL RUN PASSED!" -ForegroundColor Green
    Write-Host "System is ready for publication." -ForegroundColor Green
} else {
    Write-Host "⚠️  Results need adjustment." -ForegroundColor Yellow
    if (-not $passPrecision) {
        Write-Host ""
        Write-Host "Precision failure checklist:" -ForegroundColor Yellow
        Write-Host "  1. Verify lambda >= 2 (lambda=1 breaks system)" -ForegroundColor Gray
        Write-Host "  2. Verify maxRelaxationDepth >= 1" -ForegroundColor Gray
        Write-Host "  3. Check GFunctionRegistry initialization logs" -ForegroundColor Gray
        Write-Host "  4. Verify codes match between index/query" -ForegroundColor Gray
    }
    if (-not $passRatio) {
        Write-Host ""
        Write-Host "Ratio failure checklist:" -ForegroundColor Yellow
        Write-Host "  1. Reduce maxCandidateFactor to 3" -ForegroundColor Gray
        Write-Host "  2. Check alpha parameter (should be ~0.4-0.6)" -ForegroundColor Gray
        Write-Host "  3. Verify stabilization is enabled" -ForegroundColor Gray
    }
}

Write-Host "============================================" -ForegroundColor Cyan

exit $(if ($passRatio -and $passPrecision) { 0 } else { 1 })