# ============================================
# FSP-ANN FULL RUN - Windows PowerShell
# ============================================
# Complete end-to-end test:
# 1. Verifies/fixes config
# 2. Rebuilds JAR
# 3. Builds fresh index (83 min)
# 4. Runs queries
# 5. Reports results

param(
    [string]$Dataset = "SIFT1M",
    [string]$Profile = "M24",
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

# JVM Arguments for FULL mode (no query.only flag!)
$JvmArgs = @(
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:+AlwaysPreTouch",
    "-Xmx8g",
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

# Check critical values
$maxCandFactor = $configJson.base.runtime.maxCandidateFactor
$gtSample = $configJson.base.ratio.gtSample

Write-Host "  Current config:" -ForegroundColor Gray
Write-Host "    maxCandidateFactor: $maxCandFactor" -ForegroundColor Gray
Write-Host "    gtSample: $gtSample" -ForegroundColor Gray

if ($maxCandFactor -gt 10) {
    Write-Host "  WARNING: maxCandidateFactor=$maxCandFactor is too high (should be 3)" -ForegroundColor Yellow
    Write-Host "  This will cause high ratios!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Fix manually in: $ConfigPath" -ForegroundColor Yellow
    Write-Host "  Change 'maxCandidateFactor' from $maxCandFactor to 3" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Press Enter to continue anyway, or Ctrl+C to cancel..." -ForegroundColor Yellow
    Read-Host
}

if ($gtSample -gt 50 -or $gtSample -eq $null) {
    Write-Host "  WARNING: gtSample=$gtSample is too high or missing (should be 10)" -ForegroundColor Yellow
    Write-Host "  This will cause 2+ hour hangs!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Fix manually in: $ConfigPath" -ForegroundColor Yellow
    Write-Host "  Set 'gtSample' to 10" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Press Enter to continue anyway, or Ctrl+C to cancel..." -ForegroundColor Yellow
    Read-Host
}

Write-Host "  Config OK" -ForegroundColor Green
Write-Host ""

# ================= STEP 2: REBUILD JAR =================

Write-Host "[STEP 2/5] Rebuilding JAR..." -ForegroundColor Cyan

Push-Location "F:\fspann-query-system\fsp-anns-parent"

try {
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
    Write-Host "  JAR built: $jarTime" -ForegroundColor Green
} finally {
    Pop-Location
}

Write-Host ""

# ================= STEP 3: CLEAN OLD INDEX =================

Write-Host "[STEP 3/5] Cleaning old index..." -ForegroundColor Cyan

$runDir = Join-Path $OutRoot "$($Dataset)_$($Profile)"

if (Test-Path $runDir) {
    Write-Host "  Deleting: $runDir" -ForegroundColor Gray
    Remove-Item -Path $runDir -Recurse -Force
}

New-Item -ItemType Directory -Path $runDir -Force | Out-Null
Write-Host "  Clean workspace ready" -ForegroundColor Green
Write-Host ""

# ================= STEP 4: BUILD INDEX =================

Write-Host "[STEP 4/5] Building index (this takes ~83 minutes)..." -ForegroundColor Cyan

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
$finalConfig = Merge-Objects $baseConfig $profileObj.overrides

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

# Save config
$configPath = Join-Path $runDir "config.json"
$finalConfig | ConvertTo-Json -Depth 10 | Set-Content $configPath

# Display config
Write-Host "  Configuration:" -ForegroundColor Gray
Write-Host "    m:         $($finalConfig.base.paper.m)" -ForegroundColor Gray
Write-Host "    lambda:    $($finalConfig.base.paper.lambda)" -ForegroundColor Gray
Write-Host "    divisions: $($finalConfig.base.paper.divisions)" -ForegroundColor Gray
Write-Host "    alpha:     $($finalConfig.base.stabilization.alpha)" -ForegroundColor Gray
Write-Host ""

# Run Java (FULL MODE - no POINTS_ONLY!)
$logPath = Join-Path $runDir "run.log"
$startTimeBuild = Get-Date

Write-Host "  Starting index build at $(Get-Date -Format 'HH:mm:ss')..." -ForegroundColor Gray
Write-Host "  Log: $logPath" -ForegroundColor Gray
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
    "`"$($ds.Base)`"",      # BASE VECTORS (not POINTS_ONLY!)
    "`"$($ds.Query)`"",
    "`"$(Join-Path $runDir 'keys.blob')`"",
    $ds.Dim.ToString(),
    "`"$runDir`"",
    "`"$($ds.GT)`"",
    $Batch.ToString()
)

# Run Java process
$process = Start-Process -FilePath $javaCmd -ArgumentList $javaArgs `
    -NoNewWindow -Wait -PassThru `
    -RedirectStandardOutput $logPath `
    -RedirectStandardError (Join-Path $runDir "error.log")

$exitCode = $process.ExitCode
$buildDuration = (Get-Date) - $startTimeBuild

if ($exitCode -ne 0) {
    Write-Host "ERROR: Build failed (exit code: $exitCode)" -ForegroundColor Red
    Write-Host ""
    Write-Host "Last 50 lines of log:" -ForegroundColor Yellow
    Get-Content $logPath -Tail 50
    exit $exitCode
}

Write-Host "  Build complete: $([math]::Round($buildDuration.TotalMinutes, 1)) minutes" -ForegroundColor Green
Write-Host ""

# ================= STEP 5: ANALYZE RESULTS =================

Write-Host "[STEP 5/5] Analyzing results..." -ForegroundColor Cyan

$profilerCsv = Join-Path $resultsDir "profiler_metrics.csv"

if (-not (Test-Path $profilerCsv)) {
    Write-Host "ERROR: Results CSV not found: $profilerCsv" -ForegroundColor Red
    exit 1
}

# Parse CSV
$data = Import-Csv $profilerCsv

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
Write-Host "  Min:        $("{0:F3}" -f $precisionStats.Min)" -ForegroundColor White
Write-Host ""
Write-Host "Latency (ms):" -ForegroundColor Yellow
Write-Host "  Server:     $("{0:F1}" -f ($serverMs | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host "  Client:     $("{0:F1}" -f ($clientMs | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host "  Total:      $("{0:F1}" -f (($serverMs + $clientMs) | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host ""

# ================= STATUS CHECK =================

$passRatio = $ratioStats.Mean -le 1.30
$passPrecision = $precisionStats.Mean -ge 0.85

Write-Host "Status:" -ForegroundColor Cyan

$ratioMsg = "Ratio {0:F3}" -f $ratioStats.Mean
if ($passRatio) {
    Write-Host "  [PASS] $ratioMsg ≤ 1.30" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] $ratioMsg > 1.30" -ForegroundColor Red
}

$precisionMsg = "Precision {0:F3}" -f $precisionStats.Mean
if ($passPrecision) {
    Write-Host "  [PASS] $precisionMsg ≥ 0.85" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] $precisionMsg < 0.85" -ForegroundColor Red
}

Write-Host ""

# Check GT validation
$logContent = Get-Content $logPath -Raw
if ($logContent -match "GT VALIDATION PASSED") {
    Write-Host "  [PASS] GT Validation" -ForegroundColor Green
} elseif ($logContent -match "GT VALIDATION FAILED") {
    Write-Host "  [FAIL] GT Validation" -ForegroundColor Red
} else {
    Write-Host "  [WARN] GT Validation not found in log" -ForegroundColor Yellow
}

Write-Host ""

# ================= SUMMARY =================

$totalDuration = (Get-Date) - $startTimeTotal

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Files:" -ForegroundColor Cyan
Write-Host "  Config:    $configPath" -ForegroundColor Gray
Write-Host "  Results:   $profilerCsv" -ForegroundColor Gray
Write-Host "  Log:       $logPath" -ForegroundColor Gray
Write-Host ""
Write-Host "Total runtime: $([math]::Round($totalDuration.TotalMinutes, 1)) minutes" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

if ($passRatio -and $passPrecision) {
    Write-Host "✅ FULL RUN PASSED!" -ForegroundColor Green
    Write-Host "System is ready for publication." -ForegroundColor Green
} else {
    Write-Host "⚠️  Results need adjustment." -ForegroundColor Yellow
    Write-Host "Review config and retry." -ForegroundColor Yellow
}

Write-Host "============================================" -ForegroundColor Cyan

exit $(if ($passRatio -and $passPrecision) { 0 } else { 1 })