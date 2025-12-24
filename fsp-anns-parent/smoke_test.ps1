# ============================================
# FSP-ANN SMOKE TEST - Windows PowerShell
# ============================================
# FIXED VERSION:
# - Uses query-only mode (no re-indexing)
# - Reads "precision" column (not "recall")
# - Checks for existing index before running
# - FIXED: Counts all files recursively (not just .enc)

param(
    [string]$Dataset = "SIFT1M",
    [string]$Profile = "M24",
    [int]$QueryLimit = 200
)

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  FSP-ANN Smoke Test (Windows)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# ================= CONFIGURATION =================

$JarPath = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
$OutRoot = "G:\SMOKE_TEST"
$Batch = 100000

# JVM Arguments with query-only mode
$JvmArgs = @(
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:+AlwaysPreTouch",
    "-Xmx8g",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end",
    "-Dquery.only=true"  # CRITICAL FIX: Enable query-only mode
)

# ================= DATASET CONFIG =================

$DatasetConfig = @{
    "SIFT1M" = @{
        Config = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_sift1m.json"
        Dim = 128
        Base = "E:\Research Work\Datasets\SIFT1M\sift_base.fvecs"
        Query = "E:\Research Work\Datasets\SIFT1M\sift_query.fvecs"
        GT = "E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"
    }
    "glove-100" = @{
        Config = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_glove100.json"
        Dim = 100
        Base = "E:\Research Work\Datasets\glove-100\glove-100_base.fvecs"
        Query = "E:\Research Work\Datasets\glove-100\glove-100_query.fvecs"
        GT = "E:\Research Work\Datasets\glove-100\glove-100_groundtruth.ivecs"
    }
    "RedCaps" = @{
        Config = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_redcaps.json"
        Dim = 512
        Base = "E:\Research Work\Datasets\redcaps\redcaps_base.fvecs"
        Query = "E:\Research Work\Datasets\redcaps\redcaps_query.fvecs"
        GT = "E:\Research Work\Datasets\redcaps\redcaps_groundtruth.ivecs"
    }
}

# ================= VALIDATE INPUTS =================

if (-not $DatasetConfig.ContainsKey($Dataset)) {
    Write-Host "ERROR: Unknown dataset: $Dataset" -ForegroundColor Red
    Write-Host "Available: SIFT1M, glove-100, RedCaps" -ForegroundColor Yellow
    exit 1
}

$ds = $DatasetConfig[$Dataset]

# Check files exist
$filesToCheck = @(
    @{Path = $JarPath; Name = "JAR file"}
    @{Path = $ds.Config; Name = "Config file"}
    @{Path = $ds.Base; Name = "Base vectors"}
    @{Path = $ds.Query; Name = "Query vectors"}
    @{Path = $ds.GT; Name = "Ground truth"}
)

foreach ($file in $filesToCheck) {
    if (-not (Test-Path $file.Path)) {
        Write-Host "ERROR: $($file.Name) not found: $($file.Path)" -ForegroundColor Red
        exit 1
    }
}

Write-Host "Dataset:  $Dataset" -ForegroundColor Green
Write-Host "Profile:  $Profile" -ForegroundColor Green
Write-Host "Queries:  $QueryLimit" -ForegroundColor Green
Write-Host "Dim:      $($ds.Dim)" -ForegroundColor Green
Write-Host ""

# ================= CHECK FOR EXISTING INDEX =================

$runDir = Join-Path $OutRoot "$($Dataset)_$($Profile)"
$pointsDir = Join-Path $runDir "points"

if (-not (Test-Path $pointsDir)) {
    Write-Host "ERROR: No existing index found at: $pointsDir" -ForegroundColor Red
    Write-Host ""
    Write-Host "Query-only mode requires a pre-built index." -ForegroundColor Yellow
    Write-Host "Build the index first using FULL mode:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  .\smoke_test_build_index.ps1 -Dataset $Dataset -Profile $Profile" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}

# Detect latest version
$versions = Get-ChildItem $pointsDir -Directory | Where-Object { $_.Name -match "^v\d+$" } |
        ForEach-Object { [int]$_.Name.Substring(1) } | Sort-Object -Descending

if ($versions.Count -eq 0) {
    Write-Host "ERROR: No version directories found in: $pointsDir" -ForegroundColor Red
    Write-Host "Expected directories like: v1, v2, etc." -ForegroundColor Yellow
    exit 1
}

$latestVer = $versions[0]
$latestVerDir = Join-Path $pointsDir "v$latestVer"

# CRITICAL FIX: Count all files recursively, regardless of extension
$encFiles = @(Get-ChildItem $latestVerDir -File -Recurse -ErrorAction SilentlyContinue)

Write-Host "Using existing index: $pointsDir" -ForegroundColor Green
Write-Host "  Latest version: v$latestVer" -ForegroundColor Gray
Write-Host "  Files found: $($encFiles.Count)" -ForegroundColor Gray

# Show file extension distribution
if ($encFiles.Count -gt 0) {
    $extGroups = $encFiles | Group-Object Extension | Sort-Object Count -Descending
    if ($extGroups.Count -le 3) {
        foreach ($grp in $extGroups) {
            $extName = if ($grp.Name -eq "") { "(no extension)" } else { $grp.Name }
            Write-Host "    $extName : $($grp.Count) files" -ForegroundColor Gray
        }
    }
}
Write-Host ""

# Enhanced error reporting showing actual directory contents
if ($encFiles.Count -eq 0) {
    Write-Host "WARNING: No files found in v$latestVer" -ForegroundColor Yellow
    Write-Host "Contents of $latestVerDir :" -ForegroundColor Yellow
    Get-ChildItem $latestVerDir -ErrorAction SilentlyContinue | Select-Object -First 10 | ForEach-Object {
        Write-Host "  $($_.Name) ($($_.GetType().Name))" -ForegroundColor Gray
    }
    Write-Host ""
}

# ================= SETUP OUTPUT DIR =================

$resultsDir = Join-Path $runDir "results"

# Clear previous results but keep index
if (Test-Path $resultsDir) {
    Remove-Item -Path $resultsDir -Recurse -Force
}

New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Host "Output:   $runDir" -ForegroundColor Green
Write-Host ""

# ================= BUILD CONFIG =================

Write-Host "Building configuration..." -ForegroundColor Cyan

$configJson = Get-Content $ds.Config -Raw | ConvertFrom-Json

if ($Profile -eq "BASE") {
    $finalConfig = $configJson
    $finalConfig.PSObject.Properties.Remove('profiles')
} else {
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
}

# Set output paths
if (-not $finalConfig.output) {
    $finalConfig | Add-Member -MemberType NoteProperty -Name output -Value ([PSCustomObject]@{})
}
$finalConfig.output | Add-Member -MemberType NoteProperty -Name resultsDir -Value $resultsDir -Force

if (-not $finalConfig.ratio) {
    $finalConfig | Add-Member -MemberType NoteProperty -Name ratio -Value ([PSCustomObject]@{})
}
$finalConfig.ratio | Add-Member -MemberType NoteProperty -Name source -Value "gt" -Force
$finalConfig.ratio | Add-Member -MemberType NoteProperty -Name gtPath -Value $ds.GT -Force

# Save config
$configPath = Join-Path $runDir "config.json"
$finalConfig | ConvertTo-Json -Depth 10 | Set-Content $configPath

# ================= DISPLAY CONFIG =================

Write-Host "Configuration:" -ForegroundColor Cyan

function Get-ConfigValue($obj, $path) {
    $parts = $path -split '\.'
    $current = $obj
    foreach ($part in $parts) {
        if ($current.PSObject.Properties[$part]) {
            $current = $current.$part
        } else {
            return "N/A"
        }
    }
    return $current
}

$configToShow = @{
    "m" = Get-ConfigValue $finalConfig "base.paper.m"
    "lambda" = Get-ConfigValue $finalConfig "base.paper.lambda"
    "divisions" = Get-ConfigValue $finalConfig "base.paper.divisions"
    "alpha" = Get-ConfigValue $finalConfig "base.stabilization.alpha"
    "minCand" = Get-ConfigValue $finalConfig "base.stabilization.minCandidates"
}

if ($configToShow.m -eq "N/A") { $configToShow.m = Get-ConfigValue $finalConfig "paper.m" }
if ($configToShow.lambda -eq "N/A") { $configToShow.lambda = Get-ConfigValue $finalConfig "paper.lambda" }
if ($configToShow.divisions -eq "N/A") { $configToShow.divisions = Get-ConfigValue $finalConfig "paper.divisions" }
if ($configToShow.alpha -eq "N/A") { $configToShow.alpha = Get-ConfigValue $finalConfig "stabilization.alpha" }
if ($configToShow.minCand -eq "N/A") { $configToShow.minCand = Get-ConfigValue $finalConfig "stabilization.minCandidates" }

foreach ($key in $configToShow.Keys) {
    Write-Host "  ${key}: $($configToShow[$key])" -ForegroundColor Gray
}
Write-Host ""

# ================= RUN TEST =================

$logPath = Join-Path $runDir "run.log"

Write-Host "Starting smoke test (query-only mode)..." -ForegroundColor Cyan
Write-Host "Log: $logPath" -ForegroundColor Gray
Write-Host ""

$startTime = Get-Date

# Build Java command (query-only mode uses POINTS_ONLY)
$javaCmd = "java"
$javaArgs = $JvmArgs + @(
    "-Dcli.dataset=$Dataset",
    "-Dcli.profile=$Profile",
    "-Dquery.limit=$QueryLimit",
    "-Dbase.path=`"$($ds.Base)`"",  # CRITICAL: Set base.path for ratio computation
    "-jar",
    "`"$JarPath`"",
    "`"$configPath`"",
    "POINTS_ONLY",                   # CRITICAL FIX: Skip indexing
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
$endTime = Get-Date
$elapsed = ($endTime - $startTime).TotalSeconds

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan

if ($exitCode -ne 0) {
    Write-Host "SMOKE TEST FAILED (exit code: $exitCode)" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Last 50 lines of log:" -ForegroundColor Yellow
    Get-Content $logPath -Tail 50
    exit $exitCode
}

Write-Host "SMOKE TEST COMPLETED" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Runtime: $([math]::Round($elapsed, 1))s" -ForegroundColor Green
Write-Host ""

# ================= EXTRACT RESULTS =================

$profilerCsv = Join-Path $resultsDir "profiler_metrics.csv"

if (-not (Test-Path $profilerCsv)) {
    Write-Host "WARNING: profiler_metrics.csv not found" -ForegroundColor Yellow
    exit 1
}

Write-Host "Results:" -ForegroundColor Cyan
Write-Host "--------" -ForegroundColor Cyan

# Parse CSV
$data = Import-Csv $profilerCsv

$queries = $data.Count
$ratios = $data | ForEach-Object { [double]$_.ratio }
$precisions = $data | ForEach-Object { [double]$_.precision }  # CRITICAL FIX: Read "precision" not "recall"
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

Write-Host "Queries:      $queries" -ForegroundColor White
Write-Host ""
Write-Host "Ratio:" -ForegroundColor Yellow
Write-Host "  Mean:       $("{0:F3}" -f $ratioStats.Mean)" -ForegroundColor White
Write-Host "  Median:     $("{0:F3}" -f $ratioStats.Median)" -ForegroundColor White
Write-Host "  Min:        $("{0:F3}" -f $ratioStats.Min)" -ForegroundColor White
Write-Host "  Max:        $("{0:F3}" -f $ratioStats.Max)" -ForegroundColor White
Write-Host "  Std:        $("{0:F3}" -f $ratioStats.Std)" -ForegroundColor White
Write-Host ""
Write-Host "Precision:" -ForegroundColor Yellow  # CRITICAL FIX: Changed label
Write-Host "  Mean:       $("{0:F3}" -f $precisionStats.Mean)" -ForegroundColor White
Write-Host "  Min:        $("{0:F3}" -f $precisionStats.Min)" -ForegroundColor White
Write-Host ""
Write-Host "Latency (ms):" -ForegroundColor Yellow
Write-Host "  Server:     $("{0:F1}" -f ($serverMs | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host "  Client:     $("{0:F1}" -f ($clientMs | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host "  Total:      $("{0:F1}" -f (($serverMs + $clientMs) | Measure-Object -Average).Average)" -ForegroundColor White
Write-Host ""

# ================= CHECK STATUS =================

Write-Host "Status:" -ForegroundColor Cyan

$passRatio = $ratioStats.Mean -le 1.30
$passPrecision = $precisionStats.Mean -ge 0.85

$ratioMsg = "Ratio {0:F3}" -f $ratioStats.Mean
if ($passRatio) {
    Write-Host "  [PASS] $ratioMsg is at most 1.30" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] $ratioMsg exceeds 1.30" -ForegroundColor Red
}

$precisionMsg = "Precision {0:F3}" -f $precisionStats.Mean
if ($passPrecision) {
    Write-Host "  [PASS] $precisionMsg is at least 0.85" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] $precisionMsg is below 0.85" -ForegroundColor Red
}

Write-Host ""

# ================= CHECK GT VALIDATION =================

$logContent = Get-Content $logPath -Raw

if ($logContent -match "GT VALIDATION PASSED") {
    Write-Host "[PASS] GT Validation" -ForegroundColor Green
} elseif ($logContent -match "GT VALIDATION FAILED") {
    Write-Host "[FAIL] GT Validation" -ForegroundColor Red
    Write-Host ""
    $logContent -split "`n" | Where-Object { $_ -match "GT VALIDATION" } | Select-Object -First 10 | ForEach-Object {
        Write-Host $_ -ForegroundColor Yellow
    }
} else {
    Write-Host "[WARN] GT Validation not found in log" -ForegroundColor Yellow
}

Write-Host ""

# ================= FINAL OUTPUT =================

Write-Host "Files:" -ForegroundColor Cyan
Write-Host "  Config:    $configPath" -ForegroundColor Gray
Write-Host "  Results:   $profilerCsv" -ForegroundColor Gray
Write-Host "  Log:       $logPath" -ForegroundColor Gray
Write-Host ""

Write-Host "============================================" -ForegroundColor Cyan
if ($passRatio -and $passPrecision) {
    Write-Host "Smoke test PASSED!" -ForegroundColor Green
    Write-Host "Ready for full sweep." -ForegroundColor Green
} else {
    Write-Host "Smoke test needs adjustment." -ForegroundColor Yellow
    Write-Host "Review configuration before full sweep." -ForegroundColor Yellow
}
Write-Host "============================================" -ForegroundColor Cyan

exit $(if ($passRatio -and $passPrecision) { 0 } else { 1 })