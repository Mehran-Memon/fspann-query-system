# ============================================
# FSP-ANN INDEX BUILDER - Windows PowerShell
# ============================================
# FULL MODE: Builds the encrypted index
# Run this ONCE, then use query-only mode for testing

param(
    [string]$Dataset = "SIFT1M",
    [string]$Profile = "M24"
)

$ErrorActionPreference = "Stop"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  FSP-ANN Index Builder (FULL MODE)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "WARNING: This will take 60-90 minutes!" -ForegroundColor Yellow
Write-Host "This only needs to run once to build the index." -ForegroundColor Yellow
Write-Host ""

# ================= CONFIGURATION =================

$JarPath = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
$OutRoot = "G:\SMOKE_TEST"
$Batch = 100000

# JVM Arguments for FULL mode (with indexing)
$JvmArgs = @(
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:+AlwaysPreTouch",
    "-Xmx8g",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end"
# NO -Dquery.only flag = FULL MODE
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
Write-Host "Dim:      $($ds.Dim)" -ForegroundColor Green
Write-Host ""

# ================= CHECK FOR EXISTING INDEX =================

$runDir = Join-Path $OutRoot "$($Dataset)_$($Profile)"
$pointsDir = Join-Path $runDir "points"

if (Test-Path $pointsDir) {
    Write-Host "WARNING: Index already exists at: $pointsDir" -ForegroundColor Yellow
    Write-Host ""
    $response = Read-Host "Delete existing index and rebuild? (y/N)"
    if ($response -ne "y") {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 0
    }
    Remove-Item -Path $pointsDir -Recurse -Force
    Write-Host "Existing index deleted." -ForegroundColor Green
    Write-Host ""
}

# ================= SETUP OUTPUT DIR =================

$resultsDir = Join-Path $runDir "results"

# Clear previous results
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

# ================= RUN INDEX BUILD =================

$logPath = Join-Path $runDir "build.log"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  STARTING INDEX BUILD (FULL MODE)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "This will take approximately 60-90 minutes." -ForegroundColor Yellow
Write-Host "Log: $logPath" -ForegroundColor Gray
Write-Host ""
Write-Host "Progress updates:" -ForegroundColor Cyan
Write-Host "  - Indexing: Watch for 'Indexed X vectors' messages" -ForegroundColor Gray
Write-Host "  - Queries: Will run automatically after indexing" -ForegroundColor Gray
Write-Host ""

$startTime = Get-Date

# Build Java command (FULL mode - passes dataPath normally)
$javaCmd = "java"
$javaArgs = $JvmArgs + @(
    "-Dcli.dataset=$Dataset",
    "-Dcli.profile=$Profile",
    "-Dquery.limit=50",  # Will run 50 queries after indexing
    "-Dbase.path=`"$($ds.Base)`"",
    "-jar",
    "`"$JarPath`"",
    "`"$configPath`"",
    "`"$($ds.Base)`"",                    # FULL MODE: Pass actual data path
    "`"$($ds.Query)`"",
    "`"$(Join-Path $runDir 'keys.blob')`"",
    $ds.Dim.ToString(),
    "`"$runDir`"",
    "`"$($ds.GT)`"",
    $Batch.ToString()
)

Write-Host "Starting at: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
Write-Host ""

# Run Java process
$process = Start-Process -FilePath $javaCmd -ArgumentList $javaArgs `
    -NoNewWindow -Wait -PassThru `
    -RedirectStandardOutput $logPath `
    -RedirectStandardError (Join-Path $runDir "build_error.log")

$exitCode = $process.ExitCode
$endTime = Get-Date
$elapsed = ($endTime - $startTime).TotalMinutes

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan

if ($exitCode -ne 0) {
    Write-Host "INDEX BUILD FAILED (exit code: $exitCode)" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Last 50 lines of log:" -ForegroundColor Yellow
    Get-Content $logPath -Tail 50
    exit $exitCode
}

Write-Host "INDEX BUILD COMPLETED!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Build time: $([math]::Round($elapsed, 1)) minutes" -ForegroundColor Green
Write-Host "Finished at: $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
Write-Host ""

# ================= VERIFY INDEX =================

Write-Host "Verifying index..." -ForegroundColor Cyan

if (Test-Path $pointsDir) {
    $versions = Get-ChildItem $pointsDir -Directory | Where-Object { $_.Name -match "^v\d+$" }

    if ($versions.Count -gt 0) {
        $latestVer = ($versions | ForEach-Object { [int]$_.Name.Substring(1) } | Measure-Object -Maximum).Maximum
        $latestVerDir = Join-Path $pointsDir "v$latestVer"
        $fileCount = (Get-ChildItem $latestVerDir -File -Recurse -ErrorAction SilentlyContinue).Count

        Write-Host "  Version: v$latestVer" -ForegroundColor Green
        Write-Host "  Files: $fileCount" -ForegroundColor Green
        Write-Host ""

        if ($fileCount -eq 0) {
            Write-Host "WARNING: No files found in index!" -ForegroundColor Yellow
        }
    } else {
        Write-Host "WARNING: No version directories found!" -ForegroundColor Yellow
    }
} else {
    Write-Host "ERROR: Points directory not created!" -ForegroundColor Red
}

# ================= EXTRACT INITIAL RESULTS =================

$profilerCsv = Join-Path $resultsDir "profiler_metrics.csv"

if (Test-Path $profilerCsv) {
    Write-Host "Initial query results (from build):" -ForegroundColor Cyan
    Write-Host "------------------------------------" -ForegroundColor Cyan

    $data = Import-Csv $profilerCsv
    $queries = $data.Count

    if ($queries -gt 0) {
        $ratios = $data | ForEach-Object { [double]$_.ratio }
        $avgRatio = ($ratios | Measure-Object -Average).Average

        Write-Host "  Queries: $queries" -ForegroundColor White
        Write-Host "  Avg Ratio: $("{0:F3}" -f $avgRatio)" -ForegroundColor White
        Write-Host ""
    }
}

# ================= NEXT STEPS =================

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  INDEX BUILD SUCCESSFUL!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Run query-only tests (fast, 2-3 minutes):" -ForegroundColor Yellow
Write-Host "   .\smoke_test.ps1 -QueryLimit 50" -ForegroundColor White
Write-Host ""
Write-Host "2. Run full evaluation sweep:" -ForegroundColor Yellow
Write-Host "   .\sweep.ps1 -Dataset $Dataset -Profile $Profile" -ForegroundColor White
Write-Host ""
Write-Host "Files:" -ForegroundColor Cyan
Write-Host "  Index:     $pointsDir" -ForegroundColor Gray
Write-Host "  Config:    $configPath" -ForegroundColor Gray
Write-Host "  Log:       $logPath" -ForegroundColor Gray
Write-Host ""

exit 0