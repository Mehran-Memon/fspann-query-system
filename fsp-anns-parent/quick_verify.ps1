Write-Host "Quick GFunctionRegistry Validation Test" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

$JarPath = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-shaded.jar"
$ConfigPath = "E:\Research Work\Datasets\SIFT1M\config.json"
$BasePath = "E:\Research Work\Datasets\SIFT1M\sift_base.fvecs"
$QueryPath = "E:\Research Work\Datasets\SIFT1M\sift_query.fvecs"
$GTPath = "E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"
$MetadataPath = "E:\Research Work\Datasets\SIFT1M\metadata"
$LogPath = "quick_test.log"

# Clean old metadata
if (Test-Path $MetadataPath) {
    Write-Host "Cleaning old metadata..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $MetadataPath
}

Write-Host "Running with 20 queries (takes ~5 minutes)..." -ForegroundColor Cyan
Write-Host ""

$startTime = Get-Date

# Run Java
$process = Start-Process -FilePath "java" -ArgumentList @(
    "-Xmx8g",
    "-Dquery.limit=20",
    "-Dbase.path=`"$BasePath`"",
    "-Dreenc.mode=end",
    "-jar", "`"$JarPath`"",
    "`"$ConfigPath`"",
    "`"$BasePath`"",
    "`"$QueryPath`"",
    "keys",
    "128",
    "`"$MetadataPath`"",
    "`"$GTPath`"",
    "100000"
) -NoNewWindow -Wait -PassThru `
  -RedirectStandardOutput $LogPath `
  -RedirectStandardError "quick_test_error.log"

$duration = (Get-Date) - $startTime

Write-Host ""
Write-Host "Completed in $([math]::Round($duration.TotalMinutes, 1)) minutes" -ForegroundColor Cyan
Write-Host ""

# Check for GFunctionRegistry initialization
Write-Host "Checking logs..." -ForegroundColor Cyan
$logContent = Get-Content $LogPath -Raw

if ($logContent -match "GFunctionRegistry.*initialized.*omega") {
    Write-Host "[✓] GFunctionRegistry initialized!" -ForegroundColor Green

    # Extract omega stats
    if ($logContent -match "omega range=\[([0-9.]+), ([0-9.]+)\], mean=([0-9.]+)") {
        $omegaMin = $matches[1]
        $omegaMax = $matches[2]
        $omegaMean = $matches[3]
        Write-Host "    omega: min=$omegaMin, max=$omegaMax, mean=$omegaMean" -ForegroundColor Gray
    }
} else {
    Write-Host "[✗] GFunctionRegistry NOT initialized!" -ForegroundColor Red
    Write-Host "    The fix didn't work." -ForegroundColor Red
}

# Check results
$summaryPath = Join-Path $MetadataPath "results\summary.csv"
if (Test-Path $summaryPath) {
    $data = Import-Csv $summaryPath
    $precision = [double]$data.avg_precision
    $ratio = [double]$data.avg_ratio
    $candTotal = [double]$data.avg_cand_total

    Write-Host ""
    Write-Host "Results:" -ForegroundColor Cyan
    Write-Host "  avg_cand_total: $candTotal" -ForegroundColor $(if ($candTotal -gt 100) { "Green" } else { "Red" })
    Write-Host "  avg_precision:  $precision" -ForegroundColor $(if ($precision -gt 0.5) { "Green" } else { "Red" })
    Write-Host "  avg_ratio:      $ratio" -ForegroundColor $(if ($ratio -lt 2.0) { "Green" } else { "Yellow" })

    Write-Host ""
    if ($precision -gt 0.5 -and $candTotal -gt 100) {
        Write-Host "✅ FIX SUCCESSFUL!" -ForegroundColor Green
        Write-Host "   Ready for full evaluation." -ForegroundColor Green
        Write-Host ""
        Write-Host "Next step: Run full_run.ps1 for complete test" -ForegroundColor Cyan
    } else {
        Write-Host "❌ FIX FAILED" -ForegroundColor Red
        Write-Host "   Need diagnostic logging." -ForegroundColor Red
    }
} else {
    Write-Host "[✗] Results not found!" -ForegroundColor Red
}

Write-Host ""
Write-Host "Log file: $LogPath" -ForegroundColor Gray