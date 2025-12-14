Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---- JAR RESOLUTION ----
$ApiDir = "F:\fspann-query-system\fsp-anns-parent\api\target"
$ShadedJar = Join-Path $ApiDir "api-0.0.1-SNAPSHOT-shaded.jar"
$ThinJar   = Join-Path $ApiDir "api-0.0.1-SNAPSHOT.jar"

if (Test-Path $ShadedJar) {
    $JarPath = $ShadedJar
    Write-Host "Using SHADED JAR: $JarPath" -ForegroundColor Green
}
elseif (Test-Path $ThinJar) {
    $JarPath = $ThinJar
    Write-Host "Using THIN JAR: $JarPath" -ForegroundColor Yellow
}
else {
    throw "No runnable API JAR found in $ApiDir"
}

# ---- CONFIG FAMILIES ----
$Configs = @(
    @{ Name="SIFT1M";   Path="F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_sift1m.json" },
    @{ Name="GLOVE100"; Path="F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_glove100.json" },
    @{ Name="REDCAPS";  Path="F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config_redcaps.json" }
)

# ---- JVM ARGUMENTS ----
$JvmArgs = @(
    "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200", "-XX:+AlwaysPreTouch",
    "-Xmx16g",
    "-Ddisable.exit=true", "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end", "-Dreenc.minTouched=5000",
    "-Dreenc.batchSize=2000", "-Dlog.progress.everyN=100000",
    "-Dpaper.buildThreshold=2000000"
)

$Batch = 100000

# ---- DATASETS (AUTHORITATIVE) ----
$Datasets = @(
    @{ Name="SIFT1M";      Base="E:\Research Work\Datasets\SIFT1M\sift_base.fvecs";           Query="E:\Research Work\Datasets\SIFT1M\sift_query.fvecs";           GT="E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs";           Dim=128 },
    @{ Name="Glove-100";   Base="E:\Research Work\Datasets\glove-100\glove-100_base.fvecs";  Query="E:\Research Work\Datasets\glove-100\glove-100_query.fvecs";  GT="E:\Research Work\Datasets\glove-100\glove-100_groundtruth.ivecs";  Dim=100 },
    @{ Name="RedCaps";     Base="E:\Research Work\Datasets\redcaps\redcaps_base.fvecs";      Query="E:\Research Work\Datasets\redcaps\redcaps_query.fvecs";      GT="E:\Research Work\Datasets\redcaps\redcaps_groundtruth.ivecs";      Dim=512 }
)

# ---- DEEP MERGE FUNCTION (CRITICAL FIX) ----
function Merge-ConfigObjects {
    param(
        [Parameter(Mandatory=$true)]$Base,
        [Parameter(Mandatory=$true)]$Overrides
    )

    # Deep copy base
    $result = $Base | ConvertTo-Json -Depth 64 | ConvertFrom-Json

    # Recursively merge overrides
    function Merge-Recursive {
        param($target, $source)

        foreach ($key in $source.PSObject.Properties.Name) {
            $sourceValue = $source.$key

            if ($null -eq $sourceValue) {
                continue
            }

            # If both are objects (not arrays/primitives), merge recursively
            if ($sourceValue -is [PSCustomObject] -and $target.PSObject.Properties.Name -contains $key) {
                $targetValue = $target.$key
                if ($targetValue -is [PSCustomObject]) {
                    Merge-Recursive $targetValue $sourceValue
                    continue
                }
            }

            # Otherwise, replace
            $target | Add-Member -MemberType NoteProperty -Name $key -Value $sourceValue -Force
        }
    }

    Merge-Recursive $result $Overrides
    return $result
}

# ---- HELPERS ----
function Extract-Metrics {
    param([string]$LogPath)

    if (-not (Test-Path $LogPath)) { return "N/A|N/A|N/A|N/A" }

    $content = Get-Content $LogPath -Raw

    $server = "N/A"
    $client = "N/A"
    $ratio = "N/A"
    $precision = "N/A"

    # Look for patterns in log
    if ($content -match 'Server.*?(\d+\.\d+)') { $server = $matches[1] }
    if ($content -match 'Client.*?(\d+\.\d+)') { $client = $matches[1] }
    if ($content -match 'Ratio.*?(\d+\.\d+)') { $ratio = $matches[1] }
    if ($content -match 'Precision.*?(\d+\.\d+)') { $precision = $matches[1] }

    return "$server|$client|$ratio|$precision"
}

function Combine-CSVFiles {
    param(
        [string]$OutputPath,
        [string[]]$InputFiles
    )

    if ($InputFiles.Count -eq 0) { return }

    $outDir = Split-Path -Parent $OutputPath
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    Remove-Item -Path $OutputPath -ErrorAction SilentlyContinue

    $headerWritten = $false
    foreach ($f in $InputFiles) {
        if (-not (Test-Path $f)) { continue }

        $lines = @(Get-Content $f)
        if ($lines.Count -eq 0) { continue }

        # Extract profile name from path
        $profileDir = (Get-Item (Split-Path -Parent (Split-Path -Parent $f))).BaseName

        if (-not $headerWritten) {
            "profile,$(($lines[0]))" | Out-File $OutputPath -Encoding UTF8
            if ($lines.Count -gt 1) {
                $lines[1..($lines.Count-1)] | ForEach-Object { "$profileDir,$_" } | Add-Content $OutputPath -Encoding UTF8
            }
            $headerWritten = $true
        } else {
            if ($lines.Count -gt 1) {
                $lines[1..($lines.Count-1)] | ForEach-Object { "$profileDir,$_" } | Add-Content $OutputPath -Encoding UTF8
            }
        }
    }
}

Write-Host "Verifying system..."
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found in PATH"
}
java -version 2>&1 | Select-Object -First 1
Write-Host "All requirements satisfied`n"

# ---- MAIN LOOP ----
$allResults = @()

foreach ($cfg in $Configs) {
    $CFG_NAME = $cfg.Name
    $ConfigPath = $cfg.Path

    if (-not (Test-Path $ConfigPath)) {
        Write-Host "Skipping $CFG_NAME (config not found: $ConfigPath)" -ForegroundColor Yellow
        continue
    }

    $OutRoot = "G:\fsp-run\$CFG_NAME"
    New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

    Write-Host "CONFIG FAMILY: $CFG_NAME" -ForegroundColor Yellow

    # Load config
    try {
        $cfgObj = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    } catch {
        Write-Host "Failed to parse config: $_" -ForegroundColor Red
        continue
    }

    $baseConfig = $cfgObj.base

    # Find matching dataset
    $matchingDataset = $null
    foreach ($ds in $Datasets) {
        if ($ds.Name -eq $CFG_NAME) {
            $matchingDataset = $ds
            break
        }
    }

    if ($null -eq $matchingDataset) {
        Write-Host "⏭No dataset matching config family $CFG_NAME" -ForegroundColor Yellow
        continue
    }

    $ds = $matchingDataset

    # Verify dataset files
    if (-not (Test-Path $ds.Base -PathType Leaf)) {
        Write-Host "Missing base: $($ds.Base)" -ForegroundColor Red
        continue
    }
    if (-not (Test-Path $ds.Query -PathType Leaf)) {
        Write-Host "Missing query: $($ds.Query)" -ForegroundColor Red
        continue
    }
    if (-not (Test-Path $ds.GT -PathType Leaf)) {
        Write-Host "Missing GT: $($ds.GT)" -ForegroundColor Red
        continue
    }

    $datasetRoot = Join-Path $OutRoot $ds.Name
    New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null

    Write-Host "Dataset: $($ds.Name) (Dim=$($ds.Dim))`n" -ForegroundColor Yellow
    Write-Host "Profile                   | Status      | Server(ms) | Client(ms) | ART(ms) | Ratio  | Precision"
    Write-Host "──────────────────────────────────────────────────────────────────────────────────────────────────"

    # Process each profile
    foreach ($profile in $cfgObj.profiles) {
        $label = $profile.name
        $runDir = Join-Path $datasetRoot $label
        New-Item -ItemType Directory -Force -Path $runDir | Out-Null

        # ===== CRITICAL FIX: Deep merge config =====
        $overrides = $profile.overrides
        if ($null -eq $overrides) { $overrides = @{} }

        $finalConfig = Merge-ConfigObjects -Base $baseConfig -Overrides $overrides

        # ===== Verify override was applied =====
        $appliedDivisions = $finalConfig.paper.divisions
        $appliedAlpha = $finalConfig.stabilization.alpha

        # Set output paths
        $finalConfig.output.resultsDir = Join-Path $runDir "results"
        $finalConfig.ratio.source = "gt"
        $finalConfig.ratio.gtPath = $ds.GT
        $finalConfig.ratio.gtSample = 10000
        $finalConfig.ratio.autoComputeGT = $false
        $finalConfig.ratio.allowComputeIfMissing = $false

        # Save config
        $configFile = Join-Path $runDir "config.json"
        $finalConfig | ConvertTo-Json -Depth 64 | Set-Content $configFile -Encoding UTF8

        # Build command
        $cmd = @(
            $JvmArgs
            "-Dcli.dataset=$($ds.Name)"
            "-Dcli.profile=$label"
            "-jar", $JarPath
            $configFile
            $ds.Base
            $ds.Query
            (Join-Path $runDir "keys.blob")
            "$($ds.Dim)"
            $runDir
            $ds.GT
            "$Batch"
        )

        # Save command
        $cmdLine = $cmd -join " "
        $cmdLine | Out-File (Join-Path $runDir "cmdline.txt") -Encoding UTF8

        # Execute
        $logFile = Join-Path $runDir "run.out.log"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()

        try {
            & java @cmd 2>&1 | Tee-Object -FilePath $logFile | Where-Object { $_ -notmatch '^\[\d+/\d+\]' } | Out-Null
            $exitCode = 0
        } catch {
            $exitCode = 1
        }

        $sw.Stop()

        # Extract metrics
        $metrics = Extract-Metrics $logFile
        $parts = $metrics -split '\|'
        $serverMs = $parts[0]
        $clientMs = $parts[1]
        $ratio = $parts[2]
        $precision = $parts[3]

        $artMs = "N/A"
        if ($serverMs -ne "N/A" -and $clientMs -ne "N/A") {
            $artMs = [math]::Round([double]$serverMs + [double]$clientMs, 2)
        }

        # Display
        if ($exitCode -eq 0) {
            $status = "OK"
            $color = "Green"
        } else {
            $status = "FAILED"
            $color = "Red"
        }

        Write-Host ("{0,-25} | {1,-11} | {2,10} | {3,10} | {4,7} | {5,6} | {6,9}  [d={7} α={8}]" -f `
            $label, $status, $serverMs, $clientMs, $artMs, $ratio, $precision, `
            $appliedDivisions, $appliedAlpha) -ForegroundColor $color

        $allResults += @{
            Dataset = $ds.Name
            Profile = $label
            ExitCode = $exitCode
            ElapsedSec = [math]::Round($sw.Elapsed.TotalSeconds, 1)
            ServerMs = $serverMs
            ClientMs = $clientMs
            Ratio = $ratio
            Precision = $precision
            Divisions = $appliedDivisions
            Alpha = $appliedAlpha
        }
    }

    # ---- Combine results per dataset ----
    $csvFiles = @(Get-ChildItem -Path (Join-Path $datasetRoot "*\results\results_table.csv") -File -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName)
    if ($csvFiles.Count -gt 0) {
        Combine-CSVFiles -OutputPath (Join-Path $datasetRoot "combined_results.csv") -InputFiles $csvFiles
    }

    Write-Host ""
}

# ---- FINAL SUMMARY ----
Write-Host "                     RUN SUMMARY                                "

Write-Host "Dataset        | Profile               | Elapsed(s) | ART(ms) | Ratio | Divisions | Alpha | Status"
Write-Host "──────────────────────────────────────────────────────────────────────────────────────────────────────"

foreach ($r in $allResults) {
    $status = if ($r.ExitCode -eq 0) { "✅" } else { "❌" }

    $art = "N/A"
    if ($r.ServerMs -ne "N/A" -and $r.ClientMs -ne "N/A") {
        $art = [math]::Round([double]$r.ServerMs + [double]$r.ClientMs, 2)
    }

    Write-Host ("{0,-14} | {1,-21} | {2,10} | {3,7} | {4,5} | {5,9} | {6,5} | {7}" -f `
        $r.Dataset, $r.Profile, $r.ElapsedSec, $art, $r.Ratio, $r.Divisions, $r.Alpha, $status)
}

Write-Host "`n Results: G:\fsp-run"
Write-Host " Combined CSVs created per dataset`n"