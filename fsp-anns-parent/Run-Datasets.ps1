Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---- JAR RESOLUTION ----
$ApiDir = "F:\fspann-query-system\fsp-anns-parent\api\target"
$ShadedJar = Join-Path $ApiDir "api-0.0.1-SNAPSHOT-shaded.jar"
$ThinJar   = Join-Path $ApiDir "api-0.0.1-SNAPSHOT.jar"

if (Test-Path $ShadedJar) {
    $JarPath = $ShadedJar
    Write-Host "Using SHADED JAR: $JarPath"
}
elseif (Test-Path $ThinJar) {
    $JarPath = $ThinJar
    Write-Host "Using THIN JAR: $JarPath"
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

# ---- DATASETS ----
$Datasets = @(
    @{ Name="SIFT1M";      Base="E:\Research Work\Datasets\SIFT1M\sift_base.fvecs";           Query="E:\Research Work\Datasets\SIFT1M\sift_query.fvecs";           GT="E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs";           Dim=128 },
    @{ Name="Glove-100";   Base="E:\Research Work\Datasets\glove-100\glove-100_base.fvecs";  Query="E:\Research Work\Datasets\glove-100\glove-100_query.fvecs";  GT="E:\Research Work\Datasets\glove-100\glove-100_groundtruth.ivecs";  Dim=100 },
    @{ Name="RedCaps";     Base="E:\Research Work\Datasets\redcaps\redcaps_base.fvecs";      Query="E:\Research Work\Datasets\redcaps\redcaps_query.fvecs";      GT="E:\Research Work\Datasets\redcaps\redcaps_groundtruth.ivecs";      Dim=512 }
)

# ---- DEEP MERGE FUNCTION ----
function Merge-ConfigObjects {
    param(
        [Parameter(Mandatory=$true)]$Base,
        [Parameter(Mandatory=$true)]$Overrides
    )

    $result = $Base | ConvertTo-Json -Depth 64 | ConvertFrom-Json

    function Merge-Recursive {
        param($target, $source)

        foreach ($key in $source.PSObject.Properties.Name) {
            $sourceValue = $source.$key

            if ($null -eq $sourceValue) {
                continue
            }

            if ($sourceValue -is [PSCustomObject] -and $target.PSObject.Properties.Name -contains $key) {
                $targetValue = $target.$key
                if ($targetValue -is [PSCustomObject]) {
                    Merge-Recursive $targetValue $sourceValue
                    continue
                }
            }

            $target | Add-Member -MemberType NoteProperty -Name $key -Value $sourceValue -Force
        }
    }

    Merge-Recursive $result $Overrides
    return $result
}

# ---- SAFE PROPERTY SETTER ----
function Set-SafeProperty {
    param(
        [Parameter(Mandatory=$true)]$Object,
        [Parameter(Mandatory=$true)][string]$PropertyName,
        [Parameter(Mandatory=$true)]$Value
    )

    if ($Object.PSObject.Properties.Name -contains $PropertyName) {
        $Object.$PropertyName = $Value
    } else {
        $Object | Add-Member -MemberType NoteProperty -Name $PropertyName -Value $Value -Force
    }
}

# ---- METRICS EXTRACTION ----
function Extract-Metrics {
    param([string]$LogPath)

    if (-not (Test-Path $LogPath)) { return "N/A|N/A|N/A|N/A" }

    $content = Get-Content $LogPath -Raw

    $server = "N/A"
    $client = "N/A"
    $ratio = "N/A"
    $precision = "N/A"

    if ($content -match '(?:ServerTimeMs|Server Time|ART.*Server)[:\s=]+(\d+\.?\d*)') { $server = $matches[1] }
    if ($content -match '(?:ClientTimeMs|Client Time|ART.*Client)[:\s=]+(\d+\.?\d*)') { $client = $matches[1] }
    if ($content -match '(?:Ratio|CandidateRatio)[:\s=]+(\d+\.?\d*)') { $ratio = $matches[1] }
    if ($content -match '(?:Precision|Recall)[:\s=]+(\d+\.?\d*)') { $precision = $matches[1] }

    return "$server|$client|$ratio|$precision"
}

# ---- CSV COMBINING ----
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

        $lines = @(Get-Content $f -ErrorAction SilentlyContinue)
        if ($lines.Count -eq 0) { continue }

        $profileDir = (Get-Item (Split-Path -Parent (Split-Path -Parent $f))).BaseName

        if (-not $headerWritten) {
            "profile,$($lines[0])" | Out-File $OutputPath -Encoding UTF8
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

# ---- SYSTEM VERIFICATION ----
Write-Host "FSP-ANN 3-Dataset Evaluation Runner"
Write-Host "Starting system verification..."

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found in PATH"
}

Write-Host "Java found"
Write-Host "JAR resolved: $JarPath"
Write-Host "All requirements satisfied"
Write-Host ""

# ---- MAIN EXECUTION ----
$allResults = @()

foreach ($cfg in $Configs) {
    $CFG_NAME = $cfg.Name
    $ConfigPath = $cfg.Path

    if (-not (Test-Path $ConfigPath)) {
        Write-Host "Skipping $CFG_NAME - config not found"
        continue
    }

    $OutRoot = "G:\fsp-run\$CFG_NAME"
    New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

    Write-Host "Processing CONFIG FAMILY: $CFG_NAME"

    try {
        $cfgObj = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    } catch {
        Write-Host "ERROR: Failed to parse config: $_"
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
        Write-Host "No dataset matching config family $CFG_NAME"
        continue
    }

    $ds = $matchingDataset

    # Verify dataset files
    @($ds.Base, $ds.Query, $ds.GT) | ForEach-Object {
        if (-not (Test-Path $_ -PathType Leaf)) {
            throw "Missing dataset file: $_"
        }
    }

    $datasetRoot = Join-Path $OutRoot $ds.Name
    New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null

    Write-Host ""
    Write-Host "Dataset: $($ds.Name) Dimension: $($ds.Dim)"
    Write-Host ""
    Write-Host "Profile                   | Status  | Server(ms) | Client(ms) | ART(ms) | Ratio  | Precision"
    Write-Host "----------------------------------------------------------------------------------------------------"

    # Process each profile
    foreach ($profile in $cfgObj.profiles) {
        $label = $profile.name
        $runDir = Join-Path $datasetRoot $label
        New-Item -ItemType Directory -Force -Path $runDir | Out-Null

        $overrides = $profile.overrides
        if ($null -eq $overrides) { $overrides = @{} }

        $finalConfig = Merge-ConfigObjects -Base $baseConfig -Overrides $overrides

        $appliedDivisions = if ($finalConfig.paper) { $finalConfig.paper.divisions } else { "?" }
        $appliedAlpha = if ($finalConfig.stabilization) { $finalConfig.stabilization.alpha } else { "?" }

        if (-not $finalConfig.output) {
            $finalConfig | Add-Member -MemberType NoteProperty -Name "output" -Value @{} -Force
        }
        Set-SafeProperty -Object $finalConfig.output -PropertyName "resultsDir" -Value (Join-Path $runDir "results")

        if (-not $finalConfig.ratio) {
            $finalConfig | Add-Member -MemberType NoteProperty -Name "ratio" -Value @{} -Force
        }
        Set-SafeProperty -Object $finalConfig.ratio -PropertyName "source" -Value "gt"
        Set-SafeProperty -Object $finalConfig.ratio -PropertyName "gtPath" -Value $ds.GT
        Set-SafeProperty -Object $finalConfig.ratio -PropertyName "gtSample" -Value 10000
        Set-SafeProperty -Object $finalConfig.ratio -PropertyName "gtMismatchTolerance" -Value 0.0
        Set-SafeProperty -Object $finalConfig.ratio -PropertyName "autoComputeGT" -Value $false
        Set-SafeProperty -Object $finalConfig.ratio -PropertyName "allowComputeIfMissing" -Value $false

        $configFile = Join-Path $runDir "config.json"
        $finalConfig | ConvertTo-Json -Depth 64 | Set-Content $configFile -Encoding UTF8

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

        ($cmd -join " ") | Out-File (Join-Path $runDir "cmdline.txt") -Encoding UTF8

        $logFile = Join-Path $runDir "run.out.log"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()

        try {
            & java @cmd 2>&1 | Tee-Object -FilePath $logFile | Where-Object { $_ -notmatch '^\[\d+/\d+\]' } | Out-Null
            $exitCode = 0
        } catch {
            $exitCode = 1
        }

        $sw.Stop()
        $elapsedSec = [math]::Round($sw.Elapsed.TotalSeconds, 1)

        $metrics = Extract-Metrics $logFile
        $parts = $metrics -split '\|'
        $serverMs = $parts[0]
        $clientMs = $parts[1]
        $ratio = $parts[2]
        $precision = $parts[3]

        $artMs = "N/A"
        if ($serverMs -ne "N/A" -and $clientMs -ne "N/A") {
            try {
                $artMs = [math]::Round([double]$serverMs + [double]$clientMs, 2)
            } catch {
                $artMs = "N/A"
            }
        }

        $status = if ($exitCode -eq 0) { "OK" } else { "FAILED" }

        Write-Host ("{0,-25} | {1,-7} | {2,10} | {3,10} | {4,7} | {5,6} | {6,9}" -f `
            $label, $status, $serverMs, $clientMs, $artMs, $ratio, $precision)

        $allResults += @{
            Dataset = $ds.Name
            Profile = $label
            ExitCode = $exitCode
            ElapsedSec = $elapsedSec
            ServerMs = $serverMs
            ClientMs = $clientMs
            Ratio = $ratio
            Precision = $precision
            Divisions = $appliedDivisions
            Alpha = $appliedAlpha
        }
    }

    $csvFiles = @(Get-ChildItem -Path (Join-Path $datasetRoot "*\results\results_table.csv") -File -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName)
    if ($csvFiles.Count -gt 0) {
        Combine-CSVFiles -OutputPath (Join-Path $datasetRoot "combined_results.csv") -InputFiles $csvFiles
        Write-Host "Combined results saved"
    }

    Write-Host ""
}

# ---- FINAL SUMMARY ----
Write-Host ""
Write-Host "RUN SUMMARY"
Write-Host "======================================================================================================"
Write-Host ""
Write-Host "Dataset        | Profile               | Elapsed(s) | ART(ms) | Ratio | Divisions | Alpha | Status"
Write-Host "----------------------------------------------------------------------------------------------------"

foreach ($r in $allResults) {
    $status = if ($r.ExitCode -eq 0) { "OK" } else { "FAILED" }
    $art = "N/A"
    if ($r.ServerMs -ne "N/A" -and $r.ClientMs -ne "N/A") {
        try {
            $art = [math]::Round([double]$r.ServerMs + [double]$r.ClientMs, 2)
        } catch {
            $art = "N/A"
        }
    }

    Write-Host ("{0,-14} | {1,-21} | {2,10} | {3,7} | {4,5} | {5,9} | {6,5} | {7}" -f `
        $r.Dataset, $r.Profile, $r.ElapsedSec, $art, $r.Ratio, $r.Divisions, $r.Alpha, $status)
}

Write-Host ""
Write-Host "Results location: G:\fsp-run"
Write-Host "Combined CSV files created per dataset"
Write-Host ""