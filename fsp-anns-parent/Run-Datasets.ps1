# ============================
# FSP-ANN (multi-dataset, multi-profile batch) — PowerShell
# - Option-C: LSH Disabled, Alpha-Based Stabilization
# - Selective Reencryption (end-of-run)
# - Consolidates results with enhanced reporting
# ============================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found in PATH. Install JDK or add 'java' to PATH."
}

# ---- required paths ----
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot    = "G:\fsp-run-option-c"

# ---- alpha and JVM system props ----
$JvmArgs = @(
    "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200", "-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end",
    "-Dreenc.minTouched=5000",
    "-Dreenc.batchSize=2000",
    "-Dlog.progress.everyN=0",
    "-Dpaper.buildThreshold=2000000",
    "-Djava.security.egd=file:/dev/./urandom"
)

# ---- app batch size arg ----
$Batch = 100000

# ---- profile filter ----
$OnlyProfile = ""

# ---- toggles ----
$CleanPerRun = $true
$QueryOnly   = $false
$RestoreVersion = ""

# ---------- helpers ----------
function To-Hashtable {
    param([Parameter(Mandatory=$true)]$o)
    if ($null -eq $o) { return $null }
    if ($o -is [hashtable]) { return $o }
    if ($o -is [System.Collections.IDictionary]) {
        $ht=@{}; foreach($k in $o.Keys){ $ht[$k] = To-Hashtable $o[$k] }; return $ht
    }
    if ($o -is [System.Collections.IEnumerable] -and -not ($o -is [string])) {
        $arr=@(); foreach($e in $o){ $arr += ,(To-Hashtable $e) }; return $arr
    }
    if ($o -is [pscustomobject]) {
        $ht=@{}; foreach($p in $o.PSObject.Properties){ $ht[$p.Name] = To-Hashtable $p.Value }; return $ht
    }
    return $o
}

function Copy-Hashtable {
    param([hashtable]$h)
    $out = @{}
    foreach ($k in $h.Keys) {
        $v = $h[$k]
        if ($v -is [hashtable]) {
            $out[$k] = Copy-Hashtable $v
        } elseif ($v -is [System.Collections.IEnumerable] -and -not ($v -is [string])) {
            $arr = @()
            foreach ($e in $v) { if ($e -is [hashtable]) { $arr += ,(Copy-Hashtable $e) } else { $arr += ,$e } }
            $out[$k] = $arr
        } else { $out[$k] = $v }
    }
    return $out
}

function Apply-Overrides {
    param([hashtable]$base, [hashtable]$ovr)
    $result = Copy-Hashtable $base
    foreach ($k in $ovr.Keys){
        $ov = $ovr[$k]
        if ($result.ContainsKey($k) -and ($result[$k] -is [hashtable]) -and ($ov -is [hashtable])) {
            foreach($sub in $ov.Keys){
                $result[$k][$sub] = $ov[$sub]
            }
        } else {
            $result[$k] = $ov
        }
    }
    return $result
}

function Ensure-Files {
    param([string]$base,[string]$query,[string]$gt)
    $ok = $true
    if ([string]::IsNullOrWhiteSpace($base) -or -not (Test-Path -LiteralPath $base)) {
        Write-Error "Missing base:  $base";  $ok = $false
    }
    if ([string]::IsNullOrWhiteSpace($query) -or -not (Test-Path -LiteralPath $query)) {
        Write-Error "Missing query: $query"; $ok = $false
    }
    if ([string]::IsNullOrWhiteSpace($gt) -or -not (Test-Path -LiteralPath $gt)) {
        Write-Error "Missing GT:    $gt";  $ok = $false
    }
    return $ok
}

function Safe-Resolve([string]$Path, [bool]$AllowMissing = $false) {
    try {
        if ($AllowMissing -and -not (Test-Path -LiteralPath $Path)) { return $Path }
        return (Resolve-Path -LiteralPath $Path).Path
    } catch { if ($AllowMissing) { return $Path } else { throw } }
}

function Invoke-FastDelete([string]$PathToDelete) {
    if (-not (Test-Path -LiteralPath $PathToDelete)) { return }
    $item = Get-Item -LiteralPath $PathToDelete -ErrorAction SilentlyContinue
    if ($null -eq $item) { return }
    if (-not $item.PSIsContainer) {
        Remove-Item -LiteralPath $PathToDelete -Force -ErrorAction SilentlyContinue; return
    }
    $empty = New-Item -ItemType Directory -Path (Join-Path $env:TEMP ("empty_" + [guid]::NewGuid())) -Force
    try { robocopy $empty.FullName $PathToDelete /MIR /NFL /NDL /NJH /NJS /NC /NS /NP | Out-Null }
    finally {
        try { Remove-Item -LiteralPath $PathToDelete -Recurse -Force -ErrorAction SilentlyContinue } catch {}
        try { Remove-Item -LiteralPath $empty.FullName -Recurse -Force -ErrorAction SilentlyContinue } catch {}
    }
}

function Clean-RunMetadata([string]$RunDir) {
    $paths = @((Join-Path $RunDir "metadata"),(Join-Path $RunDir "points"),(Join-Path $RunDir "results"))
    foreach ($p in $paths) {
        if (Test-Path -LiteralPath $p) { Write-Host "Cleaning $p ..." -ForegroundColor Gray; Invoke-FastDelete $p }
        New-Item -ItemType Directory -Force -Path $p | Out-Null
    }
}

function Get-Sha256([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return "" }
    try { return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash } catch { return "" }
}

function Save-Manifest([string]$RunDir, [hashtable]$Manifest) {
    try {
        $out = Join-Path $RunDir "manifest.json"
        ($Manifest | ConvertTo-Json -Depth 64) | Out-File -FilePath $out -Encoding utf8
    } catch {}
}

function Combine-CSV {
    param([Parameter(Mandatory=$true)]$Files, [Parameter(Mandatory=$true)][string] $OutCsv)
    $Files = @([string[]]$Files)
    if (-not $Files -or $Files.Count -eq 0) { return }
    $outDir = Split-Path -Parent $OutCsv
    if ($outDir) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }
    Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue
    $headerWritten = $false
    foreach ($f in $Files) {
        if (-not (Test-Path -LiteralPath $f)) { continue }
        $lines = Get-Content -LiteralPath $f
        if (-not $lines -or $lines.Count -eq 0) { continue }
        $resultsDir = Split-Path -LiteralPath $f -Parent
        $profileDir = Split-Path -LiteralPath $resultsDir -Parent
        $profile    = Split-Path -LiteralPath $profileDir -Leaf
        $header = $lines[0]
        $rows = @()
        if ($lines.Count -gt 1) { $rows = $lines | Select-Object -Skip 1 }
        if (-not $headerWritten) {
            "profile,$header" | Set-Content -LiteralPath $OutCsv -Encoding UTF8
            foreach ($row in $rows) {
                if ([string]::IsNullOrWhiteSpace($row)) { continue }
                "$profile,$row" | Add-Content -LiteralPath $OutCsv -Encoding UTF8
            }
            $headerWritten = $true
        } else {
            foreach ($row in $rows) {
                if ([string]::IsNullOrWhiteSpace($row)) { continue }
                "$profile,$row" | Add-Content -LiteralPath $OutCsv -Encoding UTF8
            }
        }
    }
}

function Combine-TxtWithProfile {
    param([Parameter(Mandatory = $true)][string[]] $Files, [Parameter(Mandatory = $true)][string]  $OutTxt)
    $Files = @([string[]]$Files)
    if ($Files.Count -eq 0) { return }
    $outDir = Split-Path -Parent $OutTxt
    if ($outDir) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }
    Remove-Item -LiteralPath $OutTxt -ErrorAction SilentlyContinue
    foreach ($f in $Files) {
        if (-not (Test-Path -LiteralPath $f)) { continue }
        $resultsDir = Split-Path -LiteralPath $f -Parent
        $profileDir = Split-Path -LiteralPath $resultsDir -Parent
        $profile    = Split-Path -LiteralPath $profileDir -Leaf
        "===== PROFILE: $profile =====" | Add-Content -LiteralPath $OutTxt -Encoding UTF8
        Get-Content -LiteralPath $f | Add-Content -LiteralPath $OutTxt -Encoding UTF8
        "" | Add-Content -LiteralPath $OutTxt -Encoding UTF8
    }
}

function Detect-FvecsDim([string]$Path) {
    $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
    try {
        $buf = New-Object byte[] 4
        $n = $fs.Read($buf, 0, 4)
        if ($n -ne 4) { throw "Could not read dimension header from $Path" }
        return [System.BitConverter]::ToInt32($buf, 0)
    } finally { $fs.Dispose() }
}

# ---------- dataset matrix ----------
$Datasets = @(
    @{ Name="SIFT1M";       Base="E:\Datasets\SIFT1M\sift_base.fvecs";          Query="E:\Datasets\SIFT1M\sift_query.fvecs";          GT="E:\Datasets\SIFT1M\sift_query_groundtruth.ivecs"; Dim=128 },
    @{ Name="glove-100";    Base="E:\Datasets\glove-100\glove-100_base.fvecs";  Query="E:\Datasets\glove-100\glove-100_query.fvecs";  GT="E:\Datasets\glove-100\glove-100_groundtruth.ivecs"; Dim=100 },
    @{ Name="RedCaps";      Base="E:\Datasets\RedCaps\redcaps_base.fvecs";      Query="E:\Datasets\RedCaps\redcaps_query.fvecs";      GT="E:\Datasets\RedCaps\redcaps_groundtruth.ivecs";     Dim=512 },
    @{ Name="Deep1B";       Base="E:\Datasets\Deep1B\deep1b_base.fvecs";        Query="E:\Datasets\Deep1B\deep1b_query.fvecs";        GT="E:\Datasets\Deep1B\deep1b_groundtruth.ivecs";       Dim=96 },
    @{ Name="GIST1B";       Base="E:\Datasets\GIST1B\gist1b_base.fvecs";        Query="E:\Datasets\GIST1B\gist1b_query.fvecs";        GT="E:\Datasets\GIST1B\gist1b_groundtruth.ivecs";       Dim=960 }
)

# ---------- sanity ----------
if (-not (Test-Path -LiteralPath $JarPath))    { throw "Jar not found: $JarPath" }
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config not found: $ConfigPath" }

# ---------- read config ----------
$cfgObj = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$profiles = @($cfgObj.profiles)
if ($profiles.Count -eq 0 -or $null -eq $profiles) { throw "config.json must contain a non-empty 'profiles' array." }

# Build base payload
$baseHT = $null
if ($cfgObj.PSObject.Properties.Name -contains 'base') { $baseHT = To-Hashtable $cfgObj.base }
else { $tmp = To-Hashtable $cfgObj; if ($tmp.ContainsKey('profiles')) { $tmp.Remove('profiles') }; $baseHT = $tmp }
if ($null -eq $baseHT) { throw "Failed to construct base config object." }

New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

# ---------- profile filter ----------
if ($OnlyProfile -and $OnlyProfile.Trim().Length -gt 0) {
    $needle = $OnlyProfile.Trim()
    if ($needle.Contains("*") -or $needle.Contains("?")) {
        $profiles = @($profiles | Where-Object { $_.name -like $needle })
    } else {
        $profiles = @($profiles | Where-Object { $_.name -eq $needle })
    }
    if ($profiles.Count -eq 0) { throw "Profile filter '$needle' matched nothing in config.json" }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "FSP-ANN Option-C: LSH-Disabled System" -ForegroundColor Cyan
Write-Host "Alpha-Based Stabilization + End-Mode Reenc" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# ---------- MAIN LOOP: datasets x profiles ----------
$allResults = @()

foreach ($ds in $Datasets) {
    $Name  = $ds.Name
    $Base  = $ds.Base
    $Query = $ds.Query
    $GT    = $ds.GT
    $Dim   = $ds.Dim

    if ($null -eq $Dim) {
        if (-not (Test-Path -LiteralPath $Base)) { Write-Warning "Skipping $Name (missing base)"; continue }
        try { $Dim = Detect-FvecsDim $Base } catch { throw "Could not detect dimension for $Name from $Base. Set Dim explicitly." }
    }

    if (-not (Ensure-Files $Base $Query $GT)) {
        Write-Warning "Skipping $Name due to missing files."
        continue
    }

    $datasetRoot = Join-Path $OutRoot $Name
    New-Item -ItemType Directory -Force -Path $datasetRoot | Out-Null

    Write-Host "Dataset: $Name (Dim=$Dim)" -ForegroundColor Yellow

    foreach ($p in $profiles) {
        $pHT = To-Hashtable $p
        if (-not $pHT.ContainsKey('name')) { continue }
        $label = [string]$pHT['name']

        if ($pHT.ContainsKey('overrides')) {
            $ovr = To-Hashtable $pHT['overrides']
        } else {
            $ovr = Copy-Hashtable $pHT
            $ovr.Remove('name')
        }

        $runDir = Join-Path $datasetRoot $label
        New-Item -ItemType Directory -Force -Path $runDir | Out-Null
        if ($CleanPerRun) { Clean-RunMetadata -RunDir $runDir }

        # Merge configs
        $final = Apply-Overrides -base $baseHT -ovr $ovr

        # Ensure output settings
        if (-not $final.ContainsKey('output')) { $final['output'] = @{} }
        $final['output']['resultsDir'] = (Join-Path $runDir "results")
        $final['output']['exportArtifacts'] = $true
        $final['output']['suppressLegacyMetrics'] = $true

        # Eval
        if (-not $final.ContainsKey('eval')) { $final['eval'] = @{} }
        $final['eval']['computePrecision'] = $true
        $final['eval']['writeGlobalPrecisionCsv'] = $true

        # Cloak
        if (-not $final.ContainsKey('cloak')) { $final['cloak'] = @{} }
        $final['cloak']['noise'] = 0.0

        # Ratio: use GT with explicit path
        if (-not $final.ContainsKey('ratio')) { $final['ratio'] = @{} }
        $final['ratio']['source'] = "gt"
        $final['ratio']['gtPath'] = $GT
        $final['ratio']['gtSample'] = 10000
        $final['ratio']['gtMismatchTolerance'] = 0.0
        $final['ratio']['autoComputeGT'] = $false
        $final['ratio']['allowComputeIfMissing'] = $false

        # Paper + Stabilization config
        if (-not $final.ContainsKey('paper')) { $final['paper'] = @{} }
        if (-not $final['paper'].ContainsKey('enabled')) { $final['paper']['enabled'] = $true }

        if (-not $final.ContainsKey('stabilization')) { $final['stabilization'] = @{} }
        if (-not $final['stabilization'].ContainsKey('enabled')) {
            $final['stabilization']['enabled'] = $true
        }
        if (-not $final['stabilization'].ContainsKey('alpha')) {
            $final['stabilization']['alpha'] = 0.10
        }
        if (-not $final['stabilization'].ContainsKey('minCandidates')) {
            $final['stabilization']['minCandidates'] = 1200
        }

        # Reencryption
        if (-not $final.ContainsKey('reencryption')) { $final['reencryption'] = @{} }
        if (-not $final['reencryption'].ContainsKey('enabled')) {
            $final['reencryption']['enabled'] = $true
        }

        # Write run-specific config
        $tmpConf = Join-Path $runDir "config.json"
        ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

        # Build args
        $keysFile = Join-Path $runDir "keystore.blob"
        $gtArg = (Safe-Resolve $GT)

        $dataArg = $Base
        $queryArg = $Query
        $restoreFlag = @()
        if ($QueryOnly) {
            $dataArg = "POINTS_ONLY"
            $restoreFlag += "-Dquery.only=true"
            if ($RestoreVersion) { $restoreFlag += "-Drestore.version=$RestoreVersion" }
        }

        $argList = @()
        $argList += $JvmArgs
        $argList += $restoreFlag
        $argList += "-Dbase.path=$(Safe-Resolve $Base -AllowMissing:$true)"
        $argList += "-Dcli.dataset=$Name"
        $argList += "-Dcli.profile=$label"
        $argList += "-jar";           $argList += (Safe-Resolve $JarPath)
        $argList += (Safe-Resolve $tmpConf)
        $argList += (Safe-Resolve $dataArg -AllowMissing:$true)
        $argList += (Safe-Resolve $queryArg -AllowMissing:$true)
        $argList += (Safe-Resolve $keysFile -AllowMissing:$true)
        $argList += "$Dim"
        $argList += (Safe-Resolve $runDir)
        $argList += $gtArg
        $argList += "$Batch"

        # Save command
        $quotedArgs = foreach ($a in $argList) { $s = [string]$a; if ($s -match '\s') { '"' + ($s -replace '"','\"') + '"' } else { $s } }
        $argLine = [string]::Join(" ", $quotedArgs)
        $argLine | Out-File -FilePath (Join-Path $runDir "cmdline.txt") -Encoding utf8

        # Manifest
        $manifest = @{
            dataset          = $Name
            dimension        = $Dim
            profile          = $label
            queryOnly        = $QueryOnly
            batchSize        = $Batch
            jarPath          = (Safe-Resolve $JarPath)
            jarSha256        = (Get-Sha256 (Safe-Resolve $JarPath))
            configPath       = (Safe-Resolve $tmpConf)
            configSha256     = (Get-Sha256 (Safe-Resolve $tmpConf))
            baseVectors      = (Safe-Resolve $Base -AllowMissing:$true)
            queryVectors     = (Safe-Resolve $Query -AllowMissing:$true)
            gtPath           = (Safe-Resolve $GT -AllowMissing:$true)
            jvmArgs          = $JvmArgs
            timestampUtc     = ([DateTime]::UtcNow.ToString("o"))
        }
        Save-Manifest -RunDir $runDir -Manifest $manifest

        # Run
        $combinedLog   = Join-Path $runDir "run.out.log"
        $progressRegex = '^\[\d+/\d+\]\s+ queries processed'
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        & java @argList 2>&1 | Tee-Object -FilePath $combinedLog | Where-Object { $_ -notmatch $progressRegex }
        $exit = $LASTEXITCODE
        $sw.Stop()

        ("ElapsedSec={0:N1}" -f $sw.Elapsed.TotalSeconds) | Out-File -FilePath (Join-Path $runDir "elapsed.txt") -Encoding utf8

        $allResults += @{
            Dataset     = $Name
            Profile     = $label
            ExitCode    = $exit
            ElapsedSec  = $sw.Elapsed.TotalSeconds
        }

        if ($exit -ne 0) {
            Write-Host "  ✗ $label - EXIT $exit" -ForegroundColor Red
        } else {
            Write-Host "  ✓ $label - Elapsed: $($sw.Elapsed.TotalSeconds)s" -ForegroundColor Green
        }
    }

    # ---- per-dataset merges ----
    $resultsGlob   = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\results_table.csv")          -File -ErrorAction SilentlyContinue
    $precisionGlob = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\global_precision.csv")       -File -ErrorAction SilentlyContinue
    $topkGlob      = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\topk_evaluation.csv")        -File -ErrorAction SilentlyContinue
    $reencGlob     = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\reencrypt_metrics.csv")      -File -ErrorAction SilentlyContinue
    $samplesGlob   = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\retrieved_samples.csv")      -File -ErrorAction SilentlyContinue
    $worstGlob     = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\retrieved_worst.csv")        -File -ErrorAction SilentlyContinue
    $storSumGlob   = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\storage_summary.csv")        -File -ErrorAction SilentlyContinue
    $metricsTxtGlob= Get-ChildItem -Path (Join-Path $datasetRoot "*\results\metrics_summary.txt")        -File -ErrorAction SilentlyContinue
    $storBrkGlob   = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\storage_breakdown.txt")      -File -ErrorAction SilentlyContinue
    $readmeGlob    = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\README_results_columns.txt") -File -ErrorAction SilentlyContinue

    $combinedResults       = Join-Path $datasetRoot "combined_results.csv"
    $combinedPrecision     = Join-Path $datasetRoot "combined_precision.csv"
    $combinedTopk          = Join-Path $datasetRoot "combined_evaluation.csv"
    $combinedReenc         = Join-Path $datasetRoot "combined_reencrypt_metrics.csv"
    $combinedSamples       = Join-Path $datasetRoot "combined_retrieved_samples.csv"
    $combinedWorst         = Join-Path $datasetRoot "combined_retrieved_worst.csv"
    $combinedStorSummary   = Join-Path $datasetRoot "combined_storage_summary.csv"
    $combinedMetricsTxt    = Join-Path $datasetRoot "combined_metrics_summary.txt"
    $combinedStorBreakdown = Join-Path $datasetRoot "combined_storage_breakdown.txt"

    if ($resultsGlob) { Combine-CSV -Files ($resultsGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedResults }
    if ($precisionGlob) { Combine-CSV -Files ($precisionGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedPrecision }
    if ($topkGlob) { Combine-CSV -Files ($topkGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedTopk }
    if ($reencGlob) { Combine-CSV -Files ($reencGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedReenc }
    if ($samplesGlob) { Combine-CSV -Files ($samplesGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedSamples }
    if ($worstGlob) { Combine-CSV -Files ($worstGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedWorst }
    if ($storSumGlob) { Combine-CSV -Files ($storSumGlob | Select-Object -ExpandProperty FullName) -OutCsv $combinedStorSummary }
    if ($metricsTxtGlob) { Combine-TxtWithProfile -Files ($metricsTxtGlob | Select-Object -ExpandProperty FullName) -OutTxt $combinedMetricsTxt }
    if ($storBrkGlob) { Combine-TxtWithProfile -Files ($storBrkGlob | Select-Object -ExpandProperty FullName) -OutTxt $combinedStorBreakdown }

    if ($readmeGlob) {
        $readmeDir = Join-Path $datasetRoot "results_readme"
        New-Item -ItemType Directory -Force -Path $readmeDir | Out-Null
        $firstReadme = ($readmeGlob | Select-Object -First 1).FullName
        Copy-Item -LiteralPath $firstReadme -Destination (Join-Path $readmeDir "README_results_columns.txt") -Force -ErrorAction SilentlyContinue
    }

    Write-Host ""
}

# ---------- FINAL REPORT ----------
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "SUMMARY: Run Results by Dataset" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$successfulRuns = @($allResults | Where-Object { $_.ExitCode -eq 0 })
if ($successfulRuns.Count -gt 0) {
    $grouped = $successfulRuns | Group-Object { $_.Dataset }
    foreach ($g in $grouped) {
        Write-Host "`n[$($g.Name)]" -ForegroundColor Yellow
        Write-Host "  Profile                    | Elapsed(s) | Status" -ForegroundColor Gray
        Write-Host "  " + ("-" * 65) -ForegroundColor Gray
        foreach ($r in $g.Group) {
            Write-Host ("  {0,-26} | {1,10:N2} | ✓ OK" -f $r.Profile, $r.ElapsedSec) -ForegroundColor White
        }
    }
}

# Export results summary
$summaryPath = Join-Path $OutRoot "RUN_SUMMARY.csv"
$allResults | ConvertTo-Csv -NoTypeInformation | Set-Content -LiteralPath $summaryPath -Encoding UTF8
Write-Host "`n✓ Run summary: $summaryPath`n" -ForegroundColor Green

Write-Host "All datasets complete. Results in: $OutRoot`n" -ForegroundColor Cyan