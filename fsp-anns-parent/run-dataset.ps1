# ============================
# FSP-ANN (single dataset, multi-profile)
# - Per-run cleanup for metadata/points/results
# - GT is ALWAYS precomputed each run ("AUTO")
# - Works with new config (no "base") and old (with "base")
# - Adds: query-only mode, wildcard profile filter, per-run manifest/provenance
# - After runs, merges results.csv and GlobalPrecision.csv across profiles
#   into combined_results.csv and combined_precision.csv
# ============================

Set-StrictMode -Version Latest
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found in PATH. Install JDK or add 'java' to PATH."
}

# ---- required paths ----
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot    = "G:\fsp-run"

# ---- dataset: SIFT1M (edit these) ----
$Name  = "SIFT1M"
$Dim   = 128
$Base  = "E:\Research Work\Datasets\SIFT1M\sift_base.fvecs"
$Query = "E:\Research Work\Datasets\SIFT1M\sift_query.fvecs"
$GT    = "E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"   # ignored; "AUTO" is forced

# app batch size arg
$Batch = 100000

# profile selection:
# - blank/space => run all
# - single name => exact match
# - wildcard (e.g. "*precision*") => pattern filter
$OnlyProfile = "baseline"

# toggles
$CleanPerRun = $true
$CleanAllNow = $false
$QueryOnly   = $false  # when $true, uses POINTS_ONLY data path and restores latest index automatically
$RestoreVersion = ""   # e.g. "5" to force; leave empty to auto-detect latest when QueryOnly

# JVM/system flags (minimal, server-light posture; re-encryption at end)
$JvmArgs = @(
    "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end",        # accumulate touched IDs, re-encrypt once at end
    "-Dreenc.minTouched=5000", # gate the end-of-run job
    "-Dreenc.batchSize=2000",
    "-Dlog.progress.everyN=0",
    "-Dpaper.buildThreshold=2000000",
    "-Dpaper.alpha=0.01"
)

# ---------- helpers (PS5-safe) ----------
function To-Hashtable { param([Parameter(Mandatory=$true)]$o)
if ($null -eq $o) { return $null }
if ($o -is [hashtable]) { return $o }
if ($o -is [System.Collections.IDictionary]) { $ht=@{}; foreach($k in $o.Keys){ $ht[$k] = To-Hashtable $o[$k] }; return $ht }
if ($o -is [System.Collections.IEnumerable] -and -not ($o -is [string])) { $arr=@(); foreach($e in $o){ $arr += ,(To-Hashtable $e) }; return $arr }
if ($o -is [pscustomobject]) { $ht=@{}; foreach($p in $o.PSObject.Properties){ $ht[$p.Name] = To-Hashtable $p.Value }; return $ht }
return $o
}
function Copy-Hashtable { param([hashtable]$h)
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
function Apply-Overrides { param([hashtable]$base,[hashtable]$ovr)
$result = Copy-Hashtable $base
foreach($k in $ovr.Keys){
    $ov = $ovr[$k]
    if ($result.ContainsKey($k) -and ($result[$k] -is [hashtable]) -and ($ov -is [hashtable])) {
        foreach($sub in $ov.Keys){ $result[$k][$sub] = $ov[$sub] }
    } else { $result[$k] = $ov }
}
return $result
}
function Ensure-Files { param([string]$base,[string]$query)
$ok = $true
if ([string]::IsNullOrWhiteSpace($base) -or -not (Test-Path -LiteralPath $base)) { Write-Error "Missing base:  $base";  $ok = $false }
if ([string]::IsNullOrWhiteSpace($query) -or -not (Test-Path -LiteralPath $query)) { Write-Error "Missing query: $query"; $ok = $false }
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
    if (-not $item.PSIsContainer) { Remove-Item -LiteralPath $PathToDelete -Force -ErrorAction SilentlyContinue; return }
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
        if (Test-Path -LiteralPath $p) { Write-Host "Cleaning $p ..."; Invoke-FastDelete $p }
        New-Item -ItemType Directory -Force -Path $p | Out-Null
    }
}
function Clean-AllUnderOutRoot([string]$OutRootPath, [string]$DatasetName) {
    if (-not (Test-Path -LiteralPath $OutRootPath)) { return }
    $datasetRoot = Join-Path $OutRootPath $DatasetName
    if (-not (Test-Path -LiteralPath $datasetRoot)) { return }
    $targets = @("metadata","points","results")
    $nodes = Get-ChildItem -LiteralPath $datasetRoot -Recurse -Directory -ErrorAction SilentlyContinue | Where-Object { $targets -contains $_.Name }
    foreach ($n in $nodes) { Write-Host "Wiping $($n.FullName) ..."; Invoke-FastDelete $n.FullName }
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
function Get-LatestRestoreVersion {
    param([string]$RunDir)
    # Try to infer from metadata folder names like "epoch_5_*", fallback to a version file if you have one.
    $meta = Join-Path $RunDir "metadata"
    if (Test-Path -LiteralPath $meta) {
        $latest = Get-ChildItem -LiteralPath $meta -Directory -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -match '^epoch_(\d+)' } |
                ForEach-Object {
                    [pscustomobject]@{ Dir=$_; Epoch=[int]([regex]::Match($_.Name,'^epoch_(\d+)').Groups[1].Value) }
                } |
                Sort-Object Epoch -Descending |
                Select-Object -First 1
        if ($latest) { return [string]$latest.Epoch }
    }
    return ""  # unknown
}
function Get-LatestRestoreVersionForProfile {
    param([string]$RootDatasetDir, [string]$Profile)
    $profileDir = Join-Path $RootDatasetDir $Profile
    $meta = Join-Path $profileDir "metadata"
    if (-not (Test-Path -LiteralPath $meta)) { return "" }
    $latest = Get-ChildItem -LiteralPath $meta -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match '^epoch_(\d+)' } |
            ForEach-Object {
                [pscustomobject]@{ Epoch=[int]([regex]::Match($_.Name,'^epoch_(\d+)').Groups[1].Value) }
            } |
            Sort-Object Epoch -Descending | Select-Object -First 1
    if ($latest) { return [string]$latest.Epoch } else { return "" }
}
function Combine-ProfileCSVs {
    param(
        [Parameter(Mandatory = $true)][string]$RootDatasetDir,
        [Parameter(Mandatory = $true)][string]$LeafCsvName,
        [Parameter(Mandatory = $true)][string]$OutCsvPath
    )
    $pattern = Join-Path $RootDatasetDir ("*\results\" + $LeafCsvName)
    $files = @(Get-ChildItem -Path $pattern -File -ErrorAction SilentlyContinue)  # <-- wrap in @()

    if (-not $files -or $files.Count -eq 0)
    {
        Write-Warning "No $LeafCsvName found under $RootDatasetDir\*\results"
        return
    }
}

# ---------- STEP 0: optional global clean and exit ----------
if ($CleanAllNow) {
    New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null
    Clean-AllUnderOutRoot -OutRootPath $OutRoot -DatasetName $Name
    Write-Host "Global clean completed under $OutRoot\$Name."
    return
}

# ---------- sanity ----------
if (-not (Test-Path -LiteralPath $JarPath))    { throw "Jar not found: $JarPath" }
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config not found: $ConfigPath" }
if (-not $QueryOnly) {
    if (-not (Ensure-Files $Base $Query)) { throw "Abort: dataset files missing." }
}

# ---------- read config ----------
$cfgObj = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$profiles = @($cfgObj.profiles)
if ($profiles.Count -eq 0) { throw "config.json must contain a non-empty 'profiles' array." }
if ($null -eq $profiles) { throw "config.json must contain a 'profiles' array." }

# Build base payload (back-compat w/ optional 'base' node)
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

# ---------- run all (or filtered) profiles ----------
foreach ($p in $profiles)
{
    $pHT = To-Hashtable $p
    if (-not $pHT.ContainsKey('name'))
    {
        continue
    }
    $label = [string]$pHT['name']
    $ovr = @{ }; if ( $pHT.ContainsKey('overrides'))
    {
        $ovr = $pHT['overrides']
    }

    $runDir = Join-Path (Join-Path $OutRoot $Name) $label
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    if ($CleanPerRun)
    {
        Clean-RunMetadata -RunDir $runDir
    }

    # merge + ensure outputs/eval/reencryption match new system defaults
    $final = Apply-Overrides -base $baseHT -ovr $ovr

    if (-not $final.ContainsKey('output'))
    {
        $final['output'] = @{ }
    }
    $final['output']['resultsDir'] = (Join-Path $runDir "results")
    if (-not $final.ContainsKey('eval'))
    {
        $final['eval'] = @{ }
    }
    $final['eval']['computePrecision'] = $true
    $final['eval']['writeGlobalPrecisionCsv'] = $true

    if (-not $final.ContainsKey('cloak'))
    {
        $final['cloak'] = @{ }
    }
    $final['cloak']['noise'] = 0.0

    if ($cfgObj.PSObject.Properties.Name -contains 'audit')
    {
        $final['audit'] = To-Hashtable $cfgObj.audit
    }

    # --- Respect profile choice; default to multiprobe (plaintext inserts allowed) ---
    if (-not $final.ContainsKey('paper')) { $final['paper'] = @{ } }
    if (-not $final['paper'].ContainsKey('enabled')) { $final['paper']['enabled'] = $false }

    # Only when paper mode is explicitly enabled, neutralize legacy LSH knobs
    if ($final['paper']['enabled']) {
    if (-not $final['paper'].ContainsKey('seed')) { $final['paper']['seed'] = 13 }
    if (-not $final.ContainsKey('lsh')) { $final['lsh'] = @{ } }
    $final['lsh']['numTables']    = 0
    $final['lsh']['rowsPerBand']  = 0
    $final['lsh']['probeShards']  = 0
    }

    # NOW write the hardened config
    $tmpConf = Join-Path $runDir "config.json"
    ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

    $keysFile = Join-Path $runDir "keystore.blob"
    $gtArg = "AUTO"  # FORCE PRECOMPUTE EVERY RUN

    # Build app args: <config> <dataPath> <queryPath> <keysFilePath> <dimensions> <metadataPath> <groundtruth> [batch]
    $dataArg = $Base
    $queryArg = $Query
    $restoreFlag = @()
    if ($QueryOnly)
    {
        $dataArg = "POINTS_ONLY"
        $restoreFlag += "-Dquery.only=true"
        $autoVer = ""
        if (-not $RestoreVersion -or $RestoreVersion.Trim().Length -eq 0)
        {
            $autoVer = Get-LatestRestoreVersionForProfile -RootDatasetDir (Join-Path $OutRoot $Name) -Profile $label
        }
        $verToUse = if ($RestoreVersion)
        {
            $RestoreVersion
        }
        else
        {
            $autoVer
        }
        if ($verToUse)
        {
            $restoreFlag += "-Drestore.version=$verToUse"
        }
    }


    $argList = @()
    $argList += $JvmArgs
    $argList += $restoreFlag
    $argList += "-Dbase.path=$(Safe-Resolve $Base -AllowMissing:$true)"   # used for ratio-from-base + GT precompute
    $argList += "-jar";           $argList += (Safe-Resolve $JarPath)
    $argList += (Safe-Resolve $tmpConf)
    $argList += (Safe-Resolve $dataArg -AllowMissing:$true)
    $argList += (Safe-Resolve $queryArg -AllowMissing:$true)
    $argList += (Safe-Resolve $keysFile -AllowMissing:$true)
    $argList += "$Dim"
    $argList += (Safe-Resolve $runDir)
    $argList += $gtArg
    $argList += "$Batch"

    # Quote anything with spaces for logging (we pass array to java)
    $quotedArgs = foreach ($a in $argList) { $s = [string]$a; if ($s -match '\s') { '"' + ($s -replace '"','\"') + '"' } else { $s } }
    $argLine = [string]::Join(" ", $quotedArgs)
    $argLine | Out-File -FilePath (Join-Path $runDir "cmdline.txt") -Encoding utf8

    # Manifest (provenance)
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
        gtArg            = $gtArg
        jvmArgs          = $JvmArgs
        sysProps         = @("reenc.mode=end","base.path")
        timestampUtc     = ([DateTime]::UtcNow.ToString("o"))
    }
    Save-Manifest -RunDir $runDir -Manifest $manifest

    # --- Launch (single run: live console + save to run.out.log; filter progress spam from console only) ---
    $combinedLog   = Join-Path $runDir "run.out.log"
    $progressRegex = '^\[\d+/\d+\]\s+ queries processed (GT)'

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & java @argList 2>&1 |
            Tee-Object -FilePath $combinedLog |
            Where-Object { $_ -notmatch $progressRegex }
    $exit = $LASTEXITCODE
    $sw.Stop()

    ("ElapsedSec={0:N1}" -f $sw.Elapsed.TotalSeconds) | Out-File -FilePath (Join-Path $runDir "elapsed.txt") -Encoding utf8

    if ($exit -ne 0) {
        Write-Warning ("Run failed for profile {0} (exit={1})." -f $label, $exit)
    } else {
        Write-Host ("Completed: {0} ({1})" -f $Name, $label)
    }
}

# ---------- POST: merge CSVs across profiles ----------
$rootDatasetDir     = Join-Path $OutRoot $Name
$combinedResults    = Join-Path $rootDatasetDir "combined_results.csv"
$combinedPrecision  = Join-Path $rootDatasetDir "combined_precision.csv"
$combinedEvaluation = Join-Path $rootDatasetDir "combined_evaluation.csv"

Combine-ProfileCSVs -RootDatasetDir $rootDatasetDir -LeafCsvName "results_table.csv"      -OutCsvPath $combinedResults
Combine-ProfileCSVs -RootDatasetDir $rootDatasetDir -LeafCsvName "global_precision.csv"    -OutCsvPath $combinedPrecision
Combine-ProfileCSVs -RootDatasetDir $rootDatasetDir -LeafCsvName "topk_evaluation.csv"    -OutCsvPath $combinedEvaluation

Write-Host "All done."