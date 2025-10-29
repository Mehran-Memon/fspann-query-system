# ============================
# FSP-ANN (multi-dataset, multi-profile batch)
# - Disables GT recompute; uses explicit GT path
# - Integrates EvaluationSummaryPrinter (expects Java side in place)
# - Consolidates CSVs per dataset and across datasets
# ============================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Java not found in PATH. Install JDK or add 'java' to PATH."
}

# ---- required paths ----
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot    = "G:\fsp-run"

# ---- alpha and JVM system props ----
$Alpha = 0.1
$JvmArgs = @(
    "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true",
    "-Dfile.encoding=UTF-8",
    "-Dreenc.mode=end",
    "-Dreenc.minTouched=5000",
    "-Dreenc.batchSize=2000",
    "-Dlog.progress.everyN=0",
    "-Dpaper.buildThreshold=2000000",
    ("-Dpaper.alpha={0}" -f $Alpha)
)

# ---- app batch size arg ----
$Batch = 100000

# ---- profile filter ----
# - "" => all
# - exact name => that profile only
# - wildcard => pattern (e.g., "*ell3*")
$OnlyProfile = ""

# ---- toggles ----
$CleanPerRun = $true
$QueryOnly   = $false
$RestoreVersion = ""

# ---------- helpers ----------
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
function Ensure-Files { param([string]$base,[string]$query,[string]$gt)
$ok = $true
if ([string]::IsNullOrWhiteSpace($base) -or -not (Test-Path -LiteralPath $base)) { Write-Error "Missing base:  $base";  $ok = $false }
if ([string]::IsNullOrWhiteSpace($query) -or -not (Test-Path -LiteralPath $query)) { Write-Error "Missing query: $query"; $ok = $false }
if ([string]::IsNullOrWhiteSpace($gt) -or -not (Test-Path -LiteralPath $gt)) { Write-Error "Missing GT:    $gt";  $ok = $false }
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
    param(
        [Parameter(Mandatory=$true)][string[]]$Files,
        [Parameter(Mandatory=$true)][string]$OutCsv
    )
    if ($Files.Count -eq 0) { return }
    $headerWritten = $false
    $outDir = Split-Path -Parent $OutCsv
    if ($outDir) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }
    Remove-Item -LiteralPath $OutCsv -ErrorAction SilentlyContinue
    foreach ($f in $Files) {
        if (-not (Test-Path -LiteralPath $f)) { continue }
        $lines = Get-Content -LiteralPath $f
        if (-not $lines) { continue }
        if (-not $headerWritten) {
            $lines | Set-Content -LiteralPath $OutCsv -Encoding UTF8
            $headerWritten = $true
        } else {
            $lines | Select-Object -Skip 1 | Add-Content -LiteralPath $OutCsv -Encoding UTF8
        }
    }
}

function Detect-FvecsDim([string]$Path) {
    # Reads first 4 bytes little-endian as Int32
    $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
    try {
        $buf = New-Object byte[] 4
        $n = $fs.Read($buf, 0, 4)
        if ($n -ne 4) { throw "Could not read dimension header from $Path" }
        return [System.BitConverter]::ToInt32($buf, 0)
    } finally { $fs.Dispose() }
}

# ---------- dataset matrix ----------
# If Dim is $null, it will be auto-detected from base file header.
$Datasets = @(
#    @{ Name="Enron";        Base="E:\Research Work\Datasets\Enron\enron_base.fvecs";          Query="E:\Research Work\Datasets\Enron\enron_query.fvecs";          GT="E:\Research Work\Datasets\Enron\enron_groundtruth.ivecs";       Dim=1369 },
#    @{ Name="audio";        Base="E:\Research Work\Datasets\audio\audio_base.fvecs";          Query="E:\Research Work\Datasets\audio\audio_query.fvecs";          GT="E:\Research Work\Datasets\audio\audio_groundtruth.ivecs";       Dim=192 },
#    @{ Name="glove-100";    Base="E:\Research Work\Datasets\glove-100\glove-100_base.fvecs";  Query="E:\Research Work\Datasets\glove-100\glove-100_query.fvecs";  GT="E:\Research Work\Datasets\glove-100\glove-100_groundtruth.ivecs"; Dim=100 },
    @{ Name="SIFT1M";       Base="E:\Research Work\Datasets\SIFT1M\sift_base.fvecs";          Query="E:\Research Work\Datasets\SIFT1M\sift_query.fvecs";          GT="E:\Research Work\Datasets\SIFT1M\sift_query_groundtruth.ivecs"; Dim=128 },
    @{ Name="synthetic_128";   Base="E:\Research Work\Datasets\synthetic_128\base.fvecs";     Query="E:\Research Work\Datasets\synthetic_128\query.fvecs";        GT="E:\Research Work\Datasets\synthetic_128\groundtruth.ivecs";     Dim=128 },
    @{ Name="synthetic_256";   Base="E:\Research Work\Datasets\synthetic_256\base.fvecs";     Query="E:\Research Work\Datasets\synthetic_256\query.fvecs";        GT="E:\Research Work\Datasets\synthetic_256\groundtruth.ivecs";     Dim=256 },
    @{ Name="synthetic_512";   Base="E:\Research Work\Datasets\synthetic_512\base.fvecs";     Query="E:\Research Work\Datasets\synthetic_512\query.fvecs";        GT="E:\Research Work\Datasets\synthetic_512\groundtruth.ivecs";     Dim=512 },
    @{ Name="synthetic_1024";  Base="E:\Research Work\Datasets\synthetic_1024\base.fvecs";    Query="E:\Research Work\Datasets\synthetic_1024\query.fvecs";       GT="E:\Research Work\Datasets\synthetic_1024\groundtruth.ivecs";    Dim=1024 }
)

# ---------- sanity ----------
if (-not (Test-Path -LiteralPath $JarPath))    { throw "Jar not found: $JarPath" }
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config not found: $ConfigPath" }

# ---------- read config ----------
$cfgObj = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$profiles = @($cfgObj.profiles)
if ($profiles.Count -eq 0 -or $null -eq $profiles) { throw "config.json must contain a non-empty 'profiles' array." }

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

# ---------- MAIN LOOP: datasets x profiles ----------
$allSummaryFiles = @()

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

    foreach ($p in $profiles) {
        $pHT = To-Hashtable $p
        if (-not $pHT.ContainsKey('name')) { continue }
        $label = [string]$pHT['name']
        $ovr = if ($pHT.ContainsKey('overrides')) { $pHT['overrides'] } else { @{} }

        $runDir = Join-Path $datasetRoot $label
        New-Item -ItemType Directory -Force -Path $runDir | Out-Null
        if ($CleanPerRun) { Clean-RunMetadata -RunDir $runDir }

        # merge + ensure outputs/eval/ratio
        $final = Apply-Overrides -base $baseHT -ovr $ovr

        if (-not $final.ContainsKey('output')) { $final['output'] = @{} }
        $final['output']['resultsDir'] = (Join-Path $runDir "results")
        $final['output']['exportArtifacts'] = $true
        $final['output']['suppressLegacyMetrics'] = $true

        if (-not $final.ContainsKey('eval')) { $final['eval'] = @{} }
        $final['eval']['computePrecision'] = $true
        $final['eval']['writeGlobalPrecisionCsv'] = $true

        if (-not $final.ContainsKey('cloak')) { $final['cloak'] = @{} }
        $final['cloak']['noise'] = 0.0

        # ratio: enforce GT path & disable recompute
        if (-not $final.ContainsKey('ratio')) { $final['ratio'] = @{} }
        $final['ratio']['source'] = "gt"
        $final['ratio']['gtPath'] = $GT
        $final['ratio']['gtSample'] = 10000
        $final['ratio']['gtMismatchTolerance'] = 0.0
        $final['ratio']['autoComputeGT'] = $false
        $final['ratio']['allowComputeIfMissing'] = $false

        # paper vs lsh knobs (keep your logic)
        if (-not $final.ContainsKey('paper')) { $final['paper'] = @{} }
        if (-not $final['paper'].ContainsKey('enabled')) { $final['paper']['enabled'] = $false }
        if ($final['paper']['enabled']) {
            if (-not $final['paper'].ContainsKey('seed')) { $final['paper']['seed'] = 13 }
            if (-not $final.ContainsKey('lsh')) { $final['lsh'] = @{} }
            $final['lsh']['numTables']    = 0
            $final['lsh']['rowsPerBand']  = 0
            $final['lsh']['probeShards']  = 0
        }

        # write run-specific config
        $tmpConf = Join-Path $runDir "config.json"
        ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

        # args
        $keysFile = Join-Path $runDir "keystore.blob"
        $gtArg = (Safe-Resolve $GT)  # ignored by manager when gtPath provided & compute disabled

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
        $argList += "-jar";           $argList += (Safe-Resolve $JarPath)
        $argList += (Safe-Resolve $tmpConf)
        $argList += (Safe-Resolve $dataArg -AllowMissing:$true)
        $argList += (Safe-Resolve $queryArg -AllowMissing:$true)
        $argList += (Safe-Resolve $keysFile -AllowMissing:$true)
        $argList += "$Dim"
        $argList += (Safe-Resolve $runDir)
        $argList += $gtArg
        $argList += "$Batch"

        # log commandline & manifest
        $quotedArgs = foreach ($a in $argList) { $s = [string]$a; if ($s -match '\s') { '"' + ($s -replace '"','\"') + '"' } else { $s } }
        $argLine = [string]::Join(" ", $quotedArgs)
        $argLine | Out-File -FilePath (Join-Path $runDir "cmdline.txt") -Encoding utf8

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
            alpha            = $Alpha
            timestampUtc     = ([DateTime]::UtcNow.ToString("o"))
        }
        Save-Manifest -RunDir $runDir -Manifest $manifest

        # run
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
            Write-Warning ("Run failed: dataset={0}, profile={1}, exit={2}" -f $Name, $label, $exit)
        } else {
            Write-Host    ("Completed: {0} ({1})" -f $Name, $label)
        }
    }

    # ---- per-dataset merges ----
    $resultsGlob    = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\results_table.csv")     -File -ErrorAction SilentlyContinue
    $precisionGlob  = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\global_precision.csv")  -File -ErrorAction SilentlyContinue
    $topkGlob       = Get-ChildItem -Path (Join-Path $datasetRoot "*\results\topk_evaluation.csv")   -File -ErrorAction SilentlyContinue

    $combinedResults    = Join-Path $datasetRoot "combined_results.csv"
    $combinedPrecision  = Join-Path $datasetRoot "combined_precision.csv"
    $combinedTopk       = Join-Path $datasetRoot "combined_evaluation.csv"

    Combine-CSV -Files ($resultsGlob.FullName)   -OutCsv $combinedResults
    Combine-CSV -Files ($precisionGlob.FullName) -OutCsv $combinedPrecision
    Combine-CSV -Files ($topkGlob.FullName)      -OutCsv $combinedTopk
}
