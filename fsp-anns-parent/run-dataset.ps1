# ============================
# FSP-ANN (single dataset, multi-profile)
# PS 5.1 / ConstrainedLanguage safe
# - Per-run cleanup for metadata/points/results
# - Precision & Recall CSV enabled in config (belt & suspenders)
# - GT is ALWAYS precomputed each run (passes "AUTO")
# ============================

# ---- required paths ----
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot    = "G:\fsp-run"

# ---- dataset: SIFT1M (edit these to your actual files) ----
$Name  = "SIFT1M"
$Dim   = 128
$Base  = "E:\Research Work\Datasets\sift_dataset\sift_base.fvecs"
$Query = "E:\Research Work\Datasets\sift_dataset\sift_query.fvecs"
$GT    = "E:\Research Work\Datasets\sift_dataset\sift_groundtruth.ivecs"   # ignored; AUTO will be used

# app batch size arg
$Batch = 100000

# run only one profile (choose: SANNp | mSANNp | recall_first)
$OnlyProfile = "recall_first"

# cleanup toggles
$CleanPerRun = $true
$CleanAllNow = $false

# JVM/system flags
$JvmArgs = @(
    "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
    "-Ddisable.exit=true",
    "-Dpaper.mode=true",
    "-Daudit.enable=true",           # <- turn audit on
    "-Dexport.artifacts=true",       # <- allow writing artifacts
    "-Dfile.encoding=UTF-8"
)

# ---------- helpers (PS5-safe) ----------
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
    $out=@{}
    foreach($k in $h.Keys){
        $v = $h[$k]
        if ($v -is [hashtable]) { $out[$k] = Copy-Hashtable $v }
        elseif ($v -is [System.Collections.IEnumerable] -and -not ($v -is [string])) {
            $arr=@(); foreach($e in $v){ if ($e -is [hashtable]) { $arr += ,(Copy-Hashtable $e) } else { $arr += ,$e } }
            $out[$k] = $arr
        } else { $out[$k] = $v }
    }
    return $out
}

function Apply-Overrides {
    param([hashtable]$base,[hashtable]$ovr)
    $result = Copy-Hashtable $base
    foreach($k in $ovr.Keys){
        $ov = $ovr[$k]
        if ($result.ContainsKey($k) -and ($result[$k] -is [hashtable]) -and ($ov -is [hashtable])) {
            foreach($sub in $ov.Keys){ $result[$k][$sub] = $ov[$sub] }
        } else {
            $result[$k] = $ov
        }
    }
    return $result
}

function Ensure-Files([string]$base,[string]$query) {
    $ok = $true
    if ([string]::IsNullOrWhiteSpace($base)  -or -not (Test-Path -LiteralPath $base))  { Write-Error "Missing base:  $base";  $ok = $false }
    if ([string]::IsNullOrWhiteSpace($query) -or -not (Test-Path -LiteralPath $query)) { Write-Error "Missing query: $query"; $ok = $false }
    return $ok
}

function Safe-Resolve([string]$Path, [bool]$AllowMissing = $false) {
    try {
        if ($AllowMissing -and -not (Test-Path -LiteralPath $Path)) { return $Path }
        return (Resolve-Path -LiteralPath $Path).Path
    } catch {
        if ($AllowMissing) { return $Path } else { throw }
    }
}

function Invoke-FastDelete([string]$PathToDelete) {
    if (-not (Test-Path -LiteralPath $PathToDelete)) { return }
    $item = Get-Item -LiteralPath $PathToDelete -ErrorAction SilentlyContinue
    if ($null -eq $item) { return }
    if (-not $item.PSIsContainer) {
        Remove-Item -LiteralPath $PathToDelete -Force -ErrorAction SilentlyContinue
        return
    }
    $empty = New-Item -ItemType Directory -Path (Join-Path $env:TEMP ("empty_" + [guid]::NewGuid())) -Force
    try {
        robocopy $empty.FullName $PathToDelete /MIR /NFL /NDL /NJH /NJS /NC /NS /NP | Out-Null
    } finally {
        try { Remove-Item -LiteralPath $PathToDelete -Recurse -Force -ErrorAction SilentlyContinue } catch {}
        try { Remove-Item -LiteralPath $empty.FullName -Recurse -Force -ErrorAction SilentlyContinue } catch {}
    }
}

function Clean-RunMetadata([string]$RunDir) {
    $paths = @(
        (Join-Path $RunDir "metadata"),
        (Join-Path $RunDir "points"),
        (Join-Path $RunDir "results")
    )
    foreach ($p in $paths) {
        if (Test-Path -LiteralPath $p) {
            Write-Host "Cleaning $p ..."
            Invoke-FastDelete $p
        }
        New-Item -ItemType Directory -Force -Path $p | Out-Null
    }
}

function Clean-AllUnderOutRoot([string]$OutRootPath, [string]$DatasetName) {
    if (-not (Test-Path -LiteralPath $OutRootPath)) { return }
    $datasetRoot = Join-Path $OutRootPath $DatasetName
    if (-not (Test-Path -LiteralPath $datasetRoot)) { return }
    $targets = @("metadata","points","results")
    $nodes = Get-ChildItem -LiteralPath $datasetRoot -Recurse -Directory -ErrorAction SilentlyContinue |
            Where-Object { $targets -contains $_.Name }
    foreach ($n in $nodes) {
        Write-Host "Wiping $($n.FullName) ..."
        Invoke-FastDelete $n.FullName
    }
}

# ---------- STEP 0: optional global clean and exit ----------
if ($CleanAllNow) {
    New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null
    Clean-AllUnderOutRoot -OutRootPath $OutRoot -DatasetName $Name
    Write-Host "Global clean completed under $OutRoot\$Name."
    return
}

# ---------- read config ----------
if (-not (Test-Path -LiteralPath $ConfigPath)) { throw "Config not found: $ConfigPath" }
if (-not (Ensure-Files $Base $Query)) { throw "Abort: dataset files missing." }

$cfgObj   = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$baseObj  = $cfgObj.base
$profiles = $cfgObj.profiles
if ($null -eq $baseObj)  { throw "config.json must contain a 'base' object." }
if ($null -eq $profiles) { throw "config.json must contain a 'profiles' array." }
$baseHT = To-Hashtable $baseObj

New-Item -ItemType Directory -Force -Path $OutRoot | Out-Null

# ---------- run one profile ----------
if ($OnlyProfile -and $OnlyProfile.Trim().Length -gt 0) {
    $profiles = @($profiles | Where-Object { $_.name -eq $OnlyProfile })
    if ($profiles.Count -eq 0) { throw "Profile '$OnlyProfile' not found in config.json" }
}

# ---------- run all profiles ----------
foreach ($p in $profiles) {
    $pHT = To-Hashtable $p
    if (-not $pHT.ContainsKey('name')) { continue }
    $label = [string]$pHT['name']
    $ovr   = @{}; if ($pHT.ContainsKey('overrides')) { $ovr = $pHT['overrides'] }

    $runDir = Join-Path $OutRoot ("$Name\" + $label)
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    if ($CleanPerRun) { Clean-RunMetadata -RunDir $runDir }

    # merge config & force resultsDir + CSV toggles
    $final = Apply-Overrides -base $baseHT -ovr $ovr
    if ($cfgObj.audit) { $final['audit'] = To-Hashtable $cfgObj.audit }
    if (-not $final.ContainsKey('output')) { $final['output'] = @{} }
    $final['output']['resultsDir'] = (Join-Path $runDir "results")
    if (-not $final.ContainsKey('eval')) { $final['eval'] = @{} }
    $final['eval']['computePrecision'] = $true
    $final['eval']['writeGlobalPrecisionCsv'] = $true
    $final['eval']['writeGlobalRecallCsv']    = $true   # keep both paths enabled

    $tmpConf = Join-Path $runDir "config.json"
    ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

    $keysFile = Join-Path $runDir "keystore.blob"

    # ---- FORCE PRECOMPUTE EVERY RUN ----
    $gtArg = "AUTO"

    # Build FLAT argument list
    $argList = @()
    $argList += $JvmArgs
    $argList += "-Dbase.path=$Base"
    $argList += "-jar";           $argList += $JarPath
    $argList += $tmpConf;         $argList += $Base
    $argList += $Query;           $argList += $keysFile
    $argList += "$Dim";           $argList += $runDir
    $argList += $gtArg;           $argList += "$Batch"

    # Quote anything with spaces
    $quotedArgs = foreach ($a in $argList) {
        $s = [string]$a
        if ($s -match '\s') { '"' + ($s -replace '"','\"') + '"' } else { $s }
    }
    $argLine = [string]::Join(" ", $quotedArgs)
    $argLine | Out-File -FilePath (Join-Path $runDir "cmdline.txt") -Encoding utf8  # debug

    # Launch
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "java"
    $psi.Arguments = $argLine
    $psi.WorkingDirectory = $runDir
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $pobj = New-Object System.Diagnostics.Process
    $pobj.StartInfo = $psi
    [void]$pobj.Start()
    $out = $pobj.StandardOutput.ReadToEnd()
    $err = $pobj.StandardError.ReadToEnd()
    $pobj.WaitForExit()
    $sw.Stop()

    $out | Out-File -FilePath (Join-Path $runDir "run.out.log") -Encoding utf8
    $err | Out-File -FilePath (Join-Path $runDir "run.err.log") -Encoding utf8
    ("ElapsedSec={0:N1}" -f $sw.Elapsed.TotalSeconds) | Out-File -FilePath (Join-Path $runDir "elapsed.txt") -Encoding utf8

    if ($pobj.ExitCode -ne 0) {
        Write-Warning ("Run failed for profile {0} (exit={1})." -f $label, $pobj.ExitCode)
    } else {
        Write-Host ("Completed: {0} ({1})" -f $Name, $label)
    }

    foreach ($fn in $filesToCopy) {
        $src = Join-Path $resultsDir $fn
        if (Test-Path -LiteralPath $src) {
            Copy-Item -LiteralPath $src -Destination (Join-Path $runDir $fn) -Force
        }
    }
}