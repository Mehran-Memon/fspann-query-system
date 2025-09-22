<# ============================
 FSP-ANN runner (clean)
 - Streams logs to run.out/run.err
 - Prints "Running <dataset> with profile <name> ..."
 - After run, prints run.out highlights at 1000-query gaps
 - Forces config bits: audit/paper/artifacts/precision CSV
 - GT always AUTO (precompute)
============================== #>

# --------- user settings ---------
$JarPath    = "F:\fspann-query-system\fsp-anns-parent\api\target\api-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
$ConfigPath = "F:\fspann-query-system\fsp-anns-parent\config\src\main\resources\config.json"
$OutRoot    = "G:\fsp-run"

# dataset
$Dataset = "SIFT1M"
$Dim     = 128
$Base    = "E:\Research Work\Datasets\sift_dataset\sift_base.fvecs"
$Query   = "E:\Research Work\Datasets\sift_dataset\sift_query.fvecs"

# optional: run only a single profile by name (exact match); leave empty to run all
$OnlyProfile = ""

# batch size to jar main
$Batch = 100000

# print “run.out” highlights every N queries (requested: 1000)
$QueryPrintGap = 1000

# memory / JVM flags
$JvmArgs = @(
    "-XX:+UseG1GC","-XX:MaxGCPauseMillis=200","-XX:+AlwaysPreTouch",
    "-Xms8g","-Xmx8g",
    "-Ddisable.exit=true",
    "-Doutput.audit=true",          # legacy flag still honored by code
    "-Dfile.encoding=UTF-8",
    "-Dbase.path=$Base"             # makes distance-ratio work if base scan is used
)

# --------- helpers ---------
function Ensure-Files([string]$base,[string]$query) {
    $ok = $true
    if (-not (Test-Path -LiteralPath $base))  { Write-Error "Missing base:  $base";  $ok = $false }
    if (-not (Test-Path -LiteralPath $query)) { Write-Error "Missing query: $query"; $ok = $false }
    return $ok
}

function New-EmptyDir([string]$p) {
    if (Test-Path -LiteralPath $p) { Remove-Item -LiteralPath $p -Recurse -Force -ErrorAction SilentlyContinue }
    New-Item -ItemType Directory -Path $p -Force | Out-Null
}

function Reset-RunDirs([string]$runDir) {
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null
    New-EmptyDir (Join-Path $runDir "metadata")
    New-EmptyDir (Join-Path $runDir "points")
    New-EmptyDir (Join-Path $runDir "results")
}

function To-Hashtable($obj) {
    if ($null -eq $obj) { return @{} }
    if ($obj -is [hashtable]) { return $obj }
    if ($obj -is [pscustomobject]) {
        $h=@{}; foreach($p in $obj.PSObject.Properties){ $h[$p.Name]=To-Hashtable $p.Value }; return $h
    }
    if ($obj -is [System.Collections.IDictionary]) {
        $h=@{}; foreach($k in $obj.Keys){ $h[$k]=To-Hashtable $obj[$k] }; return $h
    }
    if ($obj -is [System.Collections.IEnumerable] -and -not ($obj -is [string])) {
        $arr=@(); foreach($e in $obj){ $arr += ,(To-Hashtable $e) }; return $arr
    }
    return $obj
}

function Merge-Deep([hashtable]$base,[hashtable]$over) {
    $out = @{}
    foreach($k in $base.Keys){ $out[$k] = $base[$k] }
    foreach($k in $over.Keys){
        if ($out.ContainsKey($k) -and ($out[$k] -is [hashtable]) -and ($over[$k] -is [hashtable])) {
            $out[$k] = Merge-Deep $out[$k] $over[$k]
        } else {
            $out[$k] = $over[$k]
        }
    }
    return $out
}

function Build-Args([string]$jar,[string[]]$jvm,[string]$cfg,[string]$base,[string]$qry,[string]$keys,[int]$dim,[string]$runDir,[string]$gt,[int]$batch) {
    $a = @()
    $a += $jvm
    $a += "-jar"; $a += $jar
    $a += $cfg;   $a += $base
    $a += $qry;   $a += $keys
    $a += "$dim"; $a += $runDir
    $a += $gt;    $a += "$batch"
    # quote anything with spaces
    return ($a | ForEach-Object {
        $s = [string]$_
        if ($s -match '\s') { '"' + ($s -replace '"','\"') + '"' } else { $s }
    })
}

function Run-Java([string]$workDir,[string[]]$args,[string]$outPath,[string]$errPath) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "java"
    $psi.Arguments = [string]::Join(" ", $args)
    $psi.WorkingDirectory = $workDir
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi

    $outWriter = [System.IO.StreamWriter]::new($outPath, $false, [System.Text.Encoding]::UTF8)
    $errWriter = [System.IO.StreamWriter]::new($errPath, $false, [System.Text.Encoding]::UTF8)

    $proc.add_OutputDataReceived({
        param($s,$e)
        if ($e.Data -ne $null) {
            $outWriter.WriteLine($e.Data)
            # echo a few high-signal lines live
            if ($e.Data -match 'Loaded \d+ queries' -or
                    $e.Data -match 'Candidate fanout ratios' -or
                    $e.Data -match 'Ratio trust gate' -or
                    $e.Data -match '^Sanity q\d+:') {
                Write-Host $e.Data
            }
        }
    })
    $proc.add_ErrorDataReceived({
        param($s,$e)
        if ($e.Data -ne $null) {
            $errWriter.WriteLine($e.Data)
            Write-Host $e.Data -ForegroundColor DarkYellow
        }
    })

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    [void]$proc.Start()
    $proc.BeginOutputReadLine()
    $proc.BeginErrorReadLine()
    $proc.WaitForExit()
    $sw.Stop()

    $outWriter.Flush(); $outWriter.Dispose()
    $errWriter.Flush(); $errWriter.Dispose()

    return @{ ExitCode = $proc.ExitCode; Elapsed = $sw.Elapsed }
}

function Print-RunOut-Sampled([string]$runOutPath,[int]$gap) {
    if (-not (Test-Path -LiteralPath $runOutPath)) { return }
    Write-Host "`n--- run.out highlights (every $gap queries) ---" -ForegroundColor Cyan
    $rx = [regex]'Query\s+(\d+)\b'
    $printed = 0
    Get-Content -LiteralPath $runOutPath | ForEach-Object {
        $line = $_
        $m = $rx.Match($line)
        if ($m.Success) {
            $q = [int]$m.Groups[1].Value
            if ($q % $gap -eq 0) {
                Write-Host $line
                $printed++
            }
        }
    }
    if ($printed -eq 0) {
        # fallback: if no "Query N" lines, just print every Nth non-empty line
        $i = 0
        Get-Content -LiteralPath $runOutPath | Where-Object { $_.Trim().Length -gt 0 } | ForEach-Object {
            $i++
            if ($i % $gap -eq 0) { Write-Host $_ }
        }
    }
    Write-Host "--- end highlights ---`n" -ForegroundColor Cyan
}

# --------- main ---------
if (-not (Ensure-Files $Base $Query)) { throw "Abort: dataset files missing." }
New-Item -ItemType Directory -Path $OutRoot -Force | Out-Null

# read base config / profiles
$cfgObj = (Get-Content -LiteralPath $ConfigPath -Raw) | ConvertFrom-Json
$baseCfg = To-Hashtable $cfgObj.base
$profiles = @($cfgObj.profiles)

if ([string]::IsNullOrWhiteSpace($OnlyProfile) -eq $false) {
    $profiles = @($profiles | Where-Object { $_.name -eq $OnlyProfile })
    if ($profiles.Count -eq 0) { throw "Profile '$OnlyProfile' not found in config.json" }
}

foreach ($p in $profiles) {
    $pHT = To-Hashtable $p
    if (-not $pHT.ContainsKey('name')) { continue }
    $label   = [string]$pHT['name']
    $ovr     = if ($pHT.ContainsKey('overrides')) { To-Hashtable $pHT['overrides'] } else { @{} }
    $runDir  = Join-Path $OutRoot ($Dataset + "\" + $label)
    $resultsDir = Join-Path $runDir "results"
    $keysFile = Join-Path $runDir "keystore.blob"
    $tmpConf  = Join-Path $runDir "config.json"

    Reset-RunDirs $runDir

    # merge config + force bits the Java actually reads
    $final = Merge-Deep $baseCfg $ovr
    if (-not $final.ContainsKey('output')) { $final['output'] = @{} }
    $final['output']['resultsDir'] = $resultsDir
    $final['output']['exportArtifacts'] = $true

    if (-not $final.ContainsKey('eval')) { $final['eval'] = @{} }
    $final['eval']['computePrecision'] = $true
    $final['eval']['writeGlobalPrecisionCsv'] = $true

    if (-not $final.ContainsKey('audit')) { $final['audit'] = @{} }
    $final['audit']['enable'] = $true

    if (-not $final.ContainsKey('paper')) { $final['paper'] = @{} }
    $final['paper']['enabled'] = $true

    ($final | ConvertTo-Json -Depth 64) | Out-File -FilePath $tmpConf -Encoding utf8

    # args (GT="AUTO" forces precompute each run)
    $args = Build-Args -jar $JarPath -jvm $JvmArgs -cfg $tmpConf -base $Base -qry $Query -keys $keysFile -dim $Dim -runDir $runDir -gt "AUTO" -batch $Batch
    $cmdLinePath = Join-Path $runDir "cmdline.txt"
    ([string]::Join(" ", $args)) | Out-File -FilePath $cmdLinePath -Encoding utf8

    Write-Host ""
    Write-Host ("Running {0} with profile {1} ..." -f $Dataset, $label) -ForegroundColor Green
    Write-Host ("  WorkDir : {0}" -f $runDir)
    Write-Host ("  Results : {0}" -f $resultsDir)
    Write-Host ("  Args    : {0}" -f $cmdLinePath)

    $runOut = Join-Path $runDir "run.out.log"
    $runErr = Join-Path $runDir "run.err.log"
    $res = Run-Java -workDir $runDir -args $args -outPath $runOut -errPath $runErr

    if ($res.ExitCode -ne 0) {
        Write-Host ("✖ Run FAILED for profile {0} (exit={1}, elapsed={2:n1}s)" -f $label, $res.ExitCode, $res.Elapsed.TotalSeconds) -ForegroundColor Red
    } else {
        Write-Host ("✔ Completed {0} ({1}) in {2:n1}s" -f $Dataset, $label, $res.Elapsed.TotalSeconds) -ForegroundColor Green
    }

    # print sampled highlights from run.out (every 1000 queries)
    Print-RunOut-Sampled -runOutPath $runOut -gap $QueryPrintGap
}
