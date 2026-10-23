param(
    [ValidateSet('start','status','stop','capacity')][string]$Action = 'status',
    [ValidateRange(1,43200)][int]$Seconds = 43200,
    [ValidateRange(0,9007199254740991)][long]$RequiredFreeBytes = 1073741824,
    [ValidatePattern('^[A-Za-z0-9._-]+$')][string]$Distribution = 'SparkDQ-Ubuntu',
    [ValidatePattern('^/[A-Za-z0-9_./-]+$')][string]$ProjectPath = '/home/sparkdq/projects/spark-dq'
)
$ErrorActionPreference = 'Stop'
if ($ProjectPath -match '(^|/)\.\.(/|$)') { throw 'Parent path traversal is not allowed' }
function Get-HostCapacity {
    try {
        $entries = @(Get-ChildItem HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss | Get-ItemProperty | Where-Object { $_.DistributionName -eq $Distribution })
        if ($entries.Count -ne 1) { throw 'Distribution registration unavailable' }
        $basePath = [string]$entries[0].BasePath
        if ($basePath.StartsWith('\\?\')) { $basePath = $basePath.Substring(4) }
        if ($basePath -notmatch '^([A-Za-z]):\\') { throw 'Backing drive is not a local drive' }
        $drive = New-Object System.IO.DriveInfo("$($matches[1]):\")
        return [PSCustomObject]@{
            known = $true
            backingDirectory = $basePath
            drive = $drive.Name
            freeBytes = $drive.AvailableFreeSpace
            requiredFreeBytesEstimate = $RequiredFreeBytes
            enoughForEstimate = ($drive.AvailableFreeSpace -ge $RequiredFreeBytes)
        }
    } catch {
        return [PSCustomObject]@{ known=$false; reason='Backing volume capacity unavailable; Linux free space alone is insufficient' }
    }
}
if ($Action -eq 'capacity') { Get-HostCapacity | ConvertTo-Json; exit 0 }
$leaseLogs = Join-Path $PSScriptRoot '.runtime'
New-Item -ItemType Directory -Force -Path $leaseLogs | Out-Null
$baseArguments = @('-d',$Distribution,'-u','root','--','python3',"$ProjectPath/infrastructure/runtime.py")
function Invoke-SessionCommand([string]$Command) {
    $identity = [Guid]::NewGuid().ToString('N')
    $stdout = Join-Path $leaseLogs "$identity.out.log"
    $stderr = Join-Path $leaseLogs "$identity.err.log"
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo.FileName = 'wsl.exe'
    # All argument values are validated as whitespace/quote-free; no shell is used.
    $process.StartInfo.Arguments = (($baseArguments + $Command) -join ' ')
    $process.StartInfo.UseShellExecute = $false
    $process.StartInfo.CreateNoWindow = $true
    $process.StartInfo.RedirectStandardOutput = $true
    $process.StartInfo.RedirectStandardError = $true
    [void]$process.Start()
    $outputTask = $process.StandardOutput.ReadToEndAsync()
    $errorTask = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(45000)) {
        $process.Kill()
        throw 'Session command timed out; the runtime session and containers were not stopped'
    }
    $outputText = $outputTask.GetAwaiter().GetResult()
    $errorText = $errorTask.GetAwaiter().GetResult()
    Set-Content -LiteralPath $stdout -Value $outputText
    Set-Content -LiteralPath $stderr -Value $errorText
    if ($process.ExitCode -ne 0) { throw "WSL command failed: $errorText" }
    return ($outputText | ConvertFrom-Json)
}
if ($Action -eq 'start') {
    $identity = [Guid]::NewGuid().ToString('N')
    $stdout = Join-Path $leaseLogs "$identity.lease.out.log"
    $stderr = Join-Path $leaseLogs "$identity.lease.err.log"
    $process = Start-Process -FilePath 'wsl.exe' -ArgumentList ($baseArguments + @('serve','--seconds',"$Seconds")) -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    $limit = [DateTime]::UtcNow.AddSeconds(230)
    do {
        Start-Sleep -Seconds 1
        $state = Invoke-SessionCommand 'status'
        if ($state.alive -and $state.state -eq 'running') {
            $state | Add-Member -NotePropertyName hostCapacity -NotePropertyValue (Get-HostCapacity) -Force
            $state | ConvertTo-Json -Depth 5
            exit 0
        }
        if ($process.HasExited) { throw (Get-Content -LiteralPath $stderr -Raw) }
    } while ([DateTime]::UtcNow -lt $limit)
    throw 'Session startup not healthy within limit; use status/logs to inspect. No containers were stopped.'
}
Invoke-SessionCommand $Action | ConvertTo-Json -Depth 5
