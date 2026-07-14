param(
    [switch]$KeepRedis,
    [switch]$KeepWslDependencies,
    [AllowEmptyString()]
    [string]$WslDistro = $env:MEET_WSL_DISTRO,
    [ValidateRange(1, 120)]
    [int]$DependencyWaitSeconds = 30,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$RuntimeRoot = Join-Path $env:TEMP "synq-meet-dev"
$StatePath = Join-Path $RuntimeRoot "processes.json"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerRoot = Split-Path -Parent $ScriptDir
$WslHelperPath = Join-Path $ScriptDir "wsl-dependencies.ps1"

. $WslHelperPath

function Write-Step($Message) {
    Write-Host "[meet] $Message"
}

function Stop-ProcessTree($ProcessId) {
    if (-not $ProcessId -or $ProcessId -le 0) {
        return
    }

    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree ([int]$child.ProcessId)
    }

    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if (-not $process) {
        return
    }

    if ($DryRun) {
        Write-Host "  Would stop PID $ProcessId ($($process.ProcessName))"
        return
    }

    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

if (-not (Test-Path -LiteralPath $StatePath)) {
    Write-Warning "No start state found at $StatePath. Nothing to stop."
    return
}

$state = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json

$processes = @($state.processes)
[array]::Reverse($processes)

foreach ($entry in $processes) {
    Write-Step "Stopping $($entry.name) (PID $($entry.pid))."
    $rootProcess = Get-Process -Id ([int]$entry.pid) -ErrorAction SilentlyContinue
    if ($rootProcess -and $entry.startTicks) {
        $currentTicks = $rootProcess.StartTime.ToUniversalTime().Ticks
        if ([int64]$entry.startTicks -ne [int64]$currentTicks) {
            Write-Warning "Skipping PID $($entry.pid); it no longer matches the process started by start.ps1."
            continue
        }
    }
    Stop-ProcessTree ([int]$entry.pid)
}

if (-not $KeepRedis -and $state.redis -and $state.redis.started -and $state.redis.container) {
    $dockerCommand = Get-Command "docker" -ErrorAction SilentlyContinue
    if ($dockerCommand) {
        $containerName = [string]$state.redis.container
        Write-Step "Stopping Redis container '$containerName'."
        if ($DryRun) {
            Write-Host "  $($dockerCommand.Source) stop $containerName"
        } else {
            & $dockerCommand.Source stop $containerName | Out-Null
        }
    } else {
        Write-Warning "Docker was not found; Redis container '$($state.redis.container)' was not stopped."
    }
}

if (-not $KeepWslDependencies -and $state.wsl) {
    if ($DryRun) {
        Write-Step "Would stop WSL dependencies recorded by start.ps1."
    } else {
        $wslResult = Stop-MeetWslDependencies `
            -AppName "synq-meet-dev" `
            -ServerRoot $ServerRoot `
            -Distro $WslDistro `
            -WaitSeconds $DependencyWaitSeconds
        if (-not [bool]$wslResult.Ok) {
            Write-Warning "Some WSL dependencies may still be running."
        }
    }
}

if (-not $DryRun) {
    Remove-Item -LiteralPath $StatePath -Force
}

Write-Step "Stopped services recorded in $StatePath."
