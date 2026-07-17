#Requires -Version 5.1
<#
.SYNOPSIS
Stops the Synq Meet development stack started by start.ps1.

.DESCRIPTION
For foreground-supervisor state, this script first writes a stop request and
waits for the supervisor to perform an orderly shutdown. If that is not
available or does not finish in time, it safely falls back to stopping only
recorded PID/start-time matches. Dependencies are stopped only when start.ps1
recorded that it started them.
#>

[CmdletBinding()]
param(
    [switch] $KeepRedis,
    [switch] $KeepWslDependencies,

    [AllowEmptyString()]
    [string] $WslDistro = "",

    [ValidateRange(1, 300)]
    [int] $GracefulWaitSeconds = 20,

    [ValidateRange(1, 120)]
    [int] $DependencyWaitSeconds = 30,

    [switch] $DryRun
)

$ErrorActionPreference = "Stop"

$RuntimeRoot = Join-Path $env:TEMP "synq-meet-dev"
$StatePath = Join-Path $RuntimeRoot "processes.json"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerRoot = Split-Path -Parent $ScriptDir
$WslHelperPath = Join-Path $ScriptDir "wsl-dependencies.ps1"

if (-not (Test-Path -LiteralPath $WslHelperPath -PathType Leaf)) {
    throw "Cannot find the WSL dependency helper at '$WslHelperPath'."
}

. $WslHelperPath

function Write-Step {
    param([Parameter(Mandatory = $true)][string] $Message)

    Write-Host "[meet] $Message"
}

function Get-ObjectPropertyValue {
    param(
        [AllowNull()]
        [object] $InputObject,

        [Parameter(Mandatory = $true)]
        [string] $Name
    )

    if ($null -eq $InputObject) {
        return $null
    }

    $property = $InputObject.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Read-StateFile {
    param(
        [ValidateRange(1, 10)]
        [int] $Attempts = 3,

        [switch] $Quiet
    )

    $lastError = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt += 1) {
        if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) {
            return $null
        }

        try {
            $raw = Get-Content -LiteralPath $StatePath -Raw
            if ([string]::IsNullOrWhiteSpace($raw)) {
                throw "The state file is empty."
            }

            return [pscustomobject] @{
                State = $raw | ConvertFrom-Json
                Raw   = $raw
            }
        } catch {
            $lastError = $_
            if ($attempt -lt $Attempts) {
                Start-Sleep -Milliseconds 100
            }
        }
    }

    if ($Quiet) {
        return $null
    }

    throw "Could not read development state from '$StatePath': $($lastError.Exception.Message)"
}

function Get-StateMetadata {
    param([Parameter(Mandatory = $true)][object] $State)

    $supervisor = Get-ObjectPropertyValue -InputObject $State -Name "supervisor"
    $control = Get-ObjectPropertyValue -InputObject $State -Name "control"

    $supervisorProcessId = Get-ObjectPropertyValue -InputObject $State -Name "supervisorPid"
    if ($null -eq $supervisorProcessId) {
        $supervisorProcessId = Get-ObjectPropertyValue -InputObject $supervisor -Name "pid"
    }

    $supervisorStartTicks = Get-ObjectPropertyValue -InputObject $State -Name "supervisorStartTicks"
    if ($null -eq $supervisorStartTicks) {
        $supervisorStartTicks = Get-ObjectPropertyValue -InputObject $supervisor -Name "startTicks"
    }

    $stopRequestValue = Get-ObjectPropertyValue -InputObject $State -Name "stopRequestPath"
    if ([string]::IsNullOrWhiteSpace([string] $stopRequestValue)) {
        $stopRequestValue = Get-ObjectPropertyValue -InputObject $control -Name "stopRequestFile"
    }

    return [pscustomobject] @{
        RunId                = [string] (Get-ObjectPropertyValue -InputObject $State -Name "runId")
        SupervisorProcessId  = $supervisorProcessId
        SupervisorStartTicks = $supervisorStartTicks
        StopRequestValue     = [string] $stopRequestValue
    }
}

function Get-StateStartedAt {
    param([Parameter(Mandatory = $true)][object] $State)

    $startedAt = Get-ObjectPropertyValue -InputObject $State -Name "startedAtUtc"
    if ([string]::IsNullOrWhiteSpace([string] $startedAt)) {
        $startedAt = Get-ObjectPropertyValue -InputObject $State -Name "startedAt"
    }

    return [string] $startedAt
}

function Test-StateMatchesSnapshot {
    param(
        [Parameter(Mandatory = $true)][object] $CurrentState,
        [Parameter(Mandatory = $true)][object] $SnapshotState
    )

    $currentMetadata = Get-StateMetadata -State $CurrentState
    $snapshotMetadata = Get-StateMetadata -State $SnapshotState

    if (-not [string]::IsNullOrWhiteSpace($snapshotMetadata.RunId)) {
        return [string]::Equals(
            $currentMetadata.RunId,
            $snapshotMetadata.RunId,
            [StringComparison]::OrdinalIgnoreCase
        )
    }

    $currentStartedAt = Get-StateStartedAt -State $CurrentState
    $snapshotStartedAt = Get-StateStartedAt -State $SnapshotState
    if (
        -not [string]::IsNullOrWhiteSpace($currentStartedAt) -and
        -not [string]::IsNullOrWhiteSpace($snapshotStartedAt)
    ) {
        return [string]::Equals($currentStartedAt, $snapshotStartedAt, [StringComparison]::Ordinal)
    }

    if (
        $null -ne $snapshotMetadata.SupervisorProcessId -and
        $null -ne $snapshotMetadata.SupervisorStartTicks -and
        $null -ne $currentMetadata.SupervisorProcessId -and
        $null -ne $currentMetadata.SupervisorStartTicks
    ) {
        return (
            [string] $snapshotMetadata.SupervisorProcessId -eq [string] $currentMetadata.SupervisorProcessId -and
            [string] $snapshotMetadata.SupervisorStartTicks -eq [string] $currentMetadata.SupervisorStartTicks
        )
    }

    # Legacy start.ps1 always wrote startedAt. If even that is absent, fail
    # closed rather than risk deleting or cleaning up a newer run.
    return $false
}

function Resolve-StopRequestPath {
    param([AllowEmptyString()][string] $PathValue)

    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $null
    }

    try {
        $candidate = $PathValue
        if (-not [IO.Path]::IsPathRooted($candidate)) {
            $candidate = Join-Path $RuntimeRoot $candidate
        }

        $runtimeFullPath = [IO.Path]::GetFullPath($RuntimeRoot).TrimEnd("\", "/")
        $candidateFullPath = [IO.Path]::GetFullPath($candidate)
        $runtimePrefix = $runtimeFullPath + [IO.Path]::DirectorySeparatorChar

        if (-not $candidateFullPath.StartsWith($runtimePrefix, [StringComparison]::OrdinalIgnoreCase)) {
            Write-Warning "Ignoring stop-request path outside the runtime directory: '$candidateFullPath'."
            return $null
        }

        return $candidateFullPath
    } catch {
        Write-Warning "Ignoring invalid stop-request path '$PathValue': $($_.Exception.Message)"
        return $null
    }
}

function Get-ProcessIdentityStatus {
    param(
        [AllowNull()][object] $ProcessIdValue,
        [AllowNull()][object] $StartTicksValue
    )

    try {
        $processId = [int] $ProcessIdValue
        if ($processId -le 0) {
            throw "PID must be positive."
        }
    } catch {
        return [pscustomobject] @{
            Status    = "Unverifiable"
            ProcessId = 0
            Process   = $null
            Reason    = "invalid PID '$ProcessIdValue'"
        }
    }

    $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        return [pscustomobject] @{
            Status    = "Absent"
            ProcessId = $processId
            Process   = $null
            Reason    = "process is not running"
        }
    }

    if ([string]::IsNullOrWhiteSpace([string] $StartTicksValue)) {
        return [pscustomobject] @{
            Status    = "Unverifiable"
            ProcessId = $processId
            Process   = $process
            Reason    = "recorded startTicks is missing"
        }
    }

    try {
        $expectedTicks = [int64] $StartTicksValue
        $currentTicks = [int64] $process.StartTime.ToUniversalTime().Ticks
    } catch {
        return [pscustomobject] @{
            Status    = "Unverifiable"
            ProcessId = $processId
            Process   = $process
            Reason    = "could not verify process start time: $($_.Exception.Message)"
        }
    }

    if ($expectedTicks -ne $currentTicks) {
        return [pscustomobject] @{
            Status    = "Mismatch"
            ProcessId = $processId
            Process   = $process
            Reason    = "PID has been reused by a different process"
        }
    }

    return [pscustomobject] @{
        Status    = "Match"
        ProcessId = $processId
        Process   = $process
        Reason    = ""
    }
}

function Write-StopRequest {
    param(
        [Parameter(Mandatory = $true)][string] $RequestPath,
        [AllowEmptyString()][string] $RunId
    )

    $request = [ordered] @{
        requestVersion      = 1
        runId               = $RunId
        requestedAtUtc      = (Get-Date).ToUniversalTime().ToString("o")
        requestedByPid      = $PID
        keepDependencies    = [ordered] @{
            redis = [bool] $KeepRedis
            wsl   = [bool] $KeepWslDependencies
        }
        # Top-level aliases keep the request easy to consume from older or
        # minimal supervisors while the nested object remains canonical.
        keepRedis           = [bool] $KeepRedis
        keepWslDependencies = [bool] $KeepWslDependencies
    }

    $requestDirectory = Split-Path -Parent $RequestPath
    New-Item -ItemType Directory -Path $requestDirectory -Force | Out-Null

    $temporaryPath = "$RequestPath.$PID.$([Guid]::NewGuid().ToString('N')).tmp"
    try {
        $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
        [IO.File]::WriteAllText(
            $temporaryPath,
            ($request | ConvertTo-Json -Depth 5),
            $utf8NoBom
        )

        # The temporary file is on the same volume, so the supervisor never
        # observes partially-written JSON.
        Move-Item -LiteralPath $temporaryPath -Destination $RequestPath -Force
    } finally {
        Remove-Item -LiteralPath $temporaryPath -Force -ErrorAction SilentlyContinue
    }
}

function Wait-ForSupervisorExit {
    param(
        [Parameter(Mandatory = $true)][object] $SnapshotState,
        [Parameter(Mandatory = $true)][object] $SupervisorProcessId,
        [Parameter(Mandatory = $true)][object] $SupervisorStartTicks,
        [Parameter(Mandatory = $true)][int] $Seconds
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $currentStateRecord = Read-StateFile -Attempts 1 -Quiet
        if (
            $null -ne $currentStateRecord -and
            -not (Test-StateMatchesSnapshot -CurrentState $currentStateRecord.State -SnapshotState $SnapshotState)
        ) {
            return "StateReplaced"
        }

        $identity = Get-ProcessIdentityStatus `
            -ProcessIdValue $SupervisorProcessId `
            -StartTicksValue $SupervisorStartTicks

        switch ($identity.Status) {
            "Absent" { return "Exited" }
            "Mismatch" { return "IdentityLost" }
            "Unverifiable" { return "IdentityLost" }
        }

        Start-Sleep -Milliseconds 200
    }

    return "TimedOut"
}

function Stop-ProcessTree {
    param(
        [Parameter(Mandatory = $true)][int] $ProcessId,
        [Parameter(Mandatory = $true)][hashtable] $Visited,
        [AllowNull()][object] $ExpectedStartTicks,
        [Parameter(Mandatory = $true)][string] $Label,
        [switch] $SkipChildEnumeration
    )

    if ($Visited.ContainsKey($ProcessId)) {
        return $true
    }

    # Validate the recorded root before even enumerating its children. Without
    # this guard, a recycled PID could cause an unrelated process tree to be
    # traversed before the later pre-kill identity check.
    $initialIdentity = Get-ProcessIdentityStatus `
        -ProcessIdValue $ProcessId `
        -StartTicksValue $ExpectedStartTicks
    switch ($initialIdentity.Status) {
        "Absent" { return $true }
        "Mismatch" {
            Write-Warning "Skipping PID $ProcessId for $Label because the PID now belongs to another process."
            return $true
        }
        "Unverifiable" {
            Write-Warning "Skipping PID $ProcessId for $Label because its identity is not verifiable: $($initialIdentity.Reason)."
            return $false
        }
    }

    $Visited[$ProcessId] = $true

    $ok = $true
    if (-not $SkipChildEnumeration) {
        try {
            $children = @(
                Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction Stop
            )
        } catch {
            $children = @()
            $ok = $false
            Write-Warning "Could not enumerate child processes for $Label (PID $ProcessId): $($_.Exception.Message)"
        }

        foreach ($child in $children) {
            $childProcessId = [int] $child.ProcessId
            $childProcess = Get-Process -Id $childProcessId -ErrorAction SilentlyContinue
            if ($null -eq $childProcess) {
                continue
            }

            try {
                $childStartTicks = $childProcess.StartTime.ToUniversalTime().Ticks
            } catch {
                $ok = $false
                Write-Warning "Skipping child PID $childProcessId; its start time could not be verified."
                continue
            }

            $childOk = Stop-ProcessTree `
                -ProcessId $childProcessId `
                -Visited $Visited `
                -ExpectedStartTicks $childStartTicks `
                -Label "$Label child"
            if (-not $childOk) {
                $ok = $false
            }
        }
    }

    $identity = Get-ProcessIdentityStatus `
        -ProcessIdValue $ProcessId `
        -StartTicksValue $ExpectedStartTicks

    switch ($identity.Status) {
        "Absent" {
            return $ok
        }
        "Mismatch" {
            Write-Warning "Skipping PID $ProcessId for $Label because the PID now belongs to another process."
            return $ok
        }
        "Unverifiable" {
            Write-Warning "Skipping PID $ProcessId for $Label because its identity is not verifiable: $($identity.Reason)."
            return $false
        }
    }

    if ($DryRun) {
        Write-Host "  Would force-stop $Label PID $ProcessId ($($identity.Process.ProcessName))."
        return $ok
    }

    try {
        Stop-Process -InputObject $identity.Process -Force -ErrorAction Stop
        if (-not $identity.Process.WaitForExit(5000)) {
            Write-Warning "$Label PID $ProcessId did not exit after it was stopped."
            return $false
        }
    } catch {
        # Treat a process that exited between verification and Stop-Process as
        # successfully stopped; otherwise keep the state for a retry.
        if ($null -ne (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) {
            Write-Warning "Failed to stop $Label PID ${ProcessId}: $($_.Exception.Message)"
            return $false
        }
    }

    return $ok
}

function Stop-RecordedProcess {
    param(
        [Parameter(Mandatory = $true)][object] $Entry,
        [Parameter(Mandatory = $true)][hashtable] $Visited,
        [switch] $RecordedLeaf
    )

    $name = [string] (Get-ObjectPropertyValue -InputObject $Entry -Name "name")
    if ([string]::IsNullOrWhiteSpace($name)) {
        $name = "recorded service"
    }

    $ownedOk = $true
    $ownedProcesses = @(Get-ObjectPropertyValue -InputObject $Entry -Name "ownedProcesses")
    foreach ($ownedProcess in $ownedProcesses) {
        if ($null -eq $ownedProcess) { continue }
        if (-not (Stop-RecordedProcess -Entry $ownedProcess -Visited $Visited -RecordedLeaf)) {
            $ownedOk = $false
        }
    }

    $processIdValue = Get-ObjectPropertyValue -InputObject $Entry -Name "pid"
    $startTicksValue = Get-ObjectPropertyValue -InputObject $Entry -Name "startTicks"
    $identity = Get-ProcessIdentityStatus `
        -ProcessIdValue $processIdValue `
        -StartTicksValue $startTicksValue

    switch ($identity.Status) {
        "Absent" {
            Write-Step "$name is already stopped."
            return $ownedOk
        }
        "Mismatch" {
            Write-Warning "Skipping $name PID $($identity.ProcessId); the PID has been reused."
            return $ownedOk
        }
        "Unverifiable" {
            Write-Warning "Skipping $name because its recorded identity is not verifiable: $($identity.Reason)."
            return $false
        }
    }

    Write-Step "Stopping $name (PID $($identity.ProcessId))."
    $rootOk = Stop-ProcessTree `
        -ProcessId $identity.ProcessId `
        -Visited $Visited `
        -ExpectedStartTicks $startTicksValue `
        -Label $name `
        -SkipChildEnumeration:$RecordedLeaf
    return $ownedOk -and $rootOk
}

function Stop-RecordedSupervisor {
    param(
        [Parameter(Mandatory = $true)][object] $Metadata,
        [Parameter(Mandatory = $true)][hashtable] $Visited
    )

    if ($null -eq $Metadata.SupervisorProcessId) {
        return $true
    }

    $identity = Get-ProcessIdentityStatus `
        -ProcessIdValue $Metadata.SupervisorProcessId `
        -StartTicksValue $Metadata.SupervisorStartTicks

    switch ($identity.Status) {
        "Absent" { return $true }
        "Mismatch" {
            Write-Warning "Skipping supervisor PID $($identity.ProcessId); the PID has been reused."
            return $true
        }
        "Unverifiable" {
            Write-Warning "Skipping the supervisor because its identity is not verifiable: $($identity.Reason)."
            return $false
        }
    }

    Write-Step "Stopping the foreground supervisor (PID $($identity.ProcessId))."
    return Stop-ProcessTree `
        -ProcessId $identity.ProcessId `
        -Visited $Visited `
        -ExpectedStartTicks $Metadata.SupervisorStartTicks `
        -Label "supervisor"
}

function Stop-OwnedDockerRedis {
    param([Parameter(Mandatory = $true)][object] $State)

    $redis = Get-ObjectPropertyValue -InputObject $State -Name "redis"
    if ($null -eq $redis) {
        return $true
    }

    $started = Get-ObjectPropertyValue -InputObject $redis -Name "started"
    if ($started -ne $true) {
        return $true
    }

    $containerName = [string] (Get-ObjectPropertyValue -InputObject $redis -Name "container")
    if ([string]::IsNullOrWhiteSpace($containerName)) {
        Write-Warning "Redis is marked as script-started, but its container name is missing."
        return $false
    }

    if ($KeepRedis) {
        Write-Step "Keeping Redis container '$containerName' running as requested."
        return $true
    }

    $dockerCommands = @(Get-Command "docker" -CommandType Application -ErrorAction SilentlyContinue)
    if ($dockerCommands.Count -eq 0) {
        Write-Warning "Docker was not found; Redis container '$containerName' was not stopped."
        return $false
    }

    $dockerPath = [string] $dockerCommands[0].Source
    Write-Step "Stopping Redis container '$containerName'."
    if ($DryRun) {
        Write-Host "  Would run: $dockerPath stop $containerName"
        return $true
    }

    & $dockerPath inspect $containerName *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Step "Redis container '$containerName' is already absent."
        return $true
    }

    $dockerOutput = @(& $dockerPath stop $containerName 2>&1 | ForEach-Object { [string] $_ })
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Docker could not stop Redis container '$containerName': $($dockerOutput -join ' ')"
        return $false
    }

    return $true
}

function Stop-OwnedWslDependencies {
    param([Parameter(Mandatory = $true)][object] $State)

    $wslState = Get-ObjectPropertyValue -InputObject $State -Name "wsl"
    if ($null -eq $wslState -or $wslState -eq $false) {
        return $true
    }

    if ($KeepWslDependencies) {
        Write-Step "Keeping WSL dependencies running as requested."
        return $true
    }

    if ($DryRun) {
        Write-Step "Would stop WSL dependencies recorded as started by start.ps1."
        return $true
    }

    try {
        $recordedAppName = [string] (Get-ObjectPropertyValue -InputObject $wslState -Name "appName")
        if ([string]::IsNullOrWhiteSpace($recordedAppName)) {
            $recordedAppName = "synq-meet-dev"
        }
        $recordedDistro = [string] (Get-ObjectPropertyValue -InputObject $wslState -Name "distro")
        if (-not [string]::IsNullOrWhiteSpace($WslDistro) -and -not [string]::Equals($WslDistro, $recordedDistro, [StringComparison]::OrdinalIgnoreCase)) {
            Write-Warning "Ignoring requested WSL distro '$WslDistro'; dependency ownership is recorded in '$recordedDistro'."
        }
        $result = Stop-MeetWslDependencies `
            -AppName $recordedAppName `
            -ServerRoot $ServerRoot `
            -Distro "" `
            -WaitSeconds $DependencyWaitSeconds

        if (-not [bool] $result.Ok) {
            Write-Warning "Some WSL dependencies may still be running."
            return $false
        }
    } catch {
        Write-Warning "Failed to stop WSL dependencies: $($_.Exception.Message)"
        return $false
    }

    return $true
}

function Get-CurrentMatchingState {
    param([Parameter(Mandatory = $true)][object] $SnapshotState)

    if (-not (Test-Path -LiteralPath $StatePath -PathType Leaf)) {
        return $null
    }

    $currentRecord = Read-StateFile
    if ($null -eq $currentRecord) {
        return $null
    }

    if (-not (Test-StateMatchesSnapshot -CurrentState $currentRecord.State -SnapshotState $SnapshotState)) {
        throw "The state file now belongs to a different run. Refusing to stop or remove resources from the newer run."
    }

    return $currentRecord.State
}

$initialStateRecord = Read-StateFile
if ($null -eq $initialStateRecord) {
    Write-Warning "No start state found at $StatePath. Nothing to stop."
    exit 0
}

$state = $initialStateRecord.State
$metadata = Get-StateMetadata -State $state
$stopRequestPath = Resolve-StopRequestPath -PathValue $metadata.StopRequestValue
$hadFailures = $false
$stateWasReplaced = $false

if ($null -ne $stopRequestPath -and $null -ne $metadata.SupervisorProcessId) {
    $supervisorIdentity = Get-ProcessIdentityStatus `
        -ProcessIdValue $metadata.SupervisorProcessId `
        -StartTicksValue $metadata.SupervisorStartTicks

    if ($supervisorIdentity.Status -eq "Match") {
        if ($DryRun) {
            Write-Step "Would request graceful shutdown from supervisor PID $($supervisorIdentity.ProcessId)."
            Write-Host "  Request: $stopRequestPath"
            Write-Host "  Would wait up to $GracefulWaitSeconds seconds before using recorded-process fallback."
        } else {
            Write-Step "Requesting graceful shutdown from supervisor PID $($supervisorIdentity.ProcessId)."
            Write-StopRequest -RequestPath $stopRequestPath -RunId $metadata.RunId

            $waitResult = Wait-ForSupervisorExit `
                -SnapshotState $state `
                -SupervisorProcessId $metadata.SupervisorProcessId `
                -SupervisorStartTicks $metadata.SupervisorStartTicks `
                -Seconds $GracefulWaitSeconds

            switch ($waitResult) {
                "Exited" {
                    Write-Step "The foreground supervisor exited. Verifying cleanup."
                }
                "TimedOut" {
                    Write-Warning "The supervisor did not exit within $GracefulWaitSeconds seconds; using safe fallback cleanup."
                }
                "IdentityLost" {
                    Write-Warning "The recorded supervisor identity changed while waiting; using recorded-service fallback only."
                }
                "StateReplaced" {
                    Write-Warning "A newer development run replaced the state file while shutdown was in progress; no fallback cleanup will target it."
                    $stateWasReplaced = $true
                    $hadFailures = $true
                }
            }
        }
    } elseif ($supervisorIdentity.Status -eq "Absent") {
        Write-Step "The recorded foreground supervisor is already stopped; using recorded cleanup."
    } else {
        Write-Warning "Cannot safely signal the recorded supervisor: $($supervisorIdentity.Reason). Using recorded-service fallback only."
    }
} else {
    Write-Step "No compatible foreground-supervisor control record was found; using recorded cleanup."
}

if (-not $stateWasReplaced) {
    try {
        [void] (Get-CurrentMatchingState -SnapshotState $state)
    } catch {
        Write-Warning $_.Exception.Message
        $stateWasReplaced = $true
        $hadFailures = $true
    }
}

if (-not $stateWasReplaced) {
    $visited = @{}
    $processes = @(Get-ObjectPropertyValue -InputObject $state -Name "processes")
    for ($index = $processes.Count - 1; $index -ge 0; $index -= 1) {
        if ($null -eq $processes[$index]) {
            continue
        }

        if (-not (Stop-RecordedProcess -Entry $processes[$index] -Visited $visited)) {
            $hadFailures = $true
        }
    }

    if (-not (Stop-RecordedSupervisor -Metadata $metadata -Visited $visited)) {
        $hadFailures = $true
    }

    # Re-check the state identity before touching shared dependencies. This
    # avoids stopping Redis/WSL resources that a newly-started run may reuse.
    try {
        [void] (Get-CurrentMatchingState -SnapshotState $state)
    } catch {
        Write-Warning $_.Exception.Message
        $stateWasReplaced = $true
        $hadFailures = $true
    }
}

if (-not $stateWasReplaced) {
    if (-not (Stop-OwnedDockerRedis -State $state)) {
        $hadFailures = $true
    }
    if (-not (Stop-OwnedWslDependencies -State $state)) {
        $hadFailures = $true
    }
}

if ($DryRun) {
    Write-Step "Dry run complete; no stop request was written and no resources or state files were changed."
    exit $(if ($hadFailures) { 1 } else { 0 })
}

if (-not $stateWasReplaced -and -not $hadFailures) {
    if (Test-Path -LiteralPath $StatePath -PathType Leaf) {
        try {
            $currentStateRecord = Read-StateFile
            if ($null -eq $currentStateRecord) {
                Write-Step "The supervisor already removed the completed run state."
            } elseif (Test-StateMatchesSnapshot -CurrentState $currentStateRecord.State -SnapshotState $state) {
                Remove-Item -LiteralPath $StatePath -Force
            } else {
                Write-Warning "The state file belongs to a newer run and was not removed."
                $hadFailures = $true
            }
        } catch {
            Write-Warning "Could not safely remove the state file: $($_.Exception.Message)"
            $hadFailures = $true
        }
    }

    if (-not $hadFailures -and $null -ne $stopRequestPath) {
        Remove-Item -LiteralPath $stopRequestPath -Force -ErrorAction SilentlyContinue
    }
}

if ($hadFailures) {
    Write-Warning "Shutdown was not fully confirmed. State was retained when possible so stop.ps1 can be retried."
    exit 1
}

Write-Step "Stopped services recorded in $StatePath."
exit 0
