#Requires -Version 5.1
<#
.SYNOPSIS
Runs the complete Synq Meet development stack in one supervised console.

.DESCRIPTION
Starts or reuses Redis and Janus, applies migrations, and runs Django, Celery,
Celery Beat, and Next.js. Every managed stdout/stderr line is multiplexed into
one color-labelled console and one combined plain-text log. The script remains
in the foreground so Ctrl+C can stop the owned stack cleanly.
#>

[CmdletBinding()]
param(
    [ValidateRange(1, 65535)]
    [int] $BackendPort = 8000,

    [ValidateRange(1, 65535)]
    [int] $FrontendPort = 3000,

    [ValidateRange(1, 65535)]
    [int] $RedisPort = 6379,

    [ValidateRange(1, 65535)]
    [int] $JanusWsPort = 8188,

    [ValidateRange(1, 65535)]
    [int] $JanusHttpPort = 8088,

    [string] $RedisContainerName = "synq-meet-redis",
    [switch] $NoRedis,
    [switch] $NoJanus,
    [switch] $NoMigrate,
    [switch] $NoWorker,
    [switch] $NoBeat,
    [switch] $NoFrontend,

    [ValidateSet("Auto", "Docker", "Wsl", "None")]
    [string] $DependencyMode = "Auto",

    [AllowEmptyString()]
    [string] $WslDistro = $env:MEET_WSL_DISTRO,

    [AllowEmptyString()]
    [string] $WslDependencies = $(if ([string]::IsNullOrWhiteSpace($env:MEET_WSL_DEPENDENCIES)) { "redis,janus" } else { $env:MEET_WSL_DEPENDENCIES }),

    [ValidateRange(1, 120)]
    [int] $DependencyWaitSeconds = 30,

    [ValidateRange(5, 300)]
    [int] $StartupWaitSeconds = 60,

    [switch] $SkipDependencyEnvironment,
    [switch] $Reload,
    [switch] $Visible,
    [switch] $NoCombinedLog,
    [switch] $DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerRoot = Split-Path -Parent $ScriptDir
$MeetRoot = Split-Path -Parent $ServerRoot
$ServerSrc = Join-Path $ServerRoot "src"
$WebRoot = Join-Path $MeetRoot "web"
$WebApp = Join-Path $WebRoot "app"
$RuntimeRoot = Join-Path $env:TEMP "synq-meet-dev"
$StatePath = Join-Path $RuntimeRoot "processes.json"
$LockPath = Join-Path $RuntimeRoot "supervisor.lock"
$RunId = [Guid]::NewGuid().ToString("N")
$WslAppName = "synq-meet-dev-$RunId"
$StopRequestPath = Join-Path $RuntimeRoot "stop-$RunId.json"
$CombinedLogPath = Join-Path $RuntimeRoot ("stack-{0:yyyyMMdd-HHmmss}.log" -f (Get-Date))
$WslHelperPath = Join-Path $ScriptDir "wsl-dependencies.ps1"
$ManagedPythonPath = Join-Path $ScriptDir "managed-python.py"
$DjangoIdentityPath = Join-Path $RuntimeRoot "django-$RunId.json"
$CeleryWorkerIdentityPath = Join-Path $RuntimeRoot "celery-worker-$RunId.json"
$CeleryBeatIdentityPath = Join-Path $RuntimeRoot "celery-beat-$RunId.json"

$script:Processes = @()
$script:LogWriter = $null
$script:LockStream = $null
$script:ExitCode = 0
$script:KeepRedisOnExit = $false
$script:KeepWslOnExit = $false
$script:LastLogFlush = [DateTime]::UtcNow
$escapedEscape = [Regex]::Escape([string]([char] 27))
$script:AnsiPattern = "$escapedEscape(?:\][^\x07]*(?:\x07|$escapedEscape\\)|\[[0-?]*[ -/]*[@-~]|[@-_])"
$script:ControlPattern = "[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]"

function Write-StackLog {
    param(
        [Parameter(Mandatory = $true)][string] $Label,
        [Parameter(Mandatory = $true)][ConsoleColor] $Color,
        [AllowNull()][object] $Message,
        [ValidateSet("OUT", "ERR", "SYS")][string] $Stream = "SYS"
    )

    $text = if ($null -eq $Message) { "" } else { [string] $Message }
    $text = ($text -replace $script:AnsiPattern, "") -replace $script:ControlPattern, ""
    $text = $text.TrimEnd([char] 13)
    $timestamp = Get-Date -Format "HH:mm:ss.fff"
    $displayLabel = $Label.ToUpperInvariant().PadRight(7)
    $prefix = "$timestamp [$displayLabel]"

    Write-Host $prefix -NoNewline -ForegroundColor $Color
    Write-Host " $text"

    if ($null -ne $script:LogWriter) {
        $script:LogWriter.WriteLine("$timestamp [$displayLabel] [$Stream] $text")
    }
}

function Flush-CombinedLog {
    if ($null -ne $script:LogWriter -and ([DateTime]::UtcNow - $script:LastLogFlush).TotalMilliseconds -ge 250) {
        $script:LogWriter.Flush()
        $script:LastLogFlush = [DateTime]::UtcNow
    }
}

function Resolve-CommandPath {
    param([Parameter(Mandatory = $true)][string] $Name)

    $command = Get-Command $Name -All -ErrorAction SilentlyContinue | Where-Object { $_.CommandType -eq "Application" } | Select-Object -First 1
    if ($null -eq $command) {
        $command = Get-Command $Name -ErrorAction SilentlyContinue
    }
    if ($null -eq $command) {
        throw "Required command '$Name' was not found on PATH."
    }
    return [string] $command.Source
}

function Import-DotEnv {
    param([Parameter(Mandatory = $true)][string] $Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
            continue
        }
        if ($trimmed.StartsWith("export ")) {
            $trimmed = $trimmed.Substring(7).Trim()
        }

        $parts = $trimmed -split "=", 2
        if ($parts.Count -ne 2) {
            continue
        }
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($name -notmatch "^[A-Za-z_][A-Za-z0-9_]*$") {
            continue
        }
        if ($value.Length -ge 2 -and (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'")))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        if ($null -eq [Environment]::GetEnvironmentVariable($name, "Process")) {
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

function Set-DefaultEnvironment {
    param([string] $Name, [string] $Value)
    if ($null -eq [Environment]::GetEnvironmentVariable($Name, "Process")) {
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

function Set-ProcessEnvironment {
    param([string] $Name, [string] $Value)
    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
}

function Use-Ipv4LoopbackForLocalDependency {
    param([Parameter(Mandatory = $true)][string] $Name)

    $value = [Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($value)) { return }
    try {
        $uri = [Uri] $value
        if ($uri.Host -ne "localhost") { return }
        $builder = New-Object System.UriBuilder($uri)
        $builder.Host = "127.0.0.1"
        Set-ProcessEnvironment $Name $builder.Uri.AbsoluteUri
        Write-StackLog -Label "SYSTEM" -Color DarkGray -Message "$Name uses IPv4 loopback for WSL-hosted service compatibility."
    } catch {
        throw "$Name must be an absolute endpoint URL; received '$value'."
    }
}

function Test-TcpEndpoint {
    param([string] $HostName, [int] $Port, [int] $TimeoutMilliseconds = 750)

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $task = $client.ConnectAsync($HostName, $Port)
        return $task.Wait($TimeoutMilliseconds) -and $client.Connected
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Assert-LocalTcpPortAvailable {
    param(
        [Parameter(Mandatory = $true)][string] $ServiceName,
        [Parameter(Mandatory = $true)][int] $Port,
        [Parameter(Mandatory = $true)][string] $PortOption
    )

    $listener = New-Object System.Net.Sockets.TcpListener([Net.IPAddress]::Loopback, $Port)
    try {
        # An exclusive bind detects listeners even when they are not yet
        # accepting connections, which an HTTP/TCP readiness probe cannot.
        $listener.Server.ExclusiveAddressUse = $true
        $listener.Start()
    } catch [Net.Sockets.SocketException] {
        if ($_.Exception.SocketErrorCode -eq [Net.Sockets.SocketError]::AddressAlreadyInUse) {
            throw "$ServiceName cannot start because 127.0.0.1:$Port is already in use. Stop the existing listener or choose another port with -$PortOption."
        }
        throw
    } finally {
        $listener.Stop()
    }
}

function Test-TcpPortOwnedByProcess {
    param(
        [Parameter(Mandatory = $true)][string] $Address,
        [Parameter(Mandatory = $true)][int] $Port,
        [Parameter(Mandatory = $true)][int] $ProcessId
    )

    $netstatPath = Join-Path $env:SystemRoot "System32\netstat.exe"
    if (-not (Test-Path -LiteralPath $netstatPath -PathType Leaf)) {
        return $false
    }

    $endpointPattern = [Regex]::Escape("${Address}:$Port")
    $processPattern = [Regex]::Escape([string] $ProcessId)
    try {
        foreach ($line in @(& $netstatPath -ano -p tcp 2>$null)) {
            # Requiring both the local endpoint and exact PID ties readiness
            # to the runtime this supervisor launched, not a stale server.
            if ([string] $line -match "^\s*TCP\s+$endpointPattern\s+\S+\s+\S+\s+$processPattern\s*$") {
                return $true
            }
        }
    } catch {
        return $false
    }
    return $false
}

function ConvertTo-RedisRequest {
    param([Parameter(Mandatory = $true)][string[]] $Parts)

    $crlf = [string]([char] 13) + [string]([char] 10)
    $builder = New-Object System.Text.StringBuilder
    [void] $builder.Append("*$($Parts.Count)$crlf")
    foreach ($part in $Parts) {
        $length = [Text.Encoding]::UTF8.GetByteCount($part)
        [void] $builder.Append('$')
        [void] $builder.Append($length)
        [void] $builder.Append($crlf)
        [void] $builder.Append($part)
        [void] $builder.Append($crlf)
    }
    return $builder.ToString()
}

function Test-RedisReady {
    param([Parameter(Mandatory = $true)][Uri] $Uri)

    $client = New-Object System.Net.Sockets.TcpClient
    $stream = $null
    $reader = $null
    try {
        $connectTask = $client.ConnectAsync($Uri.Host, $Uri.Port)
        if (-not $connectTask.Wait(1000) -or -not $client.Connected) {
            return $false
        }
        $client.ReceiveTimeout = 1500
        $client.SendTimeout = 1500
        $stream = $client.GetStream()
        if ($Uri.Scheme -eq "rediss") {
            $sslStream = New-Object System.Net.Security.SslStream($stream, $false)
            $sslStream.AuthenticateAsClient($Uri.Host)
            $stream = $sslStream
        }
        $reader = New-Object System.IO.StreamReader($stream, [Text.Encoding]::UTF8, $false, 1024, $true)

        if (-not [string]::IsNullOrWhiteSpace($Uri.UserInfo)) {
            $credentials = $Uri.UserInfo -split ":", 2
            if ($credentials.Count -eq 2) {
                $username = [Uri]::UnescapeDataString($credentials[0])
                $password = [Uri]::UnescapeDataString($credentials[1])
                $authParts = if ([string]::IsNullOrWhiteSpace($username)) { @("AUTH", $password) } else { @("AUTH", $username, $password) }
                $authBytes = [Text.Encoding]::UTF8.GetBytes((ConvertTo-RedisRequest -Parts $authParts))
                $stream.Write($authBytes, 0, $authBytes.Length)
                $stream.Flush()
                if (($reader.ReadLine()) -notlike "+OK*") {
                    return $false
                }
            }
        }

        $pingBytes = [Text.Encoding]::UTF8.GetBytes((ConvertTo-RedisRequest -Parts @("PING")))
        $stream.Write($pingBytes, 0, $pingBytes.Length)
        $stream.Flush()
        return ($reader.ReadLine()) -eq "+PONG"
    } catch {
        return $false
    } finally {
        if ($null -ne $reader) { $reader.Dispose() }
        elseif ($null -ne $stream) { $stream.Dispose() }
        $client.Dispose()
    }
}

function Test-JanusReady {
    param([Parameter(Mandatory = $true)][Uri] $Uri)

    if ($Uri.Scheme -in @("ws", "wss")) {
        $socket = New-Object System.Net.WebSockets.ClientWebSocket
        $cancellation = New-Object System.Threading.CancellationTokenSource
        try {
            $socket.Options.AddSubProtocol("janus-protocol")
            $cancellation.CancelAfter(2000)
            $task = $socket.ConnectAsync($Uri, $cancellation.Token)
            [void] $task.GetAwaiter().GetResult()
            return $socket.State -eq [Net.WebSockets.WebSocketState]::Open
        } catch {
            return $false
        } finally {
            $cancellation.Dispose()
            $socket.Dispose()
        }
    }

    if ($Uri.Scheme -in @("http", "https")) {
        try {
            $infoBuilder = New-Object System.UriBuilder($Uri)
            $infoBuilder.Path = $infoBuilder.Path.TrimEnd("/") + "/info"
            $request = [Net.HttpWebRequest]::Create($infoBuilder.Uri)
            $request.Method = "GET"
            $request.Timeout = 2000
            $response = $request.GetResponse()
            $response.Dispose()
            return $true
        } catch {
            return $false
        }
    }

    return $false
}

function Test-LocalHost {
    param([Parameter(Mandatory = $true)][string] $HostName)

    if ($HostName -in @("localhost", "127.0.0.1", "::1", "0.0.0.0")) {
        return $true
    }
    $address = $null
    if ([Net.IPAddress]::TryParse($HostName, [ref] $address)) {
        return [Net.IPAddress]::IsLoopback($address)
    }
    return $false
}

function ConvertTo-NativeArgument {
    param([AllowEmptyString()][string] $Value)

    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') {
        return $Value
    }

    $builder = New-Object System.Text.StringBuilder
    [void] $builder.Append('"')
    $slashCount = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq [char] 92) {
            $slashCount += 1
            continue
        }
        if ($character -eq [char] 34) {
            for ($index = 0; $index -lt (($slashCount * 2) + 1); $index += 1) { [void] $builder.Append([char] 92) }
            [void] $builder.Append([char] 34)
            $slashCount = 0
            continue
        }
        for ($index = 0; $index -lt $slashCount; $index += 1) { [void] $builder.Append([char] 92) }
        $slashCount = 0
        [void] $builder.Append($character)
    }
    for ($index = 0; $index -lt ($slashCount * 2); $index += 1) { [void] $builder.Append([char] 92) }
    [void] $builder.Append('"')
    return $builder.ToString()
}

function Start-LoggedProcess {
    param(
        [string] $Name,
        [string] $Label,
        [ConsoleColor] $Color,
        [string] $FilePath,
        [string[]] $Arguments,
        [string] $WorkingDirectory,
        [bool] $Critical = $true,
        [bool] $Persist = $true
    )

    $commandText = ((@($FilePath) + $Arguments) | ForEach-Object { ConvertTo-NativeArgument ([string] $_) }) -join " "
    Write-StackLog -Label "SYSTEM" -Color Gray -Message "Starting ${Name}: $commandText"
    if ($DryRun) {
        Write-StackLog -Label $Label -Color $Color -Message "dry-run cwd=$WorkingDirectory" -Stream "SYS"
        return $null
    }

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $extension = [IO.Path]::GetExtension($FilePath).ToLowerInvariant()
    if ($extension -in @(".cmd", ".bat")) {
        $startInfo.FileName = $env:ComSpec
        $startInfo.Arguments = '/d /s /c "' + $commandText + '"'
    } else {
        $startInfo.FileName = $FilePath
        if ($startInfo.PSObject.Properties.Name -contains "ArgumentList") {
            foreach ($argument in $Arguments) {
                [void] $startInfo.ArgumentList.Add([string] $argument)
            }
        } else {
            $startInfo.Arguments = ($Arguments | ForEach-Object { ConvertTo-NativeArgument ([string] $_) }) -join " "
        }
    }
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = -not $Visible
    $startInfo.EnvironmentVariables["NO_COLOR"] = "1"
    $startInfo.EnvironmentVariables["PY_COLORS"] = "0"
    try {
        $utf8 = New-Object System.Text.UTF8Encoding($false)
        $startInfo.StandardOutputEncoding = $utf8
        $startInfo.StandardErrorEncoding = $utf8
    } catch {
        # Encoding setters are unavailable on some Windows PowerShell runtimes.
    }

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw "Could not start $Name."
    }
    $startTicks = $process.StartTime.ToUniversalTime().Ticks
    $entry = [pscustomobject] @{
        Name = $Name
        Label = $Label
        Color = $Color
        Process = $process
        Pid = $process.Id
        StartTicks = $startTicks
        Command = $commandText
        WorkingDirectory = $WorkingDirectory
        Critical = $Critical
        Persist = $Persist
        ExpectedExit = $false
        ExitReported = $false
        OwnedProcesses = @()
        StdoutTask = $process.StandardOutput.ReadLineAsync()
        StderrTask = $process.StandardError.ReadLineAsync()
        StdoutClosed = $false
        StderrClosed = $false
    }
    $script:Processes += $entry
    return $entry
}

function Wait-ForManagedProcessIdentity {
    param(
        [Parameter(Mandatory = $true)][object] $Entry,
        [Parameter(Mandatory = $true)][string] $IdentityPath,
        [Parameter(Mandatory = $true)][string] $RunToken,
        [Parameter(Mandatory = $true)][int] $Seconds
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        Pump-AllLogs
        if ($Entry.Process.HasExited) {
            throw "$($Entry.Name) exited before publishing its runtime identity."
        }
        if (Test-Path -LiteralPath $IdentityPath -PathType Leaf) {
            try {
                $identity = Get-Content -LiteralPath $IdentityPath -Raw | ConvertFrom-Json
                if ([string] $identity.runToken -ne $RunToken) {
                    throw "runtime identity token mismatch"
                }
                $processId = [int] $identity.pid
                $process = Get-Process -Id $processId -ErrorAction Stop
                $startTicks = [int64] $process.StartTime.ToUniversalTime().Ticks
                if ($startTicks -lt [int64] $Entry.StartTicks) {
                    throw "runtime identity predates its launcher"
                }
                $record = [pscustomobject] @{
                    Name = "$($Entry.Name) runtime"
                    Pid = $processId
                    StartTicks = $startTicks
                    Process = $process
                }
                $Entry.OwnedProcesses = @($record)
                return $record
            } catch {
                throw "Could not verify $($Entry.Name) runtime identity: $($_.Exception.Message)"
            }
        }
        Start-Sleep -Milliseconds 100
    }
    throw "$($Entry.Name) did not publish its runtime identity within $Seconds seconds."
}

function Test-ManagedProcessIdentity {
    param([Parameter(Mandatory = $true)][object] $Record)

    $process = Get-Process -Id ([int] $Record.Pid) -ErrorAction SilentlyContinue
    if ($null -eq $process) { return $false }
    try {
        return [int64] $process.StartTime.ToUniversalTime().Ticks -eq [int64] $Record.StartTicks
    } catch {
        return $false
    }
}

function Pump-ProcessStream {
    param(
        [Parameter(Mandatory = $true)][object] $Entry,
        [ValidateSet("OUT", "ERR")][string] $Stream
    )

    $taskProperty = if ($Stream -eq "OUT") { "StdoutTask" } else { "StderrTask" }
    $closedProperty = if ($Stream -eq "OUT") { "StdoutClosed" } else { "StderrClosed" }
    $reader = if ($Stream -eq "OUT") { $Entry.Process.StandardOutput } else { $Entry.Process.StandardError }

    while (-not [bool] $Entry.$closedProperty -and $null -ne $Entry.$taskProperty -and $Entry.$taskProperty.IsCompleted) {
        try {
            $line = $Entry.$taskProperty.GetAwaiter().GetResult()
        } catch {
            Write-StackLog -Label $Entry.Label -Color $Entry.Color -Message "log stream failed: $($_.Exception.Message)" -Stream $Stream
            $Entry.$closedProperty = $true
            break
        }
        if ($null -eq $line) {
            $Entry.$closedProperty = $true
            $Entry.$taskProperty = $null
            break
        }
        Write-StackLog -Label $Entry.Label -Color $Entry.Color -Message $line -Stream $Stream
        $Entry.$taskProperty = $reader.ReadLineAsync()
    }
}

function Pump-AllLogs {
    foreach ($entry in $script:Processes) {
        if ($null -eq $entry.Process) { continue }
        Pump-ProcessStream -Entry $entry -Stream "OUT"
        Pump-ProcessStream -Entry $entry -Stream "ERR"
    }
    Flush-CombinedLog
}

function Wait-ForLoggedProcess {
    param([Parameter(Mandatory = $true)][object] $Entry)

    while (-not $Entry.Process.HasExited) {
        Pump-AllLogs
        Start-Sleep -Milliseconds 25
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(2)
    while ((-not $Entry.StdoutClosed -or -not $Entry.StderrClosed) -and [DateTime]::UtcNow -lt $deadline) {
        Pump-AllLogs
        Start-Sleep -Milliseconds 10
    }
    Pump-AllLogs
    return $Entry.Process.ExitCode
}

function Invoke-LoggedCommand {
    param(
        [string] $Name,
        [string] $Label,
        [ConsoleColor] $Color,
        [string] $FilePath,
        [string[]] $Arguments,
        [string] $WorkingDirectory
    )

    $entry = Start-LoggedProcess -Name $Name -Label $Label -Color $Color -FilePath $FilePath -Arguments $Arguments -WorkingDirectory $WorkingDirectory -Critical $false -Persist $false
    if ($DryRun) { return }
    $exitCode = Wait-ForLoggedProcess -Entry $entry
    $entry.ExpectedExit = $true
    if ($exitCode -ne 0) {
        throw "$Name failed with exit code $exitCode."
    }
}

function Wait-ForProbe {
    param(
        [string] $Name,
        [scriptblock] $Probe,
        [int] $Seconds
    )

    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        Pump-AllLogs
        foreach ($entry in $script:Processes) {
            if ($entry.Critical -and -not $entry.ExpectedExit -and $entry.Process.HasExited) {
                $entry.ExitReported = $true
                $script:ExitCode = if ($entry.Process.ExitCode -eq 0) { 1 } else { $entry.Process.ExitCode }
                Write-StackLog -Label "SYSTEM" -Color Red -Message "$($entry.Name) exited during startup with code $($entry.Process.ExitCode)." -Stream "ERR"
                return $false
            }
        }
        if (& $Probe) {
            Write-StackLog -Label "SYSTEM" -Color Gray -Message "$Name is ready."
            return $true
        }
        Start-Sleep -Milliseconds 250
    }
    Write-StackLog -Label "SYSTEM" -Color Red -Message "$Name was not ready within $Seconds seconds." -Stream "ERR"
    return $false
}

function Test-HttpReady {
    param([string] $Url)
    try {
        $request = [Net.HttpWebRequest]::Create($Url)
        $request.Method = "GET"
        $request.Timeout = 1000
        $response = $request.GetResponse()
        $status = [int] $response.StatusCode
        $response.Dispose()
        return $status -lt 500
    } catch {
        $responseProperty = $_.Exception.PSObject.Properties["Response"]
        if ($null -ne $responseProperty -and $null -ne $responseProperty.Value) {
            return ([int] $responseProperty.Value.StatusCode) -lt 500
        }
        return $false
    }
}

function Ensure-DockerRedis {
    param([AllowNull()][string] $DockerPath, [int] $Port)

    if ([string]::IsNullOrWhiteSpace($DockerPath)) {
        throw "Redis is not running and Docker was not found. Use WSL mode or start Redis externally."
    }
    if ($DryRun) {
        Write-StackLog -Label "REDIS" -Color Red -Message "would ensure Docker container '$RedisContainerName' on port $Port"
        return @{ started = $true; container = $RedisContainerName }
    }

    & $DockerPath inspect $RedisContainerName *> $null
    $exists = $LASTEXITCODE -eq 0
    if ($exists) {
        $running = (& $DockerPath inspect -f "{{.State.Running}}" $RedisContainerName 2>$null).Trim()
        if ($running -ne "true") {
            Write-StackLog -Label "REDIS" -Color Red -Message "starting existing Docker container '$RedisContainerName'"
            & $DockerPath start $RedisContainerName | Out-Null
            return @{ started = $true; container = $RedisContainerName }
        }
        return @{ started = $false; container = $RedisContainerName }
    }

    Write-StackLog -Label "REDIS" -Color Red -Message "creating Docker container '$RedisContainerName'"
    & $DockerPath run -d --name $RedisContainerName -p "$($Port):6379" redis:7 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker could not create Redis container '$RedisContainerName'."
    }
    return @{ started = $true; container = $RedisContainerName }
}

function Start-WslLogFollowers {
    param([AllowNull()][object] $State, [string] $WslPath)

    if ($null -eq $State -or $null -eq $State.dependencies) { return }
    foreach ($record in @($State.dependencies)) {
        if ($record.name -notin @("redis", "janus") -or $record.status -notin @("started", "already")) { continue }
        $name = [string] $record.name
        $label = $name.ToUpperInvariant()
        $color = if ($name -eq "redis") { [ConsoleColor]::Red } else { [ConsoleColor]::Magenta }
        $logPath = "$($State.stateDir)/$name.log"
        $unitArguments = if ($name -eq "redis") { "-u redis-server -u redis" } else { "-u janus -u janus-gateway" }
        if ($record.method -in @("systemd", "service") -and -not [string]::IsNullOrWhiteSpace([string] $record.serviceName)) {
            $unitArguments = "-u " + (ConvertTo-BashSingleQuotedLiteral -Value ([string] $record.serviceName))
        }
        $bash = "log_file=$(ConvertTo-BashSingleQuotedLiteral -Value $logPath); " +
            'if [ -f "$log_file" ]; then echo "following $log_file"; exec tail -n 40 -F "$log_file"; fi; ' +
            "if command -v journalctl >/dev/null 2>&1; then echo 'following system journal'; exec journalctl --no-pager -n 40 -f -o cat $unitArguments; fi; " +
            "echo 'No accessible runtime log stream was found.'"
        $arguments = @()
        if (-not [string]::IsNullOrWhiteSpace($WslDistro)) {
            $arguments += @("-d", $WslDistro)
        }
        $arguments += @("--exec", "bash", "-lc", $bash)
        [void] (Start-LoggedProcess -Name "$name logs" -Label $label -Color $color -FilePath $WslPath -Arguments $arguments -WorkingDirectory $ServerRoot -Critical $false -Persist $true)
    }
}

function Stop-ProcessTree {
    param(
        [int] $ProcessId,
        [AllowNull()][object] $ExpectedStartTicks,
        [switch] $SkipChildEnumeration
    )

    if ($ProcessId -le 0) { return }
    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($null -eq $process) { return }
    if ($null -ne $ExpectedStartTicks) {
        try {
            if ([int64] $ExpectedStartTicks -ne [int64] $process.StartTime.ToUniversalTime().Ticks) {
                Write-StackLog -Label "SYSTEM" -Color Yellow -Message "Skipping reused PID $ProcessId."
                return
            }
        } catch {
            return
        }
    }

    if (-not $SkipChildEnumeration) {
        $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue)
        foreach ($child in $children) {
            Stop-ProcessTree -ProcessId ([int] $child.ProcessId) -ExpectedStartTicks $null
        }
    }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Save-SupervisorState {
    param([object] $RedisState, [object] $WslState, [string] $EffectiveMode)

    $records = @()
    foreach ($entry in $script:Processes) {
        if (-not $entry.Persist -or $entry.Pid -le 0) { continue }
        $records += [ordered] @{
            name = $entry.Name
            label = $entry.Label
            pid = $entry.Pid
            startTicks = $entry.StartTicks
            command = $entry.Command
            cwd = $entry.WorkingDirectory
            critical = $entry.Critical
            ownedProcesses = @($entry.OwnedProcesses | ForEach-Object {
                [ordered] @{
                    name = $_.Name
                    pid = $_.Pid
                    startTicks = $_.StartTicks
                }
            })
        }
    }
    $supervisorStartTicks = (Get-Process -Id $PID).StartTime.ToUniversalTime().Ticks
    $state = [ordered] @{
        stateVersion = 3
        runId = $RunId
        startedAt = (Get-Date).ToString("o")
        supervisorPid = $PID
        supervisorStartTicks = $supervisorStartTicks
        stopRequestPath = $StopRequestPath
        supervisor = @{ pid = $PID; startTicks = $supervisorStartTicks }
        control = @{ stopRequestFile = $StopRequestPath }
        dependencyMode = $EffectiveMode
        wslAppName = $WslAppName
        backendPort = $BackendPort
        frontendPort = $FrontendPort
        combinedLog = $(if ($NoCombinedLog) { $null } else { $CombinedLogPath })
        redis = $RedisState
        wsl = $WslState
        processes = $records
    }
    $temporaryPath = "$StatePath.$RunId.tmp"
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($temporaryPath, ($state | ConvertTo-Json -Depth 8), $encoding)
    Move-Item -LiteralPath $temporaryPath -Destination $StatePath -Force
}

if (-not (Test-Path -LiteralPath $ServerSrc -PathType Container)) {
    throw "Cannot find backend src directory at '$ServerSrc'."
}
if (-not (Test-Path -LiteralPath $WebRoot -PathType Container)) {
    throw "Cannot find frontend workspace at '$WebRoot'."
}
if (-not (Test-Path -LiteralPath $WslHelperPath -PathType Leaf)) {
    throw "Cannot find WSL helper at '$WslHelperPath'."
}
if (-not (Test-Path -LiteralPath $ManagedPythonPath -PathType Leaf)) {
    throw "Cannot find managed Python launcher at '$ManagedPythonPath'."
}

. $WslHelperPath

New-Item -ItemType Directory -Path $RuntimeRoot -Force | Out-Null
if (-not $DryRun) {
    try {
        $script:LockStream = [IO.File]::Open($LockPath, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    } catch {
        throw "Another Synq Meet supervisor appears to be running. Use scripts\stop.ps1 first."
    }
    if (Test-Path -LiteralPath $StatePath -PathType Leaf) {
        throw "A prior state file exists at '$StatePath'. Run scripts\stop.ps1 before starting another stack."
    }
    if (-not $NoCombinedLog) {
        $encoding = New-Object System.Text.UTF8Encoding($false)
        $script:LogWriter = New-Object System.IO.StreamWriter($CombinedLogPath, $false, $encoding, 65536)
    }
}

$redisState = @{ started = $false; container = $null }
$wslState = $null
$wslStartupAttempted = $false
$dependencyCleanupOk = $true
$effectiveDependencyMode = $DependencyMode
$uv = $null
$npm = $null
$docker = $null
$wsl = $null

try {
    Import-DotEnv (Join-Path $ServerRoot ".env")
    Import-DotEnv (Join-Path $WebApp ".env.local")
    Import-DotEnv (Join-Path $WebApp ".env")

    $redisUrl = "redis://127.0.0.1:$RedisPort/1"
    if ($PSBoundParameters.ContainsKey("RedisPort")) {
        foreach ($name in @("REDIS_URL", "CELERY_BROKER_URL", "SOCKET_IO_REDIS_URL", "SOCKETIO_REDIS_URL")) {
            Set-ProcessEnvironment $name $redisUrl
        }
    } else {
        Set-DefaultEnvironment "REDIS_URL" $redisUrl
        Set-DefaultEnvironment "CELERY_BROKER_URL" $redisUrl
        Set-DefaultEnvironment "SOCKET_IO_REDIS_URL" $redisUrl
        Set-DefaultEnvironment "SOCKETIO_REDIS_URL" $redisUrl
    }
    if ($PSBoundParameters.ContainsKey("JanusWsPort")) {
        Set-ProcessEnvironment "JANUS_SESSION_URL" "ws://127.0.0.1:$JanusWsPort/janus"
    } else {
        Set-DefaultEnvironment "JANUS_SESSION_URL" "ws://127.0.0.1:$JanusWsPort/janus"
    }
    foreach ($endpointName in @("REDIS_URL", "CELERY_BROKER_URL", "SOCKET_IO_REDIS_URL", "SOCKETIO_REDIS_URL", "JANUS_SESSION_URL")) {
        Use-Ipv4LoopbackForLocalDependency $endpointName
    }
    $configuredRedisUri = [Uri] $env:REDIS_URL
    if ($configuredRedisUri.Port -le 0) {
        $redisBuilder = New-Object System.UriBuilder($configuredRedisUri)
        $redisBuilder.Port = $RedisPort
        Set-ProcessEnvironment "REDIS_URL" $redisBuilder.Uri.AbsoluteUri
    }
    Set-DefaultEnvironment "JANUS_PUBLIC_WS_URL" "ws://localhost:$JanusWsPort/janus"
    Set-DefaultEnvironment "JANUS_PUBLIC_HTTP_URL" "http://localhost:$JanusHttpPort/janus"
    Set-DefaultEnvironment "JANUS_STARTUP_FAIL_FAST" "true"
    Set-ProcessEnvironment "MEET_WSL_REDIS_PORT" ([Uri] $env:REDIS_URL).Port
    Set-ProcessEnvironment "MEET_WSL_JANUS_WS_PORT" ([Uri] $env:JANUS_SESSION_URL).Port
    Set-ProcessEnvironment "MEET_WSL_JANUS_HTTP_PORT" $JanusHttpPort
    Set-ProcessEnvironment "SYNQ_BACKEND_URL" "http://localhost:$BackendPort"
    Set-ProcessEnvironment "NEXT_PUBLIC_SYNQ_BACKEND_URL" "http://localhost:$BackendPort"
    Set-ProcessEnvironment "NEXT_PUBLIC_APP_URL" "http://localhost:$FrontendPort"
    Set-ProcessEnvironment "MEETING_FRONTEND_BASE_URL" "http://localhost:$FrontendPort"
    Set-DefaultEnvironment "NEXT_PUBLIC_SYNQ_SOCKET_PATH" "/socket.io"
    Set-DefaultEnvironment "NEXT_PUBLIC_SYNQ_SOCKET_NAMESPACE" "/meetings"

    $uv = Resolve-CommandPath "uv"
    if (-not $NoFrontend) { $npm = Resolve-CommandPath "bun" }
    $dockerCommand = Get-Command "docker" -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $dockerCommand) { $docker = [string] $dockerCommand.Source }
    if (Test-WslAvailable) { $wsl = Resolve-WslExecutable }

    $redisUri = [Uri] $env:REDIS_URL
    $janusUri = [Uri] $env:JANUS_SESSION_URL
    $redisReady = $NoRedis -or (Test-RedisReady -Uri $redisUri)
    $janusReady = $NoJanus -or (Test-JanusReady -Uri $janusUri)
    if (-not $redisReady -and -not (Test-LocalHost $redisUri.Host)) {
        throw "Configured Redis endpoint $($redisUri.Host):$($redisUri.Port) is unreachable; refusing to start an unrelated local Redis."
    }
    if (-not $janusReady -and -not (Test-LocalHost $janusUri.Host)) {
        throw "Configured Janus endpoint $($janusUri.Host):$($janusUri.Port) is unreachable; refusing to start an unrelated local Janus."
    }

    $neededDependencies = @()
    if (-not $redisReady) { $neededDependencies += "redis" }
    if (-not $janusReady) { $neededDependencies += "janus" }
    $allowedDependencies = ConvertTo-DependencyList -Dependencies $WslDependencies
    foreach ($needed in $neededDependencies) {
        if ($allowedDependencies -notcontains $needed) {
            throw "$needed is unavailable but excluded by WslDependencies. Start it externally or include it."
        }
    }

    if ($effectiveDependencyMode -eq "Auto") {
        if ($neededDependencies.Count -eq 0) { $effectiveDependencyMode = "Existing" }
        elseif ($null -ne $wsl) { $effectiveDependencyMode = "Wsl" }
        else { $effectiveDependencyMode = "Docker" }
    }

    if ($DryRun) {
        Write-StackLog -Label "SYSTEM" -Color Gray -Message "dry-run dependency mode=$effectiveDependencyMode needed=$($neededDependencies -join ',')"
    } elseif ($neededDependencies.Count -gt 0) {
        switch ($effectiveDependencyMode) {
            "Wsl" {
                if ($null -eq $wsl) { throw "WSL is unavailable." }
                if (-not $SkipDependencyEnvironment) {
                    Set-MeetLocalDependencyEnvironment -Dependencies $neededDependencies
                }
                $startParameters = @{
                    AppName = $WslAppName
                    ServerRoot = $ServerRoot
                    Dependencies = $neededDependencies
                    Distro = $WslDistro
                    WaitSeconds = $DependencyWaitSeconds
                }
                $wslStartupAttempted = $true
                $wslState = Start-MeetWslDependencies @startParameters
            }
            "Docker" {
                if ($neededDependencies -contains "janus") {
                    throw "Docker mode has no repository-defined local Janus image. Use WSL mode or start Janus externally."
                }
                if ($neededDependencies -contains "redis") {
                    $redisState = Ensure-DockerRedis -DockerPath $docker -Port $redisUri.Port
                }
            }
            "None" {
                throw "Dependencies are disabled, but these configured endpoints are unavailable: $($neededDependencies -join ', ')."
            }
            default {
                throw "Unsupported dependency mode '$effectiveDependencyMode'."
            }
        }
    }

    if (-not $DryRun) {
        if (-not $NoRedis -and -not (Wait-ForProbe -Name "Redis" -Probe { Test-RedisReady -Uri $redisUri } -Seconds $DependencyWaitSeconds)) {
            throw "Redis readiness failed."
        }
        if (-not $NoJanus -and -not (Wait-ForProbe -Name "Janus" -Probe { Test-JanusReady -Uri $janusUri } -Seconds $DependencyWaitSeconds)) {
            throw "Janus readiness failed."
        }
    }

    if ($null -ne $wslState -and $null -ne $wsl) {
        Start-WslLogFollowers -State $wslState -WslPath $wsl
    }
    if ($null -ne $redisState.container -and -not $DryRun) {
        [void] (Start-LoggedProcess -Name "Redis logs" -Label "REDIS" -Color Red -FilePath $docker -Arguments @("logs", "--follow", "--since", "0s", [string] $redisState.container) -WorkingDirectory $ServerRoot -Critical $false -Persist $true)
    }

    if (-not $NoMigrate) {
        Invoke-LoggedCommand -Name "Django migrations" -Label "MIGRATE" -Color DarkCyan -FilePath $uv -Arguments @("run", "python", "manage.py", "migrate", "--noinput") -WorkingDirectory $ServerSrc
    }
    if (-not $NoBeat) {
        Invoke-LoggedCommand -Name "Celery schedule reconciliation" -Label "CELERY" -Color DarkYellow -FilePath $uv -Arguments @("run", "python", "manage.py", "reconcile_celery_schedule") -WorkingDirectory $ServerSrc
    }

    if (-not $DryRun) {
        Assert-LocalTcpPortAvailable -ServiceName "Django" -Port $BackendPort -PortOption "BackendPort"
    }
    $backendArguments = @("run", "python", $ManagedPythonPath, $DjangoIdentityPath, $RunId, "manage.py", "runjanus", "127.0.0.1:$BackendPort")
    if (-not $Reload) { $backendArguments += "--noreload" }
    $djangoEntry = Start-LoggedProcess -Name "Django" -Label "DJANGO" -Color Cyan -FilePath $uv -Arguments $backendArguments -WorkingDirectory $ServerSrc -Critical $true -Persist $true
    $djangoRuntime = if ($DryRun) { $null } else {
        Wait-ForManagedProcessIdentity -Entry $djangoEntry -IdentityPath $DjangoIdentityPath -RunToken $RunId -Seconds $StartupWaitSeconds
    }
    if (-not $NoWorker) {
        $workerEntry = Start-LoggedProcess -Name "Celery worker" -Label "CELERY" -Color Yellow -FilePath $uv -Arguments @("run", "python", $ManagedPythonPath, $CeleryWorkerIdentityPath, $RunId, "-m", "celery", "-A", "conf", "worker", "-P", "solo", "-l", "INFO", "-E", "-Q", "celery,meeting_email", "--prefetch-multiplier=1") -WorkingDirectory $ServerSrc -Critical $true -Persist $true
        if (-not $DryRun) {
            [void] (Wait-ForManagedProcessIdentity -Entry $workerEntry -IdentityPath $CeleryWorkerIdentityPath -RunToken $RunId -Seconds $StartupWaitSeconds)
        }
    }
    if (-not $NoBeat) {
        $beatEntry = Start-LoggedProcess -Name "Celery beat" -Label "BEAT" -Color Blue -FilePath $uv -Arguments @("run", "python", $ManagedPythonPath, $CeleryBeatIdentityPath, $RunId, "-m", "celery", "-A", "conf.celery", "beat", "-l", "INFO", "--scheduler", "django_celery_beat.schedulers:DatabaseScheduler") -WorkingDirectory $ServerSrc -Critical $true -Persist $true
        if (-not $DryRun) {
            [void] (Wait-ForManagedProcessIdentity -Entry $beatEntry -IdentityPath $CeleryBeatIdentityPath -RunToken $RunId -Seconds $StartupWaitSeconds)
        }
    }
    if (-not $NoFrontend) {
        [void] (Start-LoggedProcess -Name "Next.js" -Label "NEXT" -Color Green -FilePath $npm -Arguments @("--workspace", "app", "run", "dev", "--", "--hostname", "localhost", "--port", [string] $FrontendPort) -WorkingDirectory $WebRoot -Critical $true -Persist $true)
    }

    if ($DryRun) {
        Write-StackLog -Label "SYSTEM" -Color Gray -Message "Dry run complete; nothing was started."
    } else {
        Save-SupervisorState -RedisState $redisState -WslState $wslState -EffectiveMode $effectiveDependencyMode
        if (-not (Wait-ForProbe -Name "Django" -Probe {
            (Test-ManagedProcessIdentity -Record $djangoRuntime) -and
            (Test-TcpPortOwnedByProcess -Address "127.0.0.1" -Port $BackendPort -ProcessId $djangoRuntime.Pid) -and
            (Test-HttpReady "http://127.0.0.1:$BackendPort/admin/login/")
        } -Seconds $StartupWaitSeconds)) {
            throw "Django readiness failed."
        }
        if (-not $NoFrontend -and -not (Wait-ForProbe -Name "Next.js" -Probe { Test-HttpReady "http://localhost:$FrontendPort/api/health" } -Seconds $StartupWaitSeconds)) {
            throw "Next.js readiness failed."
        }

        Write-StackLog -Label "SYSTEM" -Color White -Message "Stack ready. Backend http://localhost:$BackendPort"
        if (-not $NoFrontend) { Write-StackLog -Label "SYSTEM" -Color White -Message "Frontend http://localhost:$FrontendPort" }
        if (-not $NoCombinedLog) { Write-StackLog -Label "SYSTEM" -Color White -Message "Combined log $CombinedLogPath" }
        Write-StackLog -Label "SYSTEM" -Color White -Message "Press Ctrl+C to stop, or run scripts\stop.ps1 in another terminal."

        $shutdownRequested = $false
        $nextDependencyCheck = [DateTime]::UtcNow.AddSeconds(15)
        $redisReadinessFailures = 0
        $janusReadinessFailures = 0
        while (-not $shutdownRequested) {
            Pump-AllLogs
            if (Test-Path -LiteralPath $StopRequestPath -PathType Leaf) {
                try {
                    $request = Get-Content -LiteralPath $StopRequestPath -Raw | ConvertFrom-Json
                    if ($null -ne $request.keepRedis) { $script:KeepRedisOnExit = [bool] $request.keepRedis }
                    if ($null -ne $request.keepWslDependencies) { $script:KeepWslOnExit = [bool] $request.keepWslDependencies }
                } catch {
                    Write-StackLog -Label "SYSTEM" -Color Yellow -Message "Stop request could not be parsed; stopping with default cleanup."
                }
                Write-StackLog -Label "SYSTEM" -Color Gray -Message "Graceful stop requested."
                $shutdownRequested = $true
                continue
            }

            if ([DateTime]::UtcNow -ge $nextDependencyCheck) {
                if (-not $NoRedis) {
                    if (Test-RedisReady -Uri $redisUri) {
                        $redisReadinessFailures = 0
                    } else {
                        $redisReadinessFailures += 1
                        Write-StackLog -Label "REDIS" -Color Yellow -Message "readiness probe failed ($redisReadinessFailures/3)" -Stream "SYS"
                    }
                    Pump-AllLogs
                }
                if (-not $NoJanus) {
                    if (Test-JanusReady -Uri $janusUri) {
                        $janusReadinessFailures = 0
                    } else {
                        $janusReadinessFailures += 1
                        Write-StackLog -Label "JANUS" -Color Yellow -Message "readiness probe failed ($janusReadinessFailures/3)" -Stream "SYS"
                    }
                    Pump-AllLogs
                }
                if ($redisReadinessFailures -ge 3 -or $janusReadinessFailures -ge 3) {
                    Write-StackLog -Label "SYSTEM" -Color Red -Message "A required dependency failed three consecutive readiness probes; stopping the stack." -Stream "ERR"
                    $script:ExitCode = 1
                    $shutdownRequested = $true
                    continue
                }
                $nextDependencyCheck = [DateTime]::UtcNow.AddSeconds(15)
            }

            foreach ($entry in $script:Processes) {
                if ($entry.Process.HasExited -and -not $entry.ExitReported) {
                    $entry.ExitReported = $true
                    Pump-AllLogs
                    if ($entry.Critical -and -not $entry.ExpectedExit) {
                        Write-StackLog -Label "SYSTEM" -Color Red -Message "$($entry.Name) exited unexpectedly with code $($entry.Process.ExitCode)." -Stream "ERR"
                        $script:ExitCode = if ($entry.Process.ExitCode -eq 0) { 1 } else { $entry.Process.ExitCode }
                        $shutdownRequested = $true
                        break
                    }
                    if (-not $entry.ExpectedExit) {
                        Write-StackLog -Label "SYSTEM" -Color DarkGray -Message "$($entry.Name) log follower exited with code $($entry.Process.ExitCode)."
                    }
                }
            }
            Start-Sleep -Milliseconds 50
        }
    }
} catch [Management.Automation.PipelineStoppedException] {
    $script:ExitCode = 0
} catch {
    $script:ExitCode = 1
    Write-StackLog -Label "SYSTEM" -Color Red -Message $_.Exception.Message -Stream "ERR"
} finally {
    if (-not $DryRun) {
        Write-StackLog -Label "SYSTEM" -Color Gray -Message "Stopping managed services..."
        for ($index = $script:Processes.Count - 1; $index -ge 0; $index -= 1) {
            $entry = $script:Processes[$index]
            $entry.ExpectedExit = $true
            foreach ($ownedProcess in @($entry.OwnedProcesses)) {
                if (Test-ManagedProcessIdentity -Record $ownedProcess) {
                    Write-StackLog -Label "SYSTEM" -Color Gray -Message "Stopping $($ownedProcess.Name) (PID $($ownedProcess.Pid))."
                    Stop-ProcessTree -ProcessId $ownedProcess.Pid -ExpectedStartTicks $ownedProcess.StartTicks -SkipChildEnumeration
                }
            }
            if (-not $entry.Process.HasExited) {
                Write-StackLog -Label "SYSTEM" -Color Gray -Message "Stopping $($entry.Name) (PID $($entry.Pid))."
                Stop-ProcessTree -ProcessId $entry.Pid -ExpectedStartTicks $entry.StartTicks
            }
        }
        foreach ($identityPath in @($DjangoIdentityPath, $CeleryWorkerIdentityPath, $CeleryBeatIdentityPath)) {
            Remove-Item -LiteralPath $identityPath -Force -ErrorAction SilentlyContinue
        }
        Pump-AllLogs

        if (-not $script:KeepRedisOnExit -and $redisState.started -and $redisState.container -and $null -ne $docker) {
            Write-StackLog -Label "REDIS" -Color Red -Message "stopping owned Docker container '$($redisState.container)'"
            & $docker stop ([string] $redisState.container) | Out-Null
            if ($LASTEXITCODE -ne 0) {
                $dependencyCleanupOk = $false
                $script:ExitCode = 1
                Write-StackLog -Label "REDIS" -Color Red -Message "owned Docker container cleanup failed; state is being retained for retry" -Stream "ERR"
            }
        }
        $wslSidecarPath = Get-WslDependencyStatePath -AppName $WslAppName -ServerRoot $ServerRoot
        if (-not $script:KeepWslOnExit -and ($null -ne $wslState -or $wslStartupAttempted -or (Test-Path -LiteralPath $wslSidecarPath -PathType Leaf))) {
            try {
                $stopParameters = @{
                    AppName = $WslAppName
                    ServerRoot = $ServerRoot
                    Distro = ""
                    WaitSeconds = $DependencyWaitSeconds
                }
                $wslStopResult = Stop-MeetWslDependencies @stopParameters
                if (-not [bool] $wslStopResult.Ok) {
                    $dependencyCleanupOk = $false
                    $script:ExitCode = 1
                    Write-StackLog -Label "SYSTEM" -Color Red -Message "WSL dependency cleanup was incomplete; state is being retained for retry." -Stream "ERR"
                }
            } catch {
                $dependencyCleanupOk = $false
                $script:ExitCode = 1
                Write-StackLog -Label "SYSTEM" -Color Red -Message "WSL cleanup failed; state is being retained for retry: $($_.Exception.Message)" -Stream "ERR"
            }
        }

        if (-not $dependencyCleanupOk -and -not (Test-Path -LiteralPath $StatePath -PathType Leaf)) {
            try {
                if ($null -eq $wslState -and (Test-Path -LiteralPath $wslSidecarPath -PathType Leaf)) {
                    $wslState = [pscustomobject] @{
                        appName = $WslAppName
                        distro = $WslDistro
                        dependencies = @()
                    }
                }
                Save-SupervisorState -RedisState $redisState -WslState $wslState -EffectiveMode $effectiveDependencyMode
            } catch {
                Write-StackLog -Label "SYSTEM" -Color Red -Message "Could not write retry state after failed dependency cleanup: $($_.Exception.Message)" -Stream "ERR"
            }
        }

        if ($dependencyCleanupOk -and (Test-Path -LiteralPath $StatePath -PathType Leaf)) {
            try {
                $currentState = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
                if ([string] $currentState.runId -eq $RunId) {
                    Remove-Item -LiteralPath $StatePath -Force
                }
            } catch {
                Write-StackLog -Label "SYSTEM" -Color Yellow -Message "Could not remove state file safely."
            }
        }
        Remove-Item -LiteralPath $StopRequestPath -Force -ErrorAction SilentlyContinue
        Write-StackLog -Label "SYSTEM" -Color Gray -Message "Shutdown complete."
    }

    if ($null -ne $script:LogWriter) {
        $script:LogWriter.Flush()
        $script:LogWriter.Dispose()
    }
    if ($null -ne $script:LockStream) {
        $script:LockStream.Dispose()
    }
}

exit $script:ExitCode
