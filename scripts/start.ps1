param(
    [int]$BackendPort = 8000,
    [int]$FrontendPort = 3000,
    [int]$RedisPort = 6379,
    [string]$RedisContainerName = "synq-meet-redis",
    [switch]$NoRedis,
    [switch]$NoMigrate,
    [switch]$NoWorker,
    [switch]$NoBeat,
    [switch]$NoFrontend,
    [ValidateSet("Auto", "Docker", "Wsl", "None")]
    [string]$DependencyMode = "Auto",
    [AllowEmptyString()]
    [string]$WslDistro = $env:MEET_WSL_DISTRO,
    [AllowEmptyString()]
    [string]$WslDependencies = $(if ([string]::IsNullOrWhiteSpace($env:MEET_WSL_DEPENDENCIES)) { "redis,janus" } else { $env:MEET_WSL_DEPENDENCIES }),
    [ValidateRange(1, 120)]
    [int]$DependencyWaitSeconds = 30,
    [switch]$SkipDependencyEnvironment,
    [switch]$Reload,
    [switch]$Visible,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerRoot = Split-Path -Parent $ScriptDir
$MeetRoot = Split-Path -Parent $ServerRoot
$ServerSrc = Join-Path $ServerRoot "src"
$WebRoot = Join-Path $MeetRoot "web"
$WebApp = Join-Path $WebRoot "app"
$RuntimeRoot = Join-Path $env:TEMP "synq-meet-dev"
$LogRoot = Join-Path $RuntimeRoot ("logs\{0:yyyyMMdd-HHmmss}" -f (Get-Date))
$StatePath = Join-Path $RuntimeRoot "processes.json"
$WslHelperPath = Join-Path $ScriptDir "wsl-dependencies.ps1"

. $WslHelperPath

function Write-Step($Message) {
    Write-Host "[meet] $Message"
}

function Resolve-CommandPath($Name) {
    $command = Get-Command $Name -All -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandType -eq "Application" } |
        Select-Object -First 1
    if (-not $command) {
        $command = Get-Command $Name -ErrorAction SilentlyContinue
    }
    if (-not $command) {
        throw "Required command '$Name' was not found on PATH."
    }
    return $command.Source
}

function Import-DotEnv($Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }

        $name, $value = $trimmed -split "=", 2
        if (-not $name -or $null -eq $value) {
            continue
        }

        $name = $name.Trim()
        $value = $value.Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if (-not [Environment]::GetEnvironmentVariable($name, "Process")) {
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

function Set-DefaultEnv($Name, $Value) {
    if (-not [Environment]::GetEnvironmentVariable($Name, "Process")) {
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

function Set-Env($Name, $Value) {
    [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
}

function Test-TcpPort($HostName, $Port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $result = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $result.AsyncWaitHandle.WaitOne(500, $false)) {
            return $false
        }
        $client.EndConnect($result)
        return $true
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Wait-ForPort($Name, $Port, $Seconds, $HostName = "127.0.0.1") {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-TcpPort $HostName $Port) {
            Write-Step "$Name is listening on port $Port."
            return $true
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Warning "$Name did not open port $Port within $Seconds seconds. Check its log."
    return $false
}

function Ensure-Redis($DockerPath, $Port) {
    if ($NoRedis) {
        Write-Step "Skipping Redis startup."
        return @{ started = $false; container = $null }
    }

    if (Test-TcpPort "127.0.0.1" $Port) {
        Write-Step "Redis port $Port is already open; reusing it."
        return @{ started = $false; container = $null }
    }

    if (-not $DockerPath) {
        Write-Warning "Redis is not listening and Docker was not found. Backend startup may fail."
        return @{ started = $false; container = $null }
    }

    if ($DryRun) {
        Write-Step "Would start Redis container '$RedisContainerName' on port $Port."
        return @{ started = $true; container = $RedisContainerName }
    }

    $containerExists = $false
    & $DockerPath inspect $RedisContainerName *> $null
    if ($LASTEXITCODE -eq 0) {
        $containerExists = $true
    }

    if ($containerExists) {
        $running = (& $DockerPath inspect -f "{{.State.Running}}" $RedisContainerName 2>$null).Trim()
        if ($running -ne "true") {
            Write-Step "Starting existing Redis container '$RedisContainerName'."
            & $DockerPath start $RedisContainerName | Out-Null
            return @{ started = $true; container = $RedisContainerName }
        }
        Write-Step "Redis container '$RedisContainerName' is already running."
        return @{ started = $false; container = $null }
    }

    Write-Step "Creating Redis container '$RedisContainerName'."
    & $DockerPath run -d --name $RedisContainerName -p "${Port}:6379" redis:7 | Out-Null
    return @{ started = $true; container = $RedisContainerName }
}

function Invoke-Checked($FilePath, $Arguments, $WorkingDirectory, $Label) {
    Write-Step $Label
    if ($DryRun) {
        Write-Host "  $FilePath $($Arguments -join ' ')"
        return
    }
    Push-Location $WorkingDirectory
    try {
        & $FilePath @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "$Label failed with exit code $LASTEXITCODE."
        }
    } finally {
        Pop-Location
    }
}

function Start-ManagedProcess($Name, $FilePath, $Arguments, $WorkingDirectory) {
    $stdout = Join-Path $LogRoot "$Name.out.log"
    $stderr = Join-Path $LogRoot "$Name.err.log"
    $argumentText = $Arguments -join " "

    Write-Step "Starting $Name."
    if ($DryRun) {
        Write-Host "  cwd: $WorkingDirectory"
        Write-Host "  cmd: $FilePath $argumentText"
        return @{
            name = $Name
            pid = 0
            command = "$FilePath $argumentText"
            cwd = $WorkingDirectory
            stdout = $stdout
            stderr = $stderr
        }
    }

    $windowStyle = "Hidden"
    if ($Visible) {
        $windowStyle = "Normal"
    }

    $process = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $Arguments `
        -WorkingDirectory $WorkingDirectory `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WindowStyle $windowStyle `
        -PassThru

    $processStartTicks = $null
    try {
        $processStartTicks = $process.StartTime.ToUniversalTime().Ticks
    } catch {
        $processStartTicks = $null
    }

    return @{
        name = $Name
        pid = $process.Id
        startTicks = $processStartTicks
        command = "$FilePath $argumentText"
        cwd = $WorkingDirectory
        stdout = $stdout
        stderr = $stderr
    }
}

if (-not (Test-Path -LiteralPath $ServerSrc)) {
    throw "Cannot find backend src directory at $ServerSrc."
}
if (-not (Test-Path -LiteralPath $WebRoot)) {
    throw "Cannot find web workspace at $WebRoot."
}

New-Item -ItemType Directory -Force -Path $RuntimeRoot | Out-Null
New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null

Import-DotEnv (Join-Path $ServerRoot ".env")
Import-DotEnv (Join-Path $WebApp ".env.local")
Import-DotEnv (Join-Path $WebApp ".env")

$effectiveRedisPort = $RedisPort
$existingRedisUrl = [Environment]::GetEnvironmentVariable("REDIS_URL", "Process")
if (-not $PSBoundParameters.ContainsKey("RedisPort") -and $existingRedisUrl) {
    try {
        $redisUri = [Uri]$existingRedisUrl
        if ($redisUri.Port -gt 0) {
            $effectiveRedisPort = $redisUri.Port
        }
    } catch {
        $effectiveRedisPort = $RedisPort
    }
}

$redisUrl = "redis://127.0.0.1:$effectiveRedisPort/1"
if ($PSBoundParameters.ContainsKey("RedisPort")) {
    Set-Env "REDIS_URL" $redisUrl
    Set-Env "CELERY_BROKER_URL" $redisUrl
    Set-Env "SOCKET_IO_REDIS_URL" $redisUrl
    Set-Env "SOCKETIO_REDIS_URL" $redisUrl
} else {
    Set-DefaultEnv "REDIS_URL" $redisUrl
    Set-DefaultEnv "CELERY_BROKER_URL" $redisUrl
    Set-DefaultEnv "SOCKET_IO_REDIS_URL" $redisUrl
    Set-DefaultEnv "SOCKETIO_REDIS_URL" $redisUrl
}
Set-Env "SYNQ_BACKEND_URL" "http://localhost:$BackendPort"
Set-Env "NEXT_PUBLIC_SYNQ_BACKEND_URL" "http://localhost:$BackendPort"
Set-Env "NEXT_PUBLIC_APP_URL" "http://localhost:$FrontendPort"
Set-Env "MEETING_FRONTEND_BASE_URL" "http://localhost:$FrontendPort"
Set-DefaultEnv "NEXT_PUBLIC_SYNQ_SOCKET_PATH" "/socket.io"
Set-DefaultEnv "NEXT_PUBLIC_SYNQ_SOCKET_NAMESPACE" "/meetings"

$uv = Resolve-CommandPath "uv"
$npm = Resolve-CommandPath "npm"
$dockerCommand = Get-Command "docker" -ErrorAction SilentlyContinue
$docker = if ($dockerCommand) { $dockerCommand.Source } else { $null }

if ((Test-Path -LiteralPath $StatePath) -and -not $DryRun) {
    Write-Warning "A prior state file exists at $StatePath. Run scripts\stop.ps1 first if old services are still running."
}

$requestedWslDependencies = ConvertTo-DependencyList -Dependencies $WslDependencies
if ($NoRedis) {
    $requestedWslDependencies = Remove-DependencyFromList -Dependencies $requestedWslDependencies -Name "redis"
}

$effectiveDependencyMode = $DependencyMode
if ($effectiveDependencyMode -eq "Auto") {
    if ((Test-WslAvailable) -and $requestedWslDependencies.Count -gt 0) {
        $effectiveDependencyMode = "Wsl"
    } else {
        $effectiveDependencyMode = "Docker"
    }
}

$redisState = @{ started = $false; container = $null }
$wslState = $null

if ($effectiveDependencyMode -eq "Wsl") {
    if (-not $SkipDependencyEnvironment) {
        Set-MeetLocalDependencyEnvironment -Dependencies $requestedWslDependencies
    }

    if ($DryRun) {
        Write-Step "Would start WSL dependencies: $($requestedWslDependencies -join ', ')."
    } else {
        $wslState = Start-MeetWslDependencies `
            -AppName "synq-meet-dev" `
            -ServerRoot $ServerRoot `
            -Dependencies $requestedWslDependencies `
            -Distro $WslDistro `
            -WaitSeconds $DependencyWaitSeconds
    }
} elseif ($effectiveDependencyMode -eq "Docker") {
    $redisState = Ensure-Redis $docker $effectiveRedisPort
} else {
    Write-Step "Dependency startup disabled."
}

if (-not $NoMigrate) {
    Invoke-Checked $uv @("run", "python", "manage.py", "migrate", "--noinput") $ServerSrc "Applying Django migrations."
}

$processes = @()

$backendArgs = @("run", "python", "manage.py", "runjanus", "127.0.0.1:$BackendPort")
if (-not $Reload) {
    $backendArgs += "--noreload"
}
$processes += Start-ManagedProcess "backend" $uv $backendArgs $ServerSrc

if (-not $NoWorker) {
    $processes += Start-ManagedProcess "worker" $uv @("run", "python", "-m", "celery", "-A", "conf", "worker", "-P", "solo", "-l", "INFO", "-E") $ServerSrc
}

if (-not $NoBeat) {
    $processes += Start-ManagedProcess "beat" $uv @("run", "python", "-m", "celery", "-A", "conf.celery", "beat", "-l", "INFO", "--scheduler", "django_celery_beat.schedulers:DatabaseScheduler") $ServerSrc
}

if (-not $NoFrontend) {
    $processes += Start-ManagedProcess "frontend" $npm @("--workspace", "app", "run", "dev", "--", "--hostname", "localhost", "--port", "$FrontendPort") $WebRoot
}

$state = @{
    startedAt = (Get-Date).ToString("o")
    backendPort = $BackendPort
    frontendPort = $FrontendPort
    dependencyMode = $effectiveDependencyMode
    redis = $redisState
    wsl = $wslState
    logRoot = $LogRoot
    processes = $processes
}

if (-not $DryRun) {
    $state | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $StatePath -Encoding UTF8
    Wait-ForPort "Backend" $BackendPort 30 | Out-Null
    if (-not $NoFrontend) {
        Wait-ForPort "Frontend" $FrontendPort 45 "localhost" | Out-Null
    }
}

Write-Step "Startup complete."
Write-Host "Backend:  http://localhost:$BackendPort"
if (-not $NoFrontend) {
    Write-Host "Frontend: http://localhost:$FrontendPort"
}
Write-Host "Logs:     $LogRoot"
Write-Host "Stop:     $ScriptDir\stop.ps1"
