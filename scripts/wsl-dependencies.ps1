#Requires -Version 5.1
<#
.SYNOPSIS
Starts and stops WSL-hosted local dependencies for Synq Meet development.

.DESCRIPTION
The Meet backend needs Redis for cache/Celery/Socket.IO fanout and Janus for
media signaling. This helper starts those services inside WSL when requested and
records only the dependencies it started, so stop.ps1 can clean them up later.
#>

Set-StrictMode -Version Latest

function ConvertTo-BashSingleQuotedLiteral {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Value
    )

    return "'" + $Value.Replace("'", "'\''") + "'"
}

function Resolve-WslExecutable {
    $commands = @(Get-Command "wsl.exe" -CommandType Application -ErrorAction SilentlyContinue)
    if ($commands.Count -eq 0) {
        throw "wsl.exe was not found on PATH. Install WSL or use start.ps1 -DependencyMode Docker."
    }

    return [string] $commands[0].Source
}

function Test-WslAvailable {
    return $null -ne (Get-Command "wsl.exe" -CommandType Application -ErrorAction SilentlyContinue)
}

function Get-EnvironmentValueOrDefault {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Names,

        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Default
    )

    foreach ($name in $Names) {
        foreach ($target in @("Process", "User", "Machine")) {
            $value = [Environment]::GetEnvironmentVariable($name, $target)
            if (-not [string]::IsNullOrWhiteSpace($value)) {
                return $value
            }
        }
    }

    return $Default
}

function ConvertTo-DependencyList {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Dependencies
    )

    if ([string]::IsNullOrWhiteSpace($Dependencies)) {
        return @()
    }

    $disabledValues = @("0", "false", "off", "none", "no", "skip")
    $normalized = $Dependencies.Trim().ToLowerInvariant()
    if ($disabledValues -contains $normalized) {
        return @()
    }

    return @(
        $Dependencies.Split(",") |
            ForEach-Object { $_.Trim().ToLowerInvariant() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )
}

function Remove-DependencyFromList {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Dependencies,

        [Parameter(Mandatory = $true)]
        [string] $Name
    )

    return @($Dependencies | Where-Object { $_ -ne $Name.ToLowerInvariant() })
}

function Test-DependencyRequested {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Dependencies,

        [Parameter(Mandatory = $true)]
        [string] $Name
    )

    return $Dependencies -contains $Name.ToLowerInvariant()
}

function Get-WslDependencyStatePath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $AppName,

        [Parameter(Mandatory = $true)]
        [string] $ServerRoot
    )

    return Join-Path $ServerRoot ".$AppName.wsl.json"
}

function Invoke-WslBash {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Script,

        [AllowEmptyString()]
        [string] $Distro = ""
    )

    $wslPath = Resolve-WslExecutable
    $tempDirectory = Join-Path ([IO.Path]::GetTempPath()) "synq-meet-wsl"
    New-Item -ItemType Directory -Path $tempDirectory -Force | Out-Null

    $tempScriptPath = Join-Path $tempDirectory ("wsl-" + [Guid]::NewGuid().ToString("N") + ".sh")
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($tempScriptPath, $Script.Replace("`r`n", "`n"), $utf8NoBom)

    try {
        $resolvedTempScriptPath = (Resolve-Path -LiteralPath $tempScriptPath).Path
        if ($resolvedTempScriptPath -notmatch "^([A-Za-z]):\\(.*)$") {
            throw "Cannot map temporary script path '$resolvedTempScriptPath' into WSL."
        }

        $drive = $Matches[1].ToLowerInvariant()
        $pathRemainder = $Matches[2].Replace("\", "/")
        $linuxScriptPath = "/mnt/$drive/$pathRemainder"

        $runArguments = @()
        if (-not [string]::IsNullOrWhiteSpace($Distro)) {
            $runArguments += @("-d", $Distro)
        }
        $runArguments += @("--", "bash", $linuxScriptPath)

        $output = @(& $wslPath @runArguments 2>&1 | ForEach-Object { [string] $_ })
        $exitCode = if ($null -eq $global:LASTEXITCODE) { 0 } else { $global:LASTEXITCODE }

        return [pscustomobject] @{
            ExitCode = $exitCode
            Output   = $output
        }
    } finally {
        Remove-Item -LiteralPath $tempScriptPath -Force -ErrorAction SilentlyContinue
    }
}

function Set-ProcessEnvironmentDefault {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,

        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $Value
    )

    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name, "Process"))) {
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
        Write-Host "[ENV] Defaulting $Name=$Value"
    }
}

function Set-MeetLocalDependencyEnvironment {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Dependencies
    )

    $redisPort = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_REDIS_PORT", "WHITEBOARD_WSL_REDIS_PORT") -Default "6379"
    $janusWsPort = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_JANUS_WS_PORT", "WHITEBOARD_WSL_JANUS_WS_PORT") -Default "8188"
    $janusHttpPort = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_JANUS_HTTP_PORT", "WHITEBOARD_WSL_JANUS_HTTP_PORT") -Default "8088"

    Set-ProcessEnvironmentDefault -Name "DJANGO_SETTINGS_MODULE" -Value "conf.settings"

    if (Test-DependencyRequested -Dependencies $Dependencies -Name "redis") {
        Set-ProcessEnvironmentDefault -Name "REDIS_URL" -Value "redis://127.0.0.1:$redisPort/1"
        Set-ProcessEnvironmentDefault -Name "SOCKET_IO_REDIS_URL" -Value "redis://127.0.0.1:$redisPort/1"
        Set-ProcessEnvironmentDefault -Name "SOCKETIO_REDIS_URL" -Value "redis://127.0.0.1:$redisPort/1"
        Set-ProcessEnvironmentDefault -Name "CELERY_BROKER_URL" -Value "redis://127.0.0.1:$redisPort/1"
    }

    if (Test-DependencyRequested -Dependencies $Dependencies -Name "janus") {
        Set-ProcessEnvironmentDefault -Name "JANUS_SESSION_URL" -Value "ws://127.0.0.1:$janusWsPort/janus"
        Set-ProcessEnvironmentDefault -Name "JANUS_PUBLIC_WS_URL" -Value "ws://127.0.0.1:$janusWsPort/janus"
        Set-ProcessEnvironmentDefault -Name "JANUS_PUBLIC_HTTP_URL" -Value "http://127.0.0.1:$janusHttpPort/janus"
    }
}

function Start-MeetWslDependencies {
    param(
        [Parameter(Mandatory = $true)]
        [string] $AppName,

        [Parameter(Mandatory = $true)]
        [string] $ServerRoot,

        [Parameter(Mandatory = $true)]
        [string[]] $Dependencies,

        [AllowEmptyString()]
        [string] $Distro = "",

        [ValidateRange(1, 120)]
        [int] $WaitSeconds = 30
    )

    if ($Dependencies.Count -eq 0) {
        Write-Host "[WSL] Dependency startup disabled."
        return $null
    }

    $supportedDependencies = @("janus", "redis")
    $unsupportedDependencies = @($Dependencies | Where-Object { $supportedDependencies -notcontains $_ })
    if ($unsupportedDependencies.Count -gt 0) {
        throw "Unsupported WSL dependencies: $($unsupportedDependencies -join ', '). Supported values: $($supportedDependencies -join ', ')."
    }

    $statePath = Get-WslDependencyStatePath -AppName $AppName -ServerRoot $ServerRoot
    $dependencyCsv = ($Dependencies -join ",")
    $redisPort = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_REDIS_PORT", "WHITEBOARD_WSL_REDIS_PORT") -Default "6379"
    $janusWsPort = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_JANUS_WS_PORT", "WHITEBOARD_WSL_JANUS_WS_PORT") -Default "8188"
    $redisCommand = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_REDIS_COMMAND", "WHITEBOARD_WSL_REDIS_COMMAND") -Default "redis-server --bind 127.0.0.1 --port $redisPort --daemonize no --dir /tmp/$AppName"
    $janusCommand = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_JANUS_COMMAND", "WHITEBOARD_WSL_JANUS_COMMAND") -Default "__auto__"
    $redisServices = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_REDIS_SERVICES", "WHITEBOARD_WSL_REDIS_SERVICES") -Default "redis-server redis"
    $janusServices = Get-EnvironmentValueOrDefault -Names @("MEET_WSL_JANUS_SERVICES", "WHITEBOARD_WSL_JANUS_SERVICES") -Default "janus janus-gateway"

    $template = @'
set -u

APP_NAME=__APP_NAME__
DEPENDENCIES_CSV=__DEPENDENCIES_CSV__
WAIT_SECONDS=__WAIT_SECONDS__
STATE_DIR=__STATE_DIR__
REDIS_PORT=__REDIS_PORT__
JANUS_WS_PORT=__JANUS_WS_PORT__
REDIS_COMMAND=__REDIS_COMMAND__
JANUS_COMMAND=__JANUS_COMMAND__
REDIS_SERVICES=__REDIS_SERVICES__
JANUS_SERVICES=__JANUS_SERVICES__

mkdir -p "$STATE_DIR"
if [ "$JANUS_COMMAND" = "__auto__" ]; then
    JANUS_COMMAND=""
    for candidate in /opt/janus/etc/janus /usr/local/etc/janus /etc/janus; do
        if [ -f "$candidate/janus.transport.websockets.jcfg" ] ||
            [ -f "$candidate/janus.transport.http.jcfg" ]; then
            JANUS_COMMAND="janus -F $candidate -r 10000-10200"
            break
        fi
    done

    if [ -z "$JANUS_COMMAND" ]; then
        JANUS_COMMAND="janus -F /usr/local/etc/janus -r 10000-10200"
    fi
fi
NORMALIZED_DEPS="$(printf ',%s,' "$DEPENDENCIES_CSV" | tr '[:upper:]' '[:lower:]')"
ERROR_COUNT=0

emit() {
    printf 'MEETDEP\t%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6"
}

emit_error() {
    ERROR_COUNT=$((ERROR_COUNT + 1))
    printf 'MEETDEP_ERROR\t%s\t%s\n' "$1" "$2"
}

want_dependency() {
    case "$NORMALIZED_DEPS" in
        *",$1,"*) return 0 ;;
        *) return 1 ;;
    esac
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

process_start_time() {
    local pid="$1"
    [ -r "/proc/$pid/stat" ] && awk '{print $22}' "/proc/$pid/stat" 2>/dev/null
}

process_identity_matches() {
    local pid="$1"
    local expected_start="$2"
    local current_start
    [ -n "$pid" ] && [ -n "$expected_start" ] || return 1
    kill -0 "$pid" >/dev/null 2>&1 || return 1
    current_start="$(process_start_time "$pid")"
    [ -n "$current_start" ] && [ "$current_start" = "$expected_start" ]
}

wait_for_owned_exit() {
    local pid="$1"
    local expected_start="$2"
    local deadline=$((SECONDS + WAIT_SECONDS))
    while process_identity_matches "$pid" "$expected_start"; do
        [ "$SECONDS" -ge "$deadline" ] && return 1
        sleep 1
    done
    return 0
}

stop_owned_direct() {
    local pid="$1"
    local expected_start="$2"
    process_identity_matches "$pid" "$expected_start" || return 0
    kill -TERM "-$pid" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
    if ! wait_for_owned_exit "$pid" "$expected_start"; then
        process_identity_matches "$pid" "$expected_start" &&
            { kill -KILL "-$pid" >/dev/null 2>&1 || kill -KILL "$pid" >/dev/null 2>&1 || true; }
        wait_for_owned_exit "$pid" "$expected_start"
    fi
}

check_tcp() {
    local port="$1"
    timeout 1 bash -c ":</dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1
}

redis_ready() {
    if command_exists redis-cli; then
        redis-cli -h 127.0.0.1 -p "$REDIS_PORT" ping 2>/dev/null | grep -q '^PONG'
    else
        check_tcp "$REDIS_PORT"
    fi
}

janus_ready() {
    check_tcp "$JANUS_WS_PORT"
}

systemd_available() {
    command_exists systemctl && [ -d /run/systemd/system ]
}

run_privileged() {
    if [ "$(id -u)" = "0" ]; then
        "$@"
    else
        sudo -n "$@"
    fi
}

systemd_unit_exists() {
    systemctl cat "$1" >/dev/null 2>&1 ||
        systemctl list-unit-files "$1.service" --no-legend 2>/dev/null | grep -q .
}

sysv_service_exists() {
    [ -x "/etc/init.d/$1" ] || service "$1" status >/dev/null 2>&1
}

start_service() {
    local service_names="$1"
    local service_name

    if systemd_available; then
        for service_name in $service_names; do
            if systemd_unit_exists "$service_name"; then
                if systemctl is-active --quiet "$service_name"; then
                    printf 'systemd-existing\t%s\n' "$service_name"
                    return 0
                fi

                if run_privileged systemctl start "$service_name" >/dev/null 2>&1; then
                    printf 'systemd\t%s\n' "$service_name"
                    return 0
                fi
            fi
        done
    fi

    if command_exists service; then
        for service_name in $service_names; do
            if sysv_service_exists "$service_name"; then
                if service "$service_name" status >/dev/null 2>&1; then
                    printf 'service-existing\t%s\n' "$service_name"
                    return 0
                fi
                if run_privileged service "$service_name" start >/dev/null 2>&1; then
                    printf 'service\t%s\n' "$service_name"
                    return 0
                fi
            fi
        done
    fi

    return 1
}

start_direct() {
    local name="$1"
    local command_text="$2"
    local pid_file="$STATE_DIR/${name}.pid"
    local start_file="$STATE_DIR/${name}.pid.start"
    local log_file="$STATE_DIR/${name}.log"
    local pid stored_start current_start start_time

    if [ -s "$pid_file" ] && [ -s "$start_file" ]; then
        pid="$(cat "$pid_file" 2>/dev/null || true)"
        stored_start="$(cat "$start_file" 2>/dev/null || true)"
        current_start="$(process_start_time "$pid")"
        if [ -n "$pid" ] && [ -n "$stored_start" ] && [ "$stored_start" = "$current_start" ] && kill -0 "$pid" >/dev/null 2>&1; then
            printf 'direct\tdirect\t%s\t%s\n' "$pid" "$stored_start"
            return 0
        fi
        rm -f "$pid_file" "$start_file"
    fi

    if command_exists setsid; then
        nohup setsid bash -lc "exec ${command_text}" >>"$log_file" 2>&1 < /dev/null &
    else
        nohup bash -lc "exec ${command_text}" >>"$log_file" 2>&1 < /dev/null &
    fi

    pid="$!"
    start_time="$(process_start_time "$pid")"
    if [ -z "$start_time" ]; then
        kill -TERM "$pid" >/dev/null 2>&1 || true
        return 1
    fi
    printf '%s\n' "$pid" > "$pid_file"
    printf '%s\n' "$start_time" > "$start_file"
    printf 'direct\tdirect\t%s\t%s\n' "$pid" "$start_time"
}

wait_until_ready() {
    local ready_fn="$1"
    local deadline=$((SECONDS + WAIT_SECONDS))

    until "$ready_fn"; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            return 1
        fi
        sleep 1
    done

    return 0
}

start_dependency() {
    local name="$1"
    local ready_fn="$2"
    local service_names="$3"
    local command_text="$4"
    local start_result method service_name pid start_time log_tail

    if ! want_dependency "$name"; then
        emit "$name" "skipped" "" "" "" ""
        return 0
    fi

    if "$ready_fn"; then
        emit "$name" "already" "" "" "" ""
        return 0
    fi

    start_result="$(start_service "$service_names" 2>/dev/null || true)"
    if [ -n "$start_result" ]; then
        method="$(printf '%s' "$start_result" | awk -F '\t' '{print $1}')"
        service_name="$(printf '%s' "$start_result" | awk -F '\t' '{print $2}')"
        if wait_until_ready "$ready_fn"; then
            case "$method" in
                systemd-existing) emit "$name" "already" "systemd" "$service_name" "" "" ;;
                service-existing) emit "$name" "already" "service" "$service_name" "" "" ;;
                *) emit "$name" "started" "$method" "$service_name" "" "" ;;
            esac
            return 0
        fi

        case "$method" in
            systemd) run_privileged systemctl stop "$service_name" >/dev/null 2>&1 || { emit_error "$name" "started systemd service failed readiness and could not be rolled back"; return 1; } ;;
            service) run_privileged service "$service_name" stop >/dev/null 2>&1 || { emit_error "$name" "started service failed readiness and could not be rolled back"; return 1; } ;;
            systemd-existing|service-existing) emit_error "$name" "an existing service is active but did not become ready"; return 1 ;;
        esac
    fi

    start_result="$(start_direct "$name" "$command_text" 2>/dev/null || true)"
    if [ -n "$start_result" ]; then
        method="$(printf '%s' "$start_result" | awk -F '\t' '{print $1}')"
        service_name="$(printf '%s' "$start_result" | awk -F '\t' '{print $2}')"
        pid="$(printf '%s' "$start_result" | awk -F '\t' '{print $3}')"
        start_time="$(printf '%s' "$start_result" | awk -F '\t' '{print $4}')"
        if wait_until_ready "$ready_fn"; then
            emit "$name" "started" "$method" "$service_name" "$pid" "$start_time"
            return 0
        fi

        log_tail="$(tail -n 20 "$STATE_DIR/${name}.log" 2>/dev/null | tr '\n' ';' || true)"
        stop_owned_direct "$pid" "$start_time" || true
        rm -f "$STATE_DIR/${name}.pid" "$STATE_DIR/${name}.pid.start"
        emit_error "$name" "started command but health check did not pass within ${WAIT_SECONDS}s. Log tail: ${log_tail}"
        return 1
    fi

    emit_error "$name" "could not start from service candidates or direct command"
    return 1
}

start_dependency "redis" "redis_ready" "$REDIS_SERVICES" "$REDIS_COMMAND"
start_dependency "janus" "janus_ready" "$JANUS_SERVICES" "$JANUS_COMMAND"

if [ "$ERROR_COUNT" -gt 0 ]; then
    exit 1
fi

exit 0
'@

    $script = $template.
        Replace("__APP_NAME__", (ConvertTo-BashSingleQuotedLiteral -Value $AppName)).
        Replace("__DEPENDENCIES_CSV__", (ConvertTo-BashSingleQuotedLiteral -Value $dependencyCsv)).
        Replace("__WAIT_SECONDS__", (ConvertTo-BashSingleQuotedLiteral -Value ([string] $WaitSeconds))).
        Replace("__STATE_DIR__", (ConvertTo-BashSingleQuotedLiteral -Value "/tmp/$AppName")).
        Replace("__REDIS_PORT__", (ConvertTo-BashSingleQuotedLiteral -Value $redisPort)).
        Replace("__JANUS_WS_PORT__", (ConvertTo-BashSingleQuotedLiteral -Value $janusWsPort)).
        Replace("__REDIS_COMMAND__", (ConvertTo-BashSingleQuotedLiteral -Value $redisCommand)).
        Replace("__JANUS_COMMAND__", (ConvertTo-BashSingleQuotedLiteral -Value $janusCommand)).
        Replace("__REDIS_SERVICES__", (ConvertTo-BashSingleQuotedLiteral -Value $redisServices)).
        Replace("__JANUS_SERVICES__", (ConvertTo-BashSingleQuotedLiteral -Value $janusServices))

    Write-Host "[WSL] Ensuring dependencies: $dependencyCsv"
    if (-not [string]::IsNullOrWhiteSpace($Distro)) {
        Write-Host "[WSL] Distro: $Distro"
    }

    $result = Invoke-WslBash -Script $script -Distro $Distro
    $records = @()
    $errors = @()

    foreach ($line in $result.Output) {
        if ($line.StartsWith("MEETDEP`t")) {
            $parts = $line -split "`t", 7
            if ($parts.Count -ge 6) {
                $records += [pscustomobject] @{
                    name            = $parts[1]
                    status          = $parts[2]
                    method          = $parts[3]
                    serviceName     = $parts[4]
                    processId       = $parts[5]
                    processStartTime = $(if ($parts.Count -ge 7) { $parts[6] } else { "" })
                    startedByScript = $parts[2] -eq "started"
                }
            }
        } elseif ($line.StartsWith("MEETDEP_ERROR`t")) {
            $parts = $line -split "`t", 3
            if ($parts.Count -ge 3) {
                $errors += "$($parts[1]): $($parts[2])"
            } else {
                $errors += $line
            }
        } elseif (-not [string]::IsNullOrWhiteSpace($line)) {
            Write-Host "[WSL] $line"
        }
    }

    foreach ($record in $records) {
        if ($record.status -eq "already") {
            Write-Host "[WSL] $($record.name) already running."
        } elseif ($record.status -eq "started") {
            if ($record.method -eq "direct") {
                Write-Host "[WSL] Started $($record.name) directly (PID $($record.processId))."
            } else {
                Write-Host "[WSL] Started $($record.name) with $($record.method):$($record.serviceName)."
            }
        }
    }

    $state = [ordered] @{
        appName      = $AppName
        stateVersion = 2
        startedAtUtc = (Get-Date).ToUniversalTime().ToString("o")
        distro       = $Distro
        stateDir     = "/tmp/$AppName"
        dependencies = $records
    }

    $state | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $statePath -Encoding utf8

    if ($result.ExitCode -ne 0 -or $errors.Count -gt 0) {
        foreach ($errorMessage in $errors) {
            Write-Host "[WSL][ERROR] $errorMessage"
        }
        try {
            $rollback = Stop-MeetWslDependencies -AppName $AppName -ServerRoot $ServerRoot -Distro "" -WaitSeconds $WaitSeconds
            if (-not [bool] $rollback.Ok) {
                Write-Warning "WSL partial-start rollback was incomplete; ownership state was retained for retry."
            }
        } catch {
            Write-Warning "WSL partial-start rollback failed; ownership state was retained: $($_.Exception.Message)"
        }
        throw "WSL dependency startup failed. Fix the WSL service/command or use start.ps1 -DependencyMode Docker."
    }
    return $state
}

function Stop-MeetWslDependencies {
    param(
        [Parameter(Mandatory = $true)]
        [string] $AppName,

        [Parameter(Mandatory = $true)]
        [string] $ServerRoot,

        [AllowEmptyString()]
        [string] $Distro = "",

        [ValidateRange(1, 120)]
        [int] $WaitSeconds = 30
    )

    $statePath = Get-WslDependencyStatePath -AppName $AppName -ServerRoot $ServerRoot
    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) {
        Write-Host "[WSL] No managed dependency state found."
        return [pscustomobject] @{ Ok = $true; Stopped = 0 }
    }

    $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
    if ($state.appName -ne $AppName) {
        throw "WSL dependency state does not belong to $AppName."
    }

    $effectiveDistro = $Distro
    if ([string]::IsNullOrWhiteSpace($effectiveDistro) -and -not [string]::IsNullOrWhiteSpace($state.distro)) {
        $effectiveDistro = [string] $state.distro
    }

    $managedDependencies = @(
        $state.dependencies |
            Where-Object { $_.startedByScript -eq $true } |
            ForEach-Object {
                $startIdentityProperty = $_.PSObject.Properties["processStartTime"]
                $startIdentity = if ($null -eq $startIdentityProperty) { "" } else { [string] $startIdentityProperty.Value }
                "$($_.name)`t$($_.method)`t$($_.serviceName)`t$($_.processId)`t$startIdentity"
            }
    )

    if ($managedDependencies.Count -eq 0) {
        Write-Host "[WSL] No dependencies were started by this script."
        Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
        return [pscustomobject] @{ Ok = $true; Stopped = 0 }
    }

    $items = $managedDependencies -join "`n"
    $template = @'
set -u

WAIT_SECONDS=__WAIT_SECONDS__
STATE_DIR=__STATE_DIR__
STOP_ITEMS=__STOP_ITEMS__
ERROR_COUNT=0

emit() {
    printf 'MEETDEP_STOP\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5"
}

run_privileged() {
    if [ "$(id -u)" = "0" ]; then
        "$@"
    else
        sudo -n "$@"
    fi
}

process_alive() {
    local pid="$1"
    [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1
}

process_start_time() {
    local pid="$1"
    [ -r "/proc/$pid/stat" ] && awk '{print $22}' "/proc/$pid/stat" 2>/dev/null
}

process_identity_matches() {
    local pid="$1"
    local expected_start="$2"
    local current_start
    [ -n "$pid" ] && [ -n "$expected_start" ] || return 1
    process_alive "$pid" || return 1
    current_start="$(process_start_time "$pid")"
    [ -n "$current_start" ] && [ "$current_start" = "$expected_start" ]
}

wait_for_exit() {
    local pid="$1"
    local expected_start="$2"
    local deadline=$((SECONDS + WAIT_SECONDS))

    while process_identity_matches "$pid" "$expected_start"; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            return 1
        fi
        sleep 1
    done

    return 0
}

stop_direct() {
    local name="$1"
    local pid="$2"
    local expected_start="$3"
    local pid_file="$STATE_DIR/${name}.pid"
    local start_file="$STATE_DIR/${name}.pid.start"

    if [ -z "$pid" ] && [ -s "$pid_file" ]; then
        pid="$(cat "$pid_file" 2>/dev/null || true)"
    fi
    if [ -z "$expected_start" ] && [ -s "$start_file" ]; then
        expected_start="$(cat "$start_file" 2>/dev/null || true)"
    fi

    if [ -z "$pid" ] || ! process_alive "$pid"; then
        rm -f "$pid_file" "$start_file"
        emit "$name" "already_stopped" "direct" "direct" "$pid"
        return 0
    fi

    if ! process_identity_matches "$pid" "$expected_start"; then
        emit "$name" "failed" "direct_identity_mismatch" "direct" "$pid"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        return 1
    fi

    kill -TERM "-$pid" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
    if ! wait_for_exit "$pid" "$expected_start"; then
        process_identity_matches "$pid" "$expected_start" &&
            { kill -KILL "-$pid" >/dev/null 2>&1 || kill -KILL "$pid" >/dev/null 2>&1 || true; }
        wait_for_exit "$pid" "$expected_start" || {
            emit "$name" "failed" "direct" "direct" "$pid"
            ERROR_COUNT=$((ERROR_COUNT + 1))
            return 1
        }
    fi

    rm -f "$pid_file" "$start_file"
    emit "$name" "stopped" "direct" "direct" "$pid"
}

stop_service() {
    local name="$1"
    local method="$2"
    local service_name="$3"

    if [ -z "$service_name" ]; then
        emit "$name" "failed" "$method" "" ""
        ERROR_COUNT=$((ERROR_COUNT + 1))
        return 1
    fi

    case "$method" in
        systemd)
            if run_privileged systemctl stop "$service_name" >/dev/null 2>&1; then
                emit "$name" "stopped" "$method" "$service_name" ""
                return 0
            fi
            ;;
        service)
            if run_privileged service "$service_name" stop >/dev/null 2>&1; then
                emit "$name" "stopped" "$method" "$service_name" ""
                return 0
            fi
            ;;
    esac

    emit "$name" "failed" "$method" "$service_name" ""
    ERROR_COUNT=$((ERROR_COUNT + 1))
    return 1
}

while IFS="$(printf '\t')" read -r name method service_name pid expected_start; do
    [ -z "$name" ] && continue
    case "$method" in
        direct)
            stop_direct "$name" "$pid" "$expected_start" || true
            ;;
        systemd|service)
            stop_service "$name" "$method" "$service_name" || true
            ;;
        *)
            emit "$name" "skipped" "$method" "$service_name" "$pid"
            ;;
    esac
done <<EOF
$STOP_ITEMS
EOF

if [ "$ERROR_COUNT" -gt 0 ]; then
    exit 1
fi

exit 0
'@

    $script = $template.
        Replace("__WAIT_SECONDS__", (ConvertTo-BashSingleQuotedLiteral -Value ([string] $WaitSeconds))).
        Replace("__STATE_DIR__", (ConvertTo-BashSingleQuotedLiteral -Value "/tmp/$AppName")).
        Replace("__STOP_ITEMS__", (ConvertTo-BashSingleQuotedLiteral -Value $items))

    Write-Host "[WSL] Stopping dependencies started by this script..."
    $result = Invoke-WslBash -Script $script -Distro $effectiveDistro
    $stoppedCount = 0
    $failed = $false

    foreach ($line in $result.Output) {
        if ($line.StartsWith("MEETDEP_STOP`t")) {
            $parts = $line -split "`t", 6
            if ($parts.Count -ge 6) {
                $name = $parts[1]
                $status = $parts[2]
                $method = $parts[3]
                $serviceName = $parts[4]
                $processId = $parts[5]

                if ($status -eq "stopped" -or $status -eq "already_stopped") {
                    $stoppedCount += 1
                    if ($method -eq "direct") {
                        Write-Host "[WSL] $status $name direct process $processId."
                    } else {
                        Write-Host "[WSL] $status $name $method service $serviceName."
                    }
                } elseif ($status -eq "failed") {
                    $failed = $true
                    Write-Warning "[WSL] Failed to stop $name ($method $serviceName $processId)."
                }
            }
        } elseif (-not [string]::IsNullOrWhiteSpace($line)) {
            Write-Host "[WSL] $line"
        }
    }

    if ($result.ExitCode -eq 0 -and -not $failed) {
        Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
        return [pscustomobject] @{ Ok = $true; Stopped = $stoppedCount }
    }

    return [pscustomobject] @{ Ok = $false; Stopped = $stoppedCount }
}
