#Requires -Version 5.1
<#
.SYNOPSIS
Compatibility entry point for the unified Synq Meet development supervisor.
#>

$supervisorPath = Join-Path $PSScriptRoot "supervisor.ps1"
if (-not (Test-Path -LiteralPath $supervisorPath -PathType Leaf)) {
    throw "Cannot find the Synq Meet supervisor at '$supervisorPath'."
}

& $supervisorPath @args
exit $LASTEXITCODE
