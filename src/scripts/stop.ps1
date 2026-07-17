#Requires -Version 5.1
<#
.SYNOPSIS
Compatibility wrapper for the Synq Meet development stopper.
#>

$rootStopScript = Join-Path $PSScriptRoot "..\..\scripts\stop.ps1"
$resolvedStopScript = (Resolve-Path -LiteralPath $rootStopScript).Path

& $resolvedStopScript @args
exit $LASTEXITCODE

