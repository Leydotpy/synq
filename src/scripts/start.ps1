#Requires -Version 5.1
<#
.SYNOPSIS
Compatibility wrapper for the Synq Meet development launcher.
#>

$rootStartScript = Join-Path $PSScriptRoot "..\..\scripts\start.ps1"
$resolvedStartScript = (Resolve-Path -LiteralPath $rootStartScript).Path

& $resolvedStartScript @args
exit $LASTEXITCODE
