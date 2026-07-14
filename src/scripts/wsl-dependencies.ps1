#Requires -Version 5.1
<#
.SYNOPSIS
Compatibility wrapper for the Synq Meet WSL dependency helper.
#>

$rootWslHelper = Join-Path $PSScriptRoot "..\..\scripts\wsl-dependencies.ps1"
. (Resolve-Path -LiteralPath $rootWslHelper).Path
