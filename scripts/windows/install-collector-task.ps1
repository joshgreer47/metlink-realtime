<#
.SYNOPSIS
    Registers the GTFS-RT collector as a Windows scheduled task.

.DESCRIPTION
    The task runs as the current user without needing an interactive logon, starts at boot and at logon,
    restarts after failures, and is re-launched every 15 minutes if it is not running. The collector's
    single-instance lock prevents duplicates. Logs go to data\logs\poller.log.

    Run from an elevated PowerShell. Stop any collector already running in a terminal first.

.EXAMPLE
    .\scripts\windows\install-collector-task.ps1
#>
[CmdletBinding()]
param(
    [string]$TaskName = "metlink-realtime-poller",
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]$identity).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this script from an elevated PowerShell (Run as administrator)."
}

$python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { throw "Virtual environment not found at $python. See README: Install." }
if (-not (Test-Path (Join-Path $RepoRoot ".env"))) { throw ".env not found in $RepoRoot. See README: Install." }

$logFile = Join-Path $RepoRoot "data\logs\poller.log"
$user = $identity.Name

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "-m poller.metlink_poller --keep-awake --log-file `"$logFile`"" `
    -WorkingDirectory $RepoRoot

$watchdog = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 15)
$triggers = @(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -AtLogOn -User $user),
    $watchdog
)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType S4U -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Description "Collects Metlink GTFS-Realtime feeds and uploads batches to Databricks ($RepoRoot)" `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 5
$info = Get-ScheduledTaskInfo -TaskName $TaskName
$state = (Get-ScheduledTask -TaskName $TaskName).State

Write-Host "Registered '$TaskName' for $user (state: $state, last result: $($info.LastTaskResult))."
Write-Host "Logs: $logFile"
