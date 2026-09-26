<#
.SYNOPSIS
    Stops and removes the GTFS-RT collector scheduled task. Stopping the task discards polls buffered since
    the last batch (up to 5 minutes). Files already written to data\spool are uploaded by the next run.

.EXAMPLE
    .\scripts\windows\uninstall-collector-task.ps1
#>
[CmdletBinding()]
param([string]$TaskName = "metlink-realtime-poller")

$ErrorActionPreference = "Stop"

if (-not (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
    Write-Host "No task named '$TaskName'."
    return
}
Stop-ScheduledTask -TaskName $TaskName
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Write-Host "Removed '$TaskName'."
