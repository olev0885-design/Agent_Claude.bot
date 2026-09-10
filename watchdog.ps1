# =============================================================================
# watchdog.ps1 - external guard for the live bot (main.py --listen).
# =============================================================================
# Runs INDEPENDENTLY of the bot's own event loop: if the bot itself hangs
# (e.g. a stuck network call to an exchange with no timeout), the bot's
# own internal protections (asyncio.wait_for etc.) may not help - the
# only reliable way to detect and fix a true hang of this kind is an
# external process that checks "vital signs" (freshness of listen.log)
# and restarts the bot when needed.
#
# Logic:
#   1. Every $checkIntervalSeconds seconds, check:
#      - is a python.exe process with --listen running at all;
#      - when listen.log was last modified.
#   2. If the process is missing OR the log hasn't been updated for
#      longer than $staleThresholdSeconds - assume the bot is hung/dead,
#      kill any leftover python.exe processes and start it again.
#
# Run (from project root, in background):
#   powershell -NoProfile -ExecutionPolicy Bypass -File watchdog.ps1 *>> watchdog.log
#
# Stop: find and terminate the powershell.exe process running this
# script (or just close the terminal/session that started it).
# =============================================================================

$ErrorActionPreference = "Continue"

$projectDir = "d:\Claud+\Agent_Claude.bot"
$logPath = Join-Path $projectDir "listen.log"
$staleThresholdSeconds = 90   # per request: about a minute and a half of no new data
$checkIntervalSeconds = 20    # how often we check (not too often, to avoid spamming)

Set-Location $projectDir

function Get-BotProcesses {
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*bot_crew.main*--listen*' }
}

function Start-Bot {
    param([string]$Reason)

    Write-Output "[watchdog] $(Get-Date -Format o) - $Reason - restarting bot."

    $existing = Get-BotProcesses
    foreach ($proc in $existing) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
            Write-Output "[watchdog]   stopped old process PID=$($proc.ProcessId)"
        } catch {
            Write-Output "[watchdog]   failed to stop PID=$($proc.ProcessId): $_"
        }
    }
    Start-Sleep -Seconds 3

    # cmd.exe /c is needed so "> ... 2>&1" (merging stdout+stderr into ONE
    # listen.log) works the same as a manual bash launch - Start-Process
    # alone cannot merge both streams into a single file.
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList '/c', 'venv\Scripts\python.exe -u -m bot_crew.main --listen > listen.log 2>&1' `
        -WindowStyle Hidden `
        -WorkingDirectory $projectDir

    Start-Sleep -Seconds 8

    $new = Get-BotProcesses
    if ($new) {
        Write-Output "[watchdog]   started, PID=$($new.ProcessId -join ', ')"
    } else {
        Write-Output "[watchdog]   WARNING: process not found after start - check listen.log manually."
    }
}

Write-Output "[watchdog] $(Get-Date -Format o) - started. stale_threshold=${staleThresholdSeconds}s check_interval=${checkIntervalSeconds}s"

while ($true) {
    Start-Sleep -Seconds $checkIntervalSeconds

    $procs = Get-BotProcesses
    if (-not $procs) {
        Start-Bot -Reason "bot process not found"
        continue
    }

    if (Test-Path $logPath) {
        $lastWrite = (Get-Item $logPath).LastWriteTime
        $ageSeconds = (New-TimeSpan -Start $lastWrite -End (Get-Date)).TotalSeconds
    } else {
        $ageSeconds = [double]::MaxValue
    }

    if ($ageSeconds -gt $staleThresholdSeconds) {
        Start-Bot -Reason "listen.log not updated for $([int]$ageSeconds)s (threshold ${staleThresholdSeconds}s) - looks hung"
    }
}
