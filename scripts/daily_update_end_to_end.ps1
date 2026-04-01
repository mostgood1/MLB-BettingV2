[CmdletBinding()]
param(
    [string]$Date = (Get-Date).ToString('yyyy-MM-dd'),
    [int]$Season = [int](Get-Date).Year,
    [string]$NextDate = '',
    [int]$Sims = 1000,
    [int]$Workers = 4,
    [string]$Pbp = 'off',
    [int]$PbpMaxEvents = 250,
    [string]$UseRosterArtifacts = 'on',
    [string]$WriteRosterArtifacts = 'on',
    [string]$GitPush = 'on',
    [string]$GitPushRemote = 'origin',
    [string]$GitPushBranch = '',
    [string]$GitCommitMessage = 'Daily end-to-end {date} + {next_date}',
    [switch]$SpringMode,
    [switch]$SkipPriorReconcile,
    [string]$PythonExe = '',
    [switch]$AllowDirtyGit,
    [string[]]$ExtraArgs
)

$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Resolve-PythonExe {
    param(
        [string]$RepoRoot,
        [string]$Requested
    )

    $candidates = @()
    if ($Requested) {
        $candidates += $Requested
    }
    $candidates += @(
        (Join-Path $RepoRoot '.venv_x64\Scripts\python.exe'),
        (Join-Path $RepoRoot '.venv\Scripts\python.exe')
    )

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return (Resolve-Path $candidate).Path
        }
    }

    throw 'No Python executable found. Pass -PythonExe or create .venv_x64/.venv.'
}

function Get-DatePlusDays {
    param(
        [string]$BaseDate,
        [int]$Days
    )

    return ([datetime]::ParseExact($BaseDate, 'yyyy-MM-dd', $null).AddDays($Days)).ToString('yyyy-MM-dd')
}

function Get-SeasonFromDate {
    param([string]$Value)
    return ([datetime]::ParseExact($Value, 'yyyy-MM-dd', $null)).Year
}

function Invoke-ExternalCommand {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$StepName,
        [string]$WorkingDirectory
    )

    Write-Host "==> $StepName"
    Write-Host ((@($FilePath) + $Arguments) -join ' ')
    Push-Location $WorkingDirectory
    try {
        & $FilePath @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "$StepName failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

function Invoke-GitCommand {
    param(
        [string]$RepoRoot,
        [string[]]$Arguments,
        [string]$StepName
    )

    Invoke-ExternalCommand -FilePath 'git' -Arguments (@('-C', $RepoRoot) + $Arguments) -StepName $StepName -WorkingDirectory $RepoRoot
}

$repoRoot = Get-RepoRoot
$python = Resolve-PythonExe -RepoRoot $repoRoot -Requested $PythonExe
$dailyUpdatePy = Join-Path $repoRoot 'tools\daily_update.py'

if (-not (Test-Path $dailyUpdatePy)) {
    throw "Missing daily update tool: $dailyUpdatePy"
}

$resolvedNextDate = if ($NextDate) { $NextDate } else { Get-DatePlusDays -BaseDate $Date -Days 1 }
$reconcileDate = Get-DatePlusDays -BaseDate $Date -Days -1
$nextSeason = Get-SeasonFromDate -Value $resolvedNextDate

$sharedArgs = @()
if ($SpringMode.IsPresent) {
    $sharedArgs += '--spring-mode'
}

$sharedArgs += @(
    '--sims', $Sims.ToString(),
    '--workers', $Workers.ToString(),
    '--pbp', $Pbp,
    '--pbp-max-events', $PbpMaxEvents.ToString(),
    '--use-roster-artifacts', $UseRosterArtifacts,
    '--write-roster-artifacts', $WriteRosterArtifacts
)

if ($ExtraArgs) {
    $sharedArgs += $ExtraArgs
}

$initialGitStatus = ''
if ($GitPush -eq 'on') {
    $initialGitStatus = (& git -C $repoRoot status --porcelain) -join "`n"
    if ($initialGitStatus -and -not $AllowDirtyGit.IsPresent) {
        throw 'Git working tree is already dirty. Re-run with -AllowDirtyGit to permit a final auto-push.'
    }
}

$currentArgs = @(
    $dailyUpdatePy,
    '--date', $Date,
    '--season', $Season.ToString(),
    '--workflow', 'ui-daily',
    '--git-push', 'off'
) + $sharedArgs

if (-not $SkipPriorReconcile.IsPresent) {
    $currentArgs += @('--reconcile-date', $reconcileDate)
}
else {
    $currentArgs += @(
        '--refresh-prior-feed-live', 'off',
        '--settle-prior-card', 'off',
        '--refresh-season-manifests', 'off'
    )
}

$nextArgs = @(
    $dailyUpdatePy,
    '--date', $resolvedNextDate,
    '--season', $nextSeason.ToString(),
    '--workflow', 'ui-daily',
    '--reconcile-date', $Date,
    '--refresh-prior-feed-live', 'off',
    '--settle-prior-card', 'off',
    '--refresh-season-manifests', 'off',
    '--git-push', 'off'
) + $sharedArgs

Invoke-ExternalCommand -FilePath $python -Arguments $currentArgs -StepName "Current-day ui-daily ($Date)" -WorkingDirectory $repoRoot
Invoke-ExternalCommand -FilePath $python -Arguments $nextArgs -StepName "Next-day forward build ($resolvedNextDate)" -WorkingDirectory $repoRoot

if ($GitPush -eq 'on') {
    $commitMessage = $GitCommitMessage.Replace('{date}', $Date).Replace('{next_date}', $resolvedNextDate).Replace('{workflow}', 'end-to-end')
    Invoke-GitCommand -RepoRoot $repoRoot -Arguments @('add', '-A') -StepName 'Stage workflow outputs'

    $postAddStatus = (& git -C $repoRoot status --porcelain) -join "`n"
    if (-not $postAddStatus) {
        Write-Host 'No git changes detected after the workflow run.'
    }
    else {
        Invoke-GitCommand -RepoRoot $repoRoot -Arguments @('commit', '-m', $commitMessage) -StepName 'Commit workflow outputs'
        $pushArgs = @('push', $GitPushRemote)
        if ($GitPushBranch) {
            $pushArgs += $GitPushBranch
        }
        Invoke-GitCommand -RepoRoot $repoRoot -Arguments $pushArgs -StepName 'Push workflow outputs'
    }
}

Write-Host ''
Write-Host 'End-to-end daily update completed.'
Write-Host "  Reconciled prior day: $(if ($SkipPriorReconcile.IsPresent) { 'skipped' } else { $reconcileDate })"
Write-Host "  Built current day:    $Date"
Write-Host "  Built next day:       $resolvedNextDate"