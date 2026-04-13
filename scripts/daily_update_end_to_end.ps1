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
    [switch]$AllowArtifactRebase,
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

function Get-GitCurrentBranch {
    param([string]$RepoRoot)

    $branch = (& git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to determine current git branch.'
    }
    $branch = ($branch | Select-Object -First 1).Trim()
    if (-not $branch -or $branch -eq 'HEAD') {
        throw 'Detached HEAD is not supported for workflow auto-push.'
    }
    return $branch
}

function Get-GitAheadBehind {
    param(
        [string]$RepoRoot,
        [string]$RemoteRef
    )

    $counts = (& git -C $RepoRoot rev-list --left-right --count "HEAD...$RemoteRef" 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to determine git divergence against $RemoteRef."
    }

    $parts = (($counts | Select-Object -First 1) -split '\s+') | Where-Object { $_ }
    if ($parts.Count -lt 2) {
        throw "Unexpected git divergence output for ${RemoteRef}: $counts"
    }

    return @{
        Ahead = [int]$parts[0]
        Behind = [int]$parts[1]
    }
}

function Test-GitPathHasChanges {
    param(
        [string]$RepoRoot,
        [string[]]$Paths
    )

    foreach ($path in $Paths) {
        $status = (& git -C $RepoRoot status --porcelain -- $path) -join "`n"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to inspect git changes for path '$path'."
        }
        if ($status) {
            return $true
        }
    }

    return $false
}

function Test-HeadCommitTouchesPaths {
    param(
        [string]$RepoRoot,
        [string[]]$Paths
    )

    if (-not $Paths -or $Paths.Count -eq 0) {
        return $false
    }

    $pathArgs = @('show', '--pretty=format:', '--name-only', 'HEAD', '--') + $Paths
    $files = (& git -C $RepoRoot @pathArgs 2>$null) | Where-Object { $_ -and $_.Trim() }
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to inspect HEAD commit paths.'
    }

    return [bool]($files | Select-Object -First 1)
}

function Assert-SafeArtifactPush {
    param(
        [string]$RepoRoot,
        [string]$Remote,
        [string]$Branch,
        [string[]]$ArtifactPaths,
        [switch]$AllowArtifactRebase,
        [switch]$UseHeadCommit
    )

    $remoteRef = "$Remote/$Branch"
    Invoke-GitCommand -RepoRoot $RepoRoot -Arguments @('fetch', $Remote, $Branch) -StepName "Fetch $remoteRef before publish"
    $divergence = Get-GitAheadBehind -RepoRoot $RepoRoot -RemoteRef $remoteRef
    if ($divergence.Behind -le 0) {
        return
    }

    $hasArtifactChanges = if ($UseHeadCommit.IsPresent) {
        Test-HeadCommitTouchesPaths -RepoRoot $RepoRoot -Paths $ArtifactPaths
    }
    else {
        Test-GitPathHasChanges -RepoRoot $RepoRoot -Paths $ArtifactPaths
    }

    if ($hasArtifactChanges -and -not $AllowArtifactRebase.IsPresent) {
        throw @(
            "Remote branch '$remoteRef' moved by $($divergence.Behind) commit(s) while this run changed generated artifact paths.",
            'Abort publish and rerun the workflow on the updated branch, or pass -AllowArtifactRebase to force the old rebase behavior.'
        ) -join ' '
    }
}

function Sync-GitBranchBeforePush {
    param(
        [string]$RepoRoot,
        [string]$Remote,
        [string]$Branch,
        [string[]]$ArtifactPaths,
        [switch]$AllowArtifactRebase
    )

    Assert-SafeArtifactPush -RepoRoot $RepoRoot -Remote $Remote -Branch $Branch -ArtifactPaths $ArtifactPaths -AllowArtifactRebase:$AllowArtifactRebase -UseHeadCommit
    Invoke-GitCommand -RepoRoot $RepoRoot -Arguments @('rebase', "$Remote/$Branch") -StepName "Rebase onto $Remote/$Branch before push"
}

function Sync-GitBranchBeforeRun {
    param(
        [string]$RepoRoot,
        [string]$Remote,
        [string]$Branch
    )

    $remoteRef = "$Remote/$Branch"
    Invoke-GitCommand -RepoRoot $RepoRoot -Arguments @('fetch', $Remote, $Branch) -StepName "Fetch $remoteRef before workflow run"
    $divergence = Get-GitAheadBehind -RepoRoot $RepoRoot -RemoteRef $remoteRef
    if ($divergence.Behind -le 0) {
        return
    }

    Invoke-GitCommand -RepoRoot $RepoRoot -Arguments @('rebase', $remoteRef) -StepName "Rebase onto $remoteRef before workflow run"
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
$artifactPaths = @('data/daily', 'data/eval')

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
$pushBranch = ''
if ($GitPush -eq 'on') {
    $initialGitStatus = (& git -C $repoRoot status --porcelain) -join "`n"
    if ($initialGitStatus -and -not $AllowDirtyGit.IsPresent) {
        throw 'Git working tree is already dirty. Re-run with -AllowDirtyGit to permit a final auto-push.'
    }

    $pushBranch = if ($GitPushBranch) { $GitPushBranch } else { Get-GitCurrentBranch -RepoRoot $repoRoot }
    if (-not $initialGitStatus) {
        Sync-GitBranchBeforeRun -RepoRoot $repoRoot -Remote $GitPushRemote -Branch $pushBranch
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
    Assert-SafeArtifactPush -RepoRoot $repoRoot -Remote $GitPushRemote -Branch $pushBranch -ArtifactPaths $artifactPaths -AllowArtifactRebase:$AllowArtifactRebase
    Invoke-GitCommand -RepoRoot $repoRoot -Arguments @('add', '-A') -StepName 'Stage workflow outputs'

    $postAddStatus = (& git -C $repoRoot status --porcelain) -join "`n"
    if (-not $postAddStatus) {
        Write-Host 'No git changes detected after the workflow run.'
    }
    else {
        Invoke-GitCommand -RepoRoot $repoRoot -Arguments @('commit', '-m', $commitMessage) -StepName 'Commit workflow outputs'
        Sync-GitBranchBeforePush -RepoRoot $repoRoot -Remote $GitPushRemote -Branch $pushBranch -ArtifactPaths $artifactPaths -AllowArtifactRebase:$AllowArtifactRebase
        $pushArgs = @('push', $GitPushRemote, $pushBranch)
        Invoke-GitCommand -RepoRoot $repoRoot -Arguments $pushArgs -StepName 'Push workflow outputs'
    }
}

Write-Host ''
Write-Host 'End-to-end daily update completed.'
Write-Host "  Reconciled prior day: $(if ($SkipPriorReconcile.IsPresent) { 'skipped' } else { $reconcileDate })"
Write-Host "  Built current day:    $Date"
Write-Host "  Built next day:       $resolvedNextDate"