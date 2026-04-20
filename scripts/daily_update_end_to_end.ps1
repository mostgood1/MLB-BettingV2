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
    [ValidateSet('auto', 'on', 'off')]
    [string]$SkipStartedGames = 'auto',
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

function Normalize-GitPaths {
    param(
        [string[]]$Paths
    )

    $seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $normalized = [System.Collections.Generic.List[string]]::new()
    foreach ($path in @($Paths)) {
        $raw = [string]$path
        if (-not $raw) {
            continue
        }
        $value = $raw.Trim().Replace('\', '/')
        if (-not $value) {
            continue
        }
        if ($seen.Add($value)) {
            $normalized.Add($value) | Out-Null
            }
        }

        return $normalized.ToArray()
    }

    function Get-WorkingTreeChangedPaths {
        param(
            [string]$RepoRoot,
        [string[]]$Paths = @()
        )

    $collected = [System.Collections.Generic.List[string]]::new()
    $pathArgs = if ($Paths -and $Paths.Count -gt 0) { @('--') + $Paths } else { @() }

    $diffArgs = @('diff', '--name-only') + $pathArgs
    $tracked = (& git -C $RepoRoot @diffArgs 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to inspect working tree changes.'
    }
    foreach ($path in @($tracked)) {
        $collected.Add([string]$path) | Out-Null
    }

    $cachedArgs = @('diff', '--cached', '--name-only') + $pathArgs
    $cached = (& git -C $RepoRoot @cachedArgs 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to inspect staged changes.'
    }
    foreach ($path in @($cached)) {
        $collected.Add([string]$path) | Out-Null
    }

    $untrackedArgs = @('ls-files', '--others', '--exclude-standard') + $pathArgs
    $untracked = (& git -C $RepoRoot @untrackedArgs 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to inspect untracked changes.'
    }
    foreach ($path in @($untracked)) {
        $collected.Add([string]$path) | Out-Null
    }

    return Normalize-GitPaths -Paths $collected.ToArray()
}

function Get-StagedChangedPaths {
    param(
        [string]$RepoRoot,
        [string[]]$Paths = @()
    )

    $pathArgs = if ($Paths -and $Paths.Count -gt 0) { @('--') + $Paths } else { @() }
    $cachedArgs = @('diff', '--cached', '--name-only') + $pathArgs
    $cached = (& git -C $RepoRoot @cachedArgs 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to inspect staged changes.'
    }

    return Normalize-GitPaths -Paths $cached
}

function Test-PathsWithinRoots {
    param(
        [string[]]$Paths,
        [string[]]$Roots
    )

    if (-not $Paths -or $Paths.Count -eq 0) {
        return $true
    }

    $normalizedRoots = Normalize-GitPaths -Paths $Roots
    foreach ($path in $Paths) {
        $normalizedPath = ([string]$path).Trim().Replace('\\', '/')
        if (-not $normalizedPath) {
            continue
        }

        $matchesRoot = $false
        foreach ($root in $normalizedRoots) {
            if ($normalizedPath.Equals($root, [System.StringComparison]::OrdinalIgnoreCase) -or $normalizedPath.StartsWith("$root/", [System.StringComparison]::OrdinalIgnoreCase)) {
                $matchesRoot = $true
                break
            }
        }

        if (-not $matchesRoot) {
            return $false
        }
    }

    return $true
}

function Clear-ManagedArtifactPaths {
    param(
        [string]$RepoRoot,
        [string[]]$ArtifactPaths
    )

    if (-not $ArtifactPaths -or $ArtifactPaths.Count -eq 0) {
        return
    }

    Write-Host 'Clearing previously generated artifact changes before syncing with remote.'

    $restoreArgs = @('restore', '--source=HEAD', '--staged', '--worktree', '--') + $ArtifactPaths
    & git -C $RepoRoot @restoreArgs 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to reset tracked generated artifact files before sync.'
    }

    $cleanArgs = @('clean', '-fd', '--') + $ArtifactPaths
    & git -C $RepoRoot @cleanArgs 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to remove untracked generated artifact files before sync.'
    }
}

function Get-HeadCommitChangedPaths {
    param(
        [string]$RepoRoot,
        [string[]]$Paths
    )

    if (-not $Paths -or $Paths.Count -eq 0) {
        return @()
    }

    $pathArgs = @('show', '--pretty=format:', '--name-only', 'HEAD', '--') + $Paths
    $files = (& git -C $RepoRoot @pathArgs 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to inspect HEAD commit paths.'
    }

    return Normalize-GitPaths -Paths $files
}

function Get-GitMergeBase {
    param(
        [string]$RepoRoot,
        [string]$LeftRef,
        [string]$RightRef
    )

    $mergeBase = (& git -C $RepoRoot merge-base $LeftRef $RightRef 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to determine merge-base between $LeftRef and $RightRef."
    }

    return ($mergeBase | Select-Object -First 1).Trim()
}

function Get-CommitRangeChangedPaths {
    param(
        [string]$RepoRoot,
        [string]$FromRef,
        [string]$ToRef,
        [string[]]$Paths
    )

    if (-not $Paths -or $Paths.Count -eq 0) {
        return @()
    }

    $pathArgs = @('diff', '--name-only', "$FromRef..$ToRef", '--') + $Paths
    $files = (& git -C $RepoRoot @pathArgs 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect path overlap between $FromRef and $ToRef."
    }

    return Normalize-GitPaths -Paths $files
}

function Test-PathSetOverlap {
    param(
        [string[]]$LeftPaths,
        [string[]]$RightPaths
    )

    if (-not $LeftPaths -or $LeftPaths.Count -eq 0 -or -not $RightPaths -or $RightPaths.Count -eq 0) {
        return $false
    }

    $rightLookup = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($path in $RightPaths) {
        [void]$rightLookup.Add([string]$path)
    }

    foreach ($path in $LeftPaths) {
        if ($rightLookup.Contains([string]$path)) {
            return $true
        }
    }

    return $false
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

    $localArtifactPaths = if ($UseHeadCommit.IsPresent) {
        Get-HeadCommitChangedPaths -RepoRoot $RepoRoot -Paths $ArtifactPaths
    }
    else {
        Get-WorkingTreeChangedPaths -RepoRoot $RepoRoot -Paths $ArtifactPaths
    }

    if ($localArtifactPaths.Count -gt 0 -and -not $AllowArtifactRebase.IsPresent) {
        $mergeBase = Get-GitMergeBase -RepoRoot $RepoRoot -LeftRef 'HEAD' -RightRef $remoteRef
        $remoteArtifactPaths = Get-CommitRangeChangedPaths -RepoRoot $RepoRoot -FromRef $mergeBase -ToRef $remoteRef -Paths $ArtifactPaths
        $hasExactOverlap = Test-PathSetOverlap -LeftPaths $localArtifactPaths -RightPaths $remoteArtifactPaths
        if (-not $hasExactOverlap) {
            Write-Host "Remote branch '$remoteRef' moved by $($divergence.Behind) commit(s), but none of those commits touched the generated artifact paths. Proceeding with rebase."
            return
        }

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
    $initialStagedPaths = Get-StagedChangedPaths -RepoRoot $repoRoot
    if ($initialStagedPaths.Count -gt 0 -and -not (Test-PathsWithinRoots -Paths $initialStagedPaths -Roots $artifactPaths)) {
        throw 'Git index already contains staged non-artifact changes. Unstage or commit them before workflow auto-push.'
    }

    $initialGitStatus = (& git -C $repoRoot status --porcelain) -join "`n"
    if ($initialGitStatus) {
        $allChangedPaths = Get-WorkingTreeChangedPaths -RepoRoot $repoRoot
        if (Test-PathsWithinRoots -Paths $allChangedPaths -Roots $artifactPaths) {
            Clear-ManagedArtifactPaths -RepoRoot $repoRoot -ArtifactPaths $artifactPaths
            $initialGitStatus = (& git -C $repoRoot status --porcelain) -join "`n"
        }
    }

    if ($initialGitStatus -and -not $AllowDirtyGit.IsPresent) {
        throw 'Git working tree is already dirty after generated-artifact cleanup. Re-run with -AllowDirtyGit only for intentional non-artifact changes.'
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

$enableSkipStartedGames = $false
switch ($SkipStartedGames) {
    'on' { $enableSkipStartedGames = $true }
    'off' { $enableSkipStartedGames = $false }
    default {
        $enableSkipStartedGames = ($Date -eq (Get-Date).ToString('yyyy-MM-dd'))
    }
}
if ($enableSkipStartedGames) {
    $currentArgs += @('--skip-started-games', 'on')
}

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
    Invoke-GitCommand -RepoRoot $repoRoot -Arguments (@('add', '-A', '--') + $artifactPaths) -StepName 'Stage workflow outputs'

    $stagedArtifactPaths = Get-StagedChangedPaths -RepoRoot $repoRoot -Paths $artifactPaths
    if ($stagedArtifactPaths.Count -eq 0) {
        Write-Host 'No managed artifact changes detected after the workflow run.'
    }
    else {
        $stagedPaths = Get-StagedChangedPaths -RepoRoot $repoRoot
        if (-not (Test-PathsWithinRoots -Paths $stagedPaths -Roots $artifactPaths)) {
            throw 'Git index contains staged non-artifact changes. Aborting auto-commit to avoid mixing manual edits with workflow outputs.'
        }

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