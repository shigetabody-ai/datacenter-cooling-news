# =====================================================
# git_auto_sync.ps1
#
# post-commit フックが何らかの理由（ネットワーク断など）で
# pushに失敗した場合の保険として、リモートとの差分を
# 定期的に検知して同期する。
# =====================================================

$RepoDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir   = Join-Path $RepoDir "logs"
$LogFile  = Join-Path $LogDir "git_autosync_task.log"
$ts       = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

Set-Location $RepoDir

try {
    git fetch origin *>> $LogFile

    $behind = [int](git rev-list --count HEAD..origin/master)
    if ($behind -gt 0) {
        git merge origin/master --no-edit -m "Auto-sync merge (scheduled task)" *>> $LogFile
    }

    $ahead = [int](git rev-list --count origin/master..HEAD)
    if ($ahead -gt 0) {
        git push origin master *>> $LogFile
    }

    "[$ts] sync check complete (ahead=$ahead behind=$behind)" | Out-File -Append -Encoding utf8 $LogFile
}
catch {
    "[$ts] ERROR: $_" | Out-File -Append -Encoding utf8 $LogFile
}

