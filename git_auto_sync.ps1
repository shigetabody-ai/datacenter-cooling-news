# =====================================================
# git_auto_sync.ps1
#
# 1) post-commit フックが何らかの理由（ネットワーク断など）で
#    pushに失敗した場合の保険として、リモートとの差分を
#    定期的に検知して同期する。
# 2) クラウド側の日次レポート生成ルーチンがmasterに直接push
#    できず、代わりにトピックブランチ（例: claude/xxxxx）を
#    作ってしまうケースの保険として、master未マージのリモート
#    ブランチを検出し、自動でmasterへマージ・pushする。
#    （2026-07-06: reports/20260703〜05 がmasterに反映されず
#      別ブランチに孤立していた事象への恒久対応として追加）
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
    git fetch origin --prune 2>&1 | Out-File -Append -Encoding utf8 $LogFile

    $behind = [int](git rev-list --count HEAD..origin/master)
    if ($behind -gt 0) {
        git merge origin/master --no-edit -m "Auto-sync merge (scheduled task)" 2>&1 | Out-File -Append -Encoding utf8 $LogFile
    }

    # master に未マージのリモートブランチ（自分自身の作業ブランチを除く）を
    # 検出し、コンフリクトが無ければ自動でmasterへ取り込む
    $remoteBranches = git branch -r --no-merged origin/master |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -and $_ -notmatch '->' -and $_ -ne 'origin/master' }

    foreach ($branch in $remoteBranches) {
        "[$ts] 未マージブランチを検出: $branch - マージを試行" | Out-File -Append -Encoding utf8 $LogFile
        git merge $branch --no-edit -m "Auto-merge $branch (scheduled task: unmerged branch recovery)" 2>&1 | Out-File -Append -Encoding utf8 $LogFile
        if ($LASTEXITCODE -ne 0) {
            "[$ts] ERROR: $branch のマージに失敗（コンフリクトの可能性）。手動対応が必要です" | Out-File -Append -Encoding utf8 $LogFile
            git merge --abort 2>&1 | Out-File -Append -Encoding utf8 $LogFile
        }
        else {
            "[$ts] $branch のマージに成功" | Out-File -Append -Encoding utf8 $LogFile
        }
    }

    $ahead = [int](git rev-list --count origin/master..HEAD)
    if ($ahead -gt 0) {
        git push origin master 2>&1 | Out-File -Append -Encoding utf8 $LogFile
    }

    "[$ts] sync check complete (ahead=$ahead behind=$behind, unmerged_branches=$($remoteBranches.Count))" | Out-File -Append -Encoding utf8 $LogFile
}
catch {
    "[$ts] ERROR: $_" | Out-File -Append -Encoding utf8 $LogFile
}


