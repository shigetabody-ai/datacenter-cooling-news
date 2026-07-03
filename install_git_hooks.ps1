# =====================================================
# hooks/ 配下のgit hookを .git/hooks にインストールする
# （.git/hooks はバージョン管理対象外のため、クローンごとに実行が必要）
#
# 使い方: .\install_git_hooks.ps1
# =====================================================

$RepoDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$SrcDir    = Join-Path $RepoDir "hooks"
$HooksDir  = Join-Path $RepoDir ".git\hooks"

if (-not (Test-Path $HooksDir)) {
    Write-Error "$HooksDir が見つかりません。gitリポジトリのルートで実行してください。"
    exit 1
}

Get-ChildItem -Path $SrcDir -File | ForEach-Object {
    $dest = Join-Path $HooksDir $_.Name
    Copy-Item -Path $_.FullName -Destination $dest -Force
    Write-Host "インストール: $($_.Name) -> $dest"
}

Write-Host ""
Write-Host "✅ git hooksのインストールが完了しました"

