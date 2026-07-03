# =====================================================
# git_auto_sync.ps1 を1時間おきに自動実行する
# Windowsタスクスケジューラ登録スクリプト
#
# post-commitフックでの自動pushが失敗した場合の保険として、
# ローカルとGitHubの差分を定期的に検知して同期する。
#
# 使い方: PowerShell を「管理者として実行」してから
#   .\setup_git_sync_task.ps1
# =====================================================

$TaskName   = "DataCenterCoolingGitSync"
$RepoDir    = "C:\Users\shige\Desktop\Claude_週次レポート\datacenter-cooling-news"
$ScriptPath = "$RepoDir\git_auto_sync.ps1"

# 既存タスクがあれば削除してから再登録
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "既存タスク '$TaskName' を削除しました"
}

# アクション: powershell -File git_auto_sync.ps1
$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"" `
    -WorkingDirectory $RepoDir

# トリガー: 1時間おきに無期限リピート
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

# 設定: ネットワーク接続時のみ実行、バッテリー時も実行
$Settings = New-ScheduledTaskSettingsSet `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

# 現在のユーザーで登録
$Principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description "データセンター冷却ニュース: ローカルとGitHubのコミットを1時間おきに自動同期（post-commitフックの保険）" `
    -Force

Write-Host ""
Write-Host "✅ タスク '$TaskName' を登録しました"
Write-Host "   実行スケジュール: 1時間おき"
Write-Host "   スクリプト: $ScriptPath"
Write-Host ""
Write-Host "確認: Get-ScheduledTask -TaskName '$TaskName' | Format-List"
Write-Host "手動実行: Start-ScheduledTask -TaskName '$TaskName'"

