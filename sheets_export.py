"""
Google Driveの共有フォルダに置かれた日次ニュースファイル(TSV)を
Google Sheetsの記事DBへ集約するスクリプト。
GitHub Actions から実行。環境変数 GOOGLE_CREDENTIALS にサービスアカウントJSONを設定。

使い方:
    # GitHub Actions から自動実行
    # ローカルテスト:
    #   export GOOGLE_CREDENTIALS=$(cat service_account.json)
    #   python sheets_export.py
"""

import io
import json
import os
import sys

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ===== 設定 =====
SPREADSHEET_ID = "1KaQdI4KJG-r0_V5qPed0qxkKUrnHV5C_6xv3n_lTPec"
SHEET_NAME = "記事DB"
DRIVE_FOLDER_ID = "1lnOka764NArksnNZ-C5G0g3SL3_YJaFW"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "レポート日", "セクション", "No", "タイトル", "タイトル(日本語訳)",
    "媒体/Source", "記事日付", "URL", "概要", "概要(日本語訳)",
]


# ===== URL正規化(重複チェック用) =====
def normalize_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip().lower()
    u = u.removeprefix("https://").removeprefix("http://")
    u = u.removeprefix("www.")
    u = u.rstrip("/")
    if "?" in u:
        base, qs = u.split("?", 1)
        params = [p for p in qs.split("&") if not p.startswith(("utm_", "fbclid", "gclid"))]
        u = base + ("?" + "&".join(params) if params else "")
    return u


def download_text(drive, file_id: str) -> str:
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8", errors="replace")


def parse_tsv_rows(text: str) -> list[list[str]]:
    lines = [ln for ln in text.replace("\r\n", "\n").split("\n") if ln.strip()]
    if not lines:
        return []
    # 先頭行がヘッダーらしければ除外
    if lines[0].split("\t")[0].strip() in ("レポート日", "report_date"):
        lines = lines[1:]
    rows = []
    for ln in lines:
        cols = ln.split("\t")
        cols = (cols + [""] * len(HEADERS))[: len(HEADERS)]
        rows.append(cols)
    return rows


def main():
    print("=" * 50)
    print("sheets_export (Drive連携版) 開始")
    print("=" * 50)

    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        print("ERROR: 環境変数 GOOGLE_CREDENTIALS が設定されていません")
        sys.exit(1)

    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    print(f"サービスアカウント: {creds.service_account_email}")

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    print(f"スプレッドシート接続: {spreadsheet.title}")

    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS)
        print(f"シート '{SHEET_NAME}' を新規作成しました")

    drive = build("drive", "v3", credentials=creds)

    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and trashed = false "
        f"and mimeType != 'application/vnd.google-apps.folder' "
        f"and mimeType != 'application/vnd.google-apps.spreadsheet'"
    )
    resp = drive.files().list(q=query, fields="files(id, name, mimeType)", pageSize=100).execute()
    files = resp.get("files", [])
    print(f"\nDriveフォルダ内の未取込ファイル: {len(files)}件")
    for f in files:
        print(f"  - {f['name']} ({f['id']})")

    if not files:
        print("\n取り込み対象なし。終了します。")
        return

    # 既存URL収集(重複チェック)
    existing_urls: set[str] = set()
    data = ws.get_all_values()
    if len(data) > 1:
        header = data[0]
        if "URL" in header:
            url_idx = header.index("URL")
            for row in data[1:]:
                if url_idx < len(row) and row[url_idx]:
                    existing_urls.add(normalize_url(row[url_idx]))
    print(f"既存URL数: {len(existing_urls)}件")

    all_new_rows: list[list[str]] = []
    processed_ids: list[str] = []
    skipped = 0

    for f in files:
        try:
            text = download_text(drive, f["id"])
        except Exception as e:
            print(f"  ERROR: {f['name']} の読み込みに失敗: {e}")
            continue

        for row in parse_tsv_rows(text):
            url_norm = normalize_url(row[7] if len(row) > 7 else "")
            if url_norm:
                if url_norm in existing_urls:
                    skipped += 1
                    continue
                existing_urls.add(url_norm)
            all_new_rows.append(row)

        processed_ids.append(f["id"])

    print(f"\n重複スキップ: {skipped}件 / 新規追加: {len(all_new_rows)}件")

    if all_new_rows:
        ws.append_rows(all_new_rows, value_input_option="USER_ENTERED")
        print(f"シート '{SHEET_NAME}' の最終行に {len(all_new_rows)}件を追加しました")

    for fid in processed_ids:
        try:
            drive.files().delete(fileId=fid).execute()
        except Exception as e:
            print(f"  WARNING: ファイル削除に失敗 ({fid}): {e}")
    print(f"取込済みファイル {len(processed_ids)}件を削除しました")

    print(f"\n完了: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
