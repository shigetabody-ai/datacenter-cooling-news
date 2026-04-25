"""
日次ニュースレポートをGoogle Sheetsに週次集約するスクリプト
GitHub Actions で実行。環境変数 GOOGLE_CREDENTIALS にサービスアカウントJSONを設定。

使い方:
    # GitHub Actions から自動実行（毎週土曜 12:00 JST）
    # ローカルテスト:
    #   export GOOGLE_CREDENTIALS=$(cat service_account.json)
    #   python sheets_export.py
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

# ===== 設定 =====
SPREADSHEET_ID = "1KaQdI4KJG-r0_V5qPed0qxkKUrnHV5C_6xv3n_lTPec"
REPORTS_DIR = Path("reports")
DAYS = 7

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "レポート日", "セクション", "No", "タイトル", "タイトル(日本語訳)",
    "媒体/Source", "記事日付", "URL", "概要", "概要(日本語訳)",
]
KEYS = [
    "report_date", "section", "no", "title", "title_ja",
    "source", "date", "url", "summary", "summary_ja",
]


# ===== パーサー =====
ARTICLE_SPLITTER = re.compile(r"^-{10,}\s*$", re.MULTILINE)
ARTICLE_START = re.compile(r"^(\d+)\.\s+(.+)$")


def parse_report(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    m = re.search(r"(\d{8})", path.name)
    report_date = datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d") if m else ""

    articles = []
    parts = re.split(r"^【(.+?)】\s*$", text, flags=re.MULTILINE)

    for i in range(1, len(parts), 2):
        section_name = parts[i].strip()
        if not any(kw in section_name for kw in ("ニュース", "News")):
            continue
        section_body = parts[i + 1] if i + 1 < len(parts) else ""
        cleaned = ARTICLE_SPLITTER.sub("", section_body)

        lines = cleaned.split("\n")
        chunks, current = [], []
        for ln in lines:
            if ARTICLE_START.match(ln):
                if current:
                    chunks.append(current)
                current = [ln]
            elif current or ln.strip():
                current.append(ln)
        if current:
            chunks.append(current)

        for chunk in chunks:
            art = parse_chunk("\n".join(chunk), section_name, report_date)
            if art:
                articles.append(art)

    return articles


def parse_chunk(chunk: str, section: str, report_date: str) -> dict | None:
    lines = [ln.rstrip() for ln in chunk.strip().split("\n") if ln.strip()]
    if not lines:
        return None
    m = ARTICLE_START.match(lines[0])
    if not m:
        return None

    data = {
        "report_date": report_date, "section": section, "no": int(m.group(1)),
        "title": m.group(2).strip(), "title_ja": "", "source": "", "date": "",
        "url": "", "summary": "", "summary_ja": "",
    }

    ck, buf = None, []

    def flush():
        if ck and buf:
            data[ck] = " ".join(buf).strip()

    for ln in lines[1:]:
        s = ln.strip()
        if not data["title_ja"] and s.startswith("（") and s.endswith("）"):
            data["title_ja"] = s[1:-1]
            continue
        if s.startswith("【日本語訳】"):
            flush()
            ck = "summary_ja"
            buf = [s.replace("【日本語訳】", "").strip()]
            continue
        kv = re.match(r"^(媒体|Source|日付|Date|URL|概要|Summary)\s*[::]\s*(.*)$", s)
        if kv:
            flush()
            ck = {
                "媒体": "source", "Source": "source",
                "日付": "date",   "Date": "date",
                "URL": "url",
                "概要": "summary", "Summary": "summary",
            }[kv.group(1)]
            buf = [kv.group(2).strip()]
        else:
            buf.append(s)
    flush()
    return data


# ===== URL正規化 =====
def normalize_url(url: str) -> str:
    if not url:
        return ""
    u = url.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    u = u.rstrip("/")
    if "?" in u:
        base, qs = u.split("?", 1)
        params = [p for p in qs.split("&") if not p.startswith(("utm_", "fbclid", "gclid"))]
        u = base + ("?" + "&".join(params) if params else "")
    return u


# ===== メイン =====
def main():
    print("=" * 50)
    print(f"sheets_export 開始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Google認証
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        print("ERROR: 環境変数 GOOGLE_CREDENTIALS が設定されていません")
        sys.exit(1)

    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    print(f"スプレッドシート接続: {spreadsheet.title}")

    # 直近7日のレポートを収集
    today = datetime.now().date()
    start = today - timedelta(days=DAYS - 1)

    report_files = []
    for p in sorted(REPORTS_DIR.glob("*_report.txt")):
        m = re.search(r"(\d{8})", p.name)
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y%m%d").date()
        if start <= d <= today:
            report_files.append(p)

    if not report_files:
        print(f"直近{DAYS}日分のレポートが見つかりませんでした")
        sys.exit(0)

    print(f"\n対象ファイル: {len(report_files)}件 "
          f"({report_files[0].name} 〜 {report_files[-1].name})")

    # 記事をパース
    all_articles = []
    for p in report_files:
        arts = parse_report(p)
        all_articles.extend(arts)
        print(f"  {p.name}: {len(arts)}件")
    print(f"  合計: {len(all_articles)}件")

    # 既存URLを収集（全シート横断で重複チェック）
    existing_urls: set[str] = set()
    for ws in spreadsheet.worksheets():
        try:
            data = ws.get_all_values()
            if not data:
                continue
            header = data[0]
            if "URL" not in header:
                continue
            url_idx = header.index("URL")
            for row in data[1:]:
                if url_idx < len(row) and row[url_idx]:
                    existing_urls.add(normalize_url(row[url_idx]))
        except Exception as e:
            print(f"  警告: シート '{ws.title}' の読み込みエラー: {e}")

    print(f"\n既存URL数: {len(existing_urls)}件")

    # 重複除去
    new_articles, skipped, seen = [], 0, set()
    for art in all_articles:
        url_norm = normalize_url(art.get("url", ""))
        if not url_norm:
            new_articles.append(art)  # URLなしは無条件追加
            continue
        if url_norm in existing_urls or url_norm in seen:
            skipped += 1
            continue
        seen.add(url_norm)
        new_articles.append(art)

    print(f"重複スキップ: {skipped}件 / 新規追記対象: {len(new_articles)}件")

    if not new_articles:
        print("\n追加すべき新規記事がありません（全て既登録）")
        sys.exit(0)

    # 週次シート名（ISO週番号）
    year, week, _ = datetime.now().isocalendar()
    sheet_name = f"{year}-W{week:02d}"

    # シートの取得または作成
    try:
        ws = spreadsheet.worksheet(sheet_name)
        print(f"\n既存シート '{sheet_name}' に追記します")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=len(HEADERS))
        ws.append_row(HEADERS)
        print(f"\n新規シート '{sheet_name}' を作成しました")

    # データ書き込み（バッチ）
    rows = [[str(art.get(k, "")) for k in KEYS] for art in new_articles]
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    print(f"\n完了: シート '{sheet_name}' に {len(new_articles)}件を追記")
    print(f"URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


if __name__ == "__main__":
    main()
