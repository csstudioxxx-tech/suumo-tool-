"""
config.py
---------
定数・設定値をまとめたモジュール。

マジックナンバーや繰り返し使う文字列はここに集約し、
仕様変更時はこのファイルだけ見ればよいようにする。
"""
from __future__ import annotations

from pathlib import Path
from typing import Final

# ---------------------------------------------------------------------
# HTTP設定
# ---------------------------------------------------------------------
# SUUMO側にブロックされないよう、一般的なブラウザ UA を用いる。
USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# 連続アクセス間隔(秒)。要件により 10 秒以上。
REQUEST_INTERVAL_SEC: Final[float] = 10.0

# 1 リクエストあたりのタイムアウト(秒)
REQUEST_TIMEOUT_SEC: Final[float] = 30.0

# リトライ回数とリトライ前待機時間(秒)
MAX_RETRY: Final[int] = 3
RETRY_BACKOFF_SEC: Final[float] = 15.0

# ---------------------------------------------------------------------
# SUUMO
# ---------------------------------------------------------------------
SUUMO_BASE: Final[str] = "https://suumo.jp"

# ---------------------------------------------------------------------
# 出力カラム(仕様通りの 7 列)
# ---------------------------------------------------------------------
SHEET_COLUMNS = [
    "物件管理コード",   # ← 追加 (一番上推奨)
    "物件名称",
    "住所",
    "築年月",
    "構造",
    "総戸数",
    "住所予測",
    "予測住所の郵便番号",
    "予測住所のGoogle Map URL",
    "要手動確認",
    "備考",
]

# ---------------------------------------------------------------------
# ログ
# ---------------------------------------------------------------------
BASE_DIR: Final[Path] = Path(__file__).resolve().parent
LOG_DIR: Final[Path] = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# 住所予測プロンプトと結果を受け渡す作業ディレクトリ
CLAUDE_QUEUE_DIR: Final[Path] = BASE_DIR / "claude_queue"
CLAUDE_QUEUE_DIR.mkdir(exist_ok=True)

# Google Sheets 認証ファイル
CREDENTIALS_PATH: Final[Path] = BASE_DIR / "credentials" / "service_account.json"

# ---------------------------------------------------------------------
# ページネーション
# ---------------------------------------------------------------------
# 無限ループ防止のハードリミット
MAX_PAGE_HARD_LIMIT: Final[int] = 200

# ---------------------------------------------------------------------
# 住所完全性判定
# ---------------------------------------------------------------------
# 枝番(◯番◯号 / ◯-◯) があれば complete とみなす
COMPLETE_ADDRESS_PATTERNS: Final[list[str]] = [
    r"\d+[-－‐ー]\d+",
    r"\d+番地?\s*\d*\s*号?",
]
