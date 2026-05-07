"""
sheets.py
---------
Google スプレッドシート出力層。

- サービスアカウント認証
- シート自動生成(連番対応)
- ヘッダー出力
- 行追加
- スタイル適用 (ヘッダー強調 + 1行目固定 + 列幅 + チェックボックス)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional

import gspread
from google.oauth2.service_account import Credentials

from config import CREDENTIALS_PATH, SHEET_COLUMNS
from sheet_name_builder import build_sheet_name


GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ---------------------------------------------------------------------
# スタイル定義 (新規シート生成時に適用)
# ---------------------------------------------------------------------
# ヘッダー行の背景色 (柔らかい青系)
HEADER_BG_COLOR = {"red": 0.20, "green": 0.40, "blue": 0.65}
# ヘッダー行の文字色 (白)
HEADER_TEXT_COLOR = {"red": 1.0, "green": 1.0, "blue": 1.0}

# 列ごとの推奨幅 (px)。SHEET_COLUMNS のラベル名でルックアップ。
# 知らないラベルにはデフォルト 150px が当たる。
_COLUMN_WIDTHS_BY_NAME: dict[str, int] = {
    "物件名": 220,
    "SUUMO住所": 230,
    "築年月": 100,
    "構造": 90,
    "予測住所": 260,
    "郵便番号": 100,
    "GMap URL": 220,
    "要手動確認": 110,
    "備考": 320,
}
_DEFAULT_COL_WIDTH = 150

# データ検証で「チェックボックス化」したい列のラベル
_CHECKBOX_COLUMN_NAMES = ("要手動確認",)


class SheetsError(Exception):
    pass


class SheetsClient:
    """gspread を薄くラップしたクライアント。

    認証情報の読込は 2 系統に対応:
    - credentials_info (dict) を渡す → Streamlit Cloud 用 (st.secrets から)
    - credentials_path (Path) を渡す → ローカル実行用 (デフォルト)
    """

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_path: Optional[Path] = CREDENTIALS_PATH,
        credentials_info: Optional[dict] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._spreadsheet_id = spreadsheet_id
        self._logger = logger or logging.getLogger(__name__)

        # 認証情報の決定: credentials_info (dict) を優先、なければファイルから
        if credentials_info:
            try:
                creds = Credentials.from_service_account_info(
                    credentials_info, scopes=GOOGLE_SCOPES
                )
            except Exception as exc:
                raise SheetsError(
                    f"サービスアカウント情報 (dict) からの認証失敗: {exc}"
                )
        elif credentials_path and credentials_path.exists():
            creds = Credentials.from_service_account_file(
                str(credentials_path), scopes=GOOGLE_SCOPES
            )
        else:
            raise SheetsError(
                f"Google API 認証情報が見つかりません。\n"
                f"ローカル実行: credentials/service_account.json を配置してください。\n"
                f"Streamlit Cloud: st.secrets['gcp_service_account'] を設定してください。"
            )

        self._gc = gspread.authorize(creds)
        self._spreadsheet = self._gc.open_by_key(spreadsheet_id)
        self._worksheet: Optional[gspread.Worksheet] = None

    # ------------------------------------------------------------------
    # シート生成
    # ------------------------------------------------------------------
    def list_worksheet_titles(self) -> list[str]:
        return [ws.title for ws in self._spreadsheet.worksheets()]

    def create_sheet_for_region(
        self,
        pref: str,
        city: str,
        fallback_id: str = "",
    ) -> str:
        """都道府県/市区町村 からシート名を生成して新規シートを作る。
        生成後、固有スタイル (ヘッダー強調・列幅・チェックボックス) も適用する。
        """
        existing = self.list_worksheet_titles()
        sheet_name = build_sheet_name(
            pref=pref,
            city=city,
            fallback_id=fallback_id,
            existing_names=existing,
        )
        # 列数は SHEET_COLUMNS に合わせる(余裕を持って +2)
        ws = self._spreadsheet.add_worksheet(
            title=sheet_name,
            rows=1000,
            cols=max(10, len(SHEET_COLUMNS) + 2),
        )
        ws.append_row(SHEET_COLUMNS, value_input_option="USER_ENTERED")
        self._worksheet = ws

        # 固有スタイルを適用 (失敗してもデータ書込は継続)
        try:
            self._apply_sheet_style(ws)
            self._logger.info("シート生成 + スタイル適用: %s", sheet_name)
        except Exception as exc:
            self._logger.warning("スタイル適用失敗 (続行): %s", exc)
            self._logger.info("シート生成: %s", sheet_name)
        return sheet_name

    # ------------------------------------------------------------------
    # スタイル適用 (ヘッダー強調 + 1行目固定 + 列幅 + チェックボックス)
    # ------------------------------------------------------------------
    def _apply_sheet_style(self, ws: gspread.Worksheet) -> None:
        """新規シートに固有スタイルを batch_update で一括適用。
        - ヘッダー行: 背景色 + 太字 + 中央揃え + 白文字
        - 1行目を固定 (常時表示)
        - 列幅をラベルごとに調整
        - 「要手動確認」列をチェックボックス化 (データ検証 BOOLEAN)
        """
        sheet_id = ws.id
        n_cols = len(SHEET_COLUMNS)
        max_rows = ws.row_count or 1000

        requests: list[dict] = []

        # 1) ヘッダー行のフォーマット
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": n_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": HEADER_BG_COLOR,
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "foregroundColor": HEADER_TEXT_COLOR,
                            "bold": True,
                            "fontSize": 11,
                        },
                    }
                },
                "fields": (
                    "userEnteredFormat("
                    "backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
                ),
            }
        })

        # 2) 1行目を固定 (フリーズ)
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        })

        # 3) 列幅
        for idx, label in enumerate(SHEET_COLUMNS):
            width = _COLUMN_WIDTHS_BY_NAME.get(label, _DEFAULT_COL_WIDTH)
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": idx,
                        "endIndex": idx + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            })

        # 4) 「要手動確認」列をチェックボックス化
        for label in _CHECKBOX_COLUMN_NAMES:
            if label not in SHEET_COLUMNS:
                continue
            col_idx = SHEET_COLUMNS.index(label)
            requests.append({
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,        # ヘッダーは除外
                        "endRowIndex": max_rows,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "strict": True,
                        "showCustomUi": True,  # ← Google Sheets UIでチェックボックス表示
                    },
                }
            })
            # チェックボックス列は中央揃え
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": max_rows,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {"horizontalAlignment": "CENTER"}
                    },
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            })

        # 5) 行全体を上揃えにして見やすく
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": max_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": n_cols,
                },
                "cell": {
                    "userEnteredFormat": {"verticalAlignment": "TOP"}
                },
                "fields": "userEnteredFormat.verticalAlignment",
            }
        })

        if requests:
            self._spreadsheet.batch_update({"requests": requests})

    # ------------------------------------------------------------------
    # 行追加
    # ------------------------------------------------------------------
    def append_rows(self, rows: Iterable[list[str]]) -> None:
        if self._worksheet is None:
            raise SheetsError(
                "シートが未選択です。先に create_sheet_for_region を呼んでください。"
            )
        rows = list(rows)
        if not rows:
            return
        self._worksheet.append_rows(rows, value_input_option="USER_ENTERED")

    def append_row(self, row: list[str]) -> None:
        self.append_rows([row])

    # ------------------------------------------------------------------
    # 状態
    # ------------------------------------------------------------------
    @property
    def worksheet_title(self) -> str:
        return self._worksheet.title if self._worksheet else ""
