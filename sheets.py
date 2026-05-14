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
# プロフェッショナル感重視: ダーク基調ヘッダー + 縞模様 + 細い罫線
# ---------------------------------------------------------------------
# ヘッダー行: ダーク・スレート (#2D3748) + 白文字
HEADER_BG_COLOR = {"red": 0.176, "green": 0.216, "blue": 0.282}
HEADER_TEXT_COLOR = {"red": 1.0, "green": 1.0, "blue": 1.0}
# 縞模様 (バンディング) のオフホワイト (#F7FAFC)
BAND_ROW_COLOR = {"red": 0.969, "green": 0.980, "blue": 0.988}
# 罫線色 (薄いグレー #E2E8F0)
BORDER_COLOR = {"red": 0.886, "green": 0.910, "blue": 0.941}

# 列ごとの推奨幅 (px)。SHEET_COLUMNS のラベル名でルックアップ。
# config.py 側で別ラベルでも fallback で動くよう、エイリアスも持つ。
_COLUMN_WIDTHS_BY_NAME: dict[str, int] = {
    # 管理番号
    "物件管理コード": 110, "管理コード": 110, "No": 60, "ID": 70,
    # 標準ラベル
    "物件名": 240, "物件名称": 240,
    "SUUMO住所": 250, "住所": 250,
    "築年月": 110,
    "構造": 100,
    "総戸数": 90,
    "予測住所": 280, "住所予測": 280,
    "郵便番号": 110, "予測住所の郵便番号": 130,
    "GMap URL": 220, "予測住所のGoogle Map URL": 220,
    "要手動確認": 110,
    "備考": 320,
    # 再開機能用
    "物件URL": 280, "SUUMO URL": 280,
}
_DEFAULT_COL_WIDTH = 160

# データ検証で「チェックボックス化」したい列のラベル
_CHECKBOX_COLUMN_NAMES = ("要手動確認",)
# 太字にしたい列ラベル (1列目強調)
_BOLD_FIRST_COLUMN_NAMES = ("物件名", "物件名称")


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
        # 次に書き込む行 (1=ヘッダー、2=最初のデータ)
        # データ検証範囲が広く取られると append_row が末尾に飛ぶので、
        # 行位置を明示管理して update() で書く方式にする
        self._next_row: int = 2

    # ------------------------------------------------------------------
    # シート生成
    # ------------------------------------------------------------------
    def list_worksheet_titles(self) -> list[str]:
        return [ws.title for ws in self._spreadsheet.worksheets()]

    # 件数不明時のフォールバック初期行数 と 拡張ステップ
    FALLBACK_INITIAL_ROWS = 100   # SUUMO 件数が取れなかった時の初期行数
    EXPAND_STEP_ROWS = 100         # 足りなくなった時に追加する行数 (安全網)

    def create_sheet_for_region(
        self,
        pref: str,
        city: str,
        fallback_id: str = "",
        expected_data_rows: Optional[int] = None,
    ) -> str:
        """都道府県/市区町村 からシート名を生成して新規シートを作る。
        生成後、固有スタイル (ヘッダー強調・列幅・チェックボックス) も適用する。

        行数決定ロジック:
        - expected_data_rows (= SUUMO 全体件数) があれば、その+ヘッダー1+少バッファ で確保。
          → スタイル (縞模様/チェックボックス) も最初から全行に効く
          → 拡張処理が基本走らないので API 呼び出しも減る
        - 件数不明時は FALLBACK_INITIAL_ROWS (100行) で開始し、
          必要に応じて append_rows 内で EXPAND_STEP_ROWS ずつ自動拡張。
        """
        existing = self.list_worksheet_titles()
        sheet_name = build_sheet_name(
            pref=pref,
            city=city,
            fallback_id=fallback_id,
            existing_names=existing,
        )
        # 初期行数: 想定件数 + ヘッダー1行 + 安全バッファ20行
        if expected_data_rows and expected_data_rows > 0:
            initial_rows = expected_data_rows + 21
        else:
            initial_rows = self.FALLBACK_INITIAL_ROWS
        # 列数は SHEET_COLUMNS に合わせる(余裕を持って +2)
        ws = self._spreadsheet.add_worksheet(
            title=sheet_name,
            rows=initial_rows,
            cols=max(10, len(SHEET_COLUMNS) + 2),
        )
        ws.append_row(SHEET_COLUMNS, value_input_option="USER_ENTERED")
        self._worksheet = ws
        self._next_row = 2  # ヘッダー行の次から書き始める

        # 固有スタイルを適用 (失敗してもデータ書込は継続)
        try:
            self._apply_sheet_style(ws)
            self._logger.info("シート生成 + スタイル適用: %s", sheet_name)
        except Exception as exc:
            self._logger.warning("スタイル適用失敗 (続行): %s", exc)
            self._logger.info("シート生成: %s", sheet_name)
        return sheet_name

    # ------------------------------------------------------------------
    # スタイル適用 (プロフェッショナルな見た目 + チェックボックス)
    # ------------------------------------------------------------------
    def _apply_sheet_style(self, ws: gspread.Worksheet) -> None:
        """新規シートに固有スタイルを batch_update で一括適用。

        プロフェッショナル設計:
        - ヘッダー: ダークスレート背景 + 白太字 + 中央揃え + 高さ36px
        - 1行目を固定
        - 縞模様 (banding) で行を見やすく
        - 細い罫線 (薄いグレー)
        - 物件名列 (1列目) は太字で強調
        - 「要手動確認」列はチェックボックス化
        - 列幅をラベルごとに最適化

        データ検証は **必要分の行だけ** に絞って append が末尾に飛ぶのを防ぐ
        (具体的な書込は SheetsClient.append_rows が update() で位置指定する)
        """
        sheet_id = ws.id
        n_cols = len(SHEET_COLUMNS)
        max_rows = ws.row_count or 1000

        requests: list[dict] = []

        # 1) ヘッダー行のフォーマット (ダークスレート + 白太字 + 中央)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 1,
                    "startColumnIndex": 0, "endColumnIndex": n_cols,
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

        # 1.5) ヘッダー行の高さを 36px に
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0, "endIndex": 1,
                },
                "properties": {"pixelSize": 36},
                "fields": "pixelSize",
            }
        })

        # 2) 1行目を固定
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
                        "startIndex": idx, "endIndex": idx + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            })

        # 4) データ行のデフォルト書式: 中央寄せ・標準フォント
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1, "endRowIndex": max_rows,
                    "startColumnIndex": 0, "endColumnIndex": n_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontSize": 10},
                    }
                },
                "fields": "userEnteredFormat(verticalAlignment,textFormat)",
            }
        })

        # 5) 物件名 列 (1列目) を太字で強調
        bold_col_idx = None
        for label in _BOLD_FIRST_COLUMN_NAMES:
            if label in SHEET_COLUMNS:
                bold_col_idx = SHEET_COLUMNS.index(label)
                break
        if bold_col_idx is not None:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1, "endRowIndex": max_rows,
                        "startColumnIndex": bold_col_idx,
                        "endColumnIndex": bold_col_idx + 1,
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            })

        # 6) 縞模様 (バンディング) — 1行ごとにオフホワイト
        requests.append({
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0, "endRowIndex": max_rows,
                        "startColumnIndex": 0, "endColumnIndex": n_cols,
                    },
                    "rowProperties": {
                        "headerColor": HEADER_BG_COLOR,
                        "firstBandColor": {"red": 1, "green": 1, "blue": 1},
                        "secondBandColor": BAND_ROW_COLOR,
                    }
                }
            }
        })

        # 7) 罫線 (細い・薄いグレー)
        requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": max_rows,
                    "startColumnIndex": 0, "endColumnIndex": n_cols,
                },
                "innerHorizontal": {
                    "style": "SOLID", "width": 1, "color": BORDER_COLOR,
                },
                "innerVertical": {
                    "style": "SOLID", "width": 1, "color": BORDER_COLOR,
                },
            }
        })

        # 8) 「要手動確認」列をチェックボックス化
        # ※ データ検証範囲を max_rows まで適用すると append_row が末尾に飛ぶため、
        #   書込時に SheetsClient.append_rows が update() で位置指定するロジックに依存。
        for label in _CHECKBOX_COLUMN_NAMES:
            if label not in SHEET_COLUMNS:
                continue
            col_idx = SHEET_COLUMNS.index(label)
            requests.append({
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1, "endRowIndex": max_rows,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            })
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1, "endRowIndex": max_rows,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1,
                    },
                    "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                    "fields": "userEnteredFormat.horizontalAlignment",
                }
            })

        if requests:
            self._spreadsheet.batch_update({"requests": requests})

    # ------------------------------------------------------------------
    # 再開モード: 既存シートを再利用する
    # ------------------------------------------------------------------
    def find_latest_sheet_for_region(
        self, pref: str, city: str, fallback_id: str = ""
    ) -> Optional[str]:
        """指定地域のシートを探して最新を返す。
        日付プレフィックスは無視して、地域名がマッチする全シートから最新を選ぶ。
        例: 26_05/07_神奈川県横浜市 と 26_05/13_神奈川県横浜市 が両方あれば 13 の方を返す。
        """
        base = build_sheet_name(
            pref=pref, city=city, fallback_id=fallback_id,
            existing_names=[],
        )
        # base から地域部分を抽出 ("26_05/12_神奈川県横浜市" → "神奈川県横浜市")
        import re as _re
        m = _re.match(r"^\d{2}_\d{2}/\d{2}_(.+?)(_\d+)?$", base)
        region_part = m.group(1) if m else f"{pref}{city}"

        titles = self.list_worksheet_titles()
        candidates = [t for t in titles if region_part in t]
        if not candidates:
            return None

        # 名前降順で最新を選ぶ (日付プレフィックスが大きい = 新しい)
        candidates.sort(reverse=True)
        return candidates[0]

    def use_existing_sheet(self, sheet_name: str) -> int:
        """既存シートを worksheet として選択して、次の書込行を計算する。

        ロジック:
        - 物件名 (or 物件名称) 列を基準にして、ヘッダーの次から「連続データブロック」を探す
        - 最初に物件名が空の行を見つけたら、そこで終了 (それ以降は無視)
        - これにより、過去バグで深部に残ったゴミ行を無視できる
        - 物件管理コードがあれば最大値を返す (続き番号用)

        Returns: resumed_from_row (= 最大物件管理コード または データ行数)
        """
        try:
            ws = self._spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            raise SheetsError(f"シート '{sheet_name}' が見つかりません")

        self._worksheet = ws
        all_values = ws.get_all_values()

        if len(all_values) < 2:
            # ヘッダーすら無い、またはヘッダーのみ
            self._next_row = 2
            self._logger.info(
                "既存シート '%s' を再利用 (データ 0 行、空状態から開始)",
                sheet_name,
            )
            return 0

        header = all_values[0]

        def _find_col(*labels: str) -> Optional[int]:
            for label in labels:
                if label in header:
                    return header.index(label)
            return None

        name_idx = _find_col("物件名", "物件名称")
        code_idx = _find_col("物件管理コード", "管理コード", "No", "ID")

        # 連続データブロックの最終行を探す (物件名で判定)
        # 物件名列が無い古いシートは any-cell フォールバック
        last_data_idx = 0  # 0-indexed (= ヘッダー = row 1)
        max_code = 0

        if name_idx is not None:
            # 物件名列ありの場合: 物件名が空白の行で打ち切り
            for i in range(1, len(all_values)):
                row = all_values[i]
                has_name = (
                    name_idx < len(row)
                    and bool((row[name_idx] or "").strip())
                )
                if not has_name:
                    break
                last_data_idx = i
                # 管理コードを記録
                if code_idx is not None and code_idx < len(row):
                    try:
                        code = int((row[code_idx] or "").strip())
                        if code > max_code:
                            max_code = code
                    except (ValueError, TypeError):
                        pass
        else:
            # フォールバック: 物件名列が無い時は any-cell の連続性
            for i in range(1, len(all_values)):
                row = all_values[i]
                if any((cell or "").strip() for cell in row):
                    last_data_idx = i
                else:
                    break

        # 書込位置 = 連続データブロックの最終行 +1
        self._next_row = last_data_idx + 2 if last_data_idx > 0 else 2
        data_rows = last_data_idx  # 0 ならヘッダーのみ

        # resumed_from_row: 管理コードがあればその最大値、なければ単純な行数
        resumed_value = max_code if max_code > 0 else data_rows

        self._logger.info(
            "既存シート '%s' を再利用 (連続データ %d 行、最大管理コード %d、"
            "シート総行数 %d)。次の書込行: %d",
            sheet_name, data_rows, max_code, len(all_values), self._next_row,
        )
        return resumed_value

    def read_existing_property_keys(self) -> tuple:
        """選択中シートから (物件名|住所 集合, 物件URL集合) を返す。
        URL があれば詳細ページfetch前に高速スキップ可能。
        URL列が無い古いシートでも、name+address 集合は返す (遅いが動作する)。

        Returns: (dedup_keys: set[str], visited_urls: set[str])
        """
        if self._worksheet is None:
            return set(), set()

        try:
            all_values = self._worksheet.get_all_values()
        except Exception as exc:
            self._logger.warning("既存シート読み込み失敗: %s", exc)
            return set(), set()

        if len(all_values) < 2:
            return set(), set()

        header = all_values[0]

        def _find_col(*labels: str) -> Optional[int]:
            for label in labels:
                if label in header:
                    return header.index(label)
            return None

        name_idx = _find_col("物件名", "物件名称")
        addr_idx = _find_col("SUUMO住所", "住所")
        url_idx = _find_col("物件URL", "SUUMO URL")

        if name_idx is None or addr_idx is None:
            self._logger.warning(
                "既存シートのヘッダーに 物件名/住所 列が見つかりません"
            )
            return set(), set()

        dedup_keys: set[str] = set()
        visited_urls: set[str] = set()
        for row in all_values[1:]:
            if name_idx < len(row) and addr_idx < len(row):
                name = (row[name_idx] or "").strip()
                addr = (row[addr_idx] or "").strip()
                if name or addr:
                    dedup_keys.add(f"{name}|{addr}")
            if url_idx is not None and url_idx < len(row):
                u = (row[url_idx] or "").strip()
                if u:
                    visited_urls.add(u)
        return dedup_keys, visited_urls

    # ------------------------------------------------------------------
    # 行追加 (行位置を明示して書く)
    # データ検証 (チェックボックス) を 1000行分まとめて適用してると、
    # gspread の append_row が「使用済み範囲」を超えて末尾に書き込んでしまう。
    # それを防ぐため、_next_row で書込位置を管理して update() で書く。
    # ------------------------------------------------------------------
    def append_rows(self, rows: Iterable[list[str]]) -> None:
        if self._worksheet is None:
            raise SheetsError(
                "シートが未選択です。先に create_sheet_for_region を呼んでください。"
            )
        rows = list(rows)
        if not rows:
            return

        n_cols = len(SHEET_COLUMNS)
        start_row = self._next_row
        end_row = start_row + len(rows) - 1

        # 行数が足りなければシートを動的に拡張
        # 初期 10 行 → 必要に応じて 100 行ずつ追加
        # 一気に大量に書き込む場合でも必要分だけ追加する (100行単位の倍数)
        current_max = self._worksheet.row_count or self.INITIAL_ROWS
        if end_row > current_max:
            shortage = end_row - current_max
            # 不足分を EXPAND_STEP_ROWS の倍数に丸めて追加
            step = self.EXPAND_STEP_ROWS
            rows_to_add = ((shortage + step - 1) // step) * step
            try:
                self._worksheet.add_rows(rows_to_add)
                self._logger.info(
                    "シート行数を %d → %d 行に拡張 (+%d)",
                    current_max, current_max + rows_to_add, rows_to_add,
                )
            except Exception as exc:
                self._logger.warning("行数拡張失敗: %s", exc)

        # 列範囲: A〜N_COLS
        # 26列を超える想定はないが念のため A1 表記を組み立てる
        def _col_letter(idx: int) -> str:
            # 0 -> 'A', 25 -> 'Z', 26 -> 'AA' ...
            letters = ""
            n = idx
            while True:
                letters = chr(ord("A") + (n % 26)) + letters
                n = n // 26 - 1
                if n < 0:
                    break
            return letters

        last_col = _col_letter(n_cols - 1)
        a1_range = f"A{start_row}:{last_col}{end_row}"

        self._worksheet.update(
            range_name=a1_range,
            values=rows,
            value_input_option="USER_ENTERED",
        )
        self._next_row = end_row + 1

    def append_row(self, row: list[str]) -> None:
        self.append_rows([row])

    # ------------------------------------------------------------------
    # 状態
    # ------------------------------------------------------------------
    @property
    def worksheet_title(self) -> str:
        return self._worksheet.title if self._worksheet else ""
