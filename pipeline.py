"""
pipeline.py
-----------
GUI と各レイヤーを束ねる実行エンジン。

責務:
- 一覧ページの巡回
- 詳細URLの抽出・重複排除
- 詳細ページの解析
- 住所予測モジュール呼び出し
- スプレッドシート出力
- 統計・ログの収集
- 停止制御
"""
from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from address_predictor import AddressPredictor, build_gmap_url, is_rc_structure


# SUUMO 一覧ページから総件数を抽出するための正規表現候補
# ページ構造の変更にも耐えるよう、複数パターンを順に試す
# 「該当～件」「全～件」「～件中」「物件～件」などのキーワード付きパターンを優先
_TOTAL_COUNT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"([0-9,]+)\s*件\s*ありました"),       # "11020件ありました" (SUUMO library)
    re.compile(r"([0-9,]+)\s*件\s*あります"),         # "11020件あります"
    re.compile(r"該当\s*[:：]?\s*([0-9,]+)\s*件"),   # "該当：11,020 件"
    re.compile(r"該当物件\s*([0-9,]+)\s*件"),        # "該当物件 11,020 件"
    re.compile(r"全\s*([0-9,]+)\s*件"),              # "全 11,020 件"
    re.compile(r"検索結果\s*([0-9,]+)\s*件"),        # "検索結果 11,020 件"
    re.compile(r"物件数\s*[:：]?\s*([0-9,]+)\s*件"), # "物件数：11,020 件"
    re.compile(r"物件\s+([0-9,]+)\s*件"),            # "○○の物件 11,020 件"
    re.compile(r"([0-9,]+)\s*件\s*中"),              # "11,020 件中 1〜20"
    re.compile(r"([0-9,]+)\s*件\s*\(\s*[0-9]+\s*件表示"),  # "11020件(20件表示)" (連結形)
)


def extract_total_count(html: str) -> int:
    """SUUMO 一覧ページの HTML から総件数を best-effort で抽出。
    取れなかったら 0 を返す (UI 側で「件数未取得」表示)。

    抽出フロー:
    1. HTML タグを除去・空白正規化・全角数字→半角 でテキスト化
    2. 既知のキーワード付きパターン (該当～件 / 全～件 等) を順に試す
    3. それも駄目なら HTML 中の「N件」を全部拾って 100 以上の最大値を採用
       (SUUMO では総件数が最も大きい数値になる前提)
    """
    if not html:
        return 0

    # 1) HTML タグ除去 + 空白正規化 + 全角数字→半角
    #    "<span>11</span>件ありました" → "11 件ありました"
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"\s+", " ", text)
    # 全角数字 → 半角数字
    text = text.translate(str.maketrans(
        "０１２３４５６７８９",
        "0123456789",
    ))

    # 2) キーワード付きパターン
    for pat in _TOTAL_COUNT_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                continue

    # 3) フォールバック: 「N件」を全部拾って 100 以上の最大値
    #    (件数表示以外の小さい数値 - 例: ページ番号 - を除外するため 100 以上を採用)
    candidates: list[int] = []
    for m in re.finditer(r"([0-9,]+)\s*件", text):
        try:
            n = int(m.group(1).replace(",", ""))
            if n >= 100:
                candidates.append(n)
        except ValueError:
            continue
    if candidates:
        return max(candidates)

    return 0
from config import MAX_PAGE_HARD_LIMIT, SHEET_COLUMNS
from parser import (
    PropertyDetail,
    extract_detail_urls,
    extract_next_page_url,
    parse_detail,
)
from region_extractor import extract_region
from scraper import Scraper, StopRequested
from sheets import SheetsClient


# ログ進捗通知用コールバック型
StatusCallback = Callable[[str], None]
LogCallback = Callable[[str], None]


@dataclass
class RunStats:
    started_at: float = 0.0
    finished_at: float = 0.0
    target_url: str = ""
    sheet_name: str = ""
    pages_visited: int = 0
    success: int = 0
    skipped: int = 0
    duplicated: int = 0
    errors: int = 0
    prediction_success: int = 0
    prediction_failure: int = 0
    # RC系フィルタで非対象 (構造が RC/SRC/鉄筋コン/鉄骨鉄筋 ではない) として除外した件数
    rc_filtered: int = 0
    # 要手動確認 (検索結果なし or 別棟ヒット疑い) の件数
    needs_manual_check_count: int = 0
    # 一覧ページから取得した「全体件数」 (取れなかったら 0)
    total_count: int = 0
    # 直近処理中の物件名 (live UI 用)
    current_property_name: str = ""
    error_messages: list[str] = field(default_factory=list)


class Pipeline:
    """1 回の実行全体を統括するクラス。"""

    def __init__(
        self,
        scraper: Scraper,
        predictor: AddressPredictor,
        sheets: SheetsClient,
        logger: logging.Logger,
        status_cb: Optional[StatusCallback] = None,
        log_cb: Optional[LogCallback] = None,
        rc_filter_enabled: bool = True,
    ) -> None:
        self._scraper = scraper
        self._predictor = predictor
        self._sheets = sheets
        self._logger = logger
        self._status_cb = status_cb or (lambda _: None)
        self._log_cb = log_cb or (lambda _: None)
        # True なら RC系構造の物件のみシートに書き出す (非対象は予測も書込もしない)
        self._rc_filter_enabled = rc_filter_enabled

        self._stop_event = threading.Event()
        self._visited_list_urls: set[str] = set()
        self._visited_detail_urls: set[str] = set()
        self._dedup_keys: set[str] = set()

        self.stats = RunStats()

    # ------------------------------------------------------------------
    # 停止
    # ------------------------------------------------------------------
    def request_stop(self) -> None:
        self._stop_event.set()
        self._scraper.stop()

    def _check_stop(self) -> None:
        if self._stop_event.is_set():
            raise StopRequested("停止要求を受信しました")

    # ------------------------------------------------------------------
    # 公開: 実行
    # ------------------------------------------------------------------
    def run(self, start_url: str) -> RunStats:
        self.stats = RunStats()
        self.stats.started_at = time.time()
        self.stats.target_url = start_url
        self._stop_event.clear()
        self._scraper.reset_stop()

        self._log(f"開始: {start_url}")
        self._status("最初のページを取得中…")

        try:
            first_html = self._scraper.fetch(start_url)
        except StopRequested:
            self._log("停止されました(初回アクセス前)")
            self.stats.finished_at = time.time()
            return self.stats

        if not first_html:
            msg = "初回ページの取得に失敗しました。処理を中止します。"
            self._log(msg)
            self.stats.errors += 1
            self.stats.error_messages.append(msg)
            self.stats.finished_at = time.time()
            return self.stats

        # 一覧ページから総件数を抽出 (best-effort、失敗しても処理続行)
        try:
            total = extract_total_count(first_html)
            if total > 0:
                self.stats.total_count = total
                self._log(f"全体件数: {total:,} 件")
            else:
                self._log("全体件数: 取得できず (進捗% は表示されません)")
        except Exception as exc:
            self._logger.warning("総件数抽出失敗: %s", exc)

        # シート生成(最初のページから地域取得)
        try:
            pref, city, fallback_id = extract_region(first_html, start_url)
            sheet_name = self._sheets.create_sheet_for_region(pref, city, fallback_id)
            self.stats.sheet_name = sheet_name
            self._log(f"シート生成: {sheet_name}")
        except Exception as exc:
            self._logger.exception("シート生成失敗: %s", exc)
            self.stats.errors += 1
            self.stats.error_messages.append(f"シート生成失敗: {exc}")
            self.stats.finished_at = time.time()
            return self.stats

        # ページ巡回
        current_url: Optional[str] = start_url
        current_html: Optional[str] = first_html
        page_no = 0

        try:
            while current_url and current_html and page_no < MAX_PAGE_HARD_LIMIT:
                self._check_stop()
                page_no += 1
                self.stats.pages_visited = page_no
                self._status(f"ページ {page_no} 処理中…")
                self._log(f"[P{page_no}] {current_url}")

                if current_url in self._visited_list_urls:
                    self._log(f"[P{page_no}] 既訪問のためスキップ")
                    break
                self._visited_list_urls.add(current_url)

                # このページの詳細URL抽出
                detail_urls = extract_detail_urls(current_html, current_url)
                self._log(f"[P{page_no}] 詳細URL数: {len(detail_urls)}")

                for d_url in detail_urls:
                    self._check_stop()
                    self._process_detail(d_url)

                # 次ページ
                next_url = extract_next_page_url(current_html, current_url)
                if not next_url or next_url in self._visited_list_urls:
                    self._log("次ページなし。巡回終了。")
                    break

                try:
                    next_html = self._scraper.fetch(next_url)
                except StopRequested:
                    raise
                if not next_html:
                    self._log(f"次ページ取得失敗: {next_url}")
                    self.stats.errors += 1
                    break
                current_url = next_url
                current_html = next_html

        except StopRequested:
            self._log("停止要求により中断しました。")

        # 統計まとめ
        self.stats.prediction_success = self._predictor.success_count
        self.stats.prediction_failure = self._predictor.failure_count
        self.stats.finished_at = time.time()
        self._log_summary()
        return self.stats

    # ------------------------------------------------------------------
    # 詳細ページ処理
    # ------------------------------------------------------------------
    def _process_detail(self, detail_url: str) -> None:
        if detail_url in self._visited_detail_urls:
            self.stats.duplicated += 1
            return
        self._visited_detail_urls.add(detail_url)

        try:
            html = self._scraper.fetch(detail_url)
        except StopRequested:
            raise
        except Exception as exc:
            self._logger.exception("詳細取得失敗 %s: %s", detail_url, exc)
            self.stats.errors += 1
            self.stats.error_messages.append(f"詳細取得失敗 {detail_url}: {exc}")
            return

        if not html:
            self._log(f"詳細取得失敗: {detail_url}")
            self.stats.errors += 1
            return

        try:
            detail = parse_detail(html, detail_url=detail_url)
        except Exception as exc:
            self._logger.exception("詳細解析失敗 %s: %s", detail_url, exc)
            self.stats.errors += 1
            self.stats.error_messages.append(f"詳細解析失敗 {detail_url}: {exc}")
            return

        # ライブUI 用: 直近処理中の物件名
        self.stats.current_property_name = detail.name or "(名称不明)"

        # 重複排除(物件名 + 住所 + URL)
        dedup_key = f"{detail.name}|{detail.address}|{detail_url}"
        if dedup_key in self._dedup_keys:
            self.stats.duplicated += 1
            return
        if not detail.name and not detail.address:
            self.stats.skipped += 1
            return
        self._dedup_keys.add(dedup_key)

        # RC系フィルタ: 構造が RC/SRC/鉄筋コン/鉄骨鉄筋 でなければ書き出さない
        if self._rc_filter_enabled and not is_rc_structure(detail.structure):
            self.stats.rc_filtered += 1
            self._log(
                f"スキップ (RC系外): {detail.name or '(名称不明)'} | "
                f"構造={detail.structure or '(空)'}"
            )
            return

        # 住所予測
        try:
            pred = self._predictor.predict(detail)
        except Exception as exc:
            self._logger.exception("住所予測例外 %s: %s", detail.name, exc)
            from address_predictor import AddressPredictionOutput
            pred = AddressPredictionOutput()

        # 要手動確認カウンタ (検索結果なし or 別棟ヒット疑い)
        if pred.needs_manual_check:
            self.stats.needs_manual_check_count += 1

        # GMap URL 補完
        predicted_address = pred.predicted_address
        gmap = pred.google_map_url
        postal = pred.postal_code
        # 要手動確認 / 備考 はユーザー要望で追加された列
        #  ・住所が取れなかった物件 (検索結果 なし)
        #  ・別棟ヒット疑い (コートリベルテI なのに コートリベルテ2 が返ったケース等)
        #  → needs_manual_check=True の時に Google Sheets チェックボックスを ON
        # 列はデータ検証で BOOLEAN にしているので "TRUE"/"FALSE" を渡す
        # (USER_ENTERED で Google Sheets が自動的に bool として解釈)
        manual_mark = "TRUE" if pred.needs_manual_check else "FALSE"
        note = pred.note or ""

        # 住所予測 OFF でも SUUMO 住所から Google Map URL は生成しない(仕様:予測住所が空なら空)
        # ただし predict_only_incomplete=True かつ完全な住所はすでに predicted_address に入っている

        row = [
            detail.name,
            detail.address,
            detail.built_at,
            detail.structure,
            predicted_address,
            postal,
            gmap,
            manual_mark,
            note,
        ]
        # 列数を SHEET_COLUMNS に合わせる
        assert len(row) == len(SHEET_COLUMNS), (
            f"row 列数 ({len(row)}) と SHEET_COLUMNS ({len(SHEET_COLUMNS)}) が不一致。"
            f"config.py の SHEET_COLUMNS に '要手動確認' と '備考' を追加してください。"
        )

        try:
            self._sheets.append_row(row)
            self.stats.success += 1
            self._log(
                f"OK: {detail.name or '(名称不明)'} | {detail.address or '(住所不明)'}"
            )
        except Exception as exc:
            self._logger.exception("スプレッドシート書込失敗: %s", exc)
            self.stats.errors += 1
            self.stats.error_messages.append(f"書込失敗: {exc}")

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    def _log(self, msg: str) -> None:
        self._logger.info(msg)
        self._log_cb(msg)

    def _status(self, msg: str) -> None:
        self._status_cb(msg)

    def _log_summary(self) -> None:
        elapsed = self.stats.finished_at - self.stats.started_at
        summary = (
            f"=== 実行サマリ ===\n"
            f"対象URL       : {self.stats.target_url}\n"
            f"シート名      : {self.stats.sheet_name}\n"
            f"巡回ページ数  : {self.stats.pages_visited}\n"
            f"成功          : {self.stats.success}\n"
            f"重複          : {self.stats.duplicated}\n"
            f"スキップ      : {self.stats.skipped}\n"
            f"RC系外スキップ: {self.stats.rc_filtered}\n"
            f"エラー        : {self.stats.errors}\n"
            f"住所予測成功  : {self.stats.prediction_success}\n"
            f"住所予測失敗  : {self.stats.prediction_failure}\n"
            f"所要時間      : {elapsed:.1f} 秒\n"
        )
        for line in summary.splitlines():
            self._log(line)
