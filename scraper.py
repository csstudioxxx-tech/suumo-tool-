"""
scraper.py
----------
HTTP取得層。

- User-Agent を固定
- アクセス間隔をランダム化 (デフォルト 6〜11秒) してボット検出回避
- 一定回数 (デフォルト 100リクエスト) ごとに長休憩 (デフォルト 43秒)
- リトライ / タイムアウトを一元管理
- 停止フラグで中断可能
"""
from __future__ import annotations

import logging
import random
import time
from typing import Optional

import requests

from config import (
    MAX_RETRY,
    REQUEST_INTERVAL_SEC,
    REQUEST_TIMEOUT_SEC,
    RETRY_BACKOFF_SEC,
    USER_AGENT,
)


class StopRequested(Exception):
    """ユーザーが停止ボタンを押した際に送出される例外。"""


class Scraper:
    """同期 HTTP クライアントをラップしたクラス。"""

    # デフォルトのアクセス間隔範囲 (ランダム化)
    DEFAULT_INTERVAL_MIN_SEC = 6.0
    DEFAULT_INTERVAL_MAX_SEC = 11.0
    # デフォルトの長休憩設定: 300リクエストごとに 43秒
    DEFAULT_LONG_BREAK_EVERY = 300
    DEFAULT_LONG_BREAK_SEC = 43.0

    def __init__(
        self,
        interval_min_sec: float = DEFAULT_INTERVAL_MIN_SEC,
        interval_max_sec: float = DEFAULT_INTERVAL_MAX_SEC,
        long_break_every: int = DEFAULT_LONG_BREAK_EVERY,
        long_break_sec: float = DEFAULT_LONG_BREAK_SEC,
        logger: Optional[logging.Logger] = None,
        # 後方互換: 旧 interval_sec=10 が渡されたら 6〜11 のランダム範囲に変換
        interval_sec: Optional[float] = None,
    ) -> None:
        # 旧 API 互換: interval_sec から min/max を生成
        if interval_sec is not None:
            interval_min_sec = max(1.0, interval_sec - 4.0)
            interval_max_sec = interval_sec + 1.0
        self._interval_min = interval_min_sec
        self._interval_max = interval_max_sec
        self._long_break_every = long_break_every
        self._long_break_sec = long_break_sec
        self._logger = logger or logging.getLogger(__name__)
        self._last_request_at: float = 0.0
        self._request_count: int = 0
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.6",
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
                ),
            }
        )
        self._stop_flag: bool = False

    # ------------------------------------------------------------------
    # 停止制御
    # ------------------------------------------------------------------
    def stop(self) -> None:
        """外部スレッドから停止を要求する。"""
        self._stop_flag = True

    def reset_stop(self) -> None:
        self._stop_flag = False

    @property
    def stopped(self) -> bool:
        return self._stop_flag

    @property
    def request_count(self) -> int:
        """これまでに発行した HTTP リクエスト数 (デバッグ・統計用)。"""
        return self._request_count

    # ------------------------------------------------------------------
    # 内部: 中断可能な sleep
    # 細かい間隔で sleep して停止フラグを早めに検知する
    # ------------------------------------------------------------------
    def _interruptible_sleep(self, seconds: float, step: float = 0.5) -> None:
        slept = 0.0
        while slept < seconds:
            if self._stop_flag:
                raise StopRequested("停止が要求されました")
            chunk = min(step, seconds - slept)
            time.sleep(chunk)
            slept += chunk

    # ------------------------------------------------------------------
    # 内部: アクセス間隔の調整 (ランダム + 100回ごと長休憩)
    # ------------------------------------------------------------------
    def _sleep_interval(self) -> None:
        """前回リクエストから interval 経過するまで待機する。

        ロジック:
        1. 直近のリクエスト数が long_break_every の倍数 → 長休憩 (43秒など)
        2. 通常は random.uniform(min, max) のランダム間隔だけ待機
        """
        # 1) 長休憩判定 (100, 200, 300... 件目にそなえる)
        if (
            self._long_break_every > 0
            and self._request_count > 0
            and self._request_count % self._long_break_every == 0
        ):
            self._logger.info(
                "%d リクエスト達成 → %.0f 秒の長休憩 (ボット検出回避)",
                self._request_count,
                self._long_break_sec,
            )
            self._interruptible_sleep(self._long_break_sec, step=1.0)
            # 長休憩で十分時間経過したので、通常 interval は不要にする
            self._last_request_at = 0.0

        # 2) 初回はインターバル待ち不要
        if self._last_request_at == 0.0:
            return

        # 3) 通常のランダム間隔
        interval = random.uniform(self._interval_min, self._interval_max)
        elapsed = time.monotonic() - self._last_request_at
        remaining = interval - elapsed
        if remaining > 0:
            self._interruptible_sleep(remaining, step=0.5)

    # ------------------------------------------------------------------
    # 公開: ページ取得
    # ------------------------------------------------------------------
    def fetch(self, url: str) -> Optional[str]:
        """指定 URL を GET して HTML テキストを返す。失敗時は None。"""
        for attempt in range(1, MAX_RETRY + 1):
            if self._stop_flag:
                raise StopRequested("停止が要求されました")
            try:
                self._sleep_interval()
                self._logger.info("GET %s (attempt=%d)", url, attempt)
                resp = self._session.get(url, timeout=REQUEST_TIMEOUT_SEC)
                self._last_request_at = time.monotonic()
                self._request_count += 1

                if resp.status_code == 200:
                    # encoding 自動推定
                    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
                        resp.encoding = resp.apparent_encoding or "utf-8"
                    return resp.text

                self._logger.warning("HTTP %d: %s", resp.status_code, url)

                # 致命的な 4xx はリトライしても無駄なので打ち切る
                if resp.status_code in (400, 403, 404, 410):
                    return None

            except StopRequested:
                raise
            except requests.RequestException as exc:
                self._logger.error("RequestException for %s: %s", url, exc)

            if attempt < MAX_RETRY:
                # リトライ前は長めに待つ
                time.sleep(RETRY_BACKOFF_SEC)

        return None
