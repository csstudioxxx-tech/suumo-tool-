"""
scraper.py
----------
HTTP取得層。

- User-Agent を固定
- 10秒以上のアクセス間隔を保証
- リトライ / タイムアウトを一元管理
- 停止フラグで中断可能
"""
from __future__ import annotations

import logging
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

    def __init__(
        self,
        interval_sec: float = REQUEST_INTERVAL_SEC,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._interval = interval_sec
        self._logger = logger or logging.getLogger(__name__)
        self._last_request_at: float = 0.0
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

    # ------------------------------------------------------------------
    # 内部: アクセス間隔の調整
    # ------------------------------------------------------------------
    def _sleep_interval(self) -> None:
        """前回リクエストから interval_sec 経過するまで待機する。"""
        if self._last_request_at == 0.0:
            return

        elapsed = time.monotonic() - self._last_request_at
        remaining = self._interval - elapsed
        if remaining <= 0:
            return

        # 停止フラグを検知しやすいよう細かく sleep する
        step = 0.5
        slept = 0.0
        while slept < remaining:
            if self._stop_flag:
                raise StopRequested("停止が要求されました")
            time.sleep(min(step, remaining - slept))
            slept += step

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
