"""
test_run.py
-----------
5件だけで精度を試したい時用の軽量テストランナー。

特徴:
- リクエスト間隔を短縮 (TEST_INTERVAL_SEC = 3秒)
- 最大5件で打ち切り
- スプレッドシートには書き込まず、ターミナル出力 + test_result.json に保存
- 住所予測は Google Places Text Search (New API) のみ
- Claude フォールバックは使わない

使い方:
    python3 test_run.py "https://suumo.jp/library/tf_14/sc_14102/"

引数を省略した場合は user_config.json の list_url を使います。

事前準備:
    - user_config.json に "google_maps_api_key" を設定
    - または環境変数 GOOGLE_MAPS_API_KEY に設定
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

import user_config
from address_predictor import AddressPredictor, is_rc_structure
from config import BASE_DIR, USER_AGENT
from parser import extract_detail_urls, parse_detail
from places_bridge import PlacesBridge

# ---------------------------------------------------------------------
# テスト用の定数
# ---------------------------------------------------------------------
TEST_INTERVAL_SEC = 3.0       # 本番は10秒、テストは3秒
TEST_MAX_ITEMS = 10            # 10件で打ち切り
TEST_REQUEST_TIMEOUT_SEC = 30  # 1リクエストのタイムアウト
# True: 本番 (gui.py) と同じく RC系 以外はスキップする
# False: フィルタを掛けず全構造で予測 (デバッグ・調査用)
TEST_RC_FILTER_ENABLED = True


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("test_run")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(sh)
    return logger


def main() -> int:
    logger = _setup_logger()

    # URL 決定: 引数 or user_config.json
    if len(sys.argv) >= 2:
        list_url = sys.argv[1]
    else:
        cfg = user_config.load_config()
        list_url = cfg.get("list_url", "")

    if not list_url:
        logger.error("一覧URLを指定してください (引数 or user_config.json)")
        return 1

    logger.info("テスト実行開始")
    logger.info("  一覧URL: %s", list_url)
    logger.info("  最大件数: %d", TEST_MAX_ITEMS)
    logger.info("  間隔: %.1f秒 (本番は10秒)", TEST_INTERVAL_SEC)

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept-Language": "ja-JP,ja;q=0.9",
    })

    # ------------------------------------------------------------------
    # 1) 一覧ページから詳細URL取得
    # ------------------------------------------------------------------
    try:
        r = session.get(list_url, timeout=TEST_REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
    except Exception as exc:
        logger.error("一覧ページ取得失敗: %s", exc)
        return 2

    urls = extract_detail_urls(r.text, list_url)
    logger.info("詳細URL抽出数: %d", len(urls))
    if not urls:
        logger.error("詳細URLが1件も見つかりませんでした")
        return 3

    picks = urls[:TEST_MAX_ITEMS]
    logger.info("対象 %d 件:", len(picks))
    for i, u in enumerate(picks, 1):
        logger.info("  %d. %s", i, u)

    # ------------------------------------------------------------------
    # 2) 住所予測器を初期化 (Places のみ)
    # ------------------------------------------------------------------
    cfg = user_config.load_config()
    gmap_key = os.environ.get("GOOGLE_MAPS_API_KEY", "") or cfg.get(
        "google_maps_api_key", ""
    )

    places_bridge = None
    if gmap_key:
        logger.info("住所予測モード: PlacesBridge (Google Places Text Search)")
        places_bridge = PlacesBridge(
            api_key=gmap_key,
            logger=logger,
        )
    else:
        logger.warning(
            "GOOGLE_MAPS_API_KEY が未設定のため、住所予測は SUUMO住所 の完全性のみで判定します。"
            "枝番まで補完したい場合は user_config.json に google_maps_api_key を設定してください。"
        )

    predictor = AddressPredictor(
        places_bridge=places_bridge,
        enabled=True,
        logger=logger,
    )

    # ------------------------------------------------------------------
    # 3) 各詳細ページで取得→パース→予測
    # ------------------------------------------------------------------
    results = []
    for i, url in enumerate(picks, 1):
        logger.info("")
        logger.info("=== [%d/%d] %s ===", i, len(picks), url)
        time.sleep(TEST_INTERVAL_SEC)

        try:
            rr = session.get(url, timeout=TEST_REQUEST_TIMEOUT_SEC)
            rr.raise_for_status()
        except Exception as exc:
            logger.error("詳細取得失敗: %s", exc)
            results.append({"url": url, "error": str(exc)})
            continue

        detail = parse_detail(rr.text, url)
        logger.info("物件名: %s", detail.name)
        logger.info("SUUMO住所: %s", detail.address)
        logger.info("築年月: %s", detail.built_at)
        logger.info("構造: %s", detail.structure)

        # RC系フィルタ (本番 pipeline.py と同じロジック)
        if TEST_RC_FILTER_ENABLED and not is_rc_structure(detail.structure):
            logger.info(
                "→ スキップ (RC系外): 構造=%s",
                detail.structure or "(空)",
            )
            results.append({
                "url": url,
                "name": detail.name,
                "suumo_address": detail.address,
                "built_at": detail.built_at,
                "structure": detail.structure,
                "skipped_rc_filter": True,
            })
            continue

        logger.info("-> 住所予測を実行中...")
        pred = predictor.predict(detail)
        logger.info("予測住所: %s", pred.predicted_address)
        logger.info("郵便番号: %s", pred.postal_code or "(取得できず)")
        logger.info("GMap URL: %s", pred.google_map_url)
        logger.info("枝番補完されたか: %s", "YES" if pred.was_predicted else "NO")
        logger.info("要手動確認: %s", "YES" if pred.needs_manual_check else "no")
        if pred.note:
            logger.info("備考: %s", pred.note)

        results.append({
            "url": url,
            "name": detail.name,
            "suumo_address": detail.address,
            "built_at": detail.built_at,
            "structure": detail.structure,
            "predicted_address": pred.predicted_address,
            "postal_code": pred.postal_code,
            "google_map_url": pred.google_map_url,
            "was_predicted": pred.was_predicted,
            "needs_manual_check": pred.needs_manual_check,
            "note": pred.note,
        })

    # ------------------------------------------------------------------
    # 4) 結果をJSONで保存
    # ------------------------------------------------------------------
    out_path = BASE_DIR / "test_result.json"
    try:
        out_path.write_text(
            json.dumps(results, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("")
        logger.info("結果を保存: %s", out_path)
    except Exception as exc:
        logger.error("結果保存失敗: %s", exc)

    # サマリー表示
    print("\n" + "=" * 70)
    print(" テスト結果サマリー")
    print("=" * 70)
    skipped_total = 0
    for i, r in enumerate(results, 1):
        if r.get("skipped_rc_filter"):
            skipped_total += 1
            print(f"\n[{i}] {r.get('name', '(取得失敗)')} (RC系外スキップ)")
            print(f"     SUUMO住所: {r.get('suumo_address', '')}")
            print(f"     構造:      {r.get('structure', '') or '(空)'}")
            continue
        print(f"\n[{i}] {r.get('name', '(取得失敗)')}")
        print(f"     SUUMO住所: {r.get('suumo_address', '')}")
        print(f"     構造:      {r.get('structure', '')}")
        print(f"     予測住所:  {r.get('predicted_address', '')}")
        print(f"     郵便番号:  {r.get('postal_code', '') or '(なし)'}")
        gmap = r.get('google_map_url', '') or ''
        suffix = '...' if len(gmap) > 80 else ''
        print(f"     GMap URL:  {gmap[:80]}{suffix}")
        if r.get('needs_manual_check'):
            print(f"     要手動確認: ✓")
        if r.get('note'):
            print(f"     備考:      {r['note']}")
    print("=" * 70)

    # 精度サマリー
    if TEST_RC_FILTER_ENABLED:
        print(f"\n RC系フィルタ: ON / RC系外スキップ {skipped_total} 件")
    else:
        print(f"\n RC系フィルタ: OFF (テスト用に無効化)")
    if places_bridge is not None:
        print(
            f" Places API 呼び出し: {places_bridge.call_count} 回 "
            f"(ヒット={places_bridge.hit_count} / ミス={places_bridge.miss_count})"
        )
    print(
        f" 住所予測結果: 枝番補完成功={predictor.success_count} / "
        f"失敗={predictor.failure_count}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
