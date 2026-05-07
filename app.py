"""
app.py
------
Streamlit ベースの SUUMO 物件収集ツール GUI。

gui.py (tkinter) の置き換え版。同じ pipeline / predictor / sheets を再利用する。

起動:
    streamlit run app.py

特徴:
- ブラウザベース、URL アクセスで複数人同時利用可能
- ライブ進捗表示 (進捗バー、構造別カウント、住所予測成績、要手動確認)
- バックグラウンドスレッドでスクレイピング実行 → ブラウザ閉じても処理継続
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import streamlit as st

import user_config
from address_predictor import AddressPredictor
from config import LOG_DIR
from pipeline import Pipeline, RunStats
from places_bridge import PlacesBridge
from scraper import Scraper
from sheets import SheetsClient, SheetsError


# ====================================================================
# ページ設定
# ====================================================================
st.set_page_config(
    page_title="SUUMO 物件収集ツール",
    page_icon="🏠",
    layout="centered",
    initial_sidebar_state="collapsed",
)


# ====================================================================
# Streamlit Cloud / ローカル 自動判定
# ====================================================================
def _get_secret(key: str, default: str = "") -> str:
    """Streamlit secrets を優先、なければデフォルト値。"""
    try:
        return st.secrets.get(key, default) or default
    except Exception:
        return default


def _get_service_account_info() -> Optional[dict]:
    """st.secrets['gcp_service_account'] が設定されていれば dict で返す。
    ローカル実行 (secrets 未設定) なら None を返し、ファイル読込にフォールバック。
    """
    try:
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])
    except Exception:
        pass
    return None


# Cloud モード判定: secrets に gcp_service_account があれば Cloud
IS_CLOUD = _get_service_account_info() is not None
# 管理者が secrets で固定する API キー (Cloud では UI からは見えない)
SECRET_GMAP_KEY = _get_secret("google_maps_api_key", "")
# 簡易パスワードゲート (Cloud で公開URLになるため)
APP_PASSWORD = _get_secret("app_password", "")


# ====================================================================
# パスワードゲート (Cloud 公開時の最低限の保護)
# ====================================================================
def _check_password() -> bool:
    """app_password が設定されていれば、ログイン画面を出す。
    ローカル実行 (app_password 未設定) なら常に通す。
    """
    if not APP_PASSWORD:
        return True
    if st.session_state.get("authenticated"):
        return True
    st.title("🔒 SUUMO 物件収集ツール")
    st.caption("管理者から共有されたパスワードを入力してください")
    pw = st.text_input("パスワード", type="password", key="_pw_input")
    if st.button("ログイン"):
        if pw == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("パスワードが違います")
    return False


if not _check_password():
    st.stop()


# ====================================================================
# ロガーとログ収集 (スレッド共有用)
# ====================================================================
class _DequeLogHandler(logging.Handler):
    """スレッド共有 deque にログを溜める Handler。"""

    def __init__(self, buffer: deque) -> None:
        super().__init__()
        self.buffer = buffer

    def emit(self, record: logging.LogRecord) -> None:
        try:
            ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
            self.buffer.append(f"[{ts}] {record.getMessage()}")
        except Exception:
            pass


def _setup_logger(log_buffer: deque) -> logging.Logger:
    logger = logging.getLogger("suumo_streamlit")
    logger.setLevel(logging.INFO)
    # 既存ハンドラ削除して再設定 (Streamlit のリラン対策)
    logger.handlers.clear()

    # ファイル出力 (永続化)
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(LOG_DIR / f"streamlit_{ts}.log", encoding="utf-8")
        fh.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        )
        logger.addHandler(fh)
    except Exception:
        pass

    # UI 表示用 deque ハンドラ
    logger.addHandler(_DequeLogHandler(log_buffer))
    return logger


# ====================================================================
# Session state 初期化
# ====================================================================
def _init_session_state() -> None:
    defaults = {
        "running": False,
        "pipeline": None,
        "worker_thread": None,
        "log_buffer": deque(maxlen=500),  # スレッド共有, 最大500行保持
        "started_at": None,
        "completed": False,  # 一度実行が終わったか
        "last_error": "",
        "rc_filter_used": True,  # 直近実行時の RC フィルタ設定
        "predict_used": True,    # 直近実行時の予測 ON/OFF 設定
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_session_state()


# ====================================================================
# 実行中状態の判定 (スレッドの生死から)
# ====================================================================
# スレッドからの session_state 更新は信頼性が低いため、
# 各リランで worker_thread.is_alive() を見て running を更新する
_thread = st.session_state.worker_thread
if _thread is not None:
    if _thread.is_alive():
        st.session_state.running = True
    else:
        # スレッド終了 → 一度だけ完了状態に遷移
        if st.session_state.running:
            st.session_state.completed = True
        st.session_state.running = False


# ====================================================================
# 設定読み込み (前回値)
# ====================================================================
cfg = user_config.load_config() if not IS_CLOUD else {}
saved_url = cfg.get("list_url", "")
# スプレッドシート ID: Cloud は secrets 優先、ローカルは config から
SECRET_SHEET_ID = _get_secret("spreadsheet_id", "")
saved_sheet_id = SECRET_SHEET_ID or cfg.get("spreadsheet_id", "")
saved_predict = bool(cfg.get("predict_enabled", True))
saved_rc_filter = bool(cfg.get("rc_filter_enabled", True))
# API キー: Cloud は secrets を強制使用、ローカルは config or env から
if IS_CLOUD:
    saved_gmap_key = SECRET_GMAP_KEY
else:
    saved_gmap_key = cfg.get("google_maps_api_key", "") or os.environ.get(
        "GOOGLE_MAPS_API_KEY", ""
    )


# ====================================================================
# ヘッダ
# ====================================================================
st.title("SUUMO 物件収集ツール")
st.caption("一覧 URL からスプレッドシートに物件情報を書き出します")


# ====================================================================
# 入力エリア
# ====================================================================
st.subheader("入力")

is_running = st.session_state.running

url = st.text_input(
    "一覧 URL", value=saved_url, disabled=is_running, key="input_url",
    placeholder="https://suumo.jp/library/tf_14/sc_14102/",
)

# スプレッドシート ID: secrets で固定設定されてる場合は欄を隠す
if SECRET_SHEET_ID:
    sheet_id = SECRET_SHEET_ID
    st.caption(f"📊 スプレッドシート ID: 管理者により設定済み (...{sheet_id[-8:]})")
else:
    sheet_id = st.text_input(
        "スプレッドシート ID",
        value=saved_sheet_id, disabled=is_running, key="input_sheet_id",
    )
if IS_CLOUD:
    # Cloud では管理者が secrets で固定。UIには出さない
    gmap_key = SECRET_GMAP_KEY
    if gmap_key:
        st.caption("🔑 Google Maps API キー: 管理者により設定済み")
    else:
        st.warning("⚠️ Google Maps API キー未設定 (管理者にご連絡ください)")
else:
    gmap_key = st.text_input(
        "Google Maps API キー",
        value=saved_gmap_key, type="password",
        disabled=is_running, key="input_gmap_key",
    )

col_opt1, col_opt2 = st.columns(2)
with col_opt1:
    rc_filter = st.checkbox(
        "RC系のみ書き出し (推奨)",
        value=saved_rc_filter, disabled=is_running, key="input_rc_filter",
    )
with col_opt2:
    predict = st.checkbox(
        "住所予測する (Google Places)",
        value=saved_predict, disabled=is_running, key="input_predict",
    )


# ====================================================================
# バックグラウンド実行
# ====================================================================
def _start_pipeline(
    url: str,
    sheet_id: str,
    predict_enabled: bool,
    rc_filter_enabled: bool,
    gmap_key: str,
) -> None:
    """Pipeline をメインスレッドで生成し、スレッドは run() だけ呼ぶ。

    Streamlit の session_state はスレッド跨ぎで安定しないため、
    pipeline インスタンス自体はメインスレッドで作って session_state に格納する。
    バックグラウンドスレッドでは pipeline.run() を呼ぶだけ。
    """

    log_buffer: deque = st.session_state.log_buffer
    log_buffer.clear()
    logger = _setup_logger(log_buffer)

    # === メインスレッドで pipeline 一式を構築 ===
    try:
        scraper = Scraper(logger=logger)

        places_bridge = None
        if predict_enabled and gmap_key:
            logger.info("住所予測モード: Google Places Text Search [ON]")
            places_bridge = PlacesBridge(api_key=gmap_key, logger=logger)
        elif predict_enabled:
            logger.warning(
                "Google Maps APIキー未設定のため Places は呼び出しません"
            )

        predictor = AddressPredictor(
            places_bridge=places_bridge,
            enabled=predict_enabled,
            logger=logger,
        )

        try:
            sa_info = _get_service_account_info()
            if sa_info:
                # Streamlit Cloud モード: secrets の dict から認証
                sheets = SheetsClient(
                    spreadsheet_id=sheet_id,
                    credentials_info=sa_info,
                    logger=logger,
                )
            else:
                # ローカル: credentials/service_account.json ファイルから
                sheets = SheetsClient(
                    spreadsheet_id=sheet_id,
                    logger=logger,
                )
        except SheetsError as exc:
            logger.error("スプレッドシート初期化失敗: %s", exc)
            st.session_state.last_error = f"スプレッドシート初期化失敗: {exc}"
            return

        pipeline = Pipeline(
            scraper=scraper,
            predictor=predictor,
            sheets=sheets,
            logger=logger,
            log_cb=lambda msg: None,  # logger 経由でログは取れる
            rc_filter_enabled=rc_filter_enabled,
        )

    except Exception as exc:
        logger.exception("Pipeline 初期化失敗: %s", exc)
        st.session_state.last_error = f"初期化失敗: {exc}"
        return

    # === メインスレッドで session_state に確実に格納 ===
    st.session_state.pipeline = pipeline
    st.session_state.running = True
    st.session_state.completed = False
    st.session_state.last_error = ""
    st.session_state.started_at = time.time()
    st.session_state.rc_filter_used = rc_filter_enabled
    st.session_state.predict_used = predict_enabled

    # === バックグラウンドスレッドは pipeline.run() だけ呼ぶ ===
    def _worker():
        try:
            pipeline.run(url)
        except Exception as exc:
            logger.exception("pipeline.run 例外: %s", exc)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    st.session_state.worker_thread = t


# ====================================================================
# 実行 / 停止 ボタン
# ====================================================================
btn_col1, btn_col2, _ = st.columns([1, 1, 4])
with btn_col1:
    run_clicked = st.button("▶ 実行", type="primary", disabled=is_running)
with btn_col2:
    stop_clicked = st.button("■ 停止", disabled=not is_running)

if run_clicked:
    if not url:
        st.error("一覧URLを入力してください")
    elif not sheet_id:
        st.error("スプレッドシート ID を入力してください")
    elif predict and not gmap_key:
        st.warning(
            "住所予測 ON ですが Google Maps API キーが未入力です。"
            "SUUMO 住所が不完全な物件は『検索結果 なし』になります。"
            "続行する場合はもう一度「実行」を押してください。"
        )
        # APIキーが空でも進めたい場合は二回押し
        # (一回目は警告のみ、二回目は実行)
        st.session_state["_warn_no_key"] = True
    else:
        # 設定保存 (ローカル時のみ。Cloud は読込専用)
        if not IS_CLOUD:
            try:
                user_config.update(
                    list_url=url,
                    spreadsheet_id=sheet_id,
                    predict_enabled=predict,
                    rc_filter_enabled=rc_filter,
                    google_maps_api_key=gmap_key,
                )
            except Exception:
                pass  # 設定保存失敗は無視 (実行は続ける)
        _start_pipeline(url, sheet_id, predict, rc_filter, gmap_key)
        st.rerun()

if stop_clicked:
    if st.session_state.pipeline:
        st.session_state.pipeline.request_stop()
        st.toast("停止要求を送信しました…")


# ====================================================================
# 処理状況表示
# ====================================================================
st.divider()
st.subheader("処理状況")

pipeline: Optional[Pipeline] = st.session_state.pipeline
stats: Optional[RunStats] = pipeline.stats if pipeline else None

if stats is None:
    st.info("「実行」ボタンを押すと処理を開始します")

else:
    rc_filter_used = st.session_state.rc_filter_used
    predict_used = st.session_state.predict_used

    # 処理済み件数 (success + duplicate + skip + rc_filtered + errors)
    processed = (
        stats.success + stats.duplicated + stats.skipped
        + stats.rc_filtered + stats.errors
    )
    total = stats.total_count

    # 全体件数 (SUUMO 一覧の総物件数) - 取れていれば一番上に大きく表示
    if total > 0:
        st.metric(
            label="全体件数 (SUUMO 一覧の物件総数)",
            value=f"{total:,} 件",
        )
    else:
        st.info("全体件数は取得できませんでした (進捗% は表示されません)")

    # ステータス行
    if st.session_state.running:
        status_label = f"ページ {max(stats.pages_visited, 1)} 処理中…"
    elif st.session_state.completed:
        status_label = f"✅ 完了 (全 {stats.pages_visited} ページ)"
    else:
        status_label = "待機中"

    if total > 0:
        pct = min(processed / total * 100, 100)
        st.markdown(
            f"**{status_label}**　"
            f"{processed:,} / {total:,} 件 ({pct:.1f}%)"
        )
        st.progress(min(processed / total, 1.0))
    else:
        st.markdown(f"**{status_label}**　{processed:,} 件処理")

    # ETA
    if (
        st.session_state.started_at
        and processed > 0
        and total > 0
        and st.session_state.running
    ):
        elapsed = time.time() - st.session_state.started_at
        remaining_sec = elapsed * (total - processed) / processed
        eta_dt = datetime.now() + timedelta(seconds=remaining_sec)
        h = int(remaining_sec // 3600)
        m = int((remaining_sec % 3600) // 60)
        if h > 0:
            remaining_label = f"約 {h} 時間 {m} 分"
        else:
            remaining_label = f"約 {m} 分"
        st.caption(
            f"所要時間目安 {remaining_label} "
            f"({eta_dt.strftime('%-m/%-d %H:%M')} 完了予定)"
        )

    # 構造別 (RC系 / その他)
    if rc_filter_used:
        rc_count = stats.success
        other_count = stats.rc_filtered
        breakdown_total = rc_count + other_count

        st.markdown(f"**構造別**　<small>(処理済み {breakdown_total:,} 件中)</small>",
                    unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            pct_rc = (rc_count / breakdown_total * 100) if breakdown_total else 0
            st.metric(
                "RC系 (書き出し対象)",
                f"{rc_count:,} / {breakdown_total:,} 件",
                f"{pct_rc:.1f}%",
            )
        with c2:
            pct_other = (other_count / breakdown_total * 100) if breakdown_total else 0
            st.metric(
                "その他 (スキップ)",
                f"{other_count:,} / {breakdown_total:,} 件",
                f"{pct_other:.1f}%",
            )

    # 住所予測 セクション
    if predict_used:
        pred_total = stats.prediction_success + stats.prediction_failure
        st.divider()
        st.markdown(
            f"**住所予測**　<small>(対象 {pred_total:,} 件中)</small>",
            unsafe_allow_html=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            pct_s = (stats.prediction_success / pred_total * 100) if pred_total else 0
            st.metric(
                "予測 成功",
                f"{stats.prediction_success:,} / {pred_total:,} 件",
                f"{pct_s:.1f}%",
            )
        with c2:
            pct_f = (stats.prediction_failure / pred_total * 100) if pred_total else 0
            st.metric(
                "予測 失敗",
                f"{stats.prediction_failure:,} / {pred_total:,} 件",
                f"{pct_f:.1f}%",
            )

        pct_m = (stats.needs_manual_check_count / pred_total * 100) if pred_total else 0
        st.metric(
            "要手動確認",
            f"{stats.needs_manual_check_count:,} / {pred_total:,} 件",
            f"{pct_m:.1f}%",
            delta_color="inverse",  # 多いと悪いので逆色
        )

    # 完了時のサマリ
    if st.session_state.completed and not st.session_state.running:
        st.success(
            f"処理完了。シート「{stats.sheet_name}」に {stats.success:,} 件を書き出しました。"
        )
        if sheet_id:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            st.markdown(f"[📊 スプレッドシートを開く]({sheet_url})")

    if st.session_state.last_error:
        st.error(f"エラー: {st.session_state.last_error}")


# ====================================================================
# ログ表示
# ====================================================================
st.divider()
st.subheader("ログ")
log_lines = list(st.session_state.log_buffer)
if log_lines:
    log_text = "\n".join(log_lines[-200:])  # 直近200行
    st.code(log_text, language="text")
else:
    st.caption("(ログはまだありません)")


# ====================================================================
# 自動リフレッシュ (実行中のみ)
# ====================================================================
if st.session_state.running:
    time.sleep(1.5)
    st.rerun()
