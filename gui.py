"""
gui.py
------
tkinter ベースの簡易 GUI。

- 一覧URL入力
- 出力先スプレッドシートID入力(前回値を自動保存・自動ロード)
- 住所予測 ON/OFF(前回値を自動保存・自動ロード)
- Google Maps APIキー入力(前回値を自動保存・自動ロード)
- 実行/停止
- 処理状況/ログ表示
- 完了件数表示

住所予測は Google Places Text Search (New API) を使用。
Claude へのフォールバックは廃止。
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
from typing import Optional

import user_config
from address_predictor import AddressPredictor
from config import LOG_DIR
from pipeline import Pipeline, RunStats
from places_bridge import PlacesBridge
from scraper import Scraper
from sheets import SheetsClient, SheetsError


# ---------------------------------------------------------------------
# ロガー
# ---------------------------------------------------------------------
def _setup_logger() -> logging.Logger:
    from datetime import datetime

    logger = logging.getLogger("suumo_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"run_{ts}.log"
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# ---------------------------------------------------------------------
# GUI アプリ
# ---------------------------------------------------------------------
class App(tk.Tk):
    TITLE = "SUUMO物件ライブラリー収集ツール"

    def __init__(self) -> None:
        super().__init__()
        self.title(self.TITLE)
        self.geometry("880x620")

        self._logger = _setup_logger()
        self._pipeline: Optional[Pipeline] = None
        self._worker: Optional[threading.Thread] = None
        self._msg_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()

        # 前回値を読み込み
        cfg = user_config.load_config()
        self._saved_url: str = cfg.get("list_url", "")
        self._saved_sheet_id: str = cfg.get("spreadsheet_id", "")
        # 住所予測 ON/OFF: 旧 predict_mode("on"/"off"/"rc_only") からの移行対応
        if "predict_enabled" in cfg:
            self._saved_predict: bool = bool(cfg["predict_enabled"])
        elif "predict_mode" in cfg:
            self._saved_predict = cfg["predict_mode"] != "off"
        else:
            self._saved_predict = True
        # RC系のみ書き出しフラグ (デフォルト ON)
        self._saved_rc_filter: bool = bool(cfg.get("rc_filter_enabled", True))
        self._saved_gmap_key: str = cfg.get(
            "google_maps_api_key", ""
        ) or os.environ.get("GOOGLE_MAPS_API_KEY", "")

        self._build_ui()
        self.after(150, self._drain_queue)

    # ------------------------------------------------------------------
    # UI 構築
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        pad = {"padx": 8, "pady": 4}

        # 入力フレーム
        frm_input = ttk.LabelFrame(self, text="入力")
        frm_input.pack(fill="x", **pad)

        ttk.Label(frm_input, text="一覧URL").grid(row=0, column=0, sticky="w", **pad)
        self.var_url = tk.StringVar(value=self._saved_url)
        ttk.Entry(frm_input, textvariable=self.var_url, width=90).grid(
            row=0, column=1, sticky="we", **pad
        )

        # スプレッドシートID
        if self._saved_sheet_id:
            row_txt = f"スプレッドシートID (保存済み: ...{self._saved_sheet_id[-8:]})"
        else:
            row_txt = "スプレッドシートID"
        ttk.Label(frm_input, text=row_txt).grid(row=1, column=0, sticky="w", **pad)
        self.var_sheet_id = tk.StringVar(value=self._saved_sheet_id)
        ttk.Entry(frm_input, textvariable=self.var_sheet_id, width=90).grid(
            row=1, column=1, sticky="we", **pad
        )

        # 出力フィルタ + 住所予測 ON/OFF
        ttk.Label(frm_input, text="オプション").grid(row=2, column=0, sticky="w", **pad)
        frm_opts = ttk.Frame(frm_input)
        frm_opts.grid(row=2, column=1, sticky="w", **pad)
        self.var_rc_filter = tk.BooleanVar(value=self._saved_rc_filter)
        ttk.Checkbutton(
            frm_opts,
            text="RC系のみ書き出し (推奨)",
            variable=self.var_rc_filter,
        ).pack(side="left", padx=(0, 16))
        self.var_predict = tk.BooleanVar(value=self._saved_predict)
        ttk.Checkbutton(
            frm_opts,
            text="住所予測する (Google Places)",
            variable=self.var_predict,
        ).pack(side="left")

        # Google Maps APIキー
        if self._saved_gmap_key:
            gmap_lbl = (
                f"Google Maps APIキー (保存済み: ...{self._saved_gmap_key[-6:]})"
            )
        else:
            gmap_lbl = "Google Maps APIキー"
        ttk.Label(frm_input, text=gmap_lbl).grid(row=3, column=0, sticky="w", **pad)
        self.var_gmap_key = tk.StringVar(value=self._saved_gmap_key)
        ttk.Entry(
            frm_input, textvariable=self.var_gmap_key, width=90, show="*"
        ).grid(row=3, column=1, sticky="we", **pad)

        frm_input.grid_columnconfigure(1, weight=1)

        # ボタン
        frm_btn = ttk.Frame(self)
        frm_btn.pack(fill="x", **pad)

        self.btn_run = ttk.Button(frm_btn, text="実行", command=self._on_run)
        self.btn_run.pack(side="left", padx=4)

        self.btn_stop = ttk.Button(
            frm_btn, text="停止", command=self._on_stop, state="disabled"
        )
        self.btn_stop.pack(side="left", padx=4)

        # 状況
        frm_status = ttk.LabelFrame(self, text="処理状況")
        frm_status.pack(fill="x", **pad)
        self.var_status = tk.StringVar(value="待機中")
        ttk.Label(frm_status, textvariable=self.var_status, foreground="blue").pack(
            anchor="w", **pad
        )
        self.var_counts = tk.StringVar(value="成功:0  重複:0  スキップ:0  エラー:0")
        ttk.Label(frm_status, textvariable=self.var_counts).pack(anchor="w", **pad)

        # ログ
        frm_log = ttk.LabelFrame(self, text="ログ")
        frm_log.pack(fill="both", expand=True, **pad)
        self.txt_log = scrolledtext.ScrolledText(
            frm_log, height=20, state="disabled", wrap="word"
        )
        self.txt_log.pack(fill="both", expand=True, padx=4, pady=4)

    # ------------------------------------------------------------------
    # ボタンハンドラ
    # ------------------------------------------------------------------
    def _on_run(self) -> None:
        url = self.var_url.get().strip()
        sheet_id = self.var_sheet_id.get().strip()
        predict = self.var_predict.get()
        rc_filter = self.var_rc_filter.get()
        gmap_key = self.var_gmap_key.get().strip()

        if not url:
            messagebox.showwarning("入力エラー", "一覧URLを入力してください。")
            return
        if not sheet_id:
            messagebox.showwarning(
                "入力エラー", "出力先スプレッドシートIDを入力してください。"
            )
            return
        if predict and not gmap_key:
            if not messagebox.askyesno(
                "APIキー未入力",
                "住所予測が有効ですが Google Maps APIキーが未入力です。\n"
                "このまま進めると、SUUMO住所が不完全な物件は「検索結果 なし」に"
                "なります。続行しますか?",
            ):
                return

        # 次回以降のために自動保存
        user_config.update(
            list_url=url,
            spreadsheet_id=sheet_id,
            predict_enabled=predict,
            rc_filter_enabled=rc_filter,
            google_maps_api_key=gmap_key,
        )

        self.btn_run.config(state="disabled")
        self.btn_stop.config(state="normal")
        self._clear_log()
        self._set_status("初期化中…")

        self._worker = threading.Thread(
            target=self._run_pipeline,
            args=(url, sheet_id, predict, rc_filter, gmap_key),
            daemon=True,
        )
        self._worker.start()

    def _on_stop(self) -> None:
        if self._pipeline:
            self._pipeline.request_stop()
            self._set_status("停止要求を送信しました…")

    # ------------------------------------------------------------------
    # ワーカースレッド
    # ------------------------------------------------------------------
    def _run_pipeline(
        self,
        url: str,
        sheet_id: str,
        predict_enabled: bool,
        rc_filter_enabled: bool,
        gmap_key: str = "",
    ) -> None:
        try:
            scraper = Scraper(logger=self._logger)

            self._logger.info(
                "出力フィルタ: RC系のみ書き出し = %s",
                "ON" if rc_filter_enabled else "OFF",
            )

            places_bridge = None
            if predict_enabled and gmap_key:
                self._logger.info("住所予測: Google Places Text Search モード [ON]")
                places_bridge = PlacesBridge(
                    api_key=gmap_key,
                    logger=self._logger,
                )
            elif predict_enabled:
                self._logger.warning(
                    "Google Maps APIキー未設定のため Places は呼び出しません。"
                    "SUUMO住所 (完全分のみ) でスプレッドシートに記録します。"
                )

            predictor = AddressPredictor(
                places_bridge=places_bridge,
                enabled=predict_enabled,
                logger=self._logger,
            )

            try:
                sheets = SheetsClient(
                    spreadsheet_id=sheet_id,
                    logger=self._logger,
                )
            except SheetsError as exc:
                self._enqueue("log", f"スプレッドシート初期化失敗: {exc}")
                self._enqueue("status", "エラーで終了")
                self._finalize_ui()
                return

            self._pipeline = Pipeline(
                scraper=scraper,
                predictor=predictor,
                sheets=sheets,
                logger=self._logger,
                status_cb=lambda msg: self._enqueue("status", msg),
                log_cb=lambda msg: self._enqueue("log", msg),
                rc_filter_enabled=rc_filter_enabled,
            )
            stats: RunStats = self._pipeline.run(url)

            self._enqueue(
                "counts",
                f"成功:{stats.success}  重複:{stats.duplicated}  "
                f"スキップ:{stats.skipped}  エラー:{stats.errors}",
            )
            self._enqueue(
                "status",
                f"完了: シート「{stats.sheet_name}」に {stats.success} 件出力",
            )

        except Exception as exc:
            self._logger.exception("予期せぬエラー: %s", exc)
            self._enqueue("log", f"致命的エラー: {exc}")
            self._enqueue("status", "エラーで終了")
        finally:
            self._finalize_ui()

    # ------------------------------------------------------------------
    # GUI 更新(スレッド安全)
    # ------------------------------------------------------------------
    def _enqueue(self, kind: str, msg: str) -> None:
        self._msg_queue.put((kind, msg))

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, msg = self._msg_queue.get_nowait()
                if kind == "log":
                    self._append_log(msg)
                    if self._pipeline:
                        s = self._pipeline.stats
                        self.var_counts.set(
                            f"成功:{s.success}  重複:{s.duplicated}  "
                            f"スキップ:{s.skipped}  エラー:{s.errors}  "
                            f"住所予測成功:{s.prediction_success}"
                        )
                elif kind == "status":
                    self._set_status(msg)
                elif kind == "counts":
                    self.var_counts.set(msg)
        except queue.Empty:
            pass
        self.after(150, self._drain_queue)

    def _finalize_ui(self) -> None:
        def _do() -> None:
            self.btn_run.config(state="normal")
            self.btn_stop.config(state="disabled")

        self.after(0, _do)

    def _set_status(self, msg: str) -> None:
        self.var_status.set(msg)

    def _append_log(self, msg: str) -> None:
        self.txt_log.config(state="normal")
        self.txt_log.insert("end", msg + "\n")
        self.txt_log.see("end")
        self.txt_log.config(state="disabled")

    def _clear_log(self) -> None:
        self.txt_log.config(state="normal")
        self.txt_log.delete("1.0", "end")
        self.txt_log.config(state="disabled")


def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
