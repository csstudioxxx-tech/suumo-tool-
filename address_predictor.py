"""
address_predictor.py
--------------------
住所予測層。

仕様:
- Google Places Text Search (New API) のみで住所を特定する。
- 「物件名 + SUUMO住所」を Places に投げ、返ってきたピン地点の住所を採用。
- Claude フォールバックは使わない。Places でヒットしなければ「検索結果 なし」。
- 郵便番号は Places レスポンスの `formattedAddress` から抽出。
- Google Maps URL は Places が返した place_id / 住所から生成。
- ON/OFF 切替可能、予測失敗時も空欄でメイン処理は止めない。

モード:
- "on"  : Places で予測する
- "off" : 一切予測しない (SUUMO住所が完全ならそのまま、不完全なら『検索結果 なし』)

フロー:
1) SUUMO住所が既に枝番込みで完全 → SUUMO住所を採用 (Places 呼び出し不要)
2) それ以外 → Places を呼んで枝番まで特定できればそれを採用
3) Places で枝番まで特定できなければ「検索結果 なし」

備考: RC系などの構造フィルタは pipeline 側で行う (is_rc_structure 関数を export)
"""
from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

from claude_bridge import PredictionInput, PredictionResult
from parser import PropertyDetail


# モード定数 (タイポ防止)
MODE_ON = "on"
MODE_OFF = "off"
ALL_MODES = (MODE_ON, MODE_OFF)
# 後方互換 (gui.py が古いとき用)
MODE_RC_ONLY = "rc_only"  # 廃止: pipeline 側で RC フィルタを実装したため


# RC系 構造ホワイトリスト (ユーザー指定)
# - SUUMO 側の表記揺れ吸収のため NFKC 正規化 + 空白除去で比較
# - 増減したい時はこのリストを編集するだけで OK
_RC_STRUCTURE_WHITELIST_RAW = (
    "RC",
    "一部鉄骨",
    "RC一部RC",
    "RC一部鉄骨",
    "SRC",
    "ＳＲＣ・ＲＣ",
    "SRC一部RC",
    "SRC一部SRC",
    "SRC一部鉄骨",
    "鉄筋コン",
    "鉄骨鉄筋",
)


def _normalize_structure(s: str) -> str:
    """構造文字列を比較用に正規化。
    - 全角英数 → 半角  (NFKC)
    - 全角/半角空白を除去
    - 末尾の '造' を吸収 (例: 'RC造' → 'RC')
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\s　]+", "", s)
    s = re.sub(r"造$", "", s)
    return s


_RC_STRUCTURE_WHITELIST = frozenset(
    _normalize_structure(s) for s in _RC_STRUCTURE_WHITELIST_RAW
)


def is_rc_structure(structure: str) -> bool:
    """渡された構造文字列が RC系 (=住所予測対象) かどうか判定。"""
    if not structure:
        return False
    return _normalize_structure(structure) in _RC_STRUCTURE_WHITELIST


# 「住所が枝番まで揃っているか」判定用の正規表現群。
# 下記のいずれかにマッチすれば「枝番入り (採用可)」とみなす。
# ユーザー仕様: 号まで無くても「番地」まで出ていれば OK / 「丁目止まり」は NG
_COMPLETE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\d+丁目\s*\d+"),        # 2丁目13 / 2丁目13-5 / 5丁目33-39
    re.compile(r"\d+[-−ー–‐]\d+"),        # 2-8 / 2-1-1 / ハイフン区切り
    re.compile(r"[町字]\s*\d+"),         # 菅田町851 / 大字1
    re.compile(r"\d+番(?:地)?\s*\d+"),    # 1番2号 / 1番地2号
)


# 物件名比較用の正規化テーブル。
# SUUMO の I/II/III と Places の 2/3 のような揺れを吸収する。
_ROMAN_MAP = {
    # ラテン文字の I II III IV V
    "VIII": "8", "VII": "7", "III": "3", "IV": "4",
    "IX": "9", "VI": "6", "II": "2", "V": "5", "X": "10", "I": "1",
    # 全角ローマ数字 Ⅰ〜Ⅹ
    "Ⅷ": "8", "Ⅶ": "7", "Ⅲ": "3", "Ⅳ": "4", "Ⅸ": "9", "Ⅵ": "6",
    "Ⅱ": "2", "Ⅴ": "5", "Ⅹ": "10", "Ⅰ": "1",
}
_FULLWIDTH_DIGIT = str.maketrans({
    "\uFF10": "0", "\uFF11": "1", "\uFF12": "2", "\uFF13": "3",
    "\uFF14": "4", "\uFF15": "5", "\uFF16": "6", "\uFF17": "7",
    "\uFF18": "8", "\uFF19": "9",
})


def _normalize_name(name: str) -> str:
    """物件名を「末尾の棟番号揺れ」を吸収して比較できる形に正規化。
    - 全角数字 → 半角数字
    - ローマ数字 (I, II, III, Ⅰ, Ⅱ, ...) → アラビア数字
    - 空白・ハイフン・括弧などの記号を除去
    """
    if not name:
        return ""
    s = name.translate(_FULLWIDTH_DIGIT)
    # ローマ数字は 長い→短い の順に変換 (III が I の連続に誤変換されないように)
    for roman, num in _ROMAN_MAP.items():
        s = s.replace(roman, num)
    # 空白・記号を除去。
    # 注意: 「ー」(カタカナ長音 U+30FC) はカタカナの一部なので除去しない。
    # 除去するのは ASCII/全角ハイフンや minus sign など。
    s = re.sub(
        r"[\s\u3000・\-－−–—‐()（）【】\[\]「」『』]+",
        "",
        s,
    )
    return s.lower()


def _names_match(suumo_name: str, places_name: str) -> bool:
    """SUUMO の物件名と Places の建物名が「同じ棟」か判定。
    両方空なら一致扱い (警告不要)。片方だけ空も一致扱い。
    """
    if not suumo_name or not places_name:
        return True
    a = _normalize_name(suumo_name)
    b = _normalize_name(places_name)
    if not a or not b:
        return True
    # 完全一致 / 片方がもう片方の接頭辞なら同じ棟とみなす
    return a == b or a.startswith(b) or b.startswith(a)


@dataclass
class AddressPredictionOutput:
    predicted_address: str = ""
    postal_code: str = ""
    google_map_url: str = ""
    was_predicted: bool = False  # Places で枝番補完が成立したか
    needs_manual_check: bool = False  # True なら「要手動確認」列にチェック
    note: str = ""  # 備考 (例: "コートリベルテ2 の住所表示だから要チェック")


def build_gmap_url(address: str) -> str:
    """住所文字列から Google Maps 検索 URL を生成する。
    クリックすると Google Maps 上で その住所にピン立てされた状態で開く。"""
    if not address:
        return ""
    q = quote(address, safe="")
    return f"https://www.google.com/maps/search/?api=1&query={q}"


def is_address_complete(address: str) -> bool:
    """住所に「枝番(番地・号など)」が含まれているか簡易判定。

    丁目 + 番地 / 番地-号 / 町+番地 いずれかのパターンを満たせば完全扱い。
    「六角橋2」「六角橋2丁目」のような丁目止まりは不完全扱い。
    """
    if not address:
        return False
    for pat in _COMPLETE_PATTERNS:
        if pat.search(address):
            return True
    return False


POSTAL_RE = re.compile(r"(\d{3})[-－‐‑‒–—]?(\d{4})")


def _normalize_postal(postal: str) -> str:
    if not postal:
        return ""
    m = POSTAL_RE.search(postal)
    if not m:
        return ""
    return f"{m.group(1)}-{m.group(2)}"


class AddressPredictor:
    """物件詳細に対して住所予測を実行するクラス (Google Places 専用)。

    ポリシー:
    - mode="off"     : Places を呼ばない。SUUMO住所 (完全なら) + GMap URL のみ返す
    - mode="on"      : Places を呼んで枝番まで特定を試みる。失敗しても GMap URL は必ず生成
    - mode="rc_only" : 構造が RC系 のときだけ "on" と同じ挙動。それ以外は "off" と同じ
    - Claude へのフォールバックは一切しない

    後方互換: 旧 enabled=True/False も受け付ける (内部で mode に変換)
    """

    def __init__(
        self,
        places_bridge=None,
        *,
        mode: str = MODE_ON,
        enabled: Optional[bool] = None,  # 旧API互換 (廃止予定)
        logger: Optional[logging.Logger] = None,
    ) -> None:
        # places_bridge は PlacesBridge インスタンス (型を直接依存させないため型注釈なし)
        self._places_bridge = places_bridge
        # 旧 enabled が指定されていれば mode に変換
        if enabled is not None:
            mode = MODE_ON if enabled else MODE_OFF
        # 旧 mode="rc_only" を渡された場合は ON 扱い (フィルタは pipeline 側で実施)
        if mode == MODE_RC_ONLY:
            mode = MODE_ON
        if mode not in ALL_MODES:
            raise ValueError(
                f"mode は {ALL_MODES} のいずれかである必要があります (got: {mode!r})"
            )
        self._mode = mode
        self._logger = logger or logging.getLogger(__name__)

        self.success_count: int = 0
        self.failure_count: int = 0

    @property
    def mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> None:
        if mode == MODE_RC_ONLY:
            mode = MODE_ON
        if mode not in ALL_MODES:
            raise ValueError(
                f"mode は {ALL_MODES} のいずれかである必要があります (got: {mode!r})"
            )
        self._mode = mode

    # 旧 API 互換 (enabled bool)
    @property
    def enabled(self) -> bool:
        return self._mode != MODE_OFF

    def set_enabled(self, flag: bool) -> None:
        self._mode = MODE_ON if flag else MODE_OFF

    # ------------------------------------------------------------------
    # メイン処理
    # ------------------------------------------------------------------
    def predict(self, detail: PropertyDetail) -> AddressPredictionOutput:
        out = AddressPredictionOutput()
        suumo_address = detail.address or ""

        # 1) GMap URL は必ず SUUMO住所+物件名 ベースで準備
        gmap_query_base = suumo_address or ""
        if detail.name:
            gmap_query_base = f"{gmap_query_base} {detail.name}".strip()
        if gmap_query_base:
            out.google_map_url = build_gmap_url(gmap_query_base)

        # 2) mode="off" → Places は呼ばず、SUUMO住所 の完全性だけで判定
        if self._mode == MODE_OFF:
            if is_address_complete(suumo_address):
                out.predicted_address = suumo_address
            else:
                out.predicted_address = "検索結果 なし"
                out.needs_manual_check = True
                out.note = "住所予測不可"
            return out

        # 3) SUUMO住所 が既に枝番入りの完全住所 → Places を呼ばず即採用
        if is_address_complete(suumo_address):
            out.predicted_address = suumo_address
            return out

        # 4) Places 呼び出し
        if self._places_bridge is None:
            self._logger.info(
                "Places bridge 未設定のため予測スキップ (SUUMO住所が不完全)"
            )
            out.predicted_address = "検索結果 なし"
            out.needs_manual_check = True
            out.note = "住所予測不可"
            self.failure_count += 1
            return out

        pred_input = PredictionInput(
            name=detail.name,
            address=detail.address,
            nearest_station=detail.nearest_station,
            built_at=detail.built_at,
            structure=detail.structure,
            building_type=detail.building_type,
            floors=detail.floors,
            total_units=detail.total_units,
            detail_url=detail.detail_url,
            extra=detail.extra,
        )

        try:
            result: PredictionResult = self._places_bridge.predict(pred_input)
        except Exception as exc:
            self._logger.exception("Places bridge 呼び出しで例外: %s", exc)
            self.failure_count += 1
            out.predicted_address = "検索結果 なし"
            out.needs_manual_check = True
            out.note = "住所予測不可"
            return out

        # 5) Places 結果の判定
        places_addr = (result.predicted_address or "").strip()
        if places_addr and is_address_complete(places_addr):
            out.predicted_address = places_addr
            out.was_predicted = True
            if result.postal_code:
                out.postal_code = _normalize_postal(result.postal_code)
            if result.google_map_url:
                out.google_map_url = result.google_map_url
            self.success_count += 1

            # 6) Places が返した建物名が SUUMO 物件名と違う場合、備考に警告
            #    (例: SUUMO="コートリベルテI" / Places="コートリベルテ2" → 別棟ヒット)
            #    住所は取れているが別棟の住所の可能性があるので要手動確認に入れる。
            places_display = (result.display_name or "").strip()
            if places_display and not _names_match(detail.name, places_display):
                out.note = f"{places_display} の住所表示だから要チェック"
                out.needs_manual_check = True
        else:
            # 丁目止まり / 該当なし → 「検索結果 なし」
            out.predicted_address = "検索結果 なし"
            out.needs_manual_check = True
            out.note = "住所予測不可"
            self.failure_count += 1

        return out
