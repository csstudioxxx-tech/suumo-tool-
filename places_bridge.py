"""
places_bridge.py
----------------
Google Places API (New) の Text Search を使って住所特定を行うモジュール。

コンセプト:
- ユーザーが手動で Google Maps に「物件名 + 住所」を打ち込んでピンの住所を確認する
  という作業を API で自動化する。
- SUUMO 住所 (例: 神奈川県横浜市神奈川区六角橋2) + 物件名 (例: サンレイ白楽) を
  Google Places Text Search に投げると、実際にピンが立つ地点の完全住所
  (例: 神奈川県横浜市神奈川区六角橋2丁目8-10) が返ってくる。

料金 (2025年4月時点):
- Essentials fields (formattedAddress, displayName, location) のみ使用 → $5 / 1000req
- 月1万件なら $50、Google Maps Platform 月$200クレジットで実質無料

事前準備:
1. https://console.cloud.google.com でプロジェクト作成
2. Billing 紐付け (クレカ登録)
3. 「APIとサービス」→「ライブラリ」で "Places API (New)" を有効化
4. 「APIとサービス」→「認証情報」で APIキーを発行
5. キーに「Places API (New)」制限を掛ける
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional
from urllib.parse import quote

import requests

from claude_bridge import PredictionInput, PredictionResult


PLACES_ENDPOINT = "https://places.googleapis.com/v1/places:searchText"

# Reverse Geocoding (番地レベルで返ってくる) のエンドポイント
# Places が建物までしか持っていないケースで番地取得するためのフォールバック
REVERSE_GEOCODING_ENDPOINT = "https://maps.googleapis.com/maps/api/geocode/json"

# Essentials fields のみ取得 (料金を最安に抑える)
FIELD_MASK = (
    "places.id,"
    "places.displayName,"
    "places.formattedAddress,"
    "places.location"
)

# 「住所が枝番まで揃っているか」判定用 (address_predictor._COMPLETE_PATTERNS と同じ)
# ここに持つのは places_bridge 内で Reverse Geocoding フォールバック判定に使うため
# (循環 import 回避のためローカル定義)
_EDABAN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\d+丁目\s*\d+"),
    re.compile(r"\d+[-−ー–‐]\d+"),
    re.compile(r"[町字]\s*\d+"),
    re.compile(r"\d+番(?:地)?\s*\d+"),
)


def _has_edaban(address: str) -> bool:
    """住所に枝番 (番地・号) が含まれているかの簡易判定。"""
    if not address:
        return False
    for pat in _EDABAN_PATTERNS:
        if pat.search(address):
            return True
    return False

# 日本語住所からの 郵便番号 抽出用 (全角数字にも対応)
_POSTAL_RE = re.compile(
    r"〒?\s*([\d０-９]{3})[-－‐‑‒–—]?([\d０-９]{4})"
)

# "日本、〒XXX-XXXX " / "日本, 〒XXX-XXXX " のプレフィックス除去
_PREFIX_RE = re.compile(r"^日本[、,]\s*〒?\s*\d{3}[-－]?\d{4}\s*")

# 先頭の「日本」だけの時 (郵便番号なし) も除去
_LEADING_JP_RE = re.compile(r"^日本[、,]\s*")

# 先頭に 〒XXX-XXXX だけある場合の除去 (例: '〒221-0802 神奈川県...')
_POSTAL_PREFIX_RE = re.compile(r"^〒\s*\d{3}[-－‐‑‒–—]?\d{4}[\s\u3000]+")

# 末尾に「スペース + 非空白文字列」があるときに削るための正規表現
# 住所末尾に物件名がくっついてくる Places API の仕様への対応
_TRAILING_NAME_RE = re.compile(
    r"^(.+[\d\-])[\s\u3000]+\S+$"
)

# 全角数字・ハイフン・スペース → 半角に正規化するテーブル
_ZENKAKU_TO_HANKAKU = str.maketrans({
    "０": "0", "１": "1", "２": "2", "３": "3", "４": "4",
    "５": "5", "６": "6", "７": "7", "８": "8", "９": "9",
    "－": "-", "ー": "-", "−": "-", "‐": "-", "‑": "-",
    "‒": "-", "–": "-", "—": "-",
    "　": " ",
})


def _normalize_digits(s: str) -> str:
    """全角数字・全角ハイフン・全角スペースを半角に正規化。"""
    return s.translate(_ZENKAKU_TO_HANKAKU)


def _strip_trailing_name(address: str) -> str:
    """住所末尾にスペース区切りでくっついた物件名っぽい部分を削る。
    - '六角橋2丁目13 サンレイ白楽' -> '六角橋2丁目13'
    - '菅田町851 コートリベルテ2' -> '菅田町851'
    - '菅田町シャトルカワハラ'    -> '菅田町シャトルカワハラ' (数字+スペース区切り無し → 触らない)
    - '羽沢町カンナハイツ羽沢'    -> '羽沢町カンナハイツ羽沢' (同上)
    """
    if not address:
        return address
    m = _TRAILING_NAME_RE.match(address)
    if m:
        return m.group(1).strip()
    return address


def _clean_address(formatted_address: str) -> str:
    """Places API の formattedAddress を、スプレッドシートに書ける綺麗な形に整形。

    例:
    - '日本、〒221-0802 神奈川県横浜市神奈川区六角橋2丁目8-10'
       → '神奈川県横浜市神奈川区六角橋2丁目8-10'
    - '〒221-0802 神奈川県横浜市神奈川区六角橋２丁目１３ サンレイ白楽'
       → '神奈川県横浜市神奈川区六角橋2丁目13'  (郵便番号/全角/物件名を除去)
    - '〒221-0864 神奈川県横浜市神奈川区菅田町シャトルカワハラ'
       → '神奈川県横浜市神奈川区菅田町シャトルカワハラ'
       (枝番が無いので後段の完全性判定で落とされる)
    """
    s = (formatted_address or "").strip()
    # 1) 先頭のプレフィックスを順に除去
    s = _PREFIX_RE.sub("", s)         # "日本、〒XXX-XXXX "
    s = _LEADING_JP_RE.sub("", s)     # "日本、"
    s = _POSTAL_PREFIX_RE.sub("", s)  # "〒XXX-XXXX "
    # 2) 全角数字 → 半角数字、全角ハイフン・スペース → 半角
    s = _normalize_digits(s)
    # 3) 末尾の物件名を削除
    s = _strip_trailing_name(s)
    return s.strip()


def _extract_postal(formatted_address: str) -> str:
    """住所から 郵便番号 を XXX-XXXX の形で取り出す。見つからなければ空文字。"""
    m = _POSTAL_RE.search(formatted_address or "")
    if not m:
        return ""
    return f"{m.group(1)}-{m.group(2)}"


def _extract_town_key(suumo_address: str) -> Optional[str]:
    """SUUMO住所から「町域名 (丁目前まで)」を抽出して、比較用キーを作る。
    例: '神奈川県横浜市神奈川区六角橋2' → '六角橋'
        '東京都千代田区飯田橋2-1-1'    → '飯田橋'
    """
    if not suumo_address:
        return None
    s = suumo_address
    # 都道府県 → 市 → 区/町村 の順に剥がす
    s = re.sub(r"^.+?[都道府県]", "", s)
    s = re.sub(r"^.+?市", "", s)
    s = re.sub(r"^.+?区", "", s)
    s = re.sub(r"^.+?郡.+?[町村]", "", s)
    # 先頭の町域名 (数字・丁目・番地が始まる直前まで) を取る
    m = re.match(r"^([^\d\s丁目番地号]+)", s)
    if not m:
        return None
    town = m.group(1).strip()
    return town or None


def _address_matches_suumo(places_addr: str, suumo_addr: str) -> bool:
    """Places の住所が SUUMO住所と整合しているか。
    同名別物件にピンが立った場合の誤採用を防ぐためのガード。
    - 町域名 (丁目前) が両方に含まれていれば整合とみなす
    - SUUMO住所が空なら検証スキップ (True)
    """
    if not suumo_addr:
        return True
    if not places_addr:
        return False
    town = _extract_town_key(suumo_addr)
    if not town:
        return True
    return town in places_addr


class PlacesBridge:
    """Google Places API (New) Text Search を使う住所特定ブリッジ。

    claude_bridge.ClaudeBridge と同じ predict(input_) -> PredictionResult
    のインターフェースを持つが、継承はしない (別系統なので)。
    """

    def __init__(
        self,
        api_key: str,
        *,
        language_code: str = "ja",
        region_code: str = "JP",
        timeout_sec: int = 15,
        max_retries: int = 3,
        max_result_count: int = 3,
        verify_against_suumo: bool = True,
        use_reverse_geocoding_fallback: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._api_key = api_key
        self._language = language_code
        self._region = region_code
        self._timeout = timeout_sec
        self._max_retries = max_retries
        self._max_results = max_result_count
        self._verify = verify_against_suumo
        self._use_revgeo = use_reverse_geocoding_fallback
        self._logger = logger or logging.getLogger(__name__)

        self.call_count: int = 0
        self.hit_count: int = 0
        self.miss_count: int = 0
        # Reverse Geocoding 呼び出し回数 (課金監視用)
        self.revgeo_call_count: int = 0
        self.revgeo_hit_count: int = 0

    # ------------------------------------------------------------------
    def predict(self, input_: PredictionInput) -> PredictionResult:
        if not self._api_key:
            return PredictionResult()

        name = (input_.name or "").strip()
        addr = (input_.address or "").strip()
        if not name and not addr:
            return PredictionResult()

        # 検索クエリは「物件名 + SUUMO住所」
        # Google Maps 検索窓に手動で打つのと同じテキスト。
        query = " ".join(filter(None, [name, addr])).strip()
        if not query:
            return PredictionResult()

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self._api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        }
        body = {
            "textQuery": query,
            "languageCode": self._language,
            "regionCode": self._region,
            "maxResultCount": self._max_results,
        }

        data = self._call_with_retry(headers, body, query)
        self.call_count += 1
        if data is None:
            self.miss_count += 1
            return PredictionResult()

        places = data.get("places") or []
        if not places:
            self._logger.info("Places API: 該当なし (query=%s)", query)
            self.miss_count += 1
            return PredictionResult()

        # 先頭から SUUMO住所と整合するものを採用
        for place in places:
            formatted = place.get("formattedAddress", "")
            if not formatted:
                continue
            cleaned = _clean_address(formatted)
            if self._verify and not _address_matches_suumo(cleaned, addr):
                self._logger.info(
                    "Places API: SUUMO住所と不一致のためスキップ (places=%s / suumo=%s)",
                    cleaned, addr,
                )
                continue

            postal = _extract_postal(formatted)
            place_id = place.get("id", "")
            location = place.get("location") or {}
            lat = location.get("latitude")
            lng = location.get("longitude")

            # --- Reverse Geocoding フォールバック ---
            # Places が建物までしか住所を持っておらず枝番が無いケースで、
            # 座標があるなら Reverse Geocoding (parcel レベル) で番地を拾う。
            if (
                self._use_revgeo
                and not _has_edaban(cleaned)
                and lat is not None
                and lng is not None
            ):
                revgeo = self._reverse_geocode(lat, lng)
                if revgeo:
                    rev_clean = _clean_address(revgeo.get("formatted_address", ""))
                    rev_postal = _extract_postal(
                        revgeo.get("formatted_address", "")
                    )
                    # SUUMO住所と整合 & 枝番入り ならフォールバックを採用
                    if _has_edaban(rev_clean) and (
                        not self._verify or _address_matches_suumo(rev_clean, addr)
                    ):
                        self._logger.info(
                            "Reverse Geocoding 採用: %s -> %s",
                            cleaned, rev_clean,
                        )
                        cleaned = rev_clean
                        if rev_postal:
                            postal = rev_postal
                        self.revgeo_hit_count += 1

            gmap_url = self._build_gmap_url(cleaned, name, place_id, lat, lng)

            # displayName は {text, languageCode} の辞書
            display_obj = place.get("displayName") or {}
            display_name = (
                display_obj.get("text", "")
                if isinstance(display_obj, dict)
                else str(display_obj)
            )

            self.hit_count += 1
            return PredictionResult(
                predicted_address=cleaned,
                postal_code=postal,
                google_map_url=gmap_url,
                confidence=0.9,
                notes="Google Places Text Search",
                display_name=display_name,
            )

        self._logger.info(
            "Places API: 候補ありだが全て SUUMO住所と不一致 (query=%s)", query
        )
        self.miss_count += 1
        return PredictionResult()

    # ------------------------------------------------------------------
    def _reverse_geocode(
        self, lat: float, lng: float
    ) -> Optional[dict]:
        """Reverse Geocoding で緯度経度から番地付き住所を取得。
        premise / street_address / subpremise の順に優先して採用する。
        返り値: {"formatted_address": str, ...} もしくは None
        """
        if not self._api_key:
            return None

        params = {
            "latlng": f"{lat},{lng}",
            "key": self._api_key,
            "language": self._language,
            "region": self._region.lower(),
            # premise (建物/敷地) を含む結果を返してもらう
            "result_type": "premise|subpremise|street_address",
        }

        self.revgeo_call_count += 1
        try:
            r = requests.get(
                REVERSE_GEOCODING_ENDPOINT,
                params=params,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            self._logger.warning("Reverse Geocoding 通信失敗: %s", exc)
            return None

        if not r.ok:
            self._logger.warning(
                "Reverse Geocoding HTTP %d: %s", r.status_code, r.text[:200]
            )
            return None

        try:
            data = r.json()
        except ValueError:
            self._logger.warning("Reverse Geocoding JSON パース失敗")
            return None

        if data.get("status") not in ("OK",):
            # ZERO_RESULTS / REQUEST_DENIED 等
            self._logger.info(
                "Reverse Geocoding status=%s (error=%s)",
                data.get("status"), data.get("error_message", ""),
            )
            return None

        results = data.get("results") or []
        if not results:
            return None

        # premise > subpremise > street_address の優先度で選ぶ
        priority = {"premise": 0, "subpremise": 1, "street_address": 2}
        def rank(result: dict) -> int:
            types = result.get("types") or []
            best = 99
            for t in types:
                if t in priority:
                    best = min(best, priority[t])
            return best

        results.sort(key=rank)
        chosen = results[0]
        return chosen

    # ------------------------------------------------------------------
    def _call_with_retry(
        self, headers: dict, body: dict, query_for_log: str
    ) -> Optional[dict]:
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                r = requests.post(
                    PLACES_ENDPOINT,
                    headers=headers,
                    json=body,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                last_exc = exc
                self._logger.warning(
                    "Places API 通信失敗(attempt %d/%d): %s",
                    attempt + 1, self._max_retries, exc,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(1 + attempt)
                continue

            # レート制限
            if r.status_code == 429:
                wait = 2 ** attempt
                self._logger.warning(
                    "Places API 429(attempt %d/%d): %ds 待機",
                    attempt + 1, self._max_retries, wait,
                )
                time.sleep(wait)
                continue

            # 認証エラー等はリトライ不要
            if r.status_code in (400, 401, 403):
                self._logger.error(
                    "Places API エラー %d: %s (query=%s)",
                    r.status_code, r.text[:300], query_for_log,
                )
                return None

            if not r.ok:
                last_exc = RuntimeError(
                    f"HTTP {r.status_code}: {r.text[:200]}"
                )
                self._logger.warning(
                    "Places API 異常レスポンス(attempt %d/%d): %s",
                    attempt + 1, self._max_retries, last_exc,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(1 + attempt)
                continue

            try:
                return r.json()
            except ValueError as exc:
                self._logger.warning("Places API JSON パース失敗: %s", exc)
                return None

        if last_exc:
            self._logger.warning("Places API リトライ尽きて諦めました: %s", last_exc)
        return None

    # ------------------------------------------------------------------
    @staticmethod
    def _build_gmap_url(
        address: str,
        name: str,
        place_id: str = "",
        lat: Optional[float] = None,
        lng: Optional[float] = None,
    ) -> str:
        """Google Maps 検索用 URL を生成。
        place_id があるとピンが正確にその地点に立つ。"""
        # place_id 指定形式: 最も精度が高い
        if place_id:
            q_text = address or name or ""
            q = quote(q_text, safe="")
            return (
                f"https://www.google.com/maps/search/?api=1&query={q}"
                f"&query_place_id={place_id}"
            )

        # 座標があるなら座標+住所で検索
        if lat is not None and lng is not None:
            q = quote(address or name or f"{lat},{lng}", safe="")
            return (
                f"https://www.google.com/maps/search/?api=1&query={q}"
            )

        q_parts = [p for p in (address, name) if p]
        q = quote(" ".join(q_parts), safe="")
        return f"https://www.google.com/maps/search/?api=1&query={q}"
