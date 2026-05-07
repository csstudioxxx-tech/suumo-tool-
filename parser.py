"""
parser.py
---------
SUUMO物件ライブラリーのHTML解析モジュール。

- 一覧ページから詳細URL/次ページURLを抽出
- 詳細ページから th/td ラベルベースで属性を取得
- class名ベタ書きに依存しすぎない実装方針
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag


# ---------------------------------------------------------------------
# 取得対象のラベル定義(th の文字列に対する部分一致)
# ---------------------------------------------------------------------
LABEL_ADDRESS: tuple[str, ...] = ("住所", "所在地")
LABEL_BUILT: tuple[str, ...] = ("築年月", "完成時期", "竣工", "築年")
LABEL_STRUCTURE: tuple[str, ...] = ("構造", "建物構造", "建物の構造", "構造・規模")
LABEL_STATION: tuple[str, ...] = ("交通", "最寄駅", "最寄り駅")
LABEL_TYPE: tuple[str, ...] = ("種別", "建物種別", "物件種別", "物件タイプ")
LABEL_FLOORS: tuple[str, ...] = ("階建", "規模")
LABEL_UNITS: tuple[str, ...] = ("総戸数", "戸数")


@dataclass
class PropertyDetail:
    """物件詳細データ構造。必要に応じて拡張可能。"""

    name: str = ""
    address: str = ""
    built_at: str = ""
    structure: str = ""
    nearest_station: str = ""
    building_type: str = ""
    floors: str = ""
    total_units: str = ""
    detail_url: str = ""
    # th/td 全取得結果(住所予測層に流し込む)
    extra: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------
# 汎用ヘルパ
# ---------------------------------------------------------------------
def _text(el: Optional[Tag]) -> str:
    if el is None:
        return ""
    raw = el.get_text(" ", strip=True)
    # 連続空白を 1 つにまとめる
    return re.sub(r"\s+", " ", raw).strip()


def _iter_th_td_pairs(soup: BeautifulSoup) -> Iterable[tuple[Tag, Tag]]:
    """table の tr を舐め、th → td のペアを逐次返す。
    1 つの tr に th,td,th,td と並ぶ SUUMO 方式にも対応。"""
    for tr in soup.find_all("tr"):
        last_th: Optional[Tag] = None
        for child in tr.children:
            name = getattr(child, "name", None)
            if name == "th":
                last_th = child
            elif name == "td" and last_th is not None:
                yield last_th, child
                last_th = None


def _collect_pairs(soup: BeautifulSoup) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for th, td in _iter_th_td_pairs(soup):
        label = _text(th)
        value = _text(td)
        if not label:
            continue
        # 先に見つかったものを優先
        if label not in pairs:
            pairs[label] = value
    # dl/dt/dd も補助的に取得
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = _text(dt)
            val = _text(dd)
            if key and key not in pairs:
                pairs[key] = val
    return pairs


def _pick(pairs: dict[str, str], keys: tuple[str, ...]) -> str:
    """ラベル部分一致で最初にマッチした値を返す。"""
    for key in keys:
        for label, value in pairs.items():
            if key in label:
                return value
    return ""


# ---------------------------------------------------------------------
# 物件名
# ---------------------------------------------------------------------
_NAME_SUFFIX_RE = re.compile(
    r"の(?:賃貸|中古マンション|新築マンション|分譲マンション|売買|中古|新築|分譲)?物件情報$"
)


def _clean_property_name(name: str) -> str:
    """物件名から SUUMO 由来の不要サフィックスを除去する。
    例: 'サンレイ白楽の賃貸物件情報' -> 'サンレイ白楽'
        'サンレイ白楽の物件情報'     -> 'サンレイ白楽'
    """
    if not name:
        return ""
    cleaned = _NAME_SUFFIX_RE.sub("", name).strip()
    return cleaned or name


def extract_property_name(soup: BeautifulSoup) -> str:
    """物件名を取得。h1 を最優先し、なければ title を使う。
    末尾の『の物件情報』『の賃貸物件情報』などは除去する。"""
    h1 = soup.find("h1")
    if h1:
        name = _text(h1)
        if name:
            return _clean_property_name(name)

    title = soup.find("title")
    if title:
        t = _text(title)
        # "◯◯｜SUUMO …" のような文字列をカット
        for sep in ("｜", "|", " - ", "／"):
            if sep in t:
                t = t.split(sep)[0].strip()
                break
        return _clean_property_name(t)
    return ""


# ---------------------------------------------------------------------
# 詳細ページ解析
# ---------------------------------------------------------------------
def parse_detail(html: str, detail_url: str = "") -> PropertyDetail:
    soup = BeautifulSoup(html, "lxml")
    pairs = _collect_pairs(soup)

    return PropertyDetail(
        name=extract_property_name(soup),
        address=_pick(pairs, LABEL_ADDRESS),
        built_at=_pick(pairs, LABEL_BUILT),
        structure=_pick(pairs, LABEL_STRUCTURE),
        nearest_station=_pick(pairs, LABEL_STATION),
        building_type=_pick(pairs, LABEL_TYPE),
        floors=_pick(pairs, LABEL_FLOORS),
        total_units=_pick(pairs, LABEL_UNITS),
        detail_url=detail_url,
        extra=pairs,
    )


# ---------------------------------------------------------------------
# 一覧ページ解析
# ---------------------------------------------------------------------
# ライブラリーの詳細ページに該当する URL パターン
#
# 実例:
#   https://suumo.jp/library/tf_14/sc_14102/to_1000002232/
#
# /library/ 配下で、末尾が to_数字 / nc_数字 / bs_数字 の形式を詳細とみなす。
DETAIL_URL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"/library/.+/to_\d+/?$"),
    re.compile(r"/library/.+/nc_\d+/?$"),
    re.compile(r"/library/.+/bs_\d+/?$"),
)


def extract_detail_urls(list_html: str, current_url: str) -> list[str]:
    """一覧ページ HTML から物件詳細 URL 一覧を抽出する。"""
    soup = BeautifulSoup(list_html, "lxml")
    urls: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        abs_url = urljoin(current_url, href)
        # クエリを除いた部分でマッチ判定
        abs_url_path = abs_url.split("?", 1)[0]
        if any(p.search(abs_url_path) for p in DETAIL_URL_PATTERNS):
            # フラグメントやクエリは除去
            canonical = abs_url_path.rstrip("/") + "/"
            if canonical not in seen:
                seen.add(canonical)
                urls.append(canonical)
    return urls


def extract_next_page_url(list_html: str, current_url: str) -> Optional[str]:
    """次ページ URL を取得。無ければ None。"""
    soup = BeautifulSoup(list_html, "lxml")

    # rel="next" を最優先
    link_next = soup.find("link", rel="next")
    if link_next and link_next.get("href"):
        return urljoin(current_url, link_next["href"])

    # 「次へ」系テキストを探す
    next_texts = {"次へ", "次のページ", "次のページへ", "›", "»", ">"}
    for a in soup.find_all("a", href=True):
        label = _text(a)
        if label in next_texts:
            return urljoin(current_url, a["href"])

    # pagination 内の最後の要素で「次へ」相当の aria-label があるか
    for a in soup.find_all("a", href=True):
        rel = a.get("rel") or []
        if "next" in rel:
            return urljoin(current_url, a["href"])
        aria = (a.get("aria-label") or "").lower()
        if aria in ("next", "次へ", "next page"):
            return urljoin(current_url, a["href"])

    return None
