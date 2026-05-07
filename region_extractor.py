"""
region_extractor.py
-------------------
「都道府県 + 市区町村」をページ / URL から抽出する責務を持つモジュール。

優先順位:
1. 詳細ページ or 一覧ページのパンくず/見出し/本文から抽出
2. URL (/library/tf_XX/sc_YYYYY/) から抽出
3. 失敗した場合は URL の sc_XXXXX を fallback として用いる
"""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

# 都道府県コード → 名称 (SUUMOの tf_XX と一致)
PREFECTURE_BY_CODE: dict[str, str] = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県",
    "05": "秋田県", "06": "山形県", "07": "福島県", "08": "茨城県",
    "09": "栃木県", "10": "群馬県", "11": "埼玉県", "12": "千葉県",
    "13": "東京都", "14": "神奈川県", "15": "新潟県", "16": "富山県",
    "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県",
    "25": "滋賀県", "26": "京都府", "27": "大阪府", "28": "兵庫県",
    "29": "奈良県", "30": "和歌山県", "31": "鳥取県", "32": "島根県",
    "33": "岡山県", "34": "広島県", "35": "山口県", "36": "徳島県",
    "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県",
    "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県",
}

# 都道府県名の正規表現
PREFECTURE_NAMES_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 市区町村の簡易パターン(「xxx市」「xxx区」「xxx町」「xxx村」「xxx郡」)
CITY_NAMES_RE = re.compile(r"([^\s、。,，\u3000]+?(?:市|区|町|村))")

# URL 由来のコード抽出
URL_TF_RE = re.compile(r"/tf_(\d{2})")
URL_SC_RE = re.compile(r"/sc_(\d{5})")


def _normalize(text: str) -> str:
    return re.sub(r"\s+", "", text).strip()


def extract_from_url(url: str) -> tuple[Optional[str], Optional[str]]:
    """URL から (都道府県名, sc_コード) を抽出。sc_ は fallback 用。"""
    pref = None
    tf_m = URL_TF_RE.search(url)
    if tf_m:
        pref = PREFECTURE_BY_CODE.get(tf_m.group(1))

    sc_m = URL_SC_RE.search(url)
    sc_code = sc_m.group(1) if sc_m else None
    return pref, sc_code


def _find_in_breadcrumb(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
    """パンくず領域から都道府県 + 市区町村を拾う。"""
    candidates: list[str] = []
    for container in soup.find_all(
        lambda t: t.name in ("ol", "ul", "nav", "div")
        and t.get("class") is not None
        and any("bread" in c.lower() or "crumb" in c.lower() for c in t.get("class", []))
    ):
        candidates.append(container.get_text(" ", strip=True))

    if not candidates:
        return None, None

    joined = " ".join(candidates)
    return _find_in_text(joined)


def _find_in_text(text: str) -> tuple[Optional[str], Optional[str]]:
    pref_m = PREFECTURE_NAMES_RE.search(text)
    if not pref_m:
        return None, None
    pref = pref_m.group(1)
    rest = text[pref_m.end():]
    # 市区町村を 1 つだけ取る(最初の一致)
    city_m = CITY_NAMES_RE.search(rest)
    city = city_m.group(1) if city_m else None
    return pref, city


def _find_in_address(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
    """住所らしいセルから抽出する。"""
    for th in soup.find_all("th"):
        label = th.get_text(strip=True)
        if "住所" in label or "所在地" in label:
            td = th.find_next("td")
            if td:
                return _find_in_text(td.get_text(" ", strip=True))
    # dt/dd
    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True)
        if "住所" in label or "所在地" in label:
            dd = dt.find_next("dd")
            if dd:
                return _find_in_text(dd.get_text(" ", strip=True))
    return None, None


def extract_region(
    page_html: Optional[str],
    url: str,
) -> tuple[str, str, str]:
    """(都道府県, 市区町村, fallback_id) を返す。

    - ページから抽出できなければ URL から試す
    - それでもダメなら fallback_id (sc_ コード) を返す
    """
    pref_url, sc_code = extract_from_url(url)
    fallback_id = f"sc_{sc_code}" if sc_code else ""

    pref_page: Optional[str] = None
    city_page: Optional[str] = None

    if page_html:
        try:
            soup = BeautifulSoup(page_html, "lxml")
            # 1. パンくず
            pref_page, city_page = _find_in_breadcrumb(soup)
            # 2. 住所セル
            if not (pref_page and city_page):
                p2, c2 = _find_in_address(soup)
                pref_page = pref_page or p2
                city_page = city_page or c2
            # 3. 見出し(h1)
            if not (pref_page and city_page):
                h1 = soup.find("h1")
                if h1:
                    p3, c3 = _find_in_text(h1.get_text(" ", strip=True))
                    pref_page = pref_page or p3
                    city_page = city_page or c3
        except Exception:
            # 解析失敗しても処理は止めない
            pass

    pref = pref_page or pref_url or ""
    city = city_page or ""
    return _normalize(pref), _normalize(city or ""), fallback_id
