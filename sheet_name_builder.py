"""
sheet_name_builder.py
---------------------
シート名生成責務のみを担うモジュール。

形式:
    YY_MM/DD_都道府県市区町村

例:
    26_04/01_東京都渋谷区

同名シートがある場合は _2, _3 … と連番を付与する。
"""
from __future__ import annotations

from datetime import date
from typing import Callable, Iterable


def _base_name(
    pref: str,
    city: str,
    fallback_id: str,
    today: date,
) -> str:
    yy = f"{today.year % 100:02d}"
    mm = f"{today.month:02d}"
    dd = f"{today.day:02d}"

    region = ""
    if pref and city:
        region = f"{pref}{city}"
    elif pref:
        region = pref
    elif fallback_id:
        region = fallback_id
    else:
        region = "地域不明"

    return f"{yy}_{mm}/{dd}_{region}"


def build_sheet_name(
    pref: str,
    city: str,
    fallback_id: str = "",
    *,
    today: date | None = None,
    existing_names: Iterable[str] = (),
) -> str:
    """シート名を生成する。

    :param pref: 都道府県名
    :param city: 市区町村名
    :param fallback_id: fallback 用識別子 (例: sc_13113)
    :param today: 基準日 (テスト時に差し替え可能)
    :param existing_names: 既存シート名一覧 (連番付与判定用)
    """
    d = today or date.today()
    base = _base_name(pref, city, fallback_id, d)
    existing = set(existing_names)
    if base not in existing:
        return base

    # 連番を付与
    i = 2
    while True:
        candidate = f"{base}_{i}"
        if candidate not in existing:
            return candidate
        i += 1


# テスト・外部差し替え用
def build_sheet_name_from_provider(
    pref: str,
    city: str,
    fallback_id: str,
    existing_provider: Callable[[], Iterable[str]],
    today: date | None = None,
) -> str:
    return build_sheet_name(
        pref=pref,
        city=city,
        fallback_id=fallback_id,
        today=today,
        existing_names=existing_provider(),
    )
