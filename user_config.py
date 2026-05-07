"""
user_config.py
--------------
ユーザー固有の設定(前回入力したスプレッドシートID、URL、住所予測ON/OFF など)
を JSON で永続化するモジュール。

GUI の入力欄を毎回書きたくない要望に対応するための仕組み。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import BASE_DIR

CONFIG_PATH: Path = BASE_DIR / "user_config.json"


def load_config() -> dict[str, Any]:
    """設定を読み込む。ファイルがない/壊れている場合は空dict。"""
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_config(data: dict[str, Any]) -> None:
    """設定を保存。失敗しても例外を投げない。"""
    try:
        CONFIG_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def get(key: str, default: Any = "") -> Any:
    return load_config().get(key, default)


def update(**kwargs: Any) -> None:
    """複数キーを一括更新して保存。"""
    current = load_config()
    current.update(kwargs)
    save_config(current)
