"""
doctor.py
---------
環境診断ツール。「動かない…」となったらまずこれを実行。

チェック内容:
  - Python バージョン
  - 必須モジュール import 可否
  - tkinter 利用可否
  - credentials/service_account.json の有無と中身チェック
  - インターネット接続(SUUMO / Google に疎通できるか)
  - 書き込み可能性(logs ディレクトリ)
"""
from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

REQUIRED_MODULES = [
    "requests",
    "bs4",
    "gspread",
    "google.oauth2.service_account",
]

CHECK_HOSTS = [
    ("suumo.jp", 443),
    ("sheets.googleapis.com", 443),
]


def h(title: str) -> None:
    print(f"\n-- {title} --")


def ok(m: str) -> None:
    print(f"  OK  {m}")


def ng(m: str) -> None:
    print(f"  NG  {m}")


def warn(m: str) -> None:
    print(f"  !!  {m}")


def check_python() -> bool:
    h("Python バージョン")
    v = sys.version_info
    print(f"  Python {v.major}.{v.minor}.{v.micro} ({sys.executable})")
    if v < (3, 11):
        ng("3.11 以上が必要です")
        return False
    ok("バージョンOK")
    return True


def check_modules() -> bool:
    h("必須モジュール")
    all_ok = True
    for m in REQUIRED_MODULES:
        try:
            __import__(m)
            ok(m)
        except Exception as e:
            ng(f"{m}: {e}")
            all_ok = False
    return all_ok


def check_tkinter() -> bool:
    h("tkinter")
    try:
        import tkinter  # noqa: F401

        ok("利用可")
        return True
    except Exception as e:
        ng(f"{e}")
        return False


def check_credentials() -> bool:
    h("Google サービスアカウント認証ファイル")
    p = BASE_DIR / "credentials" / "service_account.json"
    if not p.exists():
        ng(f"ありません: {p}")
        return False
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        ng(f"JSON パース失敗: {e}")
        return False
    required = ("type", "project_id", "private_key", "client_email")
    missing = [k for k in required if k not in data]
    if missing:
        ng(f"必須キー不足: {missing}")
        return False
    ok(f"見つかりました (client_email = {data['client_email']})")
    print(f"      → このメールアドレスを対象スプレッドシートの『編集者』で共有してください")
    return True


def check_internet() -> bool:
    h("インターネット疎通")
    all_ok = True
    for host, port in CHECK_HOSTS:
        try:
            with socket.create_connection((host, port), timeout=5):
                ok(f"{host}:{port}")
        except Exception as e:
            ng(f"{host}:{port} に繋がりません: {e}")
            all_ok = False
    return all_ok


def check_writable() -> bool:
    h("ログ書込")
    p = BASE_DIR / "logs"
    try:
        p.mkdir(exist_ok=True)
        t = p / ".doctor_write_test"
        t.write_text("ok", encoding="utf-8")
        t.unlink()
        ok(f"{p}")
        return True
    except Exception as e:
        ng(f"{p}: {e}")
        return False


def main() -> int:
    print("=== SUUMOツール 環境診断 ===")
    results = {
        "python": check_python(),
        "modules": check_modules(),
        "tkinter": check_tkinter(),
        "credentials": check_credentials(),
        "internet": check_internet(),
        "writable": check_writable(),
    }
    h("サマリ")
    for k, v in results.items():
        print(f"  {'OK' if v else 'NG'}  {k}")
    if all(results.values()):
        print("\n→ すべてOK。python main.py で起動できます。")
        return 0
    print("\n→ 上の NG を順番に直してからもう一度 doctor.py を実行してください。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
