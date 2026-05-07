"""
setup.py
--------
ワンコマンド・セットアップスクリプト。

使い方:
    python setup.py

やること:
    1. Python バージョンをチェック
    2. 必要フォルダ(credentials / logs / claude_queue)を作成
    3. pip で requirements.txt を一括インストール
    4. tkinter が使えるか確認
    5. service_account.json の有無を確認
    6. 最後に次にやることを案内
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NEEDED_DIRS = ("credentials", "logs", "claude_queue")


def step(n: int, title: str) -> None:
    print(f"\n===== [{n}] {title} =====")


def ok(msg: str) -> None:
    print(f"  [OK] {msg}")


def warn(msg: str) -> None:
    print(f"  [!!] {msg}")


def ng(msg: str) -> None:
    print(f"  [NG] {msg}")


def check_python() -> bool:
    step(1, "Python バージョン確認")
    v = sys.version_info
    print(f"  Python {v.major}.{v.minor}.{v.micro}")
    if v < (3, 11):
        ng("Python 3.11 以上が必要です。https://www.python.org/ から新しい版を入れてください。")
        return False
    ok("Python のバージョンOK")
    return True


def make_dirs() -> None:
    step(2, "必要フォルダを作成")
    for d in NEEDED_DIRS:
        p = BASE_DIR / d
        p.mkdir(exist_ok=True)
        ok(f"{d}/")


def install_requirements() -> bool:
    step(3, "依存パッケージのインストール")
    req = BASE_DIR / "requirements.txt"
    if not req.exists():
        ng("requirements.txt が見つかりません。")
        return False
    cmd = [sys.executable, "-m", "pip", "install", "-r", str(req)]
    print(f"  実行: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        ok("インストール完了")
        return True
    except subprocess.CalledProcessError as e:
        ng(f"pip install が失敗しました: {e}")
        return False


def check_tkinter() -> bool:
    step(4, "tkinter (GUI用ライブラリ) の確認")
    try:
        import tkinter  # noqa: F401

        ok("tkinter 利用可")
        return True
    except Exception as e:
        ng(f"tkinter が使えません: {e}")
        warn("Mac: 公式 Python インストーラ版を使うか、brew install python-tk で解決できます。")
        warn("Windows: 公式インストーラ再実行時に 'tcl/tk and IDLE' をチェックしてください。")
        return False


def check_credentials() -> bool:
    step(5, "Google サービスアカウント認証ファイルの確認")
    cred = BASE_DIR / "credentials" / "service_account.json"
    if cred.exists():
        ok(f"{cred} が見つかりました")
        return True
    warn(f"{cred} がまだありません。")
    warn("Google Cloud Console で JSON 鍵を発行し、credentials/ に service_account.json として置いてください。")
    warn("詳しくは セットアップガイド.docx を参照。")
    return False


def check_modules() -> bool:
    step(6, "主要パッケージの読み込みテスト")
    mods = ["requests", "bs4", "gspread", "google.oauth2.service_account"]
    all_ok = True
    for m in mods:
        try:
            __import__(m)
            ok(f"import {m}")
        except Exception as e:
            ng(f"import {m}: {e}")
            all_ok = False
    return all_ok


def print_next_steps(cred_ok: bool) -> None:
    step(99, "つぎにやること")
    print(
        """
    1) まだなら Google Cloud Console で:
         - プロジェクト作成
         - Google Sheets API / Google Drive API を有効化
         - サービスアカウントを作成して JSON 鍵を発行
       発行した JSON を credentials/service_account.json として置く
       ※ 詳細は 同梱の『セットアップガイド.docx』

    2) 出力先の Google スプレッドシートを用意し、
       サービスアカウントのメールアドレスを『編集者』として共有する

    3) つぎのコマンドでツールを起動:
         python main.py
        """
    )
    if not cred_ok:
        warn("現時点では credentials/service_account.json が未設置なので、step 1 を完了してから起動してください。")


def main() -> int:
    print(f"=== SUUMO物件ライブラリー収集ツール セットアップ ===")
    print(f"作業ディレクトリ: {BASE_DIR}")

    if not check_python():
        return 1
    make_dirs()
    if not install_requirements():
        return 1
    check_tkinter()
    check_modules()
    cred_ok = check_credentials()
    print_next_steps(cred_ok)
    return 0


if __name__ == "__main__":
    sys.exit(main())
