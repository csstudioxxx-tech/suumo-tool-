"""
generate_secrets.py
-------------------
Streamlit Cloud に貼り付ける secrets を自動生成するヘルパー。

使い方:
    python3 generate_secrets.py

出力:
    Streamlit Cloud の Settings → Secrets 欄にそのまま貼り付ける TOML 文字列
    (クリップボードにもコピーされる)

事前準備:
    - credentials/service_account.json が同じディレクトリにあること
    - user_config.json に google_maps_api_key があること (なければ手で書き換え)
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    here = Path(__file__).resolve().parent

    # 1. service_account.json を読み込み
    sa_path = here / "credentials" / "service_account.json"
    if not sa_path.exists():
        print(f"❌ service_account.json が見つかりません: {sa_path}")
        print(f"  credentials/ フォルダに service_account.json を配置してください。")
        return 1
    sa_data = json.loads(sa_path.read_text(encoding="utf-8"))

    # 2. user_config.json から API キー取得 (あれば)
    cfg_path = here / "user_config.json"
    api_key = ""
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            api_key = cfg.get("google_maps_api_key", "")
        except Exception:
            pass

    if not api_key:
        api_key = input(
            "Google Maps API キーを入力してください (AIzaSy...): "
        ).strip()

    # 3. パスワード入力
    print()
    print("顧客に渡すパスワードを設定します。英数字10文字以上推奨。")
    pw = input("アプリのパスワード [Enter で 'Suumo2026Kousuke']: ").strip()
    if not pw:
        pw = "Suumo2026Kousuke"

    # 4. TOML 生成
    lines: list[str] = []
    lines.append(f'google_maps_api_key = "{api_key}"')
    lines.append(f'app_password = "{pw}"')
    lines.append("")
    lines.append("[gcp_service_account]")
    for k, v in sa_data.items():
        if isinstance(v, str):
            # 改行は \n のまま、ダブルクォートはエスケープ
            v_escaped = v.replace("\\", "\\\\").replace('"', '\\"')
            # JSON の private_key は実際の改行を含むので、それを \n に
            v_escaped = v_escaped.replace("\n", "\\n")
            lines.append(f'{k} = "{v_escaped}"')
        else:
            lines.append(f"{k} = {json.dumps(v, ensure_ascii=False)}")

    toml_text = "\n".join(lines)

    # 5. クリップボードへコピー (Mac)
    try:
        subprocess.run(
            ["pbcopy"], input=toml_text.encode("utf-8"), check=True
        )
        copied = True
    except Exception:
        copied = False

    # 6. ファイルにも保存 (確認用)
    out_path = here / "_secrets_for_streamlit.toml"
    out_path.write_text(toml_text, encoding="utf-8")

    # 7. 表示
    print()
    print("=" * 70)
    print(" Streamlit Cloud Secrets 用の TOML")
    print("=" * 70)
    print(toml_text)
    print("=" * 70)
    print()
    if copied:
        print("✅ 上記の内容を クリップボードにコピーしました!")
        print("   Streamlit Cloud の Secrets 欄に Cmd+V で貼り付けてください。")
    else:
        print(f"📁 ファイルに保存しました: {out_path}")
        print("   このファイルを開いて全選択 → コピー → Streamlit Secrets に貼付。")
    print()
    print("⚠️ このファイル (_secrets_for_streamlit.toml) は秘密情報です。")
    print("   GitHub にあげない (.gitignore で除外済み)、削除推奨。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
