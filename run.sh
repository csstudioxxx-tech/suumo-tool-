#!/usr/bin/env bash
# Mac / Linux 用の起動スクリプト。
# このファイルをダブルクリックか、ターミナルで ./run.sh と叩いて実行。

set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Python を探す (python3 を優先)
if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "Python が見つかりません。https://www.python.org/ からインストールしてください。"
  read -p "Enter で閉じる..."
  exit 1
fi

echo "Python: $($PY --version)"
$PY main.py
