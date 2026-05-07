# SUUMO物件ライブラリー収集ツール

SUUMO物件ライブラリーの一覧URLを起点に、後続ページまで全巡回して物件情報を
Googleスプレッドシートに出力する、デスクトップ向け業務ツールです。

住所に枝番が載っていない物件については、Claude Code / Cowork と連動して
「枝番まで含んだ正式住所 + 郵便番号 + Google Map URL」を補完します。

---

## 特長

- 壊れにくい th/td ラベルベースのHTML解析
- アクセス間隔 10 秒以上を厳守
- ページ巡回は自動判定(無限ループ防止あり)
- 物件名 + 住所 + 詳細URL をキーにした重複排除
- 実行日とURL/ページ内容から自動でシート名を生成
- 住所予測モジュールは依存性注入設計(Claude Code/Cowork/API 差し替え可)
- 例外発生時もログを残して処理を継続

---

## ディレクトリ構成

```
suumo_scraper/
├── main.py                  # エントリポイント(GUI起動)
├── gui.py                   # tkinter UI
├── pipeline.py              # 実行エンジン(各層の統合)
├── scraper.py               # HTTP取得層(UA/間隔/リトライ)
├── parser.py                # HTML解析層(th/td ベース)
├── region_extractor.py      # 都道府県・市区町村抽出
├── sheet_name_builder.py    # シート名生成
├── sheets.py                # Googleスプレッドシート出力
├── address_predictor.py     # 住所予測層(ON/OFF, 不完全住所判定)
├── claude_bridge.py         # Claude連携インターフェース(DI可)
├── config.py                # 定数・設定値
├── requirements.txt
├── credentials/
│   └── service_account.json # ★各自配置(Git管理外推奨)
├── logs/                    # 実行ログ
└── claude_queue/            # 住所予測用プロンプト/結果 受け渡しディレクトリ
```

---

## セットアップ

### 1. Python

Python **3.11 以上** を推奨。

```bash
python --version
```

### 2. 依存パッケージ

```bash
pip install -r requirements.txt
```

Windows で tkinter が入っていない場合は Python 公式インストーラで「tcl/tk」を
チェックして再インストールしてください。

### 3. Google API 認証

1. [Google Cloud Console](https://console.cloud.google.com/) でプロジェクトを作成
2. **Google Sheets API** と **Google Drive API** を有効化
3. サービスアカウントを作成 → 鍵を **JSON** で発行
4. 取得した JSON を `credentials/service_account.json` に配置
5. 書き込みたいスプレッドシートを、サービスアカウントの e-mail に
   「編集者」として共有する(超重要)

### 4. スプレッドシートID

スプレッドシートのURLが以下の形式のとき、

```
https://docs.google.com/spreadsheets/d/1AbCdefGhIjkLMNOPQrstUvwxyz/edit#gid=0
```

`1AbCdefGhIjkLMNOPQrstUvwxyz` の部分が **スプレッドシートID** です。
GUI の「スプレッドシートID」欄に貼り付けてください。

---

## 使い方

```bash
python main.py
```

GUI が起動します。

1. 「一覧URL」欄に SUUMO物件ライブラリーの一覧URLを貼り付け
   例: `https://suumo.jp/library/tf_13/sc_13113/`
2. 「スプレッドシートID」欄に出力先のIDを貼り付け
3. 必要なら「住所予測を有効にする」チェックを ON
4. 「実行」ボタンを押す
5. 進捗が「処理状況」と「ログ」に流れ、完了すると
   `YY_MM/DD_都道府県市区町村` 形式のシートに結果が書き込まれます

途中で止めたくなったら「停止」ボタン。

---

## 出力カラム

1. 物件名称
2. 住所
3. 築年月
4. 構造
5. 住所予測
6. 予測住所の郵便番号
7. 予測住所のGoogle Map URL

## シート名

実行日と対象地域から自動生成されます。

- 形式: `YY_MM/DD_都道府県市区町村`
- 例:   `26_04/17_東京都渋谷区`
- 同名シートが既にある場合は `_2`, `_3` … と連番付与

---

## 住所予測(Claude連携)の動作

住所予測 ON のとき、各詳細物件について以下を実行します。

1. SUUMO 住所に枝番(`1-2-3` や `◯番地◯号` 等) が含まれていれば、
   予測はスキップし、SUUMO 住所をそのまま「住所予測」列に入れる
2. 含まれていなければ、`claude_queue/` に以下 2 ファイルを書き出す
   - `<id>.prompt.txt` ← Claude に渡す日本語プロンプト
   - `<id>.input.json` ← 機械可読な入力データ
3. Claude Code / Cowork が `<id>.prompt.txt` を読み、同ディレクトリに
   `<id>.result.json` を書き出す
4. ツールは `<id>.result.json` を読み取り、住所/郵便番号/GMap URL を反映
5. タイムアウト(既定 180 秒)を超えた場合は空欄のまま次へ進む

この設計によりメインのスクレイピング処理を止めることなく、Claude 側の
推論を並列・非同期に走らせることができます。

### Cowork での運用例

1. このツールを起動したまま、別セッションで Claude/Cowork に
   「`claude_queue/` を監視して `.prompt.txt` を読み、指定フォーマットの
   `.result.json` を返してください」と指示
2. ツール本体は自動で結果を拾って続行

### 他の推論基盤への切替

`claude_bridge.py` に `ClaudeApiBridge` のスケルトンがあります。
`ClaudeBridge` を継承した自作クラスを `gui.py` の該当箇所に差し替えるだけで
Claude API でも手動フローでも置き換え可能です。

---

## ログ

- 実行ログ: `logs/run_YYYYMMDD_HHMMSS.log`
- 住所予測用のやりとり: `claude_queue/<id>.*`
- GUI 下部の「ログ」ウィンドウにも同内容を出力

---

## 今後の拡張ポイント

- 住所予測を Claude API 直叩きに切替(`ClaudeApiBridge` を実装)
- 失敗物件だけ再実行する CLI ツール追加
- プロキシ対応 / IP ローテーション
- CSV/Excel への並行出力
- 詳細ページ項目の追加(価格帯、売出履歴、etc.)
