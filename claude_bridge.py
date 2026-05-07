"""
claude_bridge.py
----------------
住所予測モジュールが利用する「Claude連携レイヤー」のインターフェース。
"""
from __future__ import annotations

import json
import logging
import re
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PredictionInput:
    """Claude に渡す入力情報。"""

    name: str = ""
    address: str = ""
    nearest_station: str = ""
    built_at: str = ""
    structure: str = ""
    building_type: str = ""
    floors: str = ""
    total_units: str = ""
    detail_url: str = ""
    extra: dict[str, str] = field(default_factory=dict)


@dataclass
class PredictionResult:
    """Claude から返してもらう結果。"""

    predicted_address: str = ""
    postal_code: str = ""
    google_map_url: str = ""
    confidence: float = 0.0
    notes: str = ""
    # Google Places の displayName.text (ピンが立った建物の名前)
    # SUUMO 物件名と照合して「別棟にピンが立った」ケースを検出するために使う
    display_name: str = ""


PROMPT_TEMPLATE = """\
以下はSUUMO物件ライブラリーから取得した中古マンション情報です。
この物件の「正式住所(枝番まで含めた所在地)」と「郵便番号」を特定してください。

# 入力データ
- 物件名称: {name}
- SUUMO掲載住所: {address}
- 最寄駅 / 交通: {nearest_station}
- 築年月: {built_at}
- 構造: {structure}
- 種別: {building_type}
- 階建 / 規模: {floors}
- 総戸数: {total_units}
- 詳細ページURL: {detail_url}

# 追加情報(th/td 抽出結果抜粋)
{extra_block}

# タスク
1. 物件名と所在エリアから、該当する正式な住所を可能な限り特定してください
2. 住所には番地・号まで含めてください(例: 東京都千代田区飯田橋2-1-1)
3. 郵便番号も特定してください(例: 102-0072)
4. 信頼できない場合は無理に埋めず、confidence を低く設定してください

# 出力形式(JSON)
必ず以下の厳密な JSON 形式で、コードブロックや余計な文章なしに返してください。

{{
  "predicted_address": "都道府県から始まる正式住所",
  "postal_code": "XXX-XXXX",
  "google_map_url": "https://www.google.com/maps/search/?api=1&query=...",
  "confidence": 0.0〜1.0 の数値,
  "notes": "根拠や留意点"
}}
"""


def build_prompt(input_: PredictionInput) -> str:
    extra_lines: list[str] = []
    priority = ("交通", "最寄", "駅", "バス", "総戸数", "階", "構造", "用途", "施工", "売主", "管理")
    for key, value in input_.extra.items():
        if not value:
            continue
        if any(p in key for p in priority):
            extra_lines.append(f"- {key}: {value}")
    if not extra_lines:
        extra_lines.append("- (特記情報なし)")

    return PROMPT_TEMPLATE.format(
        name=input_.name or "(不明)",
        address=input_.address or "(不明)",
        nearest_station=input_.nearest_station or "(不明)",
        built_at=input_.built_at or "(不明)",
        structure=input_.structure or "(不明)",
        building_type=input_.building_type or "(不明)",
        floors=input_.floors or "(不明)",
        total_units=input_.total_units or "(不明)",
        detail_url=input_.detail_url or "(不明)",
        extra_block="\n".join(extra_lines),
    )


class ClaudeBridge(ABC):
    @abstractmethod
    def predict(self, input_: PredictionInput) -> PredictionResult: ...


class NoopBridge(ClaudeBridge):
    def predict(self, input_: PredictionInput) -> PredictionResult:
        return PredictionResult()


class FileQueueBridge(ClaudeBridge):
    """プロンプトをファイルに書き出して、Claude Code / Cowork 側で
    推論させ、同名 .result.json を返してもらう方式。"""

    def __init__(
        self,
        queue_dir: Path,
        timeout_sec: int = 30,
        poll_interval_sec: float = 2.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._dir = Path(queue_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._timeout = timeout_sec
        self._poll = poll_interval_sec
        self._logger = logger or logging.getLogger(__name__)
        self._consecutive_timeouts = 0
        self._disabled = False

    def predict(self, input_: PredictionInput) -> PredictionResult:
        # 一度でも連続タイムアウトが閾値を超えたら自動OFF(ハマり防止)
        if self._disabled:
            return PredictionResult()

        req_id = uuid.uuid4().hex[:12]
        prompt_path = self._dir / f"{req_id}.prompt.txt"
        input_path = self._dir / f"{req_id}.input.json"
        result_path = self._dir / f"{req_id}.result.json"
        failure_path = self._dir / f"{req_id}.skip"

        try:
            prompt_path.write_text(build_prompt(input_), encoding="utf-8")
            input_path.write_text(
                json.dumps(asdict(input_), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            self._logger.error("プロンプト書き込み失敗: %s", exc)
            return PredictionResult()

        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            if result_path.exists():
                self._consecutive_timeouts = 0
                return self._read_result(result_path)
            if failure_path.exists():
                self._logger.info("Claude 側で skip 指示: %s", req_id)
                self._consecutive_timeouts = 0
                return PredictionResult()
            time.sleep(self._poll)

        self._logger.warning("住所予測タイムアウト(req_id=%s)", req_id)
        self._consecutive_timeouts += 1
        if self._consecutive_timeouts >= 3:
            self._logger.warning(
                "連続%d回タイムアウトのため、以降の住所予測をスキップします",
                self._consecutive_timeouts,
            )
            self._disabled = True
        return PredictionResult()

    def _read_result(self, path: Path) -> PredictionResult:
        try:
            raw = path.read_text(encoding="utf-8").strip()
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if m:
                raw = m.group(0)
            data = json.loads(raw)
            return PredictionResult(
                predicted_address=str(data.get("predicted_address", "")),
                postal_code=str(data.get("postal_code", "")),
                google_map_url=str(data.get("google_map_url", "")),
                confidence=float(data.get("confidence", 0.0) or 0.0),
                notes=str(data.get("notes", "")),
            )
        except Exception as exc:
            self._logger.error("結果JSONパース失敗: %s", exc)
            return PredictionResult()


WEB_SEARCH_PROMPT_SUFFIX = """

# 追加指示 (Web検索ツール利用時)
あなたは Web検索ツール `web_search` を使用できます。**粘り強く** 番地・号まで特定してください。
簡単に諦めないこと。複数のクエリを試すこと。

## 検索戦略 (最低3回、情報が出揃うまで検索する)

### 必須クエリパターン (上から順に試す)
1. `物件名 住所` (例: `サンレイ白楽 六角橋`)
2. `物件名 都道府県 市区町村` (例: `サンレイ白楽 神奈川県横浜市神奈川区`)
3. `物件名 番地` (例: `コートリベルテI 菅田町 番地`)
4. `物件名 賃貸` や `物件名 中古マンション` (物件種別併記で精度UP)
5. 最寄駅名 + 物件名 (例: `白楽駅 サンレイ白楽`)

### 検索結果の見方
- ホームズ, SUUMO, アットホーム, マンションレビュー, マンションノート, nifty不動産,
  不動産ジャパン, スマイティ等の不動産サイトの検索結果スニペットに番地が書かれている
  ことが多い
- スニペット内の「〇〇町1-2-3」「〇〇2丁目3-4」「〇〇3-5-10」のような数値パターンを
  見逃さない
- 複数のサイトで同じ番地が書かれていれば確度高
- Google マップのサーチ結果で物件名を検索するとピンが立ち、その住所は概ね正しい
- ストリートビューで番地プレートが見える物件もある

### 粘り方
- 1回の検索で出なくても、キーワードを変えて **最低 3〜5 回は試す**
- 部分的にしか出ない場合でも、「町域+数字1つ」が見つかれば採用して良い
- 例えば「六角橋2-8」のように号までは特定できなくても、番地まで出てれば OK

## 出力ルール
- 番地 (少なくとも「町域+数字」) まで特定できたら predicted_address に
  「神奈川県横浜市神奈川区六角橋2-10-5」の形式で入れる
  (号がなければ「神奈川県横浜市神奈川区六角橋2-10」でも可)
- 本当にどこにも見つからなかった場合のみ **predicted_address に「検索結果 なし」**
  (丁目止まり「〇〇2丁目」は NG、必ず「検索結果 なし」)
- 郵便番号 (postal_code) は丁目レベルで特定できればそれでOK
- google_map_url は: 番地まで特定 → 番地付き住所で生成 / 特定できず → 物件名+エリア住所で生成
- notes に「どのサイトから拾ったか」を一言添える (例: "ホームズより")
"""


class ClaudeApiBridge(ClaudeBridge):
    """Claude API (Anthropic API) を直接叩く本番用ブリッジ。
    月1万件規模でも対応可能。事前に anthropic SDK のインストールが必要:
        pip install anthropic

    use_web_search=True にすると Claude の Web検索ツール (web_search_20250305)
    を有効にして、番地まで特定するために Web 検索を行います。
    追加コスト: 1検索あたり約 $0.01
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-haiku-4-5-20251001",
        max_tokens: int = 2500,
        use_web_search: bool = True,
        web_search_max_uses: int = 6,
        max_retries: int = 4,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._max_tokens = max_tokens
        self._use_web_search = use_web_search
        self._web_search_max_uses = web_search_max_uses
        self._max_retries = max_retries
        self._logger = logger or logging.getLogger(__name__)
        self._client = None  # 遅延初期化

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "anthropic SDK がインストールされていません。\n"
                    "  python3 -m pip install anthropic\n"
                    "を実行してください。"
                ) from exc
            self._client = anthropic.Anthropic(api_key=self._api_key)
        return self._client

    def predict(self, input_: PredictionInput) -> PredictionResult:
        prompt = build_prompt(input_)
        if self._use_web_search:
            prompt = prompt + WEB_SEARCH_PROMPT_SUFFIX

        kwargs: dict = {
            "model": self._model,
            "max_tokens": self._max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if self._use_web_search:
            kwargs["tools"] = [
                {
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": self._web_search_max_uses,
                }
            ]

        # レート制限・一時エラーをバックオフ付きでリトライ
        msg = None
        client = self._get_client()
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                msg = client.messages.create(**kwargs)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                err_str = str(exc)
                # 429 (rate limit) or 529 (overloaded) の時はバックオフして再試行
                if "429" in err_str or "rate_limit" in err_str or "overloaded" in err_str.lower():
                    wait = (2 ** attempt) * 15  # 15s, 30s, 60s, 120s
                    self._logger.warning(
                        "レート制限エラー(attempt %d/%d): %ds 待機してリトライ",
                        attempt + 1, self._max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                # web_search 非対応の場合は検索を外してリトライ
                if self._use_web_search and "web_search" in err_str.lower():
                    self._logger.info("Web検索なしで再試行します")
                    kwargs.pop("tools", None)
                    try:
                        msg = client.messages.create(**kwargs)
                        last_exc = None
                        break
                    except Exception as exc2:
                        last_exc = exc2
                        break
                # それ以外のエラーは即座にあきらめる
                break

        if msg is None or last_exc is not None:
            if last_exc is not None:
                self._logger.warning("Claude API 呼び出し失敗: %s", last_exc)
            return PredictionResult()

        # 応答テキスト抽出 (web search の tool_use/tool_result ブロックはスキップ)
        try:
            text_parts = []
            for block in msg.content:
                btype = getattr(block, "type", None)
                if btype == "text":
                    text_parts.append(getattr(block, "text", ""))
                elif isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(block.get("text", ""))
                # server_tool_use / web_search_tool_result はスキップ
            raw = "".join(text_parts).strip()
        except Exception as exc:
            self._logger.warning("Claude API 応答パース失敗: %s", exc)
            return PredictionResult()

        # JSON部分を抽出
        m = re.search(r"\{[^{}]*\"predicted_address\".*?\}", raw, re.DOTALL)
        if not m:
            m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            self._logger.warning("Claude 応答にJSONが含まれませんでした: %s", raw[:300])
            return PredictionResult()
        try:
            data = json.loads(m.group(0))
        except Exception as exc:
            self._logger.warning("Claude 応答JSONパース失敗: %s / raw=%s", exc, raw[:300])
            return PredictionResult()

        return PredictionResult(
            predicted_address=str(data.get("predicted_address", "")),
            postal_code=str(data.get("postal_code", "")),
            google_map_url=str(data.get("google_map_url", "")),
            confidence=float(data.get("confidence", 0.0) or 0.0),
            notes=str(data.get("notes", "")),
        )
