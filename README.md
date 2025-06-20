# swimg

J-Quants の株価・財務データを取得し、
スクリーニングやバックテストを行うツール群です。

## 開発手順

開発用ツールをインストールし、`pre-commit` フックを設定します。

```bash
pip install pre-commit
pre-commit install
```

コミット時に `black` と `ruff` が自動で実行されます。

## セットアップ

Python 3.9 以上を想定しています。必要なライブラリは
`requirements.txt` にまとめています。

```bash
pip install -r requirements.txt
```

J‑Quants API の `idToken` を取得し、次の内容で `idtoken.json` を作成してください。

```json
{"idToken": "YOUR_TOKEN"}
```

J‑Quants の認証に使用するメールアドレスとパスワードを保存する
`account.json` を用意しておくと、`update_idtoken.py` から自動的に参照されます。

Web アプリ用の認証情報を分けたい場合は `login.json` を用意してください。
こちらには Web アプリにログインする際の **ID** とパスワード（または
`password_hash`）を保存します。`login.json` がない場合は `account.json`
が使われますが、この場合はメールアドレスが ID として扱われます。
`LOGIN_ACCOUNT` 環境変数でこのファイルの場所を変更できます。

```json
{"id": "YOUR_ID", "password": "YOUR_PASSWORD", "password_hash": "<hash>"}
```
`password_hash` は次のように生成できます。

```bash
python - <<'EOF'
from werkzeug.security import generate_password_hash
print(generate_password_hash('YOUR_PASSWORD'))
EOF
```

このファイルは `.gitignore` に含まれ、リポジトリには登録されません。

続いて SQLite データベースを初期化します。

```bash
python db/db_schema.py
```
`db/stock.db` が生成されれば準備完了です。

## 主なスクリプトと起動引数

* `fetch/daily_quotes.py`  
  日足株価を取得して `prices` テーブルへ保存します。  
  `--start` と `--end` を指定すると期間を取得します（省略時は当日分）。

* `fetch/listed_info.py`  
  上場銘柄情報を取得して `listed_info` テーブルを更新します。  
  引数はありません。

* `fetch/statements.py`  
  決算データを取得して `statements` テーブルに保存します。  
  `mode` に `1` を指定すると銘柄単位で一括取得、`2` を指定すると日付または期間を取得します。  
  `--start` と `--end` を併用することで期間を指定できます。
* `screening/screen_statements.py`
  財務データをスクリーニングし、シグナルを `fundamental_signals` に保存します。
  `--lookback` 過去参照日数、`--recent` 開示閾値日数、`--as-of` 基準日（省略時は当日）を指定できます。
* `screening/screen_technical.py`
  `indicators` または `screen` をコマンドとして指定します。
  `--as-of` で対象日を指定し、`--lookback` は遡る日数です。
  当日の `prices` データが存在しない場合は処理をスキップします。
* `backtest/backtest_statements.py`
  財務シグナルを用いたバックテストを実行します。
  `--hold` 保有日数、`--entry-offset` エントリー日のオフセット、`--capital` 資金、
  `--min-price` 最低株価、`--start` と `--end` で対象期間を指定します。結果は毎回タイムスタンプ付きの
  Excel と JSON に保存され、`--xlsx` と `--json` でファイル名を変更できます。
  `--show` を付けるとサマリーを標準出力に表示します。
* `backtest/backtest_technical.py`
  テクニカル指標を用いたスイングトレードのバックテストを行います。
  `--start` と `--end` でエントリー期間を指定し、`--hold-days` 保有日数、
  `--stop-loss` 損切り率、`--min-price` 最低株価、`--capital` 資金を与えます。結果は実行ごとに
  タイムスタンプ付きの Excel と JSON に保存されます。`--outfile` と
  `--json` で保存先を変更できます。`--show` を指定するとサマリーが
  標準出力に表示されます。
* `backtest/analyze_backtest_json.py`
  バックテスト結果として保存した JSON ファイルを読み込み、損益や
  勝率などの指標を集計するツールです。複数ファイルを指定して
  まとめて分析することもできます。GUI の「JSON分析」タブからも
  実行できます。以下は簡単な実行例です。

  ```bash
  $ python backtest/analyze_backtest_json.py sample.json --show-trades

  === Summary ===
        trades: 2
  total_profit: 500 JPY
      win_rate: 50.00%
   avg_ret_pct: 0.02%
        sharpe: 0.43

  === Trades ===
  +------+------------+---------+
  | code | profit_jpy | ret_pct |
  +------+------------+---------+
  | 1234 |       1000 |    0.05 |
  +------+------------+---------+
  | 5678 |       -500 |   -0.02 |
  +------+------------+---------+

  === Profit per Trade ===
    1 ######################################## (1000)
    2 -#################### (-500)
  ```
* `update_idtoken.py`
  J‑Quants にログインして `idtoken.json` を更新します。
  `--mail` と `--password` を省略すると `account.json` が参照されます。
* `db/db_summary.py`
  データベースの各テーブル件数と日付範囲を表示します。引数はありません。
  GUI の「DBサマリー」タブからも確認できます。
* `db/list_signals.py`
  `fundamental_signals` または `technical_indicators` テーブルから
  スクリーニング結果を表示します。引数 `fund`/`tech` で種別を選択し、
  `--start` `--end` で期間を指定できます。開始日と終了日をどちらも
  指定しない場合は当日の日付が自動的に使われます。テクニカルの場合は
  バックテストと同じ条件（`signals_count>=3` など）が自動で適用されます。

## 利用の流れ

1. `fetch` スクリプトでデータベースを更新
2. `screening` スクリプトで売買シグナルを生成
3. `backtest` スクリプトでシグナルを検証
操作をまとめた簡易 GUI (`gui.py`) を用意しており、バックテスト結果の Excel は「結果閲覧」タブから確認できます。



## 定期実行

`scheduler.py` を起動しておくと株価や決算情報の取得を自動化できます。
必要なライブラリは `requirements.txt` からインストールできます。

```bash
python scheduler.py
```

デフォルトでは毎日 20:00 に日足株価、20:30 に決算情報を取得し、
月曜 6:00 に上場銘柄情報を更新します。

