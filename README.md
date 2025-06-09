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
  `--lookback` 過去参照日数、`--recent` 開示閾値日数を指定できます。
* `screening/screen_technical.py`
  `indicators` または `screen` をコマンドとして指定します。  
  `--as-of` で対象日を指定し、`--lookback` は遡る日数です。
* `backtest/backtest_statements.py`
  財務シグナルを用いたバックテストを実行します。  
  `--hold` 保有日数、`--entry-offset` エントリー日のオフセット、`--capital` 資金、  
  `--start` と `--end` で対象期間、`--xlsx` 出力ファイル名を指定します。
* `backtest/backtest_technical.py`
  テクニカル指標を用いたスイングトレードのバックテストを行います。  
  `--as-of` エントリー日、`--hold-days` 保有日数、`--stop-loss` 損切り率、  
  `--capital` 資金、`--outfile` 出力ファイル名を指定します。
