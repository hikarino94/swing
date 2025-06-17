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

Python 3.9 以上を想定しています。必要なライブラリをインストールします。

```bash
pip install pandas requests XlsxWriter
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
  `--start` と `--end` で対象期間、`--xlsx` 出力ファイル名に加えて
  `--json` で結果を JSON として保存できます。デフォルトでは結果表示は
  行われません。グラフと表を表示したい場合は `--show` を指定します。
  標準出力へテキスト表示したい場合は `--ascii` を利用してください。
* `backtest/backtest_technical.py`
  テクニカル指標を用いたスイングトレードのバックテストを行います。
  `--start` と `--end` でエントリー期間を指定し、`--hold-days` 保有日数、
  `--stop-loss` 損切り率、`--capital` 資金、`--outfile` 出力ファイル名のほか
  `--json` オプションで結果を JSON へ保存できます。デフォルトでは結果
  表示は行われません。グラフと表を表示したい場合は `--show` を指定し
  ます。標準出力で確認したい場合は `--ascii` を利用してください。
* `update_idtoken.py`
  J‑Quants にログインして `idtoken.json` を更新します。
  `--mail` と `--password` を省略すると `account.json` が参照されます。

## 利用の流れ

1. `fetch` スクリプトでデータベースを更新
2. `screening` スクリプトで売買シグナルを生成
3. `backtest` スクリプトでシグナルを検証

操作をまとめた簡易 GUI (`gui.py`) に加えて、
ブラウザから利用できる簡易 Web アプリ (`webapp.py`) も用意しました。
すべての主要スクリプトを画面から実行でき、
バックテストを含む結果はページ下部に表示されます。
バックテスト実行時には JSON に保存した結果から簡易チャートと
取引銘柄の一覧表も表示されます。
以下のように Flask をインストールして起動します。

```bash
pip install flask
# `FLASK_SECRET_KEY` には Flask セッションを保護するための秘密鍵を
# 指定します。未設定でも起動しますが、既定値 "secret" が使われるため
# 任意の安全な文字列を環境変数で与えることを推奨します。
# 鍵は次のように生成できます。
python - <<'EOF'
import secrets
print(secrets.token_hex(16))
EOF
FLASK_SECRET_KEY=<生成した鍵> python webapp.py
```

初回アクセス時はログイン画面が表示されます。Web アプリ用の
`login.json` に記載した **ID** とパスワードでログインしてください。
`login.json` が存在しない場合は `account.json` を参照しますが、
この場合はメールアドレスが ID として扱われます。
どちらのファイルも `password_hash` を追加してハッシュ化したパスワードを
保存しておくと安全です（`update_idtoken.py` 実行時は `account.json` の
平文 `password` が参照されます）。
`LOGIN_ACCOUNT` 環境変数で認証情報ファイルを変更できます。

`FLASK_HOST`, `FLASK_PORT`, `FLASK_DEBUG` を環境変数で指定すると、
起動するホストやポート、デバッグモードを変更できます。

## 定期実行

`scheduler.py` を起動しておくと株価や決算情報の取得を自動化できます。
利用には `schedule` ライブラリが必要です。

```bash
pip install schedule
python scheduler.py
```

デフォルトでは毎日 20:00 に日足株価、20:30 に決算情報を取得し、
月曜 6:00 に上場銘柄情報を更新します。

