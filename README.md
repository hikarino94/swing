# Swing Trading Tool 🎯

J-Quants の株価・財務データを使用して、スクリーニングやバックテストを行うツールです。

## 🚀 クイックスタート（初心者向け）

### 必要なもの
- Python 3.12以上
- J-Quants API アカウント（[無料登録](https://jpx-jquants.com/)）
- Windows + WSL2 または Mac/Linux

### 1. 自動セットアップ（推奨）

すべてを自動で設定します：

```bash
# リポジトリをクローン
git clone https://github.com/yourusername/swing.git
cd swing

# 自動セットアップ実行
python scripts/setup_environment.py
```

このスクリプトが以下を自動で行います：
- 仮想環境の作成
- 必要なライブラリのインストール
- データベースの初期化
- 開発環境の設定

### 2. J-Quants API の設定

J-Quants にログインして認証情報を保存します：

```bash
# 対話形式で設定
python -m src.cli.update_idtoken

# または直接指定
python -m src.cli.update_idtoken --mail your-email@example.com --password your-password
```

### 3. Web UIを起動

```bash
# 通常起動
python -m src.ui.web

# WSL2環境で家庭内LANからアクセスする場合（推奨）
python src/ui/web_production.py
```

ブラウザで http://localhost:5000 にアクセスしてください。

## 🔧 手動セットアップ（上級者向け）

<details>
<summary>詳細な手動セットアップ手順</summary>

### 1. 仮想環境の作成

```bash
# 仮想環境作成
python -m venv venv

# 有効化（Windows）
venv\Scripts\activate

# 有効化（Mac/Linux）
source venv/bin/activate
```

### 2. 依存関係のインストール

```bash
# 本番用ライブラリ
pip install -r requirements.txt

# 開発用ツール（任意）
pip install -r requirements-dev.txt
pip install pre-commit
pre-commit install
```

### 3. 設定ファイルの作成

#### J-Quants認証情報（config/account.json）
```json
{
  "mailaddress": "your-email@example.com",
  "password": "your-password"
}
```

#### Web UI認証情報（config/login.json）- 任意
```json
{
  "id": "admin",
  "password": "your-web-password"
}
```

### 4. データベース初期化

```bash
python db/db_schema.py
```

</details>

## 📱 モバイル・家庭内LANアクセス対応

### WSL2環境でのネットワーク問題解決

WSL2環境では、MTUサイズの問題により家庭内LANからアクセスできない場合があります。

#### 症状
- localhostからはアクセスできるが、他のデバイスからタイムアウトする
- 軽いページは表示されるが、重いページが表示されない

#### 解決方法

1. **即座の解決**（WSL2内で実行）
```bash
# MTUサイズを変更
sudo ip link set dev eth0 mtu 1450

# または自動設定スクリプトを実行
./setup_wsl_network.sh
```

2. **本番用サーバーの使用**（推奨）
```bash
# Waitressサーバーで起動（より安定）
python src/ui/web_production.py
```

3. **恒久的な解決**
詳細は [WSL2_NETWORK_FIX.md](WSL2_NETWORK_FIX.md) を参照してください。

### モバイル対応UI
- レスポンシブデザイン対応
- タッチ操作に最適化
- スマホでも見やすいカード形式の表示

## 📊 基本的な使い方

### 1. データの取得

Web UIの「📊 データ取得」タブから：
- **株価取得**: 日次の株価データ
- **上場情報**: 銘柄の基本情報
- **財務諸表**: 決算データ

### 2. スクリーニング

「🔍 スクリーニング」タブから3種類の分析が可能：

- **ファンダメンタル分析**: 財務データに基づく銘柄選定
- **テクニカル分析**: チャート指標による売買シグナル
- **機械学習分析**: AIモデルによる予測

### 3. バックテスト

「📈 バックテスト」タブで過去データを使った検証：
- 各スクリーニング手法の実績確認
- リスク・リターンの分析
- 最適なパラメータの探索

### 4. 結果の確認

「📁 結果閲覧」タブから：
- Excel形式でダウンロード
- JSON形式で詳細分析
- 過去の実行結果の比較

## 🤖 自動実行

定期的にデータを自動更新：

```bash
# スケジューラーを起動
python -m src.cli.scheduler
```

デフォルトスケジュール：
- 毎日 20:00 - 株価データ取得
- 毎日 20:30 - 財務データ取得
- 毎週月曜 6:00 - 上場情報更新

## 📁 プロジェクト構造

```
swing/
├── src/                     # ソースコード
│   ├── ui/                 # ユーザーインターフェース
│   │   ├── web.py         # Web UI（推奨）
│   │   └── legacy/gui.py  # デスクトップGUI
│   ├── cli/               # コマンドラインツール
│   └── config/            # 設定管理
├── fetch/                   # データ取得スクリプト
├── screening/               # スクリーニングロジック
├── backtest/               # バックテストエンジン
├── db/                     # データベース管理
├── templates/              # Web UIテンプレート
├── tests/                  # テストコード
└── data/output/            # 結果ファイル
```

## ❓ よくある質問・トラブルシューティング

### Q: J-Quants APIのトークンエラーが出る
A: トークンの有効期限が切れています。以下を実行：
```bash
python -m src.cli.update_idtoken
```

### Q: データベースエラーが出る
A: データベースを再初期化：
```bash
# バックアップを取る
cp db/stock.db db/stock.db.backup

# 再初期化
python db/db_schema.py
```

### Q: Web UIにアクセスできない
A: ファイアウォールの設定を確認：
- Windows Defenderでポート5000を許可
- WSL2の場合は上記のネットワーク設定を確認

### Q: スクリーニング結果が出ない
A: データが不足している可能性があります：
1. まずデータ取得を実行
2. 十分な期間のデータがあるか確認（DBサマリーで確認）
3. `screening/thresholds.json` の閾値を調整

### Q: メモリ不足エラー
A: 大量データ処理時の対策：
- 期間を短くして実行
- WSL2の場合は `.wslconfig` でメモリを増やす
- `--lookback` パラメータで参照日数を減らす

## 🛠️ 主要コマンドリファレンス

<details>
<summary>コマンドライン実行例</summary>

### データ取得
```bash
# 株価データ（期間指定）
python fetch/daily_quotes.py --start 2024-01-01 --end 2024-12-31

# 上場情報
python fetch/listed_info.py

# 財務データ（日次更新）
python fetch/statements.py 2
```

### スクリーニング
```bash
# ファンダメンタル
python screening/screen_statements.py --lookback 60 --recent 30

# テクニカル
python screening/screen_technical.py screen --as-of 2024-01-10

# 機械学習
python screening/screen_ml.py train  # 学習
python screening/screen_ml.py screen --top 10  # 予測
```

### バックテスト
```bash
# ファンダメンタル戦略
python backtest/backtest_statements.py --hold 20 --capital 1000000

# テクニカル戦略
python backtest/backtest_technical.py --hold-days 10 --stop-loss 5

# ML戦略
python backtest/backtest_ml.py --top 10 --capital 1000000
```

### 分析・確認
```bash
# DBサマリー
python db/db_summary.py

# シグナル一覧
python db/list_signals.py tech --start 2024-01-01

# バックテスト結果分析
python backtest/analyze_backtest_json.py result.json --show-trades
```

</details>

## 🧪 テスト実行

```bash
# 全テスト
pytest

# カバレッジ付き
pytest --cov=. --cov-report=html

# 特定のテスト
pytest tests/test_config.py -v
```

## 📝 開発者向け情報

- コミット時に自動でコード整形（black, ruff）
- 詳細は [DEVELOPMENT.md](DEVELOPMENT.md) 参照
- 改善計画は [IMPROVEMENT_PLAN.md](IMPROVEMENT_PLAN.md) 参照

## 🆕 最近の主な更新

- **モバイル対応**: スマホでも使いやすいレスポンシブUI
- **WSL2対応**: ネットワーク問題の解決方法を実装
- **処理の排他制御**: 複数処理の同時実行を防止
- **Excel自動出力**: スクリーニング結果を自動でExcel化
- **高速化**: テクニカルスクリーニングの並列処理対応

## 🤝 貢献・サポート

- バグ報告: GitHubのIssueへ
- 機能要望: Discussionsで議論
- 質問: READMEを確認後、Issueへ

## ⚠️ 免責事項

本ツールは情報提供を目的としており、投資判断は自己責任でお願いします。
