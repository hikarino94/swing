# CLAUDE.md

このファイルは、このリポジトリでコードを扱う際のClaude Code (claude.ai/code) へのガイダンスを提供します。

## 重要な指示
1. **必ず日本語で回答してください**
2. **修正した箇所は初心者でもわかりやすいように説明してください**
3. **コードの変更理由と効果を具体的に説明してください**

## プロジェクト概要

これは、J-Quants APIと統合して株式データを取得し、ファンダメンタル・テクニカル・ML基準に基づいて株式をスクリーニングし、トレーディング戦略をバックテストする日本株式市場分析・取引システムです。

## 2025年新ディレクトリ構造 📁

### 🎯 整理された構造

```
/
├── 📋 設定・認証 (config/)
│   ├── idtoken.json - J-Quants APIトークン
│   ├── account.json - API認証情報
│   └── login.json.example - Webログイン設定例
├── 💾 データ (data/)
│   ├── db/ - SQLiteデータベース
│   │   ├── stock.db - メインデータベース
│   │   ├── db_schema.py - スキーマ初期化
│   │   ├── db_summary.py - データベース概要表示
│   │   └── backups/ - バックアップファイル
│   └── exports/ - エクスポートファイル置き場
├── 🔧 ソースコード (src/)
│   ├── api/ - データ取得API
│   ├── analysis/ - スクリーニング・分析
│   ├── backtest/ - バックテスト
│   └── utils/ - 共通ユーティリティ
├── 🌐 Webアプリ (web/)
│   ├── app.py - Flaskアプリケーション
│   ├── templates/ - HTMLテンプレート
│   └── static/ - CSS, JS, 画像
├── 🖥️ デスクトップアプリ (desktop/)
├── 🛠️ ツール (tools/)
├── 📝 ドキュメント (docs/)
└── 🗂️ 一時ファイル (tmp/)
```

### 主要ディレクトリ詳細

#### `src/utils/` - 共通ユーティリティ
- **`config.py`** - 統一設定管理（JSONファイル、トークン）
- **`db_utils.py`** - コンテキストマネージャーを使用したデータベース接続管理
- **`jquants_client.py`** - リトライ・レート制限付きJ-Quants APIクライアント
- **`logging_config.py`** - 統一ログ設定
- **`cli_utils.py`** - 共通コマンドライン引数解析ユーティリティ
- **`common.py`** - 共有ユーティリティ関数（日付処理、ファイルI/O等）
- **`exceptions.py`** - エラーハンドリング向上のためのカスタム例外クラス
- **`screening_utils.py`** - スクリーニング共通ユーティリティ（価格データ、テクニカル指標、エクスポート）
- **`backtest_utils.py`** - バックテストエンジンとユーティリティ（シグナル、トレード、結果）

#### `src/api/` - データ取得
- **`daily_quotes.py`** - 高速化版日次株価取得（並列処理対応）
- **`listed_info.py`** - 上場企業情報取得
- **`statements.py`** - 財務諸表データ取得

#### `src/analysis/` - 株式スクリーニング
- **`screen_statements.py`** - ファンダメンタル分析
- **`screen_technical.py`** - テクニカル指標スクリーニング
- **`screen_ml.py`** - 機械学習スクリーニング
- **`thresholds.py`** - スクリーニング閾値設定管理

#### `src/backtest/` - 戦略バックテスト
- **`backtest_statements.py`** - ファンダメンタル戦略バックテスト
- **`backtest_technical.py`** - テクニカル戦略バックテスト
- **`backtest_ml.py`** - ML戦略バックテスト（リファクタリング済み）
- **`analyze_backtest_json.py`** - バックテスト結果分析（リファクタリング済み）

#### `/tests/` - テストインフラ
- **`conftest.py`** - 共通テストフィクスチャと設定
- **`test_utils.py`** - ユーティリティモジュールの単体テスト
- **`test_config.py`** - 設定モジュールテスト
- **`test_db_utils.py`** - データベースユーティリティテスト
- **`test_exceptions.py`** - 例外処理テスト
- **`test_common.py`** - 共通関数テスト
- pytest統合用の基本テスト構造

### 適用されたデザインパターン
1. **サービス層パターン** - ビジネスロジックとデータアクセスの分離
2. **ファクトリパターン** - マネージャーとクライアントの共通作成
3. **ストラテジーパターン** - プラガブルなスクリーニング・バックテストアルゴリズム
4. **リポジトリパターン** - 統一データアクセスインターフェース
5. **テンプレートメソッドパターン** - 特化実装による共通バックテストワークフロー

## 必須コマンド

### セットアップ・開発
```bash
# 依存関係のインストールとpre-commitフックのセットアップ
pip install -r requirements.txt
pip install pre-commit
pre-commit install

# データベース初期化
python data/db/db_schema.py

# データベース情報とシグナル確認
python data/db/db_summary.py
python data/db/list_signals.py [fund|tech] [--start DATE --end DATE]

# IDトークン更新
python tools/update_idtoken.py
```

### コード品質管理
- Pre-commitフックが自動で`black`と`ruff`を実行
- 手動のlint/testコマンドは不要 - pre-commit経由で自動フォーマット
- 品質チェックスクリプト: `scripts/quality-check.sh`

### データ取得
```bash
# 日次株価データ（当日または日付範囲指定）
python src/api/daily_quotes.py [--start DATE --end DATE --workers N]

# 財務諸表データ（mode 1=企業別一括、mode 2=日付・期間別）
python src/api/statements.py [1|2] [--start DATE --end DATE]

# 上場企業情報
python src/api/listed_info.py
```

### 株式スクリーニング
```bash
# ファンダメンタル分析スクリーニング
python src/analysis/screen_statements.py [--lookback N --recent N --as-of DATE]

# テクニカル指標スクリーニング
python src/analysis/screen_technical.py [indicators|screen] [--as-of DATE --lookback N]

# 機械学習スクリーニング
python src/analysis/screen_ml.py [train|screen] [--top N --lookback N]
```

### バックテスト
```bash
# ファンダメンタルシグナルバックテスト
python src/backtest/backtest_statements.py [--hold N --capital N --start DATE --end DATE --xlsx FILE --json FILE --show]

# テクニカルシグナルバックテスト
python src/backtest/backtest_technical.py [--start DATE --end DATE --hold-days N --stop-loss N --capital N --outfile FILE --show]

# MLモデルバックテスト
python src/backtest/backtest_ml.py [--start DATE --end DATE --top N --capital N --outfile FILE --show]

# バックテスト結果分析
python src/backtest/analyze_backtest_json.py FILE.json [--side long|short --show-trades]
```

### アプリケーション
```bash
# デスクトップGUIインターフェース
python desktop/gui.py

# Webインターフェース（Flask）
python web/app.py

# データ更新自動スケジューラー
python tools/scheduler.py

# データベースビューアー（SQLite Browser代替）
python tools/db_viewer.py [--tables --info TABLE --sample TABLE]
```

## アーキテクチャ

### データベーススキーマ（SQLite: `data/db/stock.db`）
- `prices` - 日次株価データ
- `statements` - 財務諸表データ
- `listed_info` - 企業上場情報
- `fundamental_signals` - ファンダメンタル分析結果
- `technical_indicators` - テクニカル分析結果

### コアモジュール
- **`src/api/`** - データ取得用J-Quants API統合
- **`src/analysis/`** - 株式スクリーニングアルゴリズム（ファンダメンタル、テクニカル、ML）
- **`src/backtest/`** - 戦略バックテスト・パフォーマンス分析
- **`data/db/`** - データベーススキーマ・ユーティリティ関数
- **`src/utils/`** - 共通ユーティリティ・ヘルパー関数
- **`tests/`** - テストスイート・品質保証
- **`web/`** - Webアプリケーション
- **`desktop/`** - デスクトップGUIアプリ
- **`tools/`** - 運用ツール

### 必須設定ファイル
- `config/idtoken.json` - J-Quants APIトークン: `{"idToken": "YOUR_TOKEN"}`
- `config/account.json` - J-Quants認証情報（トークン更新用、オプション）
- `config/login.json` - Webアプリ認証（オプション、account.jsonにフォールバック）
- `src/analysis/thresholds.json` - スクリーニングパラメータ設定

### 主要統合ポイント
- 全モジュールが中央SQLiteデータベースを使用してデータ永続化
- J-Quants APIは有効な`idToken`によるデータアクセス必須
- スクリーニング結果はバックテスト用シグナルとして保存
- 結果はタイムスタンプ付きでExcel/JSON形式にエクスポート

### 自動化ワークフロー
- `tools/scheduler.py`による日次データ取得（20:00株価、20:30財務諸表、月曜6:00上場情報）
- Pre-commitフックによるblack/ruffコードフォーマット保証
- バックテスト結果に包括的パフォーマンス指標・取引詳細を含む

### 追加ユーティリティ
- `tools/update_idtoken.py` - J-Quants APIトークン更新
- `tools/db_viewer.py` - SQLiteデータベースビューアー
- `scripts/quality-check.sh` - コード品質チェック
- `start_gui.sh` - GUI起動スクリプト
- `setup.sh` - 環境セットアップ
