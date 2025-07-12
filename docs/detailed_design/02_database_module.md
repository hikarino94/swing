# データベース管理機能（db/）詳細設計書

## 1. 概要

### 1.1 目的
SQLiteデータベースを使用して、株式市場データの永続化とクエリ機能を提供する。高速な読み書き、データ整合性、並行アクセスのサポートを実現する。

### 1.2 機能概要
- **スキーマ管理**：テーブル定義とマイグレーション機能
- **データ要約**：格納データの統計情報表示
- **シグナル管理**：スクリーニング結果の参照機能
- **リポジトリパターン**：データアクセス層の抽象化

### 1.3 設計方針
- WAL（Write-Ahead Logging）モードによる並行性向上
- 適切なインデックス設計によるクエリ最適化
- 論理削除による履歴保持
- 外部キー制約によるデータ整合性

## 2. アーキテクチャ

### 2.1 コンポーネント構成
```
db/
├── db_schema.py      # スキーマ定義とマイグレーション
├── db_summary.py     # データベース統計情報
├── list_signals.py   # シグナル一覧表示
├── models/          # 機械学習モデル保存
└── stock.db         # SQLiteデータベースファイル（.gitignore）
```

### 2.2 データベース構造

#### 2.2.1 テーブル一覧
1. **市場データ系**
   - prices: 日次株価データ
   - listed_info: 上場企業情報
   - statements: 財務諸表データ

2. **分析結果系**
   - fundamental_signals: ファンダメンタル分析結果
   - technical_indicators: テクニカル分析結果

3. **ユーザー管理系**
   - users: ユーザー情報
   - sessions: セッション管理

4. **ポートフォリオ系**
   - holdings: 株式保有情報
   - transactions: 株式取引履歴
   - fund_master: 投資信託マスター
   - fund_prices: 投資信託価格
   - fund_holdings: 投資信託保有
   - fund_transactions: 投資信託取引

## 3. 詳細設計

### 3.1 スキーマ定義（db_schema.py）

#### 3.1.1 主要テーブル設計

##### prices テーブル
```sql
CREATE TABLE IF NOT EXISTS prices (
    code TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    turnover_value REAL,
    adjustment_factor REAL,
    adjustment_open REAL,
    adjustment_high REAL,
    adjustment_low REAL,
    adjustment_close REAL,
    adjustment_volume REAL,
    PRIMARY KEY (code, date)
);
```
- **用途**: 日次の株価データ（OHLCV）
- **主キー**: 銘柄コードと日付の複合キー
- **調整済み価格**: 株式分割を考慮した価格データ

##### listed_info テーブル
```sql
CREATE TABLE IF NOT EXISTS listed_info (
    date TEXT,
    code TEXT PRIMARY KEY,
    company_name TEXT,
    company_name_english TEXT,
    sector17_code TEXT,
    sector17_code_name TEXT,
    sector33_code TEXT,
    sector33_code_name TEXT,
    scale_category TEXT,
    market_code TEXT,
    market_code_name TEXT,
    delete_flag TEXT DEFAULT '0'
);
```
- **用途**: 上場企業のマスターデータ
- **delete_flag**: 上場廃止企業の論理削除

##### statements テーブル
```sql
CREATE TABLE IF NOT EXISTS statements (
    DisclosedDate TEXT,
    DisclosedTime TEXT,
    code TEXT,
    TypeOfCurrentPeriod TEXT,
    -- 以下、163カラムの財務データ項目
    -- 売上高、営業利益、純利益、ROE、配当等
    PRIMARY KEY (DisclosureNumber)
);
```
- **用途**: 決算情報の包括的な保存
- **特徴**: 163カラムの大規模テーブル
- **主キー**: 開示番号によるユニーク識別

##### fundamental_signals テーブル
```sql
CREATE TABLE IF NOT EXISTS fundamental_signals (
    code TEXT NOT NULL,
    company_name TEXT,
    DisclosedAt TEXT NOT NULL,
    NetSales REAL,
    NetSales_YoY REAL,
    OrdinaryProfit REAL,
    OrdinaryProfit_YoY REAL,
    Profit REAL,
    Profit_YoY REAL,
    EPS REAL,
    EPS_FY_Est REAL,
    Dividend_FY_Est REAL,
    EquityToAssetRatio REAL,
    ROE REAL,
    PBR REAL,
    close_price REAL,
    PRIMARY KEY (code, DisclosedAt)
);
```
- **用途**: ファンダメンタル分析の結果保存
- **指標**: 売上高、利益、配当、財務健全性指標

##### technical_indicators テーブル
```sql
CREATE TABLE IF NOT EXISTS technical_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    signal_date TEXT NOT NULL,
    close_price REAL,
    ma_signal INTEGER DEFAULT 0,
    rsi_signal INTEGER DEFAULT 0,
    adx_signal INTEGER DEFAULT 0,
    bollinger_signal INTEGER DEFAULT 0,
    volatility_signal INTEGER DEFAULT 0,
    macd_signal INTEGER DEFAULT 0,
    volume_signal INTEGER DEFAULT 0,
    composite_score REAL,
    signal_count INTEGER,
    side TEXT,
    UNIQUE(code, signal_date)
);
```
- **用途**: テクニカル分析のシグナル保存
- **シグナル**: MA、RSI、ADX、ボリンジャーバンド等
- **複合スコア**: 各シグナルの総合評価

#### 3.1.2 インデックス設計

```sql
-- 検索パフォーマンス最適化のためのインデックス
CREATE INDEX idx_prices_date ON prices(date);
CREATE INDEX idx_prices_code_date ON prices(code, date);
CREATE INDEX idx_prices_date_code ON prices(date, code);

CREATE INDEX idx_statements_code ON statements(code);
CREATE INDEX idx_statements_disclosed ON statements(DisclosedDate);

CREATE INDEX idx_fund_signals_disclosed ON fundamental_signals(DisclosedAt);
CREATE INDEX idx_fund_signals_code_disclosed ON fundamental_signals(code, DisclosedAt);

CREATE INDEX idx_tech_date ON technical_indicators(signal_date);
CREATE INDEX idx_tech_code_date ON technical_indicators(code, signal_date);
CREATE INDEX idx_tech_date_count ON technical_indicators(signal_date, signal_count);
```

### 3.2 データベース最適化設定

#### 3.2.1 WALモード
```python
conn.execute("PRAGMA journal_mode = WAL")
conn.execute("PRAGMA synchronous = NORMAL")
```
- **WAL（Write-Ahead Logging）**: 読み取りと書き込みの並行実行
- **NORMAL同期**: パフォーマンスとデータ安全性のバランス

#### 3.2.2 パフォーマンス設定（src.utils.db_utils経由）
```python
conn.execute("PRAGMA cache_size = -64000")    # 64MB キャッシュ
conn.execute("PRAGMA temp_store = MEMORY")    # 一時データをメモリに
conn.execute("PRAGMA mmap_size = 268435456")  # 256MB メモリマップ
```

### 3.3 ポートフォリオ管理テーブル

#### 3.3.1 holdings テーブル
```sql
CREATE TABLE IF NOT EXISTS holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    shares REAL NOT NULL,
    average_cost REAL NOT NULL,
    current_price REAL,
    current_value REAL,
    profit_loss REAL,
    profit_loss_rate REAL,
    account_name TEXT NOT NULL DEFAULT 'default',
    account_type TEXT NOT NULL DEFAULT 'specific',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE(user_id, code, account_name, account_type)
);
```
- **用途**: ユーザーの株式保有情報
- **特徴**: 口座種別（特定/一般）、論理削除対応

#### 3.3.2 transactions テーブル
```sql
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    transaction_type TEXT NOT NULL,
    shares REAL NOT NULL,
    price REAL NOT NULL,
    amount REAL NOT NULL,
    commission REAL DEFAULT 0,
    tax REAL DEFAULT 0,
    transaction_date TEXT NOT NULL,
    account_name TEXT NOT NULL DEFAULT 'default',
    account_type TEXT NOT NULL DEFAULT 'specific',
    realized_profit REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
```
- **用途**: 売買履歴の記録
- **特徴**: 手数料、税金、実現利益の追跡

### 3.4 データベース操作ユーティリティ

#### 3.4.1 db_summary.py
```python
def main() -> None:
    """データベースの統計情報を表示"""
    tables = ['prices', 'listed_info', 'statements',
              'fundamental_signals', 'technical_indicators']

    for table in tables:
        # 行数と日付範囲を取得
        count, min_date, max_date = get_table_stats(table)
        print(f"{table}: {count:,} rows, {min_date} ~ {max_date}")
```

#### 3.4.2 list_signals.py
```python
def main() -> None:
    """スクリーニングシグナルの一覧表示"""
    signal_type = args.signal_type  # 'fund' or 'tech'

    if signal_type == 'fund':
        query = """
        SELECT code, company_name, DisclosedAt,
               NetSales_YoY, OrdinaryProfit_YoY, ROE, PBR
        FROM fundamental_signals
        WHERE DisclosedAt BETWEEN ? AND ?
        ORDER BY DisclosedAt DESC, code
        """
    else:
        query = """
        SELECT code, signal_date, close_price,
               signal_count, composite_score, side
        FROM technical_indicators
        WHERE signal_date BETWEEN ? AND ?
        ORDER BY signal_date DESC, signal_count DESC
        """
```

## 4. マイグレーション機能

### 4.1 スキーマ更新
```python
def init_schema(db_path: str | Path) -> None:
    """スキーマの初期化とマイグレーション"""
    # 1. 基本テーブルの作成
    create_base_tables(conn)

    # 2. 既存テーブルへの新カラム追加
    add_column_if_not_exists(conn, 'holdings', 'account_type')

    # 3. 制約の更新（必要に応じて再作成）
    update_unique_constraints(conn)

    # 4. インデックスの作成
    create_indexes(conn)
```

### 4.2 データ移行
- 新カラムのデフォルト値設定
- 既存データの変換処理
- バックアップテーブルによる安全な更新

## 5. セキュリティ

### 5.1 SQLインジェクション対策
- パラメータバインディングの徹底
- プリペアドステートメントの使用

### 5.2 アクセス制御
- ユーザーテーブルによる認証
- roleベースの権限管理（admin/portfolio_only）

### 5.3 セッション管理
- セッショントークンの安全な生成
- 有効期限の管理
- remember_me機能のセキュアな実装

## 6. パフォーマンス最適化

### 6.1 インデックス戦略
- 頻繁に使用される検索条件に対するインデックス
- 複合インデックスによるカバリングインデックス
- 統計情報の定期的な更新（ANALYZE）

### 6.2 クエリ最適化
- 適切なJOIN順序
- サブクエリの最小化
- バッチ処理の活用

### 6.3 接続管理
- コネクションプーリング（db_utils経由）
- トランザクションの適切な粒度
- 長時間実行クエリの監視

## 7. 運用と保守

### 7.1 バックアップ
- 定期的なデータベースファイルのバックアップ
- WALファイルの適切な管理
- ポイントインタイムリカバリの検討

### 7.2 監視項目
- データベースファイルサイズ
- インデックスの断片化
- 長時間実行クエリ
- デッドロックの発生

### 7.3 メンテナンス
- VACUUM コマンドによる最適化
- インデックスの再構築
- 統計情報の更新

## 8. 今後の拡張計画

### 8.1 機能拡張
- パーティショニングの導入（大規模データ対応）
- リアルタイムデータ対応
- 分散データベースへの移行準備

### 8.2 性能改善
- インメモリキャッシュの導入
- 読み取り専用レプリカの検討
- クエリ結果のキャッシング
