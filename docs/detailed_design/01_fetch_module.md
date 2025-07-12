# データ取得機能（fetch/）詳細設計書

## 1. 概要

### 1.1 目的
J-Quants APIから日本株式市場のデータ（株価、上場企業情報、財務諸表）を取得し、SQLiteデータベースに格納する機能を提供する。

### 1.2 機能概要
- **日次株価データ取得**：日足の株価データ（OHLCV）を取得
- **上場企業情報取得**：上場企業の基本情報を取得
- **財務諸表データ取得**：企業の財務諸表データを取得

### 1.3 設計方針
- API レート制限の遵守（3リクエスト/秒）
- 大量データの効率的な処理（並列処理、バッチ挿入）
- エラーに対する堅牢性（リトライ、部分的失敗の継続処理）
- データの一貫性保証（トランザクション、UPSERT）

## 2. アーキテクチャ

### 2.1 コンポーネント構成
```
fetch/
├── daily_quotes.py    # 日次株価データ取得
├── listed_info.py     # 上場企業情報取得
└── statements.py      # 財務諸表データ取得
```

### 2.2 依存関係
- **外部API**: J-Quants API (https://jpx-jquants.com/)
- **データベース**: SQLite（stock.db）
- **設定管理**: src.config モジュール
- **認証**: src.utils.api_utils.get_idtoken()

## 3. 詳細設計

### 3.1 日次株価データ取得（daily_quotes.py）

#### 3.1.1 クラス設計

##### RateLimiter クラス
```python
class RateLimiter:
    def __init__(self, max_per_second: int = 3)
    def wait_if_needed(self) -> None
```
- **責務**: APIレート制限の管理
- **属性**:
  - `max_per_second`: 秒間最大リクエスト数（デフォルト: 3）
  - `min_interval`: リクエスト間の最小間隔（0.35秒）
  - `last_request_time`: 最後のリクエスト時刻

#### 3.1.2 主要関数

##### fetch_daily_quotes()
```python
def fetch_daily_quotes(
    from_: str = None,
    to_: str = None,
    overwrite: bool = False
) -> None
```
- **目的**: 指定期間の日次株価データを取得
- **パラメータ**:
  - `from_`: 開始日（YYYY-MM-DD形式）
  - `to_`: 終了日（YYYY-MM-DD形式）
  - `overwrite`: 既存データの上書きフラグ
- **処理フロー**:
  1. 日付パラメータの検証と正規化
  2. 単日または期間での取得モード決定
  3. 並列処理での日次データ取得（期間モード時）
  4. データベースへの保存

##### _fetch_single_date()
```python
def _fetch_single_date(
    date: str,
    idtoken: str,
    rate_limiter: RateLimiter
) -> pd.DataFrame
```
- **目的**: 特定日の株価データを取得
- **処理**:
  1. APIエンドポイントへのリクエスト
  2. ページネーション処理
  3. データフレームの結合と返却

#### 3.1.3 データ構造

##### APIレスポンス
```json
{
    "daily_quotes": [
        {
            "Code": "13010",
            "Date": "2024-01-04",
            "Open": 1000,
            "High": 1050,
            "Low": 990,
            "Close": 1040,
            "Volume": 100000,
            "TurnoverValue": 104000000,
            "AdjustmentFactor": 1.0,
            "AdjustmentOpen": 1000,
            "AdjustmentHigh": 1050,
            "AdjustmentLow": 990,
            "AdjustmentClose": 1040,
            "AdjustmentVolume": 100000
        }
    ],
    "pagination_key": "..."
}
```

##### データベーススキーマ（prices テーブル）
```sql
CREATE TABLE prices (
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

#### 3.1.4 エラーハンドリング
- **HTTP 429 (Too Many Requests)**: 指数バックオフでリトライ（最大3回）
- **HTTP 5xx エラー**: 同上
- **空データ**: 休場日として正常終了
- **個別エラー**: ログ記録後、次の処理へ継続

### 3.2 上場企業情報取得（listed_info.py）

#### 3.2.1 主要関数

##### update_listed_info()
```python
def update_listed_info() -> None
```
- **目的**: 上場企業情報の一括更新
- **処理フロー**:
  1. J-Quants APIから全上場企業情報を取得
  2. カラム名のマッピング処理
  3. 一時テーブル経由でのデータベース更新
  4. delete_flagの更新（本日以外を論理削除）

##### _fetch_listed_info()
```python
def _fetch_listed_info(idtoken: str) -> pd.DataFrame
```
- **目的**: APIから上場企業情報を取得
- **処理**:
  1. APIリクエストの送信
  2. レスポンスの検証
  3. DataFrameへの変換

#### 3.2.2 データ構造

##### APIレスポンス
```json
{
    "info": [
        {
            "Code": "13010",
            "CompanyName": "極洋",
            "CompanyNameEnglish": "KYOKUYO CO.,LTD.",
            "Sector17Code": "1",
            "Sector17CodeName": "食品",
            "MarketCode": "0111",
            "MarketCodeName": "プライム"
        }
    ]
}
```

##### データベーススキーマ（listed_info テーブル）
```sql
CREATE TABLE listed_info (
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    company_name TEXT,
    company_name_english TEXT,
    sector17_code TEXT,
    sector17_code_name TEXT,
    sector33_code TEXT,
    sector33_code_name TEXT,
    scale_category TEXT,
    market_code TEXT,
    market_code_name TEXT,
    delete_flag TEXT DEFAULT '0',
    PRIMARY KEY (date, code)
);
```

### 3.3 財務諸表データ取得（statements.py）

#### 3.3.1 主要関数

##### fetch_statements()
```python
def fetch_statements(
    mode: int = 1,
    date: str = None,
    from_: str = None,
    to_: str = None
) -> None
```
- **目的**: 財務諸表データの取得
- **モード**:
  - モード1: 全有効銘柄の最新データ取得
  - モード2: 指定日付または期間のデータ取得
- **処理フロー**:
  1. モードに応じた取得対象の決定
  2. 並列処理での大量データ取得（モード1）
  3. データの正規化とデータベース保存

##### _fetch_statements_by_code()
```python
def _fetch_statements_by_code(
    code: str,
    session: requests.Session,
    idtoken: str
) -> pd.DataFrame
```
- **目的**: 特定銘柄の財務諸表を取得
- **特徴**:
  - HTTPセッションの再利用
  - ページネーション対応
  - エラー時の個別処理

#### 3.3.2 データ正規化

##### _normalize()
```python
def _normalize(df: pd.DataFrame) -> pd.DataFrame
```
- **目的**: 163カラムの標準スキーマへの正規化
- **処理**:
  1. カラム名のマッピング（LocalCode → code）
  2. 不足カラムへのpd.NA設定
  3. データ型の最適化

## 4. 性能とスケーラビリティ

### 4.1 並列処理
- **daily_quotes**: ThreadPoolExecutor（最大10ワーカー）
- **statements**: ThreadPoolExecutor（最大5ワーカー）
- **レート制限**: RateLimiterによる秒間3リクエスト制限

### 4.2 データベース最適化
```python
# パフォーマンス設定
PRAGMA cache_size = -64000;      # 64MB キャッシュ
PRAGMA temp_store = MEMORY;      # 一時データをメモリに
PRAGMA mmap_size = 268435456;   # 256MB メモリマップ
```

### 4.3 バッチ処理
- executemany() によるバルクインサート
- トランザクション単位での大量データ処理

## 5. セキュリティ

### 5.1 認証
- JWT トークンによるBearer認証
- トークンの自動更新機能との連携

### 5.2 データ保護
- SQLインジェクション対策（パラメータバインディング）
- APIトークンのメモリ内保持（ファイル保存なし）

## 6. エラー処理とログ

### 6.1 エラー処理戦略
- **一時的エラー**: リトライ（指数バックオフ）
- **永続的エラー**: ログ記録と処理継続
- **致命的エラー**: 例外発生とロールバック

### 6.2 ログ出力
```python
# ログフォーマット例
2024-01-01 10:00:00 INFO Fetching daily quotes for 2024-01-01
2024-01-01 10:00:01 WARNING Retrying after 429 error, attempt 2/3
2024-01-01 10:00:05 ERROR Failed to fetch data for code 1234: Connection timeout
```

## 7. 運用と保守

### 7.1 定期実行
- **daily_quotes**: 毎日20:00（スケジューラー経由）
- **statements**: 毎日20:30（スケジューラー経由）
- **listed_info**: 毎週月曜6:00（スケジューラー経由）

### 7.2 監視項目
- API応答時間
- エラー発生率
- データ取得完全性
- データベースサイズ

### 7.3 トラブルシューティング
- **レート制限エラー**: RateLimiterの設定確認
- **認証エラー**: トークン有効期限の確認
- **データ不整合**: UPSERTロジックの検証

## 8. 今後の拡張計画

### 8.1 機能拡張
- リアルタイムデータ取得対応
- 追加データソースの統合
- データ品質チェック機能

### 8.2 性能改善
- キャッシュ機構の導入
- 差分更新の実装
- データ圧縮の検討
