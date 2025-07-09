# Swing API リファレンス

## 概要

swingプロジェクトは、日本株式市場の分析とポートフォリオ管理のためのPythonツールキットです。J-Quants APIを使用してデータを取得し、ファンダメンタル分析、テクニカル分析、機械学習ベースのスクリーニングを提供します。

## 主要モジュール

### fetch - データ取得

#### `fetch.daily_quotes`
日次株価データをJ-Quants APIから取得します。

```python
from fetch.daily_quotes import fetch_all_pages, save_to_db

# 特定期間のデータを取得
start_date = "2024-01-01"
end_date = "2024-01-31"
df = fetch_all_pages(idtoken, start_date, end_date)

# データベースに保存
rows_saved = save_to_db(df, db_path)
```

**主要関数:**
- `fetch_all_pages(idtoken: str, from_date: str, to_date: str) -> pd.DataFrame`
  - 指定期間の全株価データを取得
  - ページネーション対応
  - レート制限対応（3リクエスト/秒）

- `save_to_db(df: pd.DataFrame, db_path: str | Path) -> int`
  - DataFrameをSQLiteデータベースに保存
  - 重複を自動的に処理

#### `fetch.listed_info`
上場企業情報を取得します。

```python
from fetch.listed_info import fetch_and_save_listed_info

# 上場企業情報を更新
fetch_and_save_listed_info()
```

#### `fetch.statements`
財務諸表データを取得します。

```python
from fetch.statements import fetch_statements_range

# 財務諸表を取得
df = fetch_statements_range(idtoken, start_date, end_date)
```

### screening - スクリーニング

#### `screening.screen_statements`
ファンダメンタル分析によるスクリーニング。

```python
from screening.screen_statements import main

# CLIから実行
# python screen_statements.py --lookback 30 --recent 7 --as-of 2024-01-15
```

**スクリーニング条件:**
- EPS成長率 (EPS_YOY_MIN)
- キャッシュフロー品質 (CF_QUALITY_MIN)
- 自己資本比率の改善 (ETA_DELTA_MIN)
- 自己株式取得 (TREASURY_DELTA_MAX)

#### `screening.screen_technical`
テクニカル指標によるスクリーニング。

```python
from screening.screen_technical import run_technical_screening

# テクニカルスクリーニングを実行
signals = run_technical_screening(as_of_date="2024-01-15", lookback_days=180)
```

**テクニカル指標:**
- ゴールデンクロス/デッドクロス
- MACD
- RSI
- ボリンジャーバンド
- ボリュームレシオ

#### `screening.screen_ml`
機械学習モデルによるスクリーニング。

```python
from screening.screen_ml import train_model, screen_with_model

# モデルの訓練
train_model()

# スクリーニング実行
predictions = screen_with_model(top_n=50, lookback_days=180)
```

### backtest - バックテスト

#### `backtest.backtest_statements`
ファンダメンタル戦略のバックテスト。

```python
from backtest.backtest_statements import backtest_fundamental

# バックテスト実行
results = backtest_fundamental(
    hold_days=20,
    capital=1000000,
    start_date="2023-01-01",
    end_date="2023-12-31"
)
```

#### `backtest.backtest_technical`
テクニカル戦略のバックテスト。

```python
from backtest.backtest_technical import run_backtest

# ロング戦略
long_results = run_backtest(
    signal_type="long",
    hold_days=10,
    capital=1000000,
    stop_loss=0.05
)

# ショート戦略
short_results = run_backtest(
    signal_type="short",
    hold_days=5,
    capital=1000000,
    stop_loss=0.03
)
```

### portfolio - ポートフォリオ管理

#### `portfolio.manager.PortfolioManager`
ポートフォリオの総合管理。

```python
from src.portfolio.manager import PortfolioManager

manager = PortfolioManager(user_id=1)

# CSVファイルのインポート
manager.import_from_csv("portfolio.csv", csv_type="sbi")

# 保有銘柄の更新
manager.update_market_values()

# パフォーマンス計算
performance = manager.calculate_performance()
```

#### `portfolio.visualization.PortfolioVisualizer`
ポートフォリオの可視化。

```python
from src.portfolio.visualization import PortfolioVisualizer

visualizer = PortfolioVisualizer(user_id=1)

# 構成比の円グラフ
charts = visualizer.create_composition_pie_charts()

# パフォーマンス推移
performance = visualizer.create_performance_charts(days=180)

# ヒートマップ
heatmap = visualizer.create_heatmap()
```

### データベーススキーマ

#### `prices` テーブル
```sql
CREATE TABLE prices (
    code TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    PRIMARY KEY (code, date)
);
```

#### `statements` テーブル
```sql
CREATE TABLE statements (
    code TEXT NOT NULL,
    DisclosedAt TEXT NOT NULL,
    TypeOfCurrentPeriod TEXT,
    NetSales REAL,
    OperatingProfit REAL,
    OrdinaryProfit REAL,
    Profit REAL,
    EarningsPerShare REAL,
    -- ... その他多数のフィールド
    PRIMARY KEY (code, DisclosedAt)
);
```

#### `fundamental_signals` テーブル
```sql
CREATE TABLE fundamental_signals (
    code TEXT NOT NULL,
    DisclosedAt TEXT NOT NULL,
    TypeOfCurrentPeriod TEXT,
    eps_yoy_fy REAL,
    eps_yoy_q REAL,
    op_margin_delta REAL,
    -- ... その他のシグナル
    created_at TEXT,
    PRIMARY KEY (code, DisclosedAt)
);
```

### 設定ファイル

#### `config/account.json`
```json
{
    "mail": "your-email@example.com",
    "password": "your-password"
}
```

#### `screening/thresholds.json`
```json
{
    "fundamental": {
        "EPS_YOY_MIN": 0.1,
        "CF_QUALITY_MIN": 0.8,
        "ETA_DELTA_MIN": 0.05,
        "TREASURY_DELTA_MAX": 0.0
    },
    "technical": {
        "RSI_OVERSOLD": 30,
        "RSI_OVERBOUGHT": 70,
        "VOLUME_RATIO_MIN": 1.5
    }
}
```

## エラーハンドリング

### 共通例外クラス

```python
from src.utils.exceptions import (
    SwingException,
    DatabaseError,
    APIError,
    ValidationError,
    PortfolioError
)

# 例外の使用例
try:
    result = risky_operation()
except DatabaseError as e:
    logger.error(f"Database error: {e.code} - {e.message}")
    logger.debug(f"Details: {e.details}")
```

### エラーハンドリングデコレータ

```python
from src.utils.exceptions import handle_exceptions
from src.utils.logging_config import get_logger

logger = get_logger("my_module")

@handle_exceptions(logger, default_return=None, reraise=False)
def safe_function():
    # エラーが発生する可能性のある処理
    pass
```

## ログ設定

### 統一ログフォーマット

```python
from src.utils.logging_config import get_logger

logger = get_logger("module.name")

# 構造化ログ
logger.info("処理完了", extra={
    "user_id": 123,
    "items_processed": 50,
    "duration_ms": 1234
})
```

### JSONログ出力

```python
from src.utils.logging_config import configure_logging

# JSON形式でログ出力
configure_logging(log_level="INFO", json_output=True)
```

## 型ヒント

プロジェクト全体で型ヒントを使用しています：

```python
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

def process_data(
    data: pd.DataFrame,
    config: Dict[str, Any],
    output_path: Optional[Path] = None
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    # 処理
    pass
```

## パフォーマンス最適化

### ベクトル化された操作

```python
# 推奨: ベクトル化
df['result'] = df['value'] * 2 + df['offset']

# 非推奨: iterrows
for idx, row in df.iterrows():
    df.at[idx, 'result'] = row['value'] * 2 + row['offset']
```

### データベースインデックス

パフォーマンス向上のため、以下のインデックスが設定されています：
- `idx_prices_code_date`
- `idx_statements_code_disclosed`
- `idx_holdings_user_code`

## 開発ガイドライン

1. **コーディング規約**
   - Black (line-length: 88)
   - Ruff (Pythonコード品質チェック)
   - 型ヒントの使用を推奨

2. **テスト**
   - pytest使用
   - カバレッジ目標: 80%以上
   - `python -m pytest tests/ -v --cov`

3. **ログ**
   - 統一されたログフォーマットを使用
   - 適切なログレベルの選択
   - 構造化ログの活用

4. **エラーハンドリング**
   - 共通例外クラスの使用
   - 適切なエラーメッセージ
   - ユーザー向けとシステム向けの区別
