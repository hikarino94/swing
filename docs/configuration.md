# 設定

## 設定ファイルの概要

プロジェクトでは以下の設定ファイルを使用します：

- `config/config.json` - アプリケーション全体の設定
- `config/account.json` - J-Quants認証情報
- `screening/thresholds.json` - スクリーニング閾値

## config/config.json

中央設定ファイルで、以下の項目を管理します：

```json
{
  "database": {
    "path": "db/stock.db"
  },
  "api": {
    "base_url": "https://api.jquants.com/v1",
    "endpoints": {
      "auth": "/token/auth_user",
      "refresh": "/token/auth_refresh",
      "daily_quotes": "/prices/daily_quotes",
      "listed_info": "/listed/info",
      "statements": "/fins/statements"
    },
    "rate_limit": {
      "sleep_seconds": 0.35
    }
  },
  "scheduler": {
    "tasks": {
      "fetch_quotes": {
        "time": "20:00",
        "frequency": "daily"
      },
      "fetch_statements": {
        "time": "20:30",
        "frequency": "daily"
      },
      "update_listed_info": {
        "time": "06:00",
        "frequency": "monday"
      }
    }
  },
  "files": {
    "account": "config/account.json",
    "idtoken": "config/idtoken.json",
    "thresholds": "screening/thresholds.json"
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  }
}
```

### 設定項目の説明

#### database
- `path`: SQLiteデータベースファイルのパス

#### api
- `base_url`: J-Quants APIのベースURL
- `endpoints`: 各APIエンドポイントのパス
- `rate_limit.sleep_seconds`: API呼び出し間のスリープ時間（レート制限対策）

#### scheduler
- 各タスクの実行時刻と頻度を設定
- `frequency`: "daily"（毎日）または"monday"（毎週月曜）

#### files
- 各種設定ファイルのパスを定義

#### logging
- ログレベルとフォーマットの設定

## config/account.json

J-Quantsの認証情報を保存します：

```json
{
  "mailaddress": "your-email@example.com",
  "password": "your-password"
}
```

**注意**: このファイルは`.gitignore`に含まれており、Gitリポジトリには含まれません。

## screening/thresholds.json

スクリーニングで使用する閾値を定義します：

```json
{
  "EPS_YOY_MIN": 0.30,
  "CF_QUALITY_MIN": 0.8,
  "ETA_DELTA_MIN": 0.0,
  "TREASURY_DELTA_MAX": 0.0,
  "RSI_THRESHOLD": 50,
  "ADX_THRESHOLD": 20,
  "OVERHEAT_FACTOR": 1.1,
  "OVERSOLD_FACTOR": 0.95,
  "SIGNAL_COUNT_MIN": 3,
  "SHORT_SIGNAL_COUNT_MIN": 4,
  "FIRST_LOOKBACK_DAYS": 30
}
```

### 閾値の説明

#### ファンダメンタル分析
- `EPS_YOY_MIN`: EPS前年比成長率の最小値（30%）
- `CF_QUALITY_MIN`: キャッシュフロー品質の最小値（0.8）
- `ETA_DELTA_MIN`: 自己資本比率の変化の最小値
- `TREASURY_DELTA_MAX`: 自己株式変化の最大値

#### テクニカル分析
- `RSI_THRESHOLD`: RSIの閾値（50）
- `ADX_THRESHOLD`: ADXの閾値（20）
- `OVERHEAT_FACTOR`: 過熱判定係数（1.1）
- `OVERSOLD_FACTOR`: 売られ過ぎ判定係数（0.95）
- `SIGNAL_COUNT_MIN`: ロングシグナルの最小数（3）
- `SHORT_SIGNAL_COUNT_MIN`: ショートシグナルの最小数（4）
- `FIRST_LOOKBACK_DAYS`: 初回シグナル判定の遡り日数（30）

## 環境変数

設定ファイルの代わりに環境変数を使用することも可能です：

```bash
export SWING_DB_PATH="/path/to/stock.db"
export SWING_LOG_LEVEL="DEBUG"
```

## 設定の優先順位

1. コマンドライン引数
2. 環境変数
3. 設定ファイル
4. デフォルト値
