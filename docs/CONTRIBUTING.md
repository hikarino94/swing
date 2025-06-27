# Contributing to Swing

Swingプロジェクトへの貢献に興味を持っていただき、ありがとうございます！

## 開発環境のセットアップ

### 前提条件

- Python 3.9以上
- Git
- J-Quants APIアカウント（テスト用）

### 開発環境の準備

1. リポジトリをフォーク・クローン

```bash
git clone https://github.com/YOUR_USERNAME/swing.git
cd swing
```

2. 仮想環境の作成（推奨）

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# または
venv\Scripts\activate  # Windows
```

3. 依存関係のインストール

```bash
pip install -r requirements.txt
pip install pre-commit black ruff mypy bandit pytest pytest-cov
```

4. pre-commitフックの設定

```bash
pre-commit install
```

5. テスト用設定ファイルの作成

```bash
echo '{"idToken": "test_token"}' > idtoken.json
echo '{"mailaddress": "test@example.com", "password": "test_password"}' > account.json
```

6. データベースの初期化

```bash
python db/db_schema.py
```

## 開発ワークフロー

### 1. 機能開発

1. 新しいブランチを作成

```bash
git checkout -b feature/your-feature-name
```

2. コードを編集

3. テストを追加・実行

```bash
python -m pytest
```

4. コード品質チェック

```bash
./scripts/quality-check.sh
```

### 2. コミット

pre-commitフックにより以下が自動実行されます：

- Black: コードフォーマット
- Ruff: リント
- MyPy: 型チェック
- Bandit: セキュリティチェック
- 基本的なファイルチェック

### 3. プルリクエスト

1. 変更をプッシュ

```bash
git push origin feature/your-feature-name
```

2. GitHubでプルリクエストを作成

## コーディング規約

### コードスタイル

- **フォーマッター**: Black（自動適用）
- **リンター**: Ruff（自動修正可能）
- **行長**: 120文字まで
- **インポート順**: isortによる自動整理

### 型ヒント

- 全ての公開関数・メソッドに型ヒントを追加
- MyPyによる型チェックでエラーがないこと
- Optional型の適切な使用

```python
def process_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """データを処理してDataFrameを返す"""
    pass
```

### ドキュメント

- 公開関数・クラスにはdocstringを追加
- Google/NumPy形式のdocstring
- 引数・戻り値・例外の説明

```python
def fetch_data(code: str, start_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """指定銘柄のデータを取得

    Args:
        code: 銘柄コード
        start_date: 開始日（Noneの場合は全期間）

    Returns:
        データのリスト

    Raises:
        APIError: API呼び出しエラー
    """
    pass
```

## テスト

### テストの種類

- **Unit Tests**: 個別関数・クラスのテスト
- **Integration Tests**: モジュール間の連携テスト
- **API Tests**: 外部API呼び出しのテスト（モック使用）

### テストファイルの配置

```
tests/
├── conftest.py          # pytest設定・フィクスチャ
├── test_config.py       # ConfigManagerテスト
├── test_db_utils.py     # DatabaseManagerテスト
├── test_exceptions.py   # カスタム例外テスト
└── test_common.py       # 共通ユーティリティテスト
```

### テストの実行

```bash
# 全テスト
python -m pytest

# カバレッジ付き
python -m pytest --cov=utils --cov=fetch --cov-report=html

# 特定ファイル
python -m pytest tests/test_config.py

# 特定のテスト
python -m pytest tests/test_config.py::TestConfigManager::test_load_json_success
```

### テストのマーカー

```python
@pytest.mark.slow
def test_slow_operation():
    """時間のかかるテスト"""
    pass

@pytest.mark.api
def test_api_call():
    """API呼び出しが必要なテスト"""
    pass
```

実行時の除外:
```bash
python -m pytest -m "not slow"  # 遅いテストを除外
python -m pytest -m "not api"   # APIテストを除外
```

## アーキテクチャガイドライン

### Service-Oriented Architecture

新しい機能は以下のパターンに従って実装してください：

```python
# 1. データ取得クラス
class DataFetcher:
    def fetch_data(self) -> List[Dict[str, Any]]:
        pass

# 2. データ処理クラス
class DataProcessor:
    def process_data(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        pass

# 3. データ保存クラス
class DataSaver:
    def save_data(self, df: pd.DataFrame) -> int:
        pass

# 4. オーケストレーションクラス
class DataService:
    def __init__(self):
        self.fetcher = DataFetcher()
        self.processor = DataProcessor()
        self.saver = DataSaver()

    def update_data(self) -> Dict[str, Any]:
        # 全体の処理フロー
        pass
```

### 共通ユーティリティの使用

- **ConfigManager**: 設定ファイル読み込み
- **DatabaseManager**: データベース操作
- **JQuantsClient**: J-Quants API呼び出し
- **カスタム例外**: エラーハンドリング

### エラーハンドリング

```python
from utils.exceptions import APIError, DatabaseError, DataError

try:
    result = api_call()
except requests.RequestException as e:
    raise APIError(f"API呼び出しに失敗: {e}")
```

## リリースプロセス

### バージョニング

セマンティックバージョニングを使用：

- **Major**: 破綻的変更
- **Minor**: 後方互換性のある機能追加
- **Patch**: バグフィックス

### リリース手順

1. 変更内容をCHANGELOG.mdに記録
2. バージョンタグを作成
3. GitHub Releasesで公開

## 問題報告

バグ報告や機能要求は[Issues](https://github.com/YOUR_USERNAME/swing/issues)でお願いします。

### バグ報告テンプレート

- **環境**: Python バージョン、OS
- **再現手順**: 詳細な手順
- **期待する動作**:
- **実際の動作**:
- **エラーメッセージ**: 完全なスタックトレース

## ライセンス

このプロジェクトに貢献することで、あなたの貢献が同じライセンスの下でライセンスされることに同意するものとします。
