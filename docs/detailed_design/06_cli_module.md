# CLI機能（src/cli/）詳細設計書

## 1. 概要

### 1.1 目的
コマンドラインインターフェースを通じて、定期的なデータ取得の自動化、認証管理、ログ監視機能を提供する。無人運用を可能にし、システムの保守性を向上させる。

### 1.2 機能概要
- **スケジューラー**：J-Quants APIからのデータ取得を定期実行
- **認証管理**：APIトークンの取得と更新
- **ログビューアー**：システムログの閲覧と検索

### 1.3 設計方針
- 堅牢性（エラー時も継続動作）
- 保守性（詳細なログ出力）
- セキュリティ（認証情報の適切な管理）
- 使いやすさ（直感的なコマンド体系）

## 2. アーキテクチャ

### 2.1 コンポーネント構成
```
src/cli/
├── scheduler.py        # 定期実行スケジューラー
└── update_idtoken.py   # トークン更新ツール

scripts/
└── log_viewer.py      # ログビューアー
```

### 2.2 依存関係
- **scheduler**: schedule ライブラリによる定期実行
- **update_idtoken**: requests による API 通信
- **log_viewer**: 標準ライブラリのみ使用

## 3. 詳細設計

### 3.1 スケジューラー（scheduler.py）

#### 3.1.1 設計思想
市場データの定期的な取得を自動化し、手動介入なしでデータベースを最新状態に保つ。エラー発生時も他のタスクは継続実行。

#### 3.1.2 スケジュール設定

##### 設定の読み込み
```python
from src.config import config

# デフォルト設定
DEFAULT_CONFIG = {
    'quotes_schedule': '20:00',      # 日次株価
    'statements_schedule': '20:30',  # 財務諸表
    'listed_schedule': 'monday 06:00'  # 上場企業情報
}

# 設定の取得（config.jsonまたはデフォルト）
quotes_time = config.scheduler['quotes_schedule']
statements_time = config.scheduler['statements_schedule']
listed_time = config.scheduler['listed_schedule']
```

##### スケジュール登録
```python
def setup_schedules():
    """定期実行スケジュールの設定"""

    # 日次株価取得（毎日20:00）
    schedule.every().day.at(quotes_time).do(fetch_quotes)

    # 財務諸表取得（毎日20:30）
    schedule.every().day.at(statements_time).do(fetch_statements)

    # 上場企業情報更新（毎週月曜6:00）
    schedule.every().monday.at(listed_time.split()[1]).do(update_listed_info)

    log.info(f"スケジュール設定完了:")
    log.info(f"- 日次株価: 毎日 {quotes_time}")
    log.info(f"- 財務諸表: 毎日 {statements_time}")
    log.info(f"- 上場企業情報: {listed_time}")
```

#### 3.1.3 タスク実装

##### 共通実行関数
```python
def _run(cmd: str) -> None:
    """コマンドの安全な実行とエラーハンドリング"""
    log.info(f"実行開始: {cmd}")

    try:
        # シェルインジェクション対策
        args = shlex.split(cmd)

        # サブプロセス実行
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            shell=False,  # セキュリティのため
            timeout=3600  # 1時間のタイムアウト
        )

        # エラーチェック
        if proc.returncode != 0:
            log.error(f"実行失敗 (code={proc.returncode}): {cmd}")
            log.error(f"標準エラー出力: {proc.stderr}")
        else:
            log.info(f"実行成功: {cmd}")

    except subprocess.TimeoutExpired:
        log.error(f"タイムアウト: {cmd}")
    except Exception as e:
        log.error(f"予期しないエラー: {e}")
```

##### 個別タスク
```python
def fetch_quotes():
    """日次株価データの取得（当日分）"""
    _run("python -m fetch.daily_quotes")

def fetch_statements():
    """財務諸表データの取得（直近2期分）"""
    _run("python -m fetch.statements 2")

def update_listed_info():
    """上場企業情報の更新"""
    _run("python -m fetch.listed_info")
```

#### 3.1.4 メインループ
```python
def main():
    """スケジューラーのメインループ"""
    log.info("スケジューラー起動")

    # スケジュール設定
    setup_schedules()

    # 起動時の即時実行オプション
    if config.scheduler.get('run_on_startup', False):
        log.info("起動時実行を開始")
        fetch_quotes()
        fetch_statements()

    # メインループ
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 1分ごとにチェック

    except KeyboardInterrupt:
        log.info("スケジューラー停止（ユーザー操作）")
    except Exception as e:
        log.error(f"スケジューラーエラー: {e}")
        raise
```

### 3.2 トークン更新（update_idtoken.py）

#### 3.2.1 設計思想
J-Quants API の認証トークンを安全に取得・更新。認証情報の保護と、自動化可能な設計を両立。

#### 3.2.2 認証フロー

##### Step 1: RefreshToken の取得
```python
def _auth_user(mail: str, password: str) -> str:
    """メールアドレスとパスワードで認証"""
    url = "https://jpx-jquants.com/v1/token/auth_user"

    payload = {
        "mailaddress": mail,
        "password": password
    }

    response = requests.post(
        url,
        json=payload,
        timeout=30,
        headers={'User-Agent': 'swing-trader/1.0'}
    )

    response.raise_for_status()

    data = response.json()
    if "refreshToken" not in data:
        raise RuntimeError("認証に失敗しました")

    return data["refreshToken"]
```

##### Step 2: IDToken の取得
```python
def _get_id_token(refresh_token: str) -> str:
    """RefreshToken から IDToken を取得"""
    url = "https://jpx-jquants.com/v1/token/auth_refresh"

    params = {"refreshtoken": refresh_token}

    response = requests.post(
        url,
        params=params,
        timeout=30,
        headers={'User-Agent': 'swing-trader/1.0'}
    )

    response.raise_for_status()

    data = response.json()
    if "idToken" not in data:
        raise RuntimeError("トークン更新に失敗しました")

    return data["idToken"]
```

#### 3.2.3 認証情報管理

##### 認証情報の読み込み
```python
def _load_account(path: Path) -> tuple[str, str]:
    """認証情報ファイルから読み込み"""
    if not path.exists():
        raise FileNotFoundError(f"認証情報ファイルが見つかりません: {path}")

    with open(path, 'r') as f:
        data = json.load(f)

    mail = data.get("mail")
    password = data.get("password")

    if not mail or not password:
        raise ValueError("認証情報が不完全です")

    return mail, password
```

##### トークンの保存
```python
def save_token(token: str, outfile: Path) -> None:
    """トークンを安全に保存"""
    # ディレクトリ作成
    outfile.parent.mkdir(parents=True, exist_ok=True)

    # トークン保存
    data = {
        "idToken": token,
        "timestamp": datetime.now().isoformat(),
        "expires_in": 86400  # 24時間
    }

    with open(outfile, 'w') as f:
        json.dump(data, f, indent=2)

    # ファイル権限を制限（Unixシステム）
    if hasattr(os, 'chmod'):
        os.chmod(outfile, 0o600)
```

#### 3.2.4 コマンドライン処理
```python
def _cli():
    """コマンドラインインターフェース"""
    parser = argparse.ArgumentParser(
        description="J-Quants API の認証トークンを更新します"
    )

    # 引数定義
    parser.add_argument(
        "--mail",
        help="J-Quants 登録メールアドレス"
    )
    parser.add_argument(
        "--password",
        help="ログインパスワード"
    )
    parser.add_argument(
        "--account",
        type=Path,
        default=Path("config/account.json"),
        help="認証情報ファイル (default: config/account.json)"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("config/idtoken.json"),
        help="出力ファイル (default: config/idtoken.json)"
    )

    args = parser.parse_args()

    # 認証情報の取得
    if args.mail and args.password:
        mail, password = args.mail, args.password
    else:
        try:
            mail, password = _load_account(args.account)
        except Exception as e:
            parser.error(f"認証情報の読み込みに失敗: {e}")

    # トークン更新実行
    try:
        update(mail, password, args.out)
        print("トークンを更新しました。")
    except Exception as e:
        parser.error(f"トークン更新エラー: {e}")
```

### 3.3 ログビューアー（scripts/log_viewer.py）

#### 3.3.1 設計思想
ログファイルの効率的な閲覧・検索・監視を提供。tail -f 相当の機能を Python で実装し、プラットフォーム非依存にする。

#### 3.3.2 主要機能実装

##### ログファイル一覧
```python
def list_log_files():
    """ログファイル一覧を更新時刻順で表示"""
    log_dir = Path("data/logs")

    if not log_dir.exists():
        print("ログディレクトリが存在しません")
        return

    # ログファイルを収集
    log_files = []
    for file in log_dir.glob("*.log*"):
        stat = file.stat()
        log_files.append({
            'name': file.name,
            'size': stat.st_size,
            'mtime': stat.st_mtime,
            'mtime_str': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        })

    # 更新時刻でソート（新しい順）
    log_files.sort(key=lambda x: x['mtime'], reverse=True)

    # 表示
    print(f"{'ファイル名':<30} {'サイズ':>10} {'更新日時':>20}")
    print("-" * 65)

    for log in log_files:
        size_str = format_file_size(log['size'])
        print(f"{log['name']:<30} {size_str:>10} {log['mtime_str']:>20}")
```

##### リアルタイム追跡
```python
def tail_log_file(file_path: Path, lines: int = 10, follow: bool = False):
    """ログファイルの末尾を表示（-f オプション対応）"""
    if not file_path.exists():
        print(f"ファイルが見つかりません: {file_path}")
        return

    # 末尾N行を取得
    with open(file_path, 'r', encoding='utf-8') as f:
        # ファイルサイズが小さい場合は全行読み込み
        f.seek(0, 2)  # ファイル末尾へ
        file_size = f.tell()

        if file_size < 8192:  # 8KB未満
            f.seek(0)
            all_lines = f.readlines()
            tail_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        else:
            # 大きいファイルは効率的に読み込み
            tail_lines = get_tail_lines(f, lines)

        # 表示
        for line in tail_lines:
            print(line.rstrip())

    # リアルタイム追跡
    if follow:
        print(f"\n--- {file_path} を監視中 (Ctrl+C で終了) ---\n")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # 現在の末尾へ移動
                f.seek(0, 2)

                while True:
                    line = f.readline()
                    if line:
                        print(line.rstrip())
                    else:
                        time.sleep(0.1)  # 新しい行を待つ

        except KeyboardInterrupt:
            print("\n監視を終了しました")
```

##### パターン検索
```python
def search_logs(pattern: str, file_path: Path = None):
    """ログファイル内をパターン検索"""
    import re

    # 大文字小文字を区別しない検索
    regex = re.compile(pattern, re.IGNORECASE)

    # 検索対象ファイル
    if file_path:
        files = [file_path]
    else:
        log_dir = Path("data/logs")
        files = list(log_dir.glob("*.log*"))

    # 各ファイルを検索
    total_matches = 0

    for file in files:
        matches = []

        try:
            with open(file, 'r', encoding='utf-8') as f:
                for line_no, line in enumerate(f, 1):
                    if regex.search(line):
                        matches.append((line_no, line.rstrip()))

            # 結果表示
            if matches:
                print(f"\n=== {file.name} ({len(matches)} 件) ===")
                for line_no, line in matches[:10]:  # 最初の10件
                    print(f"{line_no:6d}: {line}")

                if len(matches) > 10:
                    print(f"... 他 {len(matches) - 10} 件")

                total_matches += len(matches)

        except Exception as e:
            print(f"エラー ({file.name}): {e}")

    print(f"\n合計: {total_matches} 件の一致")
```

#### 3.3.3 ユーティリティ関数
```python
def format_file_size(size: int) -> str:
    """ファイルサイズを人間が読みやすい形式に変換"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"

def get_tail_lines(file_obj, n: int) -> list:
    """大きなファイルから効率的に末尾N行を取得"""
    BLOCK_SIZE = 1024
    lines = []
    block_count = -1

    while len(lines) < n:
        try:
            file_obj.seek(block_count * BLOCK_SIZE, 2)
        except IOError:
            file_obj.seek(0)
            lines = file_obj.readlines()
            break

        block = file_obj.read(BLOCK_SIZE)
        lines = block.splitlines() + lines
        block_count -= 1

    return lines[-n:]
```

## 4. ログ管理システム

### 4.1 ログ設定（src/utils/logging_config.py）
```python
def setup_logging(name: str) -> logging.Logger:
    """統一的なログ設定"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # ログディレクトリ作成
    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # ハンドラー設定
    handlers = []

    # 1. 日次ローテーション
    daily_handler = TimedRotatingFileHandler(
        log_dir / f"{name}.log",
        when='midnight',
        interval=1,
        backupCount=30,  # 30日分保持
        encoding='utf-8'
    )

    # 2. サイズベースローテーション
    size_handler = RotatingFileHandler(
        log_dir / f"{name}_size.log",
        maxBytes=100 * 1024 * 1024,  # 100MB
        backupCount=5,
        encoding='utf-8'
    )

    # フォーマッター
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    for handler in [daily_handler, size_handler]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
```

### 4.2 ログディレクトリ構造
```
data/logs/
├── scheduler.log           # スケジューラーログ（日次）
├── scheduler_size.log      # スケジューラーログ（サイズ）
├── fetch.log              # データ取得ログ
├── screening.log          # スクリーニングログ
├── backtest.log           # バックテストログ
└── web.log                # Web UIログ
```

## 5. エラーハンドリングとリカバリ

### 5.1 スケジューラーのエラー処理
- 個別タスクの失敗は他のタスクに影響しない
- エラーログを記録し、次回実行で自動リトライ
- ネットワークエラー時の exponential backoff

### 5.2 トークン更新のエラー処理
- HTTP エラーの詳細な記録
- 認証情報の検証
- タイムアウト設定（30秒）

### 5.3 ログビューアーのエラー処理
- ファイル読み込みエラーの graceful な処理
- 大きなファイルでのメモリ効率
- エンコーディングエラーの回避

## 6. セキュリティ考慮事項

### 6.1 認証情報の保護
- ファイル権限の制限（600）
- 認証情報の暗号化検討
- 環境変数からの読み込みサポート

### 6.2 コマンドインジェクション対策
- `shell=False` での subprocess 実行
- `shlex.split()` による安全な引数分割

## 7. 運用と保守

### 7.1 スケジューラーの運用
```bash
# systemd サービスとして登録（推奨）
[Unit]
Description=Swing Scheduler
After=network.target

[Service]
Type=simple
User=swing
WorkingDirectory=/path/to/swing
ExecStart=/usr/bin/python -m src.cli.scheduler
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 7.2 監視項目
- スケジューラープロセスの死活監視
- ログファイルサイズ
- 最終実行時刻
- エラー率

### 7.3 トラブルシューティング
```bash
# ログの確認
python scripts/log_viewer.py tail scheduler.log -f

# エラーログの検索
python scripts/log_viewer.py search ERROR

# トークンの手動更新
python -m src.cli.update_idtoken --mail user@example.com --password xxxxx
```

## 8. 今後の拡張計画

### 8.1 機能拡張
- スケジュールの動的変更 API
- Slack/Discord 通知連携
- 複数環境対応（dev/staging/prod）
- トークン自動更新機能

### 8.2 性能改善
- 非同期実行の導入
- 分散スケジューリング対応
- ログのリアルタイム圧縮

### 8.3 運用改善
- Web UI からのスケジュール管理
- ログの可視化ダッシュボード
- アラート機能の強化
