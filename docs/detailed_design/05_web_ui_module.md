# Web UI機能（src/ui/）詳細設計書

## 1. 概要

### 1.1 目的
Flask フレームワークを使用した Web インターフェースを提供し、株式分析システムの全機能をブラウザから操作可能にする。直感的なタブ型 UI により、データ取得からバックテストまでをシームレスに実行できる。

### 1.2 機能概要
- **認証・認可システム**：ロールベースのアクセス制御
- **データ取得管理**：J-Quants API からのデータ取得制御
- **スクリーニング実行**：3種類のスクリーニング手法の実行と結果表示
- **バックテスト実行**：戦略検証と結果分析
- **ポートフォリオ管理**：保有銘柄と取引履歴の管理、パフォーマンス分析
- **結果管理**：実行結果のダウンロードと分析

### 1.3 設計方針
- Blueprint による機能のモジュール化
- RESTful API 設計
- セキュリティファースト（CSRF 対策、認証必須）
- レスポンシブデザイン
- リアルタイム処理フィードバック

## 2. アーキテクチャ

### 2.1 コンポーネント構成
```
src/ui/
├── web.py                    # メインアプリケーション
├── web_core.py              # コア設定
├── common.py                # 共通ユーティリティ
├── blueprints/              # 機能別モジュール
│   ├── auth/               # 認証機能
│   ├── backtest/           # バックテスト実行
│   ├── fetch/              # データ取得
│   ├── portfolio/          # ポートフォリオ管理
│   ├── results/            # 結果表示
│   ├── screening/          # スクリーニング実行
│   └── utils/              # ユーティリティ
└── templates/               # HTMLテンプレート
```

### 2.2 技術スタック
- **フレームワーク**: Flask 3.0
- **テンプレートエンジン**: Jinja2
- **セッション管理**: サーバーサイドセッション
- **データベース**: SQLite（WALモード）
- **フロントエンド**: Bootstrap 5 + Vanilla JavaScript

## 3. 詳細設計

### 3.1 メインアプリケーション（web.py）

#### 3.1.1 アプリケーション初期化
```python
def create_app(create_db: bool = True) -> Flask:
    """Flaskアプリケーションの作成と設定"""
    app = Flask(__name__)

    # 設定読み込み
    configure_app(app)

    # シークレットキー設定
    app.secret_key = get_secret_key()

    # セッション設定
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE='Lax',
        SESSION_COOKIE_SECURE=False,  # HTTPS時はTrue
        PERMANENT_SESSION_LIFETIME=timedelta(hours=24)
    )

    # Blueprint登録
    register_blueprints(app)

    # データベース初期化
    if create_db:
        init_database(app)

    return app
```

#### 3.1.2 ルーティング
```python
@app.route('/')
@login_required
def index():
    """メインページ（タブ型UI）"""
    return render_template('index.html',
                         username=session.get('username'),
                         is_admin=(session.get('role') == 'admin'))

@app.route('/screening')
@login_required
def screening():
    """スクリーニングタブへのリダイレクト"""
    return redirect(url_for('index') + '#screening')
```

#### 3.1.3 認証デコレータ
```python
def login_required(f):
    """ログイン必須デコレータ"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'session_id' not in session:
            return redirect(url_for('auth_bp.login'))

        # セッション検証
        if not validate_session(session['session_id']):
            session.clear()
            return redirect(url_for('auth_bp.login'))

        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """管理者権限必須デコレータ"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get('role') != 'admin':
            return jsonify({'error': '管理者権限が必要です'}), 403
        return f(*args, **kwargs)
    return decorated_function
```

### 3.2 認証システム（blueprints/auth/）

#### 3.2.1 ユーザー認証フロー
```python
@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """ログイン処理"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        remember = request.form.get('remember') == 'on'

        # ユーザー検証
        user = authenticate_user(username, password)
        if user:
            # セッション作成
            session_id = create_session(user['id'], remember)
            session['session_id'] = session_id
            session['username'] = user['username']
            session['role'] = user['role']
            session['user_id'] = user['id']

            return redirect(url_for('index'))

        flash('ユーザー名またはパスワードが正しくありません', 'error')

    return render_template('login.html')
```

#### 3.2.2 セッション管理
```python
def create_session(user_id: int, remember: bool = False) -> str:
    """セッション作成"""
    session_id = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(days=30 if remember else 1)

    with get_db_connection() as conn:
        conn.execute("""
            INSERT INTO sessions (session_id, user_id, expires_at, remember_me)
            VALUES (?, ?, ?, ?)
        """, (session_id, user_id, expires_at, remember))

    return session_id

def validate_session(session_id: str) -> bool:
    """セッション検証"""
    with get_db_connection() as conn:
        result = conn.execute("""
            SELECT user_id, expires_at FROM sessions
            WHERE session_id = ? AND expires_at > datetime('now')
        """, (session_id,)).fetchone()

    return result is not None
```

### 3.3 ポートフォリオ管理（blueprints/portfolio/）

#### 3.3.1 保有銘柄管理
```python
@portfolio_bp.route('/api/portfolio/holdings', methods=['GET'])
@login_required
def get_holdings():
    """保有銘柄一覧取得（株式と投資信託の統合ビュー）"""
    user_id = session.get('user_id')
    account_name = request.args.get('account_name')

    # 株式保有情報
    stock_holdings = get_stock_holdings(user_id, account_name)

    # 投資信託保有情報
    fund_holdings = get_fund_holdings(user_id, account_name)

    # 統合して返却
    all_holdings = {
        'stocks': stock_holdings,
        'funds': fund_holdings,
        'summary': calculate_portfolio_summary(stock_holdings, fund_holdings)
    }

    return jsonify(all_holdings)
```

#### 3.3.2 CSV インポート機能
```python
@portfolio_bp.route('/api/portfolio/holdings/upload', methods=['POST'])
@login_required
def upload_holdings():
    """CSVファイルから保有銘柄をインポート"""
    if 'file' not in request.files:
        return jsonify({'error': 'ファイルが選択されていません'}), 400

    file = request.files['file']
    account_name = request.form.get('account_name', 'default')
    account_type = request.form.get('account_type', 'specific')

    try:
        # CSVパース（SBI証券フォーマット対応）
        parser = SBICSVParser()
        holdings_data = parser.parse_holdings(file)

        # データベースに保存
        save_holdings_to_db(holdings_data, user_id, account_name, account_type)

        return jsonify({'success': True, 'count': len(holdings_data)})

    except Exception as e:
        log.error(f"CSV upload error: {e}")
        return jsonify({'error': str(e)}), 400
```

#### 3.3.3 パフォーマンス分析
```python
@portfolio_bp.route('/api/portfolio/transactions/performance')
@login_required
def get_performance():
    """取引パフォーマンス計算（キャッシュ付き）"""
    user_id = session.get('user_id')
    cache_key = f"perf_{user_id}_{request.args.get('account_name', 'all')}"

    # キャッシュチェック（5分間有効）
    cached = performance_cache.get(cache_key)
    if cached and cached['expires'] > datetime.now():
        return jsonify(cached['data'])

    # パフォーマンス計算
    performance = calculate_portfolio_performance(user_id)

    # キャッシュ保存
    performance_cache[cache_key] = {
        'data': performance,
        'expires': datetime.now() + timedelta(minutes=5)
    }

    return jsonify(performance)
```

### 3.4 スクリーニング実行（blueprints/screening/）

#### 3.4.1 ファンダメンタルスクリーニング
```python
@screening_bp.route('/api/screen/fundamental', methods=['POST'])
@admin_required
def run_fundamental_screening():
    """ファンダメンタルスクリーニング実行"""
    params = request.json

    try:
        # パラメータ設定
        cmd = ['python', 'screening/screen_statements.py']
        if params.get('lookback'):
            cmd.extend(['--lookback', str(params['lookback'])])
        if params.get('recent'):
            cmd.extend(['--recent', str(params['recent'])])
        if params.get('as_of'):
            cmd.extend(['--as-of', params['as_of']])

        # 実行
        output = run_command(cmd)

        # DBから結果を取得してExcel生成
        results = get_fundamental_signals_from_db(params.get('as_of'))
        excel_path = export_to_excel(results, 'fundamental')

        return jsonify({
            'success': True,
            'output': output,
            'excel_file': excel_path,
            'signal_count': len(results)
        })

    except Exception as e:
        log.error(f"Fundamental screening error: {e}")
        return jsonify({'error': str(e)}), 500
```

#### 3.4.2 Excel エクスポート機能
```python
def export_to_excel(data: pd.DataFrame, screen_type: str) -> str:
    """スクリーニング結果をExcelファイルにエクスポート"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{screen_type}_screen_{timestamp}.xlsx"
    filepath = OUTPUT_DIR / filename

    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        # データ書き込み
        data.to_excel(writer, sheet_name='Results', index=False)

        # フォーマット設定
        worksheet = writer.sheets['Results']

        # 列幅自動調整
        for idx, col in enumerate(data.columns):
            max_len = max(
                data[col].astype(str).map(len).max(),
                len(col)
            ) + 2
            worksheet.set_column(idx, idx, max_len)

        # 条件付き書式（利益率の色分け等）
        if 'return_pct' in data.columns:
            worksheet.conditional_format('E2:E1000', {
                'type': '3_color_scale',
                'min_color': '#FF0000',
                'mid_color': '#FFFFFF',
                'max_color': '#00FF00'
            })

    return filename
```

### 3.5 バックテスト実行（blueprints/backtest/）

#### 3.5.1 テクニカルバックテスト
```python
@backtest_bp.route('/api/backtest/technical', methods=['POST'])
@admin_required
def run_technical_backtest():
    """テクニカル戦略バックテスト実行"""
    params = request.json

    try:
        # コマンド構築
        cmd = ['python', 'backtest/backtest_technical.py']
        cmd.extend(['--side', params.get('side', 'long')])
        cmd.extend(['--hold-days', str(params.get('hold_days', 60))])
        cmd.extend(['--capital', str(params.get('capital', 1000000))])

        if params.get('stop_loss'):
            cmd.extend(['--stop-loss', str(params['stop_loss'])])

        # 期間指定
        if params.get('start_date'):
            cmd.extend(['--start', params['start_date']])
        if params.get('end_date'):
            cmd.extend(['--end', params['end_date']])

        # 実行とリアルタイム出力
        output = []
        for line in run_command_realtime(cmd):
            output.append(line)
            # Server-Sent Events形式で送信も可能

        # 結果ファイルパス取得
        result_file = extract_result_filename(output)

        return jsonify({
            'success': True,
            'output': '\n'.join(output),
            'result_file': result_file
        })

    except Exception as e:
        log.error(f"Technical backtest error: {e}")
        return jsonify({'error': str(e)}), 500
```

### 3.6 ユーティリティ機能（blueprints/utils/）

#### 3.6.1 閾値設定管理
```python
@utils_bp.route('/api/utils/thresholds', methods=['GET', 'POST'])
@admin_required
def manage_thresholds():
    """スクリーニング閾値の管理"""
    if request.method == 'GET':
        # 現在の設定を読み込み
        with open('screening/thresholds.json', 'r') as f:
            thresholds = json.load(f)
        return jsonify(thresholds)

    else:  # POST
        # 新しい設定を保存
        new_thresholds = request.json

        # バリデーション
        validate_thresholds(new_thresholds)

        # バックアップ作成
        backup_file = f"thresholds_backup_{datetime.now():%Y%m%d_%H%M%S}.json"
        shutil.copy('screening/thresholds.json', f'backup/{backup_file}')

        # 保存
        with open('screening/thresholds.json', 'w') as f:
            json.dump(new_thresholds, f, indent=2)

        return jsonify({'success': True, 'backup': backup_file})
```

### 3.7 共通機能（common.py）

#### 3.7.1 セキュリティ機能
```python
def generate_csrf_token() -> str:
    """CSRFトークン生成"""
    if '_csrf_token' not in session:
        session['_csrf_token'] = secrets.token_hex(16)
    return session['_csrf_token']

def validate_csrf_token(token: str) -> bool:
    """CSRFトークン検証"""
    return token == session.get('_csrf_token')

def get_secret_key() -> str:
    """シークレットキー取得（永続化）"""
    secret_file = Path('config/.secret_key')

    if secret_file.exists():
        return secret_file.read_text().strip()

    # 新規生成
    secret_key = secrets.token_hex(32)
    secret_file.parent.mkdir(exist_ok=True)
    secret_file.write_text(secret_key)
    secret_file.chmod(0o600)  # 読み取り権限を制限

    return secret_key
```

#### 3.7.2 レスポンス最適化
```python
def compress_response(f):
    """gzip圧縮デコレータ"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        @after_this_request
        def after_request(response):
            if 'gzip' not in request.headers.get('Accept-Encoding', ''):
                return response

            response.direct_passthrough = False

            if response.status_code < 200 or response.status_code >= 300:
                return response

            gzip_buffer = BytesIO()
            with gzip.GzipFile(mode='wb', fileobj=gzip_buffer) as gzip_file:
                gzip_file.write(response.get_data())

            response.set_data(gzip_buffer.getvalue())
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Content-Length'] = len(response.get_data())

            return response

        return f(*args, **kwargs)

    return decorated_function
```

## 4. フロントエンド設計

### 4.1 タブ型インターフェース（index.html）
```html
<div class="container-fluid mt-3">
    <ul class="nav nav-tabs" id="mainTabs">
        <li class="nav-item">
            <a class="nav-link active" data-bs-toggle="tab" href="#screening">
                <i class="bi bi-funnel"></i> スクリーニング
            </a>
        </li>
        <li class="nav-item">
            <a class="nav-link" data-bs-toggle="tab" href="#backtest">
                <i class="bi bi-graph-up"></i> バックテスト
            </a>
        </li>
        {% if is_admin %}
        <li class="nav-item">
            <a class="nav-link" data-bs-toggle="tab" href="#fetch">
                <i class="bi bi-cloud-download"></i> データ取得
            </a>
        </li>
        {% endif %}
        <li class="nav-item">
            <a class="nav-link" data-bs-toggle="tab" href="#portfolio">
                <i class="bi bi-briefcase"></i> ポートフォリオ
            </a>
        </li>
    </ul>
</div>
```

### 4.2 Ajax 通信パターン
```javascript
async function runScreening(type, params) {
    try {
        // ローディング表示
        showLoading();

        const response = await fetch(`/api/screen/${type}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRF-Token': getCSRFToken()
            },
            body: JSON.stringify(params)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();

        if (result.success) {
            showSuccess(`スクリーニング完了: ${result.signal_count}件`);
            if (result.excel_file) {
                showDownloadLink(result.excel_file);
            }
        } else {
            showError(result.error);
        }

    } catch (error) {
        showError('実行中にエラーが発生しました: ' + error.message);
    } finally {
        hideLoading();
    }
}
```

## 5. セキュリティ設計

### 5.1 認証・認可
- セッションベース認証（サーバーサイド管理）
- ロールベースアクセス制御（admin / portfolio_only）
- セッション有効期限管理（通常24時間、Remember Me で30日）

### 5.2 CSRF 対策
- すべての POST リクエストで CSRF トークン検証
- トークンはセッションごとに生成

### 5.3 入力検証
- SQLインジェクション対策（パラメータバインディング）
- パストラバーサル対策（ファイルパス検証）
- XSS対策（テンプレートの自動エスケープ）

### 5.4 セキュアな設定
```python
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,    # JavaScript からのアクセス禁止
    SESSION_COOKIE_SAMESITE='Lax',   # CSRF 攻撃の緩和
    SESSION_COOKIE_SECURE=True,      # HTTPS 必須（本番環境）
    WTF_CSRF_ENABLED=True,          # CSRF 保護有効化
)
```

## 6. パフォーマンス最適化

### 6.1 キャッシング
- 銘柄検索結果（10秒）
- ポートフォリオパフォーマンス（5分）
- 静的ファイル（ブラウザキャッシュ）

### 6.2 レスポンス圧縮
- gzip 圧縮による転送量削減
- 大きな JSON レスポンスで特に効果的

### 6.3 非同期処理
- 長時間実行タスクの非同期化検討
- Server-Sent Events によるリアルタイム進捗表示

## 7. エラーハンドリング

### 7.1 グローバルエラーハンドラ
```python
@app.errorhandler(404)
def not_found(error):
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Not found'}), 404
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    log.error(f"Internal error: {error}")
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Internal server error'}), 500
    return render_template('500.html'), 500
```

### 7.2 API エラーレスポンス
```python
def api_error_response(message: str, status_code: int = 400) -> Response:
    """統一的なAPIエラーレスポンス"""
    return jsonify({
        'error': message,
        'status': status_code,
        'timestamp': datetime.now().isoformat()
    }), status_code
```

## 8. デプロイメント

### 8.1 本番環境設定
```python
if app.config.get('ENV') == 'production':
    app.config.update(
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE='Strict',
        SEND_FILE_MAX_AGE_DEFAULT=31536000,  # 1年
    )
```

### 8.2 WSGI サーバー
- Gunicorn または uWSGI 推奨
- ワーカー数: CPU コア数 × 2 + 1
- タイムアウト: 300秒（長時間実行タスク対応）

## 9. 今後の拡張計画

### 9.1 機能拡張
- WebSocket によるリアルタイム通信
- RESTful API の OpenAPI 仕様書生成
- 多言語対応（i18n）
- ダークモード対応

### 9.2 性能改善
- Redis によるセッション管理
- CDN 活用による静的ファイル配信
- データベースコネクションプーリング

### 9.3 セキュリティ強化
- 2要素認証（2FA）対応
- API レート制限
- 監査ログの実装
- セキュリティヘッダーの追加（CSP、HSTS等）
