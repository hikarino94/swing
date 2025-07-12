# PowerShell 実行ポリシー設定
# このスクリプトを実行するには、PowerShellで以下のコマンドを実行してください:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "`n" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "     株式分析ツール - Windows環境構築" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 管理者権限チェック
function Test-IsAdmin {
    $currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    return $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-IsAdmin)) {
    Write-Host "⚠️  管理者権限が必要です。PowerShellを「管理者として実行」で起動してください。" -ForegroundColor Yellow
    Read-Host "Enterキーを押して終了してください"
    exit 1
}

# Pythonのインストール確認
try {
    $pythonVersion = python --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Python not found"
    }
    Write-Host "✓ $pythonVersion が見つかりました" -ForegroundColor Green
} catch {
    Write-Host "❌ Pythonが見つかりません" -ForegroundColor Red
    Write-Host ""
    Write-Host "以下からPython 3.9以上をインストールしてください：" -ForegroundColor Yellow
    Write-Host "https://www.python.org/downloads/"
    Write-Host ""
    Write-Host "インストール時に「Add Python to PATH」にチェックを入れてください" -ForegroundColor Yellow
    Read-Host "Enterキーを押して終了してください"
    exit 1
}

# プロジェクトディレクトリの確認
if (-not (Test-Path "requirements.txt")) {
    Write-Host "❌ requirements.txtが見つかりません" -ForegroundColor Red
    Write-Host "プロジェクトのルートディレクトリで実行してください" -ForegroundColor Yellow
    Read-Host "Enterキーを押して終了してください"
    exit 1
}

# 仮想環境の作成
Write-Host ""
Write-Host "📦 仮想環境を作成しています..." -ForegroundColor Blue
if (Test-Path "venv") {
    Write-Host "既存の仮想環境が見つかりました" -ForegroundColor Yellow
    $overwrite = Read-Host "削除して再作成しますか？ (y/N)"
    if ($overwrite -eq "y" -or $overwrite -eq "Y") {
        Write-Host "既存の仮想環境を削除中..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force "venv"
        python -m venv venv
        Write-Host "✓ 仮想環境を再作成しました" -ForegroundColor Green
    } else {
        Write-Host "既存の仮想環境を使用します" -ForegroundColor Green
    }
} else {
    python -m venv venv
    Write-Host "✓ 仮想環境を作成しました" -ForegroundColor Green
}

# 仮想環境の有効化
Write-Host ""
Write-Host "📚 依存関係をインストールしています..." -ForegroundColor Blue
& "venv\Scripts\Activate.ps1"

# pipのアップグレード
python -m pip install --upgrade pip | Out-Null
Write-Host "✓ pipをアップグレードしました" -ForegroundColor Green

# 依存関係のインストール
Write-Host "  → requirements.txt から本番環境の依存関係をインストール中..." -ForegroundColor White
try {
    pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install requirements"
    }
} catch {
    Write-Host "❌ 依存関係のインストールに失敗しました" -ForegroundColor Red
    Read-Host "Enterキーを押して終了してください"
    exit 1
}

# 開発環境の依存関係
if (Test-Path "requirements-dev.txt") {
    Write-Host "  → requirements-dev.txt から開発環境の依存関係をインストール中..." -ForegroundColor White
    try {
        pip install -r requirements-dev.txt
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install dev requirements"
        }
    } catch {
        Write-Host "❌ 開発環境の依存関係のインストールに失敗しました" -ForegroundColor Red
        Read-Host "Enterキーを押して終了してください"
        exit 1
    }
}

Write-Host "✓ 依存関係のインストールが完了しました" -ForegroundColor Green

# 設定ファイルの初期化
Write-Host ""
Write-Host "⚙️  設定ファイルを初期化しています..." -ForegroundColor Blue

# config.jsonの作成
if (-not (Test-Path "config.json") -and (Test-Path "config.json.example")) {
    Copy-Item "config.json.example" "config.json"
    Write-Host "  → config.json を作成しました" -ForegroundColor White
}

# account.jsonの作成
if (-not (Test-Path "account.json") -and (Test-Path "account.json.example")) {
    Copy-Item "account.json.example" "account.json"
    Write-Host "  → account.json を作成しました" -ForegroundColor White
    Write-Host ""
    Write-Host "📝 J-Quants APIの認証情報を設定してください" -ForegroundColor Yellow
    Write-Host "   後でaccount.jsonを編集することもできます" -ForegroundColor White
    $mailaddress = Read-Host "メールアドレス (スキップする場合はEnter)"
    if ($mailaddress -ne "") {
        $password = Read-Host "パスワード" -AsSecureString
        $passwordPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($password))

        $accountData = @{
            mailaddress = $mailaddress
            password = $passwordPlain
        }
        $accountData | ConvertTo-Json -Depth 10 | Out-File -FilePath "account.json" -Encoding UTF8
        Write-Host "  → 認証情報を保存しました" -ForegroundColor White
    }
}

# thresholds.jsonの作成
if (-not (Test-Path "screening")) {
    New-Item -ItemType Directory -Path "screening" | Out-Null
}
if (-not (Test-Path "screening\thresholds.json") -and (Test-Path "screening\thresholds.json.example")) {
    Copy-Item "screening\thresholds.json.example" "screening\thresholds.json"
    Write-Host "  → screening\thresholds.json を作成しました" -ForegroundColor White
}

Write-Host "✓ 設定ファイルの初期化が完了しました" -ForegroundColor Green

# データベースの初期化
Write-Host ""
Write-Host "🗄️  データベースを初期化しています..." -ForegroundColor Blue
if (-not (Test-Path "db")) {
    New-Item -ItemType Directory -Path "db" | Out-Null
}
if (Test-Path "db\db_schema.py") {
    try {
        python "db\db_schema.py"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to initialize database"
        }
        Write-Host "✓ データベースの初期化が完了しました" -ForegroundColor Green
    } catch {
        Write-Host "❌ データベースの初期化に失敗しました" -ForegroundColor Red
        Read-Host "Enterキーを押して終了してください"
        exit 1
    }
} else {
    Write-Host "⚠️  db_schema.py が見つかりません。データベースの初期化をスキップします" -ForegroundColor Yellow
}

# pre-commitフックの設定
Write-Host ""
Write-Host "🔧 pre-commitフックを設定しています..." -ForegroundColor Blue
try {
    pip install pre-commit | Out-Null
    if (Test-Path ".pre-commit-config.yaml") {
        & "venv\Scripts\pre-commit.exe" install
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to install pre-commit hooks"
        }
        Write-Host "✓ pre-commitフックの設定が完了しました" -ForegroundColor Green
    }
} catch {
    Write-Host "❌ pre-commitフックの設定に失敗しました" -ForegroundColor Red
    Read-Host "Enterキーを押して終了してください"
    exit 1
}

# ディレクトリ構造の作成
Write-Host ""
Write-Host "📁 ディレクトリ構造を作成しています..." -ForegroundColor Blue
$directories = @("db", "fetch", "screening", "backtest", "templates", "docs", "tests", "logs", "output")
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }
}
Write-Host "✓ ディレクトリ構造の作成が完了しました" -ForegroundColor Green

# 完了メッセージ
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "🎉 環境構築が正常に完了しました！" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📋 次のステップ:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. 仮想環境を有効化してください:" -ForegroundColor White
Write-Host "   > venv\Scripts\Activate.ps1" -ForegroundColor Gray
Write-Host ""
Write-Host "2. 設定ファイルを確認・編集してください:" -ForegroundColor White
Write-Host "   - account.json: J-Quants APIの認証情報" -ForegroundColor Gray
Write-Host "   - config.json: アプリケーション設定" -ForegroundColor Gray
Write-Host "   - screening\thresholds.json: スクリーニング閾値" -ForegroundColor Gray
Write-Host ""
Write-Host "3. IDトークンを取得してください:" -ForegroundColor White
Write-Host "   > python src\cli\update_idtoken.py" -ForegroundColor Gray
Write-Host ""
Write-Host "4. データを取得してください:" -ForegroundColor White
Write-Host "   > python fetch\daily_quotes.py" -ForegroundColor Gray
Write-Host "   > python fetch\listed_info.py" -ForegroundColor Gray
Write-Host "   > python fetch\statements.py" -ForegroundColor Gray
Write-Host ""
Write-Host "5. アプリケーションを起動してください:" -ForegroundColor White
Write-Host "   > python -m src.ui.web        (Webインターフェース)" -ForegroundColor Gray
Write-Host ""
Write-Host "詳細は README.md を参照してください。" -ForegroundColor Yellow
Write-Host ""
Read-Host "Enterキーを押して終了してください"
