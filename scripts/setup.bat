@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo.
echo ========================================
echo     株式分析ツール - Windows環境構築
echo ========================================
echo.

:: 管理者権限チェック
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ⚠️  管理者権限が必要です。右クリックで「管理者として実行」を選択してください。
    pause
    exit /b 1
)

:: Pythonのインストール確認
python --version >nul 2>&1
if %errorLevel% neq 0 (
    echo ❌ Pythonが見つかりません
    echo.
    echo 以下からPython 3.9以上をインストールしてください：
    echo https://www.python.org/downloads/
    echo.
    echo インストール時に「Add Python to PATH」にチェックを入れてください
    pause
    exit /b 1
)

:: Pythonバージョン確認
for /f "tokens=2" %%i in ('python --version') do set PYTHON_VERSION=%%i
echo ✓ Python %PYTHON_VERSION% が見つかりました

:: プロジェクトディレクトリの確認
if not exist "requirements.txt" (
    echo ❌ requirements.txtが見つかりません
    echo プロジェクトのルートディレクトリで実行してください
    pause
    exit /b 1
)

:: 仮想環境の作成
echo.
echo 📦 仮想環境を作成しています...
if exist "venv" (
    echo 既存の仮想環境が見つかりました
    set /p OVERWRITE="削除して再作成しますか？ (y/N): "
    if /i "!OVERWRITE!"=="y" (
        echo 既存の仮想環境を削除中...
        rmdir /s /q venv
        python -m venv venv
        echo ✓ 仮想環境を再作成しました
    ) else (
        echo 既存の仮想環境を使用します
    )
) else (
    python -m venv venv
    echo ✓ 仮想環境を作成しました
)

:: 仮想環境の有効化
echo.
echo 📚 依存関係をインストールしています...
call venv\Scripts\activate.bat

:: pipのアップグレード
python -m pip install --upgrade pip
echo ✓ pipをアップグレードしました

:: 依存関係のインストール
echo   → requirements.txt から本番環境の依存関係をインストール中...
pip install -r requirements.txt
if %errorLevel% neq 0 (
    echo ❌ 依存関係のインストールに失敗しました
    pause
    exit /b 1
)

:: 開発環境の依存関係
if exist "requirements-dev.txt" (
    echo   → requirements-dev.txt から開発環境の依存関係をインストール中...
    pip install -r requirements-dev.txt
    if %errorLevel% neq 0 (
        echo ❌ 開発環境の依存関係のインストールに失敗しました
        pause
        exit /b 1
    )
)

echo ✓ 依存関係のインストールが完了しました

:: 設定ファイルの初期化
echo.
echo ⚙️  設定ファイルを初期化しています...

:: config.jsonの作成
if not exist "config.json" (
    if exist "config.json.example" (
        copy "config.json.example" "config.json" >nul
        echo   → config.json を作成しました
    )
)

:: account.jsonの作成
if not exist "account.json" (
    if exist "account.json.example" (
        copy "account.json.example" "account.json" >nul
        echo   → account.json を作成しました
        echo.
        echo 📝 J-Quants APIの認証情報を設定してください
        echo    後でaccount.jsonを編集することもできます
        set /p MAILADDRESS="メールアドレス (スキップする場合はEnter): "
        if not "!MAILADDRESS!"=="" (
            set /p PASSWORD="パスワード: "
            echo {"mailaddress": "!MAILADDRESS!", "password": "!PASSWORD!"} > account.json
            echo   → 認証情報を保存しました
        )
    )
)

:: thresholds.jsonの作成
if not exist "screening" mkdir screening
if not exist "screening\thresholds.json" (
    if exist "screening\thresholds.json.example" (
        copy "screening\thresholds.json.example" "screening\thresholds.json" >nul
        echo   → screening\thresholds.json を作成しました
    )
)

echo ✓ 設定ファイルの初期化が完了しました

:: データベースの初期化
echo.
echo 🗄️  データベースを初期化しています...
if not exist "db" mkdir db
if exist "db\db_schema.py" (
    python db\db_schema.py
    if %errorLevel% neq 0 (
        echo ❌ データベースの初期化に失敗しました
        pause
        exit /b 1
    )
    echo ✓ データベースの初期化が完了しました
) else (
    echo ⚠️  db_schema.py が見つかりません。データベースの初期化をスキップします
)

:: pre-commitフックの設定
echo.
echo 🔧 pre-commitフックを設定しています...
pip install pre-commit >nul 2>&1
if exist ".pre-commit-config.yaml" (
    venv\Scripts\pre-commit.exe install
    if %errorLevel% neq 0 (
        echo ❌ pre-commitフックの設定に失敗しました
        pause
        exit /b 1
    )
    echo ✓ pre-commitフックの設定が完了しました
)

:: ディレクトリ構造の作成
echo.
echo 📁 ディレクトリ構造を作成しています...
for %%d in (db fetch screening backtest templates docs tests logs output) do (
    if not exist "%%d" mkdir "%%d"
)
echo ✓ ディレクトリ構造の作成が完了しました

:: 完了メッセージ
echo.
echo ============================================================
echo 🎉 環境構築が正常に完了しました！
echo ============================================================
echo.
echo 📋 次のステップ:
echo.
echo 1. 仮想環境を有効化してください:
echo    ^> venv\Scripts\activate.bat
echo.
echo 2. 設定ファイルを確認・編集してください:
echo    - account.json: J-Quants APIの認証情報
echo    - config.json: アプリケーション設定
echo    - screening\thresholds.json: スクリーニング閾値
echo.
echo 3. IDトークンを取得してください:
echo    ^> python src\cli\update_idtoken.py
echo.
echo 4. データを取得してください:
echo    ^> python fetch\daily_quotes.py
echo    ^> python fetch\listed_info.py
echo    ^> python fetch\statements.py
echo.
echo 5. アプリケーションを起動してください:
echo    ^> python -m src.ui.web        (Webインターフェース)
echo.
echo 詳細は README.md を参照してください。
echo.
pause
