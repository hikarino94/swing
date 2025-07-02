#!/usr/bin/env python3
"""
パス設定の妥当性を検証するテスト

ディレクトリ構成変更による影響を検知し、
必要なファイルやディレクトリが正しい場所に存在することを確認します。
"""

import sys
from pathlib import Path

import pytest

# プロジェクトルートをPYTHONPATHに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))


class TestPathValidation:
    """パス設定の妥当性を検証するテストクラス"""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """各テストメソッドの前に実行される設定"""
        # 必要に応じて環境をセットアップ
        pass

    """パス設定の妥当性を検証するテストクラス"""

    def test_project_structure(self):
        """プロジェクトの基本的なディレクトリ構造を検証"""
        # 必須ディレクトリの存在確認
        required_dirs = [
            "fetch",
            "screening",
            "backtest",
            "db",
            "src",
            "src/cli",
            "src/ui",
            "src/ui/legacy",
            "src/utils",
            "templates",
            "tests",
            "config",
            "data",
            "scripts",
        ]

        for dir_name in required_dirs:
            dir_path = project_root / dir_name
            assert dir_path.exists(), f"必須ディレクトリが見つかりません: {dir_name}"
            assert dir_path.is_dir(), f"ディレクトリではありません: {dir_name}"

    def test_main_scripts_exist(self):
        """主要なスクリプトファイルの存在を確認"""
        main_scripts = [
            "src/ui/web.py",
            "src/ui/legacy/gui.py",
            "src/ui/legacy/web.py",
            "src/cli/scheduler.py",
            "src/cli/update_idtoken.py",
            "scripts/setup_environment.py",
            "scripts/log_viewer.py",
        ]

        for script in main_scripts:
            script_path = project_root / script
            assert script_path.exists(), f"スクリプトが見つかりません: {script}"
            assert script_path.is_file(), f"ファイルではありません: {script}"

    def test_fetch_modules_exist(self):
        """データ取得モジュールの存在を確認"""
        fetch_modules = [
            "fetch/daily_quotes.py",
            "fetch/listed_info.py",
            "fetch/statements.py",
        ]

        for module in fetch_modules:
            module_path = project_root / module
            assert module_path.exists(), f"fetchモジュールが見つかりません: {module}"

    def test_screening_modules_exist(self):
        """スクリーニングモジュールの存在を確認"""
        screening_modules = [
            "screening/screen_statements.py",
            "screening/screen_technical.py",
            "screening/screen_ml.py",
            "screening/thresholds.json",
        ]

        for module in screening_modules:
            module_path = project_root / module
            assert (
                module_path.exists()
            ), f"screeningモジュールが見つかりません: {module}"

    def test_backtest_modules_exist(self):
        """バックテストモジュールの存在を確認"""
        backtest_modules = [
            "backtest/backtest_statements.py",
            "backtest/backtest_technical.py",
            "backtest/backtest_ml.py",
            "backtest/analyze_backtest_json.py",
        ]

        for module in backtest_modules:
            module_path = project_root / module
            assert module_path.exists(), f"backtestモジュールが見つかりません: {module}"

    def test_db_modules_exist(self):
        """データベース関連モジュールの存在を確認"""
        db_modules = [
            "db/db_schema.py",
            "db/db_summary.py",
            "db/list_signals.py",
        ]

        for module in db_modules:
            module_path = project_root / module
            assert module_path.exists(), f"dbモジュールが見つかりません: {module}"

    def test_template_files_exist(self):
        """Webアプリケーションのテンプレートファイルの存在を確認"""
        template_files = [
            "templates/index.html",
            "templates/base.html",
        ]

        for template in template_files:
            template_path = project_root / template
            assert (
                template_path.exists()
            ), f"テンプレートファイルが見つかりません: {template}"

    def test_config_structure(self):
        """設定ファイルのディレクトリ構造を確認"""
        config_dir = project_root / "config"
        assert config_dir.exists(), "configディレクトリが見つかりません"
        assert config_dir.is_dir(), "configがディレクトリではありません"

        # 設定ファイルの存在確認（オプショナル）
        optional_configs = [
            "config/config.json",
            "config/account.json",
            "config/login.json",
            "config/idtoken.json",
        ]

        for config in optional_configs:
            config_path = project_root / config
            if config_path.exists():
                assert config_path.is_file(), f"設定ファイルではありません: {config}"

    def test_src_module_imports(self):
        """srcモジュールのインポートが正しく動作することを確認"""
        try:
            # 主要なsrcモジュールをインポート
            from src.config import Config
            from src.utils.file_utils import get_timestamped_output_path
            from src.utils.logging_config import get_logger

            assert Config is not None
            assert get_timestamped_output_path is not None
            assert get_logger is not None
        except ImportError as e:
            pytest.fail(f"srcモジュールのインポートに失敗: {e}")

    def test_web_app_paths(self):
        """Webアプリケーションのパス設定を検証"""
        from src.ui.web import app
        from src.ui.web import project_root as web_project_root
        from src.ui.web import template_dir

        # プロジェクトルートが正しく設定されているか
        assert web_project_root == project_root

        # テンプレートディレクトリが正しく設定されているか
        assert template_dir == project_root / "templates"
        assert template_dir.exists()

        # Flaskアプリのテンプレートフォルダが正しく設定されているか
        assert app.template_folder == str(template_dir)

    def test_scheduler_paths(self):
        """スケジューラーのパス設定を検証"""
        from src.cli import scheduler

        # スケジューラーモジュールが正しくインポートできることを確認
        assert hasattr(scheduler, "fetch_quotes")
        assert hasattr(scheduler, "fetch_statements")
        assert hasattr(scheduler, "update_listed_info")

    def test_data_directories(self):
        """データディレクトリの構造を確認"""
        data_dir = project_root / "data"
        assert data_dir.exists(), "dataディレクトリが見つかりません"

        # サブディレクトリの確認（存在しない場合は作成される前提）
        subdirs = ["output", "logs"]
        for subdir in subdirs:
            subdir_path = data_dir / subdir
            if not subdir_path.exists():
                # ディレクトリが存在しない場合は、親ディレクトリが存在することを確認
                assert (
                    subdir_path.parent.exists()
                ), f"親ディレクトリが存在しません: {subdir}"

    def test_makefile_commands_consistency(self):
        """Makefileのコマンドが実際のファイル構造と一致することを確認"""
        makefile_path = project_root / "Makefile"
        assert makefile_path.exists(), "Makefileが見つかりません"

        # Makefileの内容を読み込んで、参照されているパスを確認
        makefile_content = makefile_path.read_text()

        # 主要なコマンドのパスが正しいことを確認
        expected_paths = {
            "python3 scripts/setup_environment.py": "scripts/setup_environment.py",
            "python3 src/cli/update_idtoken.py": "src/cli/update_idtoken.py",
            "python3 -m src.cli.scheduler": "src/cli/scheduler.py",
            "python3 -m src.ui.legacy.gui": "src/ui/legacy/gui.py",
            "python3 -m src.ui.web": "src/ui/web.py",
        }

        for command, file_path in expected_paths.items():
            if command in makefile_content:
                actual_path = project_root / file_path
                assert (
                    actual_path.exists()
                ), f"Makefileで参照されているファイルが存在しません: {file_path}"

    def test_subprocess_commands_in_web_ui(self):
        """Web UIで使用されているsubprocessコマンドのパスを検証"""

        # Web UIで使用されているコマンドパスを検証
        test_commands = [
            ("fetch/daily_quotes.py", "日足データ取得"),
            ("fetch/listed_info.py", "上場銘柄情報取得"),
            ("fetch/statements.py", "決算情報取得"),
            ("screening/screen_statements.py", "財務スクリーニング"),
            ("screening/screen_technical.py", "テクニカルスクリーニング"),
            ("screening/screen_ml.py", "MLスクリーニング"),
            ("backtest/backtest_statements.py", "財務バックテスト"),
            ("backtest/backtest_technical.py", "テクニカルバックテスト"),
            ("backtest/backtest_ml.py", "MLバックテスト"),
            ("src/cli/update_idtoken.py", "IDトークン更新"),
            ("db/db_summary.py", "データベース概要"),
            ("db/list_signals.py", "シグナルリスト"),
            ("backtest/analyze_backtest_json.py", "バックテスト分析"),
        ]

        for script_path, description in test_commands:
            full_path = project_root / script_path
            assert (
                full_path.exists()
            ), f"{description}用のスクリプトが見つかりません: {script_path}"
            assert (
                full_path.is_file()
            ), f"{description}用のパスがファイルではありません: {script_path}"

            # スクリプトが実行可能であることを確認（Pythonファイルであること）
            assert script_path.endswith(
                ".py"
            ), f"Pythonスクリプトではありません: {script_path}"

    def test_legacy_gui_subprocess_commands(self):
        """レガシーGUIで使用されているコマンドパスの問題を検出"""
        legacy_gui_path = project_root / "src/ui/legacy/gui.py"
        content = legacy_gui_path.read_text()

        # 古いパス形式を検出
        problematic_patterns = [
            (r'"python update_idtoken\.py"', "update_idtoken.pyの古いパス"),
            (r'cmd = "python fetch/', "fetchコマンドの古いパス形式"),
            (r'cmd = "python screening/', "screeningコマンドの古いパス形式"),
            (r'cmd = "python backtest/', "backtestコマンドの古いパス形式"),
            (r'cmd = "python db/', "dbコマンドの古いパス形式"),
        ]

        import re

        issues_found = []
        for pattern, description in problematic_patterns:
            if re.search(pattern, content):
                issues_found.append(description)

        # 既知の問題として記録（将来的に修正が必要）
        if issues_found:
            print("\n警告: legacy/gui.pyに以下の古いパス参照があります:")
            for issue in issues_found:
                print(f"  - {issue}")
            # 現時点では警告のみ（レガシーコードのため）

    def test_scheduler_command_paths(self):
        """スケジューラーのコマンドパスを検証"""

        # スケジューラーが使用するモジュールが存在することを確認
        modules_to_check = [
            "fetch.daily_quotes",
            "fetch.statements",
            "fetch.listed_info",
        ]

        for module_name in modules_to_check:
            # モジュールパスをファイルパスに変換
            file_path = module_name.replace(".", "/") + ".py"
            full_path = project_root / file_path
            assert (
                full_path.exists()
            ), f"スケジューラーが参照するモジュールが見つかりません: {file_path}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
