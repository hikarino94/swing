#!/usr/bin/env python3
"""
環境構築スクリプト

このスクリプトは開発環境を自動的にセットアップします。
以下の処理を実行します：
1. Python仮想環境の作成
2. 依存関係のインストール
3. 設定ファイルの初期化
4. データベースの初期化
5. pre-commitフックの設定
"""
import argparse
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path


class EnvironmentSetup:
    """環境構築クラス"""

    def __init__(self, base_dir: Path | None = None):
        """
        初期化

        Args:
            base_dir: プロジェクトのベースディレクトリ
        """
        self.base_dir = base_dir or Path(__file__).resolve().parent
        self.venv_dir = self.base_dir / "venv"
        self.is_windows = platform.system() == "Windows"
        self.python_executable = sys.executable

    def run(self, skip_venv: bool = False, skip_db: bool = False) -> None:
        """
        環境構築を実行します

        Args:
            skip_venv: 仮想環境の作成をスキップ
            skip_db: データベースの初期化をスキップ
        """
        print("🚀 環境構築を開始します...")

        # 1. 仮想環境の作成
        if not skip_venv:
            self.create_virtual_environment()

        # 2. 依存関係のインストール
        self.install_dependencies()

        # 3. 設定ファイルの初期化
        self.setup_config_files()

        # 4. データベースの初期化
        if not skip_db:
            self.initialize_database()

        # 5. pre-commitフックの設定
        self.setup_precommit()

        # 6. ディレクトリ構造の作成
        self.create_directory_structure()

        print("\n✅ 環境構築が完了しました！")
        self.print_next_steps()

    def create_virtual_environment(self) -> None:
        """仮想環境を作成します"""
        print("\n📦 仮想環境を作成しています...")

        if self.venv_dir.exists():
            response = input("既存の仮想環境が見つかりました。削除して再作成しますか？ (y/N): ")
            if response.lower() == "y":
                shutil.rmtree(self.venv_dir)
            else:
                print("既存の仮想環境を使用します。")
                return

        subprocess.run(
            [self.python_executable, "-m", "venv", str(self.venv_dir)], check=True
        )
        print("✓ 仮想環境を作成しました")

    def get_pip_command(self) -> list:
        """pipコマンドを取得します"""
        if self.venv_dir.exists():
            if self.is_windows:
                return [str(self.venv_dir / "Scripts" / "pip.exe")]
            else:
                return [str(self.venv_dir / "bin" / "pip")]
        else:
            return [self.python_executable, "-m", "pip"]

    def get_python_command(self) -> str:
        """Pythonコマンドを取得します"""
        if self.venv_dir.exists():
            if self.is_windows:
                return str(self.venv_dir / "Scripts" / "python.exe")
            else:
                return str(self.venv_dir / "bin" / "python")
        else:
            return self.python_executable

    def install_dependencies(self) -> None:
        """依存関係をインストールします"""
        print("\n📚 依存関係をインストールしています...")

        pip_cmd = self.get_pip_command()

        # pipをアップグレード
        subprocess.run([*pip_cmd, "install", "--upgrade", "pip"], check=True)

        # 本番環境の依存関係
        if (self.base_dir / "requirements.txt").exists():
            print("  → requirements.txt から本番環境の依存関係をインストール中...")
            subprocess.run([*pip_cmd, "install", "-r", "requirements.txt"], check=True)

        # 開発環境の依存関係
        if (self.base_dir / "requirements-dev.txt").exists():
            print("  → requirements-dev.txt から開発環境の依存関係をインストール中...")
            subprocess.run(
                [*pip_cmd, "install", "-r", "requirements-dev.txt"], check=True
            )

        print("✓ 依存関係のインストールが完了しました")

    def setup_config_files(self) -> None:
        """設定ファイルを初期化します"""
        print("\n⚙️  設定ファイルを初期化しています...")

        # config.json
        self._setup_single_config("config.json", "config.json.example")

        # account.json
        self._setup_single_config("account.json", "account.json.example")

        # thresholds.json
        thresholds_dir = self.base_dir / "screening"
        thresholds_dir.mkdir(exist_ok=True)
        self._setup_single_config(
            "screening/thresholds.json", "screening/thresholds.json.example"
        )

        print("✓ 設定ファイルの初期化が完了しました")

    def _setup_single_config(self, target: str, template: str) -> None:
        """単一の設定ファイルをセットアップします"""
        target_path = self.base_dir / target
        template_path = self.base_dir / template

        if not target_path.exists() and template_path.exists():
            shutil.copy2(template_path, target_path)
            print(f"  → {target} を作成しました")

            # account.jsonの場合は認証情報の入力を促す
            if target == "account.json":
                self._prompt_for_credentials(target_path)

    def _prompt_for_credentials(self, account_path: Path) -> None:
        """認証情報の入力を促します"""
        print("\n📝 J-Quants APIの認証情報を設定します")
        print("   (後で account.json を編集することもできます)")

        mailaddress = input("メールアドレス (スキップする場合はEnter): ").strip()
        if mailaddress:
            password = input("パスワード: ").strip()

            with open(account_path, encoding="utf-8") as f:
                account_data = json.load(f)

            account_data["mailaddress"] = mailaddress
            account_data["password"] = password

            with open(account_path, "w", encoding="utf-8") as f:
                json.dump(account_data, f, indent=4, ensure_ascii=False)

            print("  → 認証情報を保存しました")

    def initialize_database(self) -> None:
        """データベースを初期化します"""
        print("\n🗄️  データベースを初期化しています...")

        db_dir = self.base_dir / "db"
        db_dir.mkdir(exist_ok=True)

        python_cmd = self.get_python_command()
        db_schema_path = db_dir / "db_schema.py"

        if db_schema_path.exists():
            subprocess.run([python_cmd, str(db_schema_path)], check=True)
            print("✓ データベースの初期化が完了しました")
        else:
            print("⚠️  db_schema.py が見つかりません。データベースの初期化をスキップします")

    def setup_precommit(self) -> None:
        """pre-commitフックを設定します"""
        print("\n🔧 pre-commitフックを設定しています...")

        pip_cmd = self.get_pip_command()

        # pre-commitがインストールされているか確認
        try:
            subprocess.run(
                [*pip_cmd, "show", "pre-commit"], check=True, capture_output=True
            )
        except subprocess.CalledProcessError:
            print("  → pre-commitをインストール中...")
            subprocess.run([*pip_cmd, "install", "pre-commit"], check=True)

        # pre-commitフックをインストール
        if (self.base_dir / ".pre-commit-config.yaml").exists():
            if self.venv_dir.exists():
                if self.is_windows:
                    pre_commit_cmd = str(self.venv_dir / "Scripts" / "pre-commit.exe")
                else:
                    pre_commit_cmd = str(self.venv_dir / "bin" / "pre-commit")
            else:
                pre_commit_cmd = "pre-commit"

            subprocess.run([pre_commit_cmd, "install"], check=True)
            print("✓ pre-commitフックの設定が完了しました")

    def create_directory_structure(self) -> None:
        """ディレクトリ構造を作成します"""
        print("\n📁 ディレクトリ構造を作成しています...")

        directories = [
            "db",
            "fetch",
            "screening",
            "backtest",
            "templates",
            "docs",
            "tests",
            "logs",
            "output",
        ]

        for directory in directories:
            dir_path = self.base_dir / directory
            dir_path.mkdir(exist_ok=True)

        print("✓ ディレクトリ構造の作成が完了しました")

    def print_next_steps(self) -> None:
        """次のステップを表示します"""
        print("\n" + "=" * 60)
        print("🎉 環境構築が正常に完了しました！")
        print("=" * 60)

        print("\n📋 次のステップ:")

        if self.venv_dir.exists():
            print("\n1. 仮想環境を有効化してください:")
            if self.is_windows:
                print(f"   > {self.venv_dir}\\Scripts\\activate")
            else:
                print(f"   $ source {self.venv_dir}/bin/activate")

        print("\n2. 設定ファイルを確認・編集してください:")
        print("   - account.json: J-Quants APIの認証情報")
        print("   - config.json: アプリケーション設定")
        print("   - screening/thresholds.json: スクリーニング閾値")

        print("\n3. IDトークンを取得してください:")
        print("   $ python update_idtoken.py")

        print("\n4. データを取得してください:")
        print("   $ python fetch/daily_quotes.py")
        print("   $ python fetch/listed_info.py")
        print("   $ python fetch/statements.py")

        print("\n詳細は README.md を参照してください。")


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description="開発環境を自動的にセットアップします")
    parser.add_argument("--skip-venv", action="store_true", help="仮想環境の作成をスキップ")
    parser.add_argument("--skip-db", action="store_true", help="データベースの初期化をスキップ")

    args = parser.parse_args()

    setup = EnvironmentSetup()

    try:
        setup.run(skip_venv=args.skip_venv, skip_db=args.skip_db)
    except KeyboardInterrupt:
        print("\n\n⚠️  環境構築が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
