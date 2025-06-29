"""環境構築スクリプトのテスト"""
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from setup_environment import EnvironmentSetup


class TestEnvironmentSetup:
    """EnvironmentSetupクラスのテスト"""
    
    def test_init(self, tmp_path: Path):
        """初期化のテスト"""
        setup = EnvironmentSetup(tmp_path)
        assert setup.base_dir == tmp_path
        assert setup.venv_dir == tmp_path / "venv"
    
    def test_setup_single_config(self, tmp_path: Path):
        """単一設定ファイルのセットアップテスト"""
        setup = EnvironmentSetup(tmp_path)
        
        # テンプレートファイルを作成
        template_path = tmp_path / "config.json.example"
        template_data = {"key": "value"}
        template_path.write_text(json.dumps(template_data))
        
        # セットアップ実行
        setup._setup_single_config("config.json", "config.json.example")
        
        # ターゲットファイルが作成されたことを確認
        target_path = tmp_path / "config.json"
        assert target_path.exists()
        
        # 内容が正しくコピーされたことを確認
        with open(target_path) as f:
            copied_data = json.load(f)
        assert copied_data == template_data
    
    @patch('subprocess.run')
    def test_create_virtual_environment(self, mock_run, tmp_path: Path):
        """仮想環境作成のテスト"""
        setup = EnvironmentSetup(tmp_path)
        setup.create_virtual_environment()
        
        # subprocessが正しく呼ばれたことを確認
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[1] == "-m"
        assert args[2] == "venv"
        assert str(tmp_path / "venv") in args
    
    def test_get_pip_command_with_venv(self, tmp_path: Path):
        """仮想環境ありの場合のpipコマンド取得テスト"""
        setup = EnvironmentSetup(tmp_path)
        
        # 仮想環境ディレクトリを作成
        venv_dir = tmp_path / "venv"
        venv_dir.mkdir()
        
        # Windowsの場合
        with patch('platform.system', return_value='Windows'):
            setup_win = EnvironmentSetup(tmp_path)
            pip_cmd = setup_win.get_pip_command()
            assert str(venv_dir / "Scripts" / "pip.exe") in pip_cmd[0]
        
        # Linux/macOSの場合
        with patch('platform.system', return_value='Linux'):
            setup_linux = EnvironmentSetup(tmp_path)
            pip_cmd = setup_linux.get_pip_command()
            assert str(venv_dir / "bin" / "pip") in pip_cmd[0]
    
    def test_create_directory_structure(self, tmp_path: Path):
        """ディレクトリ構造作成のテスト"""
        setup = EnvironmentSetup(tmp_path)
        setup.create_directory_structure()
        
        # 必要なディレクトリが作成されたことを確認
        expected_dirs = ["db", "fetch", "screening", "backtest", "templates", 
                        "docs", "tests", "logs", "output"]
        
        for dir_name in expected_dirs:
            assert (tmp_path / dir_name).exists()
            assert (tmp_path / dir_name).is_dir()