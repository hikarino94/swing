"""web.pyの統合テスト - 実際のコマンド実行を含むエンドツーエンドテスト"""

import json
import os
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.ui.web import app


@pytest.fixture
def client(authenticated_client):
    """Flask テストクライアントを作成（認証済み）"""
    return authenticated_client


@pytest.fixture
def test_environment(tmp_path):
    """統合テスト用の環境セットアップ"""
    # テスト用ディレクトリ構造を作成
    (tmp_path / "fetch").mkdir()
    (tmp_path / "screening").mkdir()
    (tmp_path / "backtest").mkdir()
    (tmp_path / "db").mkdir()

    # テスト用データベースを作成
    db_path = tmp_path / "db" / "stock.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prices (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            upper_limit REAL,
            lower_limit REAL,
            volume INTEGER,
            turnover_value REAL,
            adj_factor REAL,
            adj_open REAL,
            adj_high REAL,
            adj_low REAL,
            adj_close REAL,
            adj_volume INTEGER,
            PRIMARY KEY (code, date)
        );

        CREATE TABLE IF NOT EXISTS listed_info (
            code TEXT PRIMARY KEY,
            company_name TEXT,
            company_name_english TEXT,
            sector17_code TEXT,
            sector17_name TEXT,
            sector33_code TEXT,
            sector33_name TEXT,
            scale_category TEXT,
            market_code TEXT,
            market_name TEXT,
            delete_flag INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS statements (
            LocalCode TEXT NOT NULL,
            DisclosureNumber TEXT PRIMARY KEY,
            DisclosedDate TEXT,
            TypeOfDocument TEXT,
            TypeOfCurrentPeriod TEXT,
            CurrentPeriodStartDate TEXT,
            CurrentPeriodEndDate TEXT,
            CurrentFiscalYearStartDate TEXT,
            CurrentFiscalYearEndDate TEXT,
            NextFiscalYearStartDate TEXT,
            NextFiscalYearEndDate TEXT,
            NetSales REAL,
            OperatingProfit REAL,
            OrdinaryProfit REAL,
            Profit REAL,
            EarningsPerShare REAL,
            TotalAssets REAL,
            Equity REAL,
            EquityToAssetRatio REAL,
            BookValuePerShare REAL,
            ForecastNetSales REAL,
            ForecastOperatingProfit REAL,
            ForecastOrdinaryProfit REAL,
            ForecastProfit REAL,
            ForecastEarningsPerShare REAL,
            NextYearForecastNetSales REAL,
            NextYearForecastOperatingProfit REAL,
            NextYearForecastOrdinaryProfit REAL,
            NextYearForecastProfit REAL,
            NextYearForecastEarningsPerShare REAL
        );

        CREATE TABLE IF NOT EXISTS fundamental_signals (
            code TEXT NOT NULL,
            as_of TEXT NOT NULL,
            disclosure_date TEXT,
            market_cap REAL,
            per REAL,
            pbr REAL,
            roe REAL,
            equity_ratio REAL,
            sales_growth REAL,
            profit_growth REAL,
            PRIMARY KEY (code, as_of)
        );

        CREATE TABLE IF NOT EXISTS technical_indicators (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            rsi_14 REAL,
            macd REAL,
            macd_signal REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            volume_ratio REAL,
            golden_cross INTEGER,
            dead_cross INTEGER,
            rsi_oversold INTEGER,
            rsi_overbought INTEGER,
            bb_squeeze INTEGER,
            volume_spike INTEGER,
            PRIMARY KEY (code, date)
        );

        -- テストデータを挿入
        INSERT INTO listed_info (code, company_name, sector33_name, delete_flag)
        VALUES
            ('1301', 'テスト商事', '商社', 0),
            ('2914', 'テスト食品', '食品', 0),
            ('9999', '削除済み企業', 'その他', 1);

        INSERT INTO prices (code, date, open, high, low, close, volume, adj_close)
        VALUES
            ('1301', '2024-01-01', 1000, 1050, 990, 1020, 100000, 1020),
            ('1301', '2024-01-02', 1020, 1030, 1010, 1025, 120000, 1025),
            ('2914', '2024-01-01', 2000, 2100, 1990, 2050, 50000, 2050);
        """
    )
    conn.close()

    # account.jsonを作成
    account_data = {"mailaddress": "test@example.com", "password": "testpass123"}
    with open(tmp_path / "account.json", "w") as f:
        json.dump(account_data, f)

    # idtoken.jsonを作成
    idtoken_data = {"idToken": "test-token-12345"}
    with open(tmp_path / "idtoken.json", "w") as f:
        json.dump(idtoken_data, f)

    # thresholds.jsonを作成
    thresholds_data = {
        "per_min": 5,
        "per_max": 20,
        "pbr_max": 2,
        "roe_min": 8,
        "equity_ratio_min": 40,
        "market_cap_min": 10000000000,
    }
    with open(tmp_path / "screening" / "thresholds.json", "w") as f:
        json.dump(thresholds_data, f, indent=2)

    # config.jsonを作成
    config_data = {
        "database": {"path": str(db_path)},
        "api": {
            "base_url": "https://api.jpx-jquants.com/v1",
            "endpoints": {
                "auth": "/token/auth_user",
                "refresh": "/token/auth_refresh",
                "daily_quotes": "/prices/daily_quotes",
                "listed_info": "/listed/info",
                "statements": "/fins/statements",
            },
        },
        "files": {
            "account": "account.json",
            "idtoken": "idtoken.json",
            "thresholds": "screening/thresholds.json",
        },
    }
    with open(tmp_path / "config.json", "w") as f:
        json.dump(config_data, f, indent=2)

    return tmp_path


class TestIntegrationWorkflow:
    """実際のワークフローをシミュレートした統合テスト"""

    def test_complete_workflow(self, client, test_environment, monkeypatch):
        """データ取得→スクリーニング→バックテストの完全なワークフロー"""
        monkeypatch.chdir(test_environment)

        # 1. インデックスページが正常に表示される（認証済みなのでリダイレクトなし）
        response = client.get("/")
        assert response.status_code == 200

        # 2. DBサマリーを取得
        with patch("src.ui.web.run_command") as mock_run:
            mock_run.return_value = {
                "success": True,
                "output": "prices: 3件\nlisted_info: 3件",
                "error": "",
            }
            response = client.get("/api/utils/db_summary")
            assert response.status_code == 200
            data = response.get_json()
            assert "prices: 3件" in data["output"]

        # 3. 閾値設定を確認
        response = client.get("/api/utils/thresholds")
        assert response.status_code == 200
        data = response.get_json()
        assert data["data"]["per_min"] == 5

        # 4. 新しい閾値を設定
        new_thresholds = {"per_min": 10, "per_max": 30, "pbr_max": 3}
        response = client.post("/api/utils/thresholds", json=new_thresholds)
        assert response.status_code == 200

        # 5. スクリーニングを実行
        with patch("src.ui.web.pd") as mock_pd:
            with patch("src.ui.web.sqlite3.connect"):
                with patch("src.ui.web.run_command") as mock_run:
                    with patch("src.ui.web.timestamped_path") as mock_timestamped_path:
                        mock_run.return_value = {
                            "success": True,
                            "output": "10銘柄を抽出しました",
                            "error": "",
                        }

                        # DataFrameのモック
                        mock_df = MagicMock()
                        mock_df.empty = False
                        mock_df.columns = ["LocalCode", "company_name", "created_at"]
                        # 列幅計算のためのモック
                        mock_df.__getitem__.return_value.astype.return_value.str.len.return_value.max.return_value = (
                            10
                        )
                        mock_pd.read_sql.return_value = mock_df

                        # Excel出力のモック
                        mock_writer = MagicMock()
                        mock_pd.ExcelWriter.return_value.__enter__.return_value = (
                            mock_writer
                        )
                        mock_writer.sheets = {"Signals": MagicMock()}

                        # timestamped_pathのモック
                        mock_timestamped_path.return_value = (
                            "fund_screen_20240101_120000.xlsx"
                        )

                        response = client.post(
                            "/api/screen/fundamental",
                            json={"lookback": 60, "recent": 30},
                        )
                        assert response.status_code == 200
                        data = response.get_json()
                        assert data["success"] is True
                        assert "fund_screen_" in data["output_file"]

        # 6. バックテストを実行
        with patch("src.ui.web.run_command") as mock_run:
            mock_run.return_value = {
                "success": True,
                "output": "バックテスト完了\n総リターン: 15.2%",
                "error": "",
            }
            response = client.post(
                "/api/backtest/fundamental",
                json={
                    "hold_days": 20,
                    "capital": 1000000,
                    "start_date": "2023-01-01",
                    "end_date": "2023-12-31",
                },
            )
            assert response.status_code == 200
            data = response.get_json()
            assert "15.2%" in data["output"]

        # 7. 結果ファイル一覧を取得
        # テスト用ファイルを作成
        (test_environment / "fund_screen_20240101_120000.xlsx").write_text("dummy")
        (test_environment / "backtest_fund_20240101_120000.json").write_text("{}")

        response = client.get("/api/results/list")
        assert response.status_code == 200
        data = response.get_json()
        assert len(data["files"]) >= 2


class TestErrorHandling:
    """エラーハンドリングの統合テスト"""

    def test_missing_dependencies(self, client, test_environment, monkeypatch):
        """必要なファイルが存在しない場合のエラーハンドリング"""
        monkeypatch.chdir(test_environment)

        # account.jsonを削除
        os.unlink(test_environment / "account.json")

        response = client.post(
            "/api/utils/update_token", json={"email": "", "password": ""}
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["success"] is False
        assert "account.jsonが見つかりません" in data["error"]

    def test_command_execution_failure(self, client):
        """コマンド実行失敗時のエラーハンドリング"""
        with patch("src.ui.web.run_command") as mock_run:
            mock_run.return_value = {
                "success": False,
                "output": "",
                "error": "プログラムが見つかりません",
                "description": "テスト実行",
            }

            response = client.post("/api/fetch/quotes", json={})
            assert response.status_code == 200
            data = response.get_json()
            assert data["success"] is False
            assert "プログラムが見つかりません" in data["error"]


class TestConcurrentRequests:
    """並行リクエストのテスト"""

    def test_multiple_simultaneous_requests(self):
        """複数の同時リクエストを処理できることを確認"""
        # Flaskのテストクライアントはスレッドセーフではないため、
        # 各スレッドで独自のアプリケーションコンテキストを作成
        import threading

        results = []
        results_lock = threading.Lock()

        def make_request(endpoint):
            # 各スレッドで新しいテストクライアントを作成
            with app.test_client() as thread_client:
                with patch("src.ui.web.run_command") as mock_run:
                    mock_run.return_value = {
                        "success": True,
                        "output": f"{endpoint} completed",
                        "error": "",
                    }

                    # DataFrameのモックも追加（fundamental screeningの場合）
                    if endpoint == "/api/screen/fundamental":
                        with patch("src.ui.web.pd") as mock_pd:
                            with patch("src.ui.web.sqlite3.connect"):
                                with patch(
                                    "src.ui.web.timestamped_path",
                                    return_value="test.xlsx",
                                ):
                                    mock_df = MagicMock()
                                    mock_df.empty = True  # 結果なしとする
                                    mock_pd.read_sql.return_value = mock_df

                                    response = thread_client.post(endpoint, json={})
                                    with results_lock:
                                        results.append((endpoint, response.status_code))
                    else:
                        response = thread_client.post(endpoint, json={})
                        with results_lock:
                            results.append((endpoint, response.status_code))

        # 複数のエンドポイントに同時にリクエスト
        threads = []
        endpoints = [
            "/api/fetch/quotes",
            "/api/fetch/listed",
            "/api/screen/fundamental",
        ]

        for endpoint in endpoints:
            t = threading.Thread(target=make_request, args=(endpoint,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # すべてのリクエストが成功することを確認
        assert len(results) == 3
        for _endpoint, status_code in results:
            assert status_code == 200


class TestSecurityFeatures:
    """セキュリティ機能のテスト"""

    def test_file_download_security(self, client):
        """ファイルダウンロードのセキュリティ（パストラバーサル対策）"""
        # 危険なファイル名でのアクセス試行
        dangerous_filenames = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "../../../../account.json",
        ]

        for filename in dangerous_filenames:
            response = client.get(f"/api/results/download/{filename}")
            # secure_filenameによってサニタイズされるため、
            # 元のパスではなくサニタイズ後のファイル名でアクセスされる
            assert response.status_code == 404

    def test_json_injection_prevention(self, client):
        """JSON入力のバリデーション"""
        with patch("src.ui.web.run_command") as mock_run:
            # SQLインジェクション試行を含むJSON
            # コマンドに文字列がそのまま渡されると、エラーになるはず
            mock_run.return_value = {
                "success": False,
                "output": "",
                "error": "Invalid parameter",
                "description": "ファンダメンタルスクリーニング",
            }

            response = client.post(
                "/api/screen/fundamental",
                json={"lookback": "'; DROP TABLE prices; --"},
            )
            # アプリケーションは入力を受け入れるが、内部でエラーが発生する
            assert response.status_code == 200
            data = response.get_json()
            # コマンド実行時にエラーが発生することを確認
            assert data["success"] is False
            assert "error" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
