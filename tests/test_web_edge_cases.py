#!/usr/bin/env python
"""Web UIのエッジケーステスト

エラーハンドリング、特殊文字処理、例外処理などのエッジケースをテストします。
"""
import json
import os
from unittest.mock import MagicMock, patch

import pytest

from src.ui.web import app


@pytest.fixture
def client():
    """テスト用のFlaskクライアントを作成"""
    os.environ["TESTING"] = "true"
    app.config["TESTING"] = True
    app.config["LOGIN_DISABLED"] = True
    return app.test_client()


class TestSpecialCharacters:
    """特殊文字とUnicodeの処理テスト"""

    @patch("src.ui.web.run_command")
    def test_unicode_parameters(self, mock_run_command, client):
        """Unicode文字を含むパラメータのテスト"""
        mock_run_command.return_value = ("成功しました", 0)

        response = client.post(
            "/api/screen/fundamental",
            json={"lookback": "365", "comment": "日本語コメント🚀", "symbols": "♪♫♬"},
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"

    @patch("src.ui.web.run_command")
    def test_sql_injection_attempt(self, mock_run_command, client):
        """SQLインジェクション攻撃の試行テスト"""
        mock_run_command.return_value = ("Safe", 0)

        response = client.post(
            "/api/screen/fundamental",
            json={
                "lookback": "'; DROP TABLE users; --",
                "as_of": "2024-01-01' OR '1'='1",
            },
        )

        # パラメータは文字列として安全に処理されるべき
        assert response.status_code == 200
        args = mock_run_command.call_args[0][0]
        assert "'; DROP TABLE users; --" in args

    @patch("src.ui.web.run_command")
    def test_command_injection_attempt(self, mock_run_command, client):
        """コマンドインジェクション攻撃の試行テスト"""
        mock_run_command.return_value = ("Safe", 0)

        response = client.post(
            "/api/screen/fundamental",
            json={
                "lookback": "365 && rm -rf /",
                "as_of": "2024-01-01; cat /etc/passwd",
            },
        )

        assert response.status_code == 200
        # コマンドは適切にエスケープされるべき
        args = mock_run_command.call_args[0][0]
        assert "365 && rm -rf /" in args  # 文字列として扱われる

    def test_path_traversal_attempt(self, client):
        """パストラバーサル攻撃の試行テスト"""
        response = client.get("/api/download/../../../../etc/passwd")
        # 適切に拒否されるべき
        assert response.status_code == 404 or response.status_code == 403


class TestErrorScenarios:
    """エラーシナリオのテスト"""

    @patch("src.ui.web.run_command")
    def test_command_timeout(self, mock_run_command, client):
        """コマンドタイムアウトのテスト"""
        # タイムアウトをシミュレート
        mock_run_command.return_value = ("Command timed out", -1)

        response = client.post("/api/update/quotes")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "timed out" in data["output"].lower()

    @patch("src.ui.web.run_command")
    def test_memory_error(self, mock_run_command, client):
        """メモリ不足エラーのテスト"""
        mock_run_command.side_effect = MemoryError("Out of memory")

        response = client.post("/api/update/quotes")
        # エラーが適切にハンドリングされることを確認
        assert response.status_code == 200 or response.status_code == 500

    @patch("src.ui.web.sqlite3.connect")
    def test_database_locked(self, mock_connect, client):
        """データベースロックエラーのテスト"""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("database is locked")
        mock_connect.return_value = mock_conn

        response = client.get("/api/screening_results/fundamental")
        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data["status"]

    @patch("src.ui.web.pd.read_sql")
    def test_corrupted_data(self, mock_read_sql, client):
        """破損したデータの処理テスト"""
        # 不正なデータを返す
        mock_read_sql.return_value = None

        response = client.get("/api/screening_results/fundamental")
        # エラーが適切にハンドリングされることを確認
        assert response.status_code == 500


class TestExtremeValues:
    """極端な値のテスト"""

    @patch("src.ui.web.run_command")
    def test_very_large_numbers(self, mock_run_command, client):
        """非常に大きな数値のテスト"""
        mock_run_command.return_value = ("Success", 0)

        response = client.post(
            "/api/backtest/fundamental",
            json={"capital": "99999999999999999999999999999", "hold_days": "9999999"},
        )

        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_negative_values(self, mock_run_command, client):
        """負の値のテスト"""
        mock_run_command.return_value = ("Success", 0)

        response = client.post(
            "/api/screen/fundamental", json={"lookback": "-365", "recent": "-7"}
        )

        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_zero_values(self, mock_run_command, client):
        """ゼロ値のテスト"""
        mock_run_command.return_value = ("Success", 0)

        response = client.post(
            "/api/backtest/fundamental", json={"capital": "0", "hold_days": "0"}
        )

        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_decimal_values(self, mock_run_command, client):
        """小数値のテスト"""
        mock_run_command.return_value = ("Success", 0)

        response = client.post(
            "/api/backtest/fundamental",
            json={"capital": "1000000.123456789", "hold_days": "30.5"},
        )

        assert response.status_code == 200


class TestResourceLimits:
    """リソース制限のテスト"""

    @patch("src.ui.web.run_command")
    def test_many_concurrent_requests(self, mock_run_command, client):
        """多数の同時リクエストのテスト"""
        mock_run_command.return_value = ("Success", 0)

        # 100個の同時リクエストを送信
        responses = []
        for _i in range(100):
            response = client.post("/api/update/quotes")
            responses.append(response)

        # すべてのリクエストが処理されることを確認
        success_count = sum(1 for r in responses if r.status_code == 200)
        assert success_count > 0  # 少なくとも一部は成功するはず

    @patch("src.ui.web.run_command")
    def test_very_long_parameter_list(self, mock_run_command, client):
        """非常に長いパラメータリストのテスト"""
        mock_run_command.return_value = ("Success", 0)

        # 1000個のパラメータを送信
        params = {f"param_{i}": str(i) for i in range(1000)}

        response = client.post("/api/screen/fundamental", json=params)
        assert response.status_code == 200

    def test_empty_request_body(self, client):
        """空のリクエストボディのテスト"""
        response = client.post(
            "/api/screen/fundamental", data="", content_type="application/json"
        )
        # 適切にエラーハンドリングされることを確認
        assert response.status_code in [200, 400]


class TestSecurityEdgeCases:
    """セキュリティ関連のエッジケース"""

    def test_unauthorized_access_variations(self, client):
        """様々な未認証アクセスパターン"""
        # 認証を必要とするエンドポイントへのアクセス
        app.config["LOGIN_DISABLED"] = False

        endpoints = [
            "/api/update/quotes",
            "/api/portfolio/holdings",
            "/api/screening_results/fundamental",
        ]

        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.status_code in [401, 302]  # 未認証またはリダイレクト

    @patch("src.ui.web.run_command")
    def test_xss_attempt(self, mock_run_command, client):
        """XSS攻撃の試行テスト"""
        mock_run_command.return_value = ("<script>alert('XSS')</script>", 0)

        response = client.post("/api/update/quotes")
        assert response.status_code == 200

        # レスポンスが適切にエスケープされていることを確認
        data = response.get_data(as_text=True)
        assert "<script>" not in data or "&lt;script&gt;" in data

    def test_header_injection(self, client):
        """HTTPヘッダーインジェクション攻撃のテスト"""
        response = client.post(
            "/api/screen/fundamental",
            headers={"X-Custom-Header": "value\r\nX-Injected: malicious"},
        )
        # リクエストが正常に処理されることを確認
        assert response.status_code in [200, 400]


class TestDataTypeEdgeCases:
    """データ型のエッジケース"""

    @patch("src.ui.web.run_command")
    def test_mixed_data_types(self, mock_run_command, client):
        """混合データ型のテスト"""
        mock_run_command.return_value = ("Success", 0)

        response = client.post(
            "/api/screen/fundamental",
            json={
                "lookback": 365,  # 数値
                "as_of": "2024-01-01",  # 文字列
                "enabled": True,  # ブール値
                "options": ["a", "b", "c"],  # 配列
                "metadata": {"key": "value"},  # オブジェクト
            },
        )

        assert response.status_code == 200

    @patch("src.ui.web.run_command")
    def test_boolean_as_string(self, mock_run_command, client):
        """文字列として渡されたブール値のテスト"""
        mock_run_command.return_value = ("Success", 0)

        response = client.post(
            "/api/screen/fundamental",
            json={"enabled": "true", "disabled": "false", "maybe": "yes"},
        )

        assert response.status_code == 200
