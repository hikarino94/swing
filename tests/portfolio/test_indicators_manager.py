"""IndicatorsManagerクラスのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.indicators_manager import IndicatorsManager


class TestIndicatorsManager:
    """IndicatorsManagerクラスのテスト"""

    @pytest.fixture
    def mock_connect(self):
        """データベース接続のモック"""
        with patch("src.portfolio.indicators_manager.sqlite3.connect") as mock:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock.return_value = mock_conn
            yield mock, mock_conn, mock_cursor

    def test_update_stock_indicators_success(self, mock_connect):
        """株価指標更新の成功テスト"""
        _, mock_conn, mock_cursor = mock_connect

        # 保有銘柄の取得
        mock_cursor.fetchall.side_effect = [
            [("1234",), ("5678",)],  # 保有銘柄コード
        ]

        # 各銘柄の価格とステートメントデータ
        mock_cursor.fetchone.side_effect = [
            # 1234の処理
            (1500.0, "2024-01-10"),  # 価格データ
            (100, 1000, 50, 60, 110, 120, 45),  # ステートメントデータ
            # 5678の処理
            (2000.0, "2024-01-10"),  # 価格データ
            (150, 1200, 80, 90, 160, 170, 75),  # ステートメントデータ
        ]

        mock_cursor.rowcount = 1  # 各更新で1件

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 2  # 2銘柄更新
        assert mock_conn.commit.called
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_with_specific_codes(self, mock_connect):
        """特定銘柄の指標更新テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (100, 1000, 50, 60, 110, 120, 45),  # ステートメントデータ
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1, codes=["1234"])

        assert result == 1
        assert mock_conn.commit.called
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_no_price_data(self, mock_connect):
        """価格データがない場合のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            None,  # 価格データなし
        ]

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 0
        assert mock_conn.commit.called
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_no_statement_data(self, mock_connect):
        """ステートメントデータがない場合のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            None,  # ステートメントデータなし
        ]

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 0
        assert mock_conn.commit.called
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_with_null_eps(self, mock_connect):
        """EPSがNullの場合の処理テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (None, 1000, 50, 60, None, None, 45),  # EPS関連がNull
            (None, None, 100),  # EPSの別レコード検索結果
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 1
        # PER計算のためのEPS取得クエリが実行されている
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("ForecastEarningsPerShare" in call for call in execute_calls)
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_with_null_bps(self, mock_connect):
        """BPSがNullの場合の処理テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (100, None, 50, 60, 110, 120, 45),  # BPSがNull
            (1200,),  # BPSの別レコード検索結果
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 1
        # PBR計算のためのBPS取得クエリが実行されている
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("BookValuePerShare" in call for call in execute_calls)
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_with_null_dividend(self, mock_connect):
        """配当がNullの場合の処理テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (100, 1000, None, None, 110, 120, None),  # 配当関連がNull
            (80,),  # 配当の別レコード検索結果
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 1
        # 配当利回り計算のための配当取得クエリが実行されている
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any(
            "NextYearForecastDividendPerShareAnnual" in call for call in execute_calls
        )
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_zero_eps(self, mock_connect):
        """EPSがゼロの場合のPER計算テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (0, 1000, 50, 60, 0, 0, 45),  # EPSがゼロ
            None,  # EPSの別レコード検索結果もなし
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 1
        # expected_perはNoneになるはず
        update_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if call[0][0].strip().startswith("UPDATE holdings")
        ]
        assert len(update_calls) == 1
        # expected_per (最初のパラメータ) がNone
        assert update_calls[0][0][1][0] is None
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_database_error(self, mock_connect):
        """データベースエラー時のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 0
        mock_conn.rollback.assert_called()
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_multiple_holdings_same_code(self, mock_connect):
        """同一銘柄の複数保有（口座別）更新テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (100, 1000, 50, 60, 110, 120, 45),  # ステートメントデータ
        ]
        mock_cursor.rowcount = 3  # 3件更新（複数口座）

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 3
        assert mock_conn.commit.called
        mock_conn.close.assert_called_once()

    def test_get_codes_needing_update_success(self, mock_connect):
        """更新必要銘柄取得の成功テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",), ("5678",), ("9012",)]

        result = IndicatorsManager.get_codes_needing_update(user_id=1)

        assert result == ["1234", "5678", "9012"]
        mock_conn.close.assert_called_once()

    def test_get_codes_needing_update_no_results(self, mock_connect):
        """更新必要銘柄がない場合のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = []

        result = IndicatorsManager.get_codes_needing_update(user_id=1)

        assert result == []
        mock_conn.close.assert_called_once()

    def test_get_codes_needing_update_database_error(self, mock_connect):
        """更新必要銘柄取得時のエラーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        with pytest.raises(sqlite3.Error):
            IndicatorsManager.get_codes_needing_update(user_id=1)

        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_code_conversion(self, mock_connect):
        """銘柄コードの5桁変換テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("123",)]  # 3桁コード
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (100, 1000, 50, 60, 110, 120, 45),  # ステートメントデータ
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 1
        # 5桁変換されていることを確認
        price_query_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "FROM prices" in call[0][0]
        ]
        assert price_query_calls[0][0][1][0] == "12300"  # 5桁に変換
        mock_conn.close.assert_called_once()

    def test_update_stock_indicators_priority_logic(self, mock_connect):
        """EPS・配当の優先順位ロジックテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchall.return_value = [("1234",)]
        mock_cursor.fetchone.side_effect = [
            (1500.0, "2024-01-10"),  # 価格データ
            (100, 1000, 50, 60, None, 120, 45),  # forecast_epsがNull、next_year_epsあり
        ]
        mock_cursor.rowcount = 1

        result = IndicatorsManager.update_stock_indicators(user_id=1)

        assert result == 1
        # expected_epsはnext_year_eps(120)を使用するはず
        update_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if call[0][0].strip().startswith("UPDATE holdings")
        ]
        assert update_calls[0][0][1][4] == 120  # expected_eps
        mock_conn.close.assert_called_once()
