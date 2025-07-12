"""src.portfolio.holdings_manager モジュールのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.holdings_manager import HoldingsManager
from src.portfolio.models import Holding


class TestHoldingsManagerUpdateHoldingsFromCSV:
    """HoldingsManager.update_holdings_from_csv のテスト"""

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    @patch("src.portfolio.holdings_manager.Holding.find_by_user_code_and_account")
    @patch("src.portfolio.models.holding.Holding.save")
    def test_update_holdings_standard_format_new(
        self, mock_save, mock_find_holding, mock_connect
    ):
        """標準形式CSV - 新規銘柄追加のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # 既存データなし
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_find_holding.return_value = None
        mock_save.return_value = True

        # テストデータ（標準形式）
        holdings_data = [
            {
                "code": "1234",
                "quantity": 100,
                "average_price": 1000.0,
                "expected_per": 15.5,
                "actual_pbr": 1.2,
                "dividend_yield": 2.5,
                "expected_eps": 64.5,
                "actual_bps": 833.3,
                "expected_dividend": 25.0,
                "lending_type": None,
                "is_fund": False,
                "account_type": "特定",
            }
        ]

        # 実行
        updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証
        assert updated == 0
        assert new == 1
        assert is_standard is True
        mock_save.assert_called_once()

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    @patch("src.portfolio.holdings_manager.Holding.find_by_user_code_and_account")
    @patch("src.portfolio.models.holding.Holding.save")
    def test_update_holdings_standard_format_existing(
        self, mock_save, mock_find_holding, mock_connect
    ):
        """標準形式CSV - 既存銘柄更新のテスト"""
        # 既存の保有銘柄をモック
        existing_holding = MagicMock(spec=Holding)
        existing_holding.id = 1
        existing_holding.quantity = 50
        existing_holding.average_price = 900.0

        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            1,
            None,
        )  # 既存データあり、削除されていない
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_find_holding.return_value = existing_holding
        mock_save.return_value = True

        # テストデータ（標準形式）
        holdings_data = [
            {
                "code": "1234",
                "quantity": 100,
                "average_price": 1000.0,
                "expected_per": 15.5,
                "actual_pbr": 1.2,
                "dividend_yield": 2.5,
                "is_fund": False,
                "account_type": "特定",
            }
        ]

        # 実行
        updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証
        assert updated == 1
        assert new == 0
        assert is_standard is True
        # 既存の保有銘柄が更新されたことを確認
        assert existing_holding.quantity == 100
        assert existing_holding.average_price == 1000.0
        assert existing_holding.expected_per == 15.5
        existing_holding.save.assert_called_once()

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    @patch("src.portfolio.holdings_manager.Holding")
    def test_update_holdings_savefile_format_new(
        self, mock_holding_class, mock_connect
    ):
        """SaveFile形式CSV - 新規銘柄追加のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # 既存データなし
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_holding = MagicMock(spec=Holding)
        mock_holding.save.return_value = True
        mock_holding_class.return_value = mock_holding
        mock_holding_class.find_by_user_code_and_account.return_value = None

        # テストデータ（SaveFile形式 - PER等の指標なし）
        holdings_data = [
            {
                "code": "5678",
                "quantity": 200,
                "average_price": 2000.0,
                "market_value": 440000,
                "profit_loss": 40000,
                "profit_loss_ratio": 10.0,
                "is_fund": False,
                "account_type": "NISA",
            }
        ]

        # 実行
        updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証
        assert updated == 0
        assert new == 1
        assert is_standard is False
        # SaveFile形式ではPER等はNoneが設定される
        assert mock_holding.expected_per is None
        assert mock_holding.actual_pbr is None
        assert mock_holding.dividend_yield is None
        mock_holding.save.assert_called_once()

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    def test_update_holdings_deleted_record_recovery(self, mock_connect):
        """論理削除された銘柄の復活テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # 論理削除されたレコードが存在
        mock_cursor.fetchone.return_value = (1, "2024-01-01 00:00:00")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # テストデータ
        holdings_data = [
            {
                "code": "9999",
                "quantity": 300,
                "average_price": 3000.0,
                "is_fund": False,
                "account_type": "特定",
            }
        ]

        # 実行
        with patch("src.portfolio.models.holding.Holding.save", return_value=True):
            updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
                user_id=1, holdings_data=holdings_data, account_name="default"
            )

        # 検証 - 論理削除を解除するUPDATE文が実行されたか
        mock_cursor.execute.assert_any_call(
            """
                        UPDATE holdings
                        SET deleted_at = NULL, updated_at = datetime('now')
                        WHERE id = ?
                        """,
            (1,),
        )
        mock_conn.commit.assert_called()

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    @patch("src.portfolio.holdings_manager.Holding")
    def test_update_holdings_filter_fund_data(self, mock_holding_class, mock_connect):
        """投資信託データのフィルタリングテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # 既存データなし
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_holding = MagicMock()
        mock_holding.save.return_value = True
        mock_holding_class.return_value = mock_holding
        mock_holding_class.find_by_user_code_and_account.return_value = None

        # テストデータ（投資信託を含む）
        holdings_data = [
            {
                "code": "1234",
                "quantity": 100,
                "is_fund": False,  # 株式
                "account_type": "特定",
            },
            {
                "code": "FUND001",
                "quantity": 10000,
                "is_fund": True,  # 投資信託
                "account_type": "特定",
            },
        ]

        # 実行
        updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証 - 株式1件のみ処理される
        assert new == 1
        assert updated == 0
        # Holdingクラスは1回だけ呼ばれる（株式分のみ）
        assert mock_holding_class.call_count == 1

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    @patch("src.portfolio.holdings_manager.Holding.find_by_user_code_and_account")
    @patch("src.portfolio.models.holding.Holding.save")
    def test_update_holdings_savefile_format_preserve_existing_values(
        self, mock_save, mock_find_holding, mock_connect
    ):
        """SaveFile形式 - 既存値の保持テスト"""
        # 既存の保有銘柄（PER等の値あり）
        existing_holding = MagicMock(spec=Holding)
        existing_holding.id = 1
        existing_holding.quantity = 100
        existing_holding.average_price = 1000.0
        existing_holding.expected_per = 20.0  # 既存値
        existing_holding.actual_pbr = 1.5  # 既存値
        existing_holding.dividend_yield = 3.0  # 既存値
        existing_holding.expected_dividend = 50.0  # 既存値

        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_find_holding.return_value = existing_holding
        mock_save.return_value = True

        # テストデータ（SaveFile形式）
        holdings_data = [
            {
                "code": "1234",
                "quantity": 150,  # 数量更新
                "average_price": 1100.0,  # 単価更新
                "is_fund": False,
                "account_type": "特定",
            }
        ]

        # 実行
        HoldingsManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証 - 数量と単価は更新、PER等は既存値を保持
        assert existing_holding.quantity == 150
        assert existing_holding.average_price == 1100.0
        assert existing_holding.expected_per == 20.0  # 保持
        assert existing_holding.actual_pbr == 1.5  # 保持
        assert existing_holding.dividend_yield == 3.0  # 保持

    def test_update_holdings_empty_data(self):
        """空データの処理テスト"""
        with patch("src.portfolio.holdings_manager.sqlite3.connect"):
            # 空のリスト
            updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
                user_id=1, holdings_data=[], account_name="default"
            )

            # 検証
            assert updated == 0
            assert new == 0
            assert is_standard is False  # デフォルト値

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    def test_update_holdings_database_error_handling(self, mock_connect):
        """データベースエラーのハンドリングテスト"""
        # エラーを発生させる設定
        mock_connect.side_effect = sqlite3.Error("Database error")

        holdings_data = [
            {"code": "1234", "quantity": 100, "is_fund": False, "account_type": "特定"}
        ]

        # エラーが発生してもクラッシュしないことを確認
        with pytest.raises(sqlite3.Error):
            HoldingsManager.update_holdings_from_csv(
                user_id=1, holdings_data=holdings_data, account_name="default"
            )

    @patch("src.portfolio.holdings_manager.sqlite3.connect")
    @patch("src.portfolio.holdings_manager.Holding")
    def test_update_holdings_multiple_account_types(
        self, mock_holding_class, mock_connect
    ):
        """複数の口座タイプの処理テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        mock_holdings = []
        for _ in range(3):
            mock_holding = MagicMock(spec=Holding)
            mock_holding.save.return_value = True
            mock_holdings.append(mock_holding)

        mock_holding_class.side_effect = mock_holdings
        mock_holding_class.find_by_user_code_and_account.return_value = None

        # テストデータ（異なる口座タイプ）
        holdings_data = [
            {
                "code": "1234",
                "quantity": 100,
                "average_price": 1000.0,
                "is_fund": False,
                "account_type": "特定",
            },
            {
                "code": "1234",
                "quantity": 200,
                "average_price": 1000.0,
                "is_fund": False,
                "account_type": "NISA",
            },
            {
                "code": "5678",
                "quantity": 300,
                "average_price": 2000.0,
                "is_fund": False,
                "account_type": "特定",
            },
        ]

        # 実行
        updated, new, is_standard = HoldingsManager.update_holdings_from_csv(
            user_id=1, holdings_data=holdings_data, account_name="default"
        )

        # 検証
        assert new == 3  # 3件とも新規
        assert updated == 0
        # 3つの異なる保有銘柄が作成されたことを確認
        assert mock_holding_class.call_count == 3
