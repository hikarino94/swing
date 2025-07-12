"""FundManagerクラスのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.fund_manager import FundManager


class TestFundManager:
    """FundManagerクラスのテスト"""

    @pytest.fixture
    def mock_connect(self):
        """データベース接続のモック"""
        with patch("src.portfolio.fund_manager.sqlite3.connect") as mock:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock.return_value = mock_conn
            yield mock, mock_conn, mock_cursor

    @pytest.fixture
    def sample_fund_data(self):
        """テスト用投資信託データ"""
        return [
            {
                "fund_name": "日本株ファンドA",
                "quantity": 1000,
                "average_price": 15000,
                "current_price": 16000,
                "market_value": 16000000,
                "profit_loss": 1000000,
                "profit_loss_ratio": 6.67,
                "account_type": "特定",
                "is_fund": True,
            },
            {
                "fund_name": "米国株ファンドB",
                "quantity": 500,
                "average_price": 20000,
                "current_price": 22000,
                "market_value": 11000000,
                "profit_loss": 1000000,
                "profit_loss_ratio": 10.0,
                "account_type": "NISA",
                "is_fund": True,
            },
        ]

    def test_update_funds_from_csv_new_funds(self, mock_connect, sample_fund_data):
        """新規投資信託の追加テスト"""
        _, mock_conn, mock_cursor = mock_connect

        # fund_masterのレコードが存在しない
        mock_cursor.fetchone.side_effect = [None, None, None, None, None, None]
        mock_cursor.lastrowid = 1

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=sample_fund_data
        )

        assert updated == 0
        assert new == 2
        assert mock_conn.commit.call_count >= 2
        # 各ファンドごとに接続が作成されるため
        assert mock_conn.close.call_count == 2

    def test_update_funds_from_csv_existing_funds(self, mock_connect, sample_fund_data):
        """既存投資信託の更新テスト"""
        _, mock_conn, mock_cursor = mock_connect

        # fund_masterに既存レコードがある
        mock_cursor.fetchone.side_effect = [
            (1,),  # fund_id for 日本株ファンドA
            (100,),  # existing fund_holding id
            None,  # no price data
            (2,),  # fund_id for 米国株ファンドB
            (200,),  # existing fund_holding id
            None,  # no price data
        ]

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=sample_fund_data
        )

        assert updated == 2
        assert new == 0
        assert mock_conn.commit.call_count >= 2
        assert mock_conn.close.call_count == 2

    def test_update_funds_from_csv_mixed(self, mock_connect, sample_fund_data):
        """新規と既存の混在テスト"""
        _, mock_conn, mock_cursor = mock_connect

        # 1つ目は既存、2つ目は新規
        mock_cursor.fetchone.side_effect = [
            (1,),  # fund_id for 日本株ファンドA
            (100,),  # existing fund_holding id
            None,  # no price data
            None,  # no fund_id for 米国株ファンドB (new)
            None,  # no existing fund_holding
            None,  # no price data
        ]
        mock_cursor.lastrowid = 2

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=sample_fund_data
        )

        assert updated == 1
        assert new == 1
        assert mock_conn.commit.call_count >= 2
        assert mock_conn.close.call_count == 2

    def test_update_funds_from_csv_with_price_update(
        self, mock_connect, sample_fund_data
    ):
        """基準価額更新も含むテスト"""
        _, mock_conn, mock_cursor = mock_connect

        # fund_masterに既存レコード、価格データもある
        mock_cursor.fetchone.side_effect = [
            (1,),  # fund_id for 日本株ファンドA
            (100,),  # existing fund_holding id
            (15500,),  # existing price data
            (2,),  # fund_id for 米国株ファンドB
            (200,),  # existing fund_holding id
            None,  # no price data (will insert)
        ]

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=sample_fund_data
        )

        assert updated == 2
        assert new == 0
        # 価格更新の確認
        update_calls = [
            call[0][0]
            for call in mock_cursor.execute.call_args_list
            if "UPDATE" in call[0][0]
        ]
        assert any("UPDATE fund_prices" in call for call in update_calls)
        assert mock_conn.close.call_count == 2

    def test_update_funds_from_csv_invalid_data(self, mock_connect):
        """無効なデータのスキップテスト"""
        _, mock_conn, mock_cursor = mock_connect

        invalid_data = [
            {"is_fund": True},  # fund_name missing
            {"fund_name": "", "is_fund": True},  # empty fund_name
            {
                "fund_name": "テストファンド",
                "quantity": 0,
                "is_fund": True,
            },  # zero quantity
            {
                "fund_name": "テストファンド",
                "quantity": None,
                "is_fund": True,
            },  # None quantity
        ]

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=invalid_data
        )

        assert updated == 0
        assert new == 0
        # 無効なデータのため接続が作成されない
        assert mock_conn.close.call_count == 0

    def test_update_funds_from_csv_database_error(self, mock_connect, sample_fund_data):
        """データベースエラー時のテスト"""
        _, mock_conn, mock_cursor = mock_connect

        # データベースエラーを発生させる
        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=sample_fund_data
        )

        assert updated == 0
        assert new == 0
        mock_conn.rollback.assert_called()
        assert mock_conn.close.call_count == 2

    def test_delete_funds_not_in_csv_with_funds(self, mock_connect, sample_fund_data):
        """CSVに含まれない投資信託の削除テスト"""
        _, mock_conn, mock_cursor = mock_connect

        # CSVのファンド名に対応するfund_idを返す
        mock_cursor.fetchall.return_value = [(1,), (2,)]
        mock_cursor.rowcount = 3  # 3件削除

        deleted = FundManager.delete_funds_not_in_csv(
            user_id=1, holdings_data=sample_fund_data, account_name="default"
        )

        assert deleted == 3
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_delete_funds_not_in_csv_no_funds_in_csv(self, mock_connect):
        """CSVに投資信託がない場合の削除テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.rowcount = 5  # 5件削除

        deleted = FundManager.delete_funds_not_in_csv(
            user_id=1, holdings_data=[], account_name="default"
        )

        assert deleted == 5
        # 全削除のクエリが実行されることを確認
        delete_calls = [
            call[0][0]
            for call in mock_cursor.execute.call_args_list
            if "DELETE" in call[0][0]
        ]
        assert any("DELETE FROM fund_holdings" in call for call in delete_calls)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_delete_funds_not_in_csv_database_error(
        self, mock_connect, sample_fund_data
    ):
        """削除時のデータベースエラーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        deleted = FundManager.delete_funds_not_in_csv(
            user_id=1, holdings_data=sample_fund_data, account_name="default"
        )

        assert deleted == 0
        mock_conn.rollback.assert_called()
        mock_conn.close.assert_called_once()

    def test_delete_all_funds_success(self, mock_connect):
        """全投資信託削除の成功テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchone.return_value = (10,)  # 10件の投資信託

        deleted = FundManager.delete_all_funds(user_id=1)

        assert deleted == 10
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_delete_all_funds_database_error(self, mock_connect):
        """全削除時のデータベースエラーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        deleted = FundManager.delete_all_funds(user_id=1)

        assert deleted == 0
        mock_conn.rollback.assert_called()
        mock_conn.close.assert_called_once()

    def test_delete_funds_by_account_success(self, mock_connect):
        """口座別投資信託削除の成功テスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.fetchone.return_value = (7,)  # 7件の投資信託

        deleted = FundManager.delete_funds_by_account(user_id=1, account_name="NISA")

        assert deleted == 7
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_delete_funds_by_account_database_error(self, mock_connect):
        """口座別削除時のデータベースエラーテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mock_cursor.execute.side_effect = sqlite3.Error("Database error")

        deleted = FundManager.delete_funds_by_account(user_id=1, account_name="NISA")

        assert deleted == 0
        mock_conn.rollback.assert_called()
        mock_conn.close.assert_called_once()

    def test_update_funds_filter_non_fund_data(self, mock_connect):
        """投資信託以外のデータがフィルタリングされることのテスト"""
        _, mock_conn, mock_cursor = mock_connect

        mixed_data = [
            {
                "fund_name": "日本株ファンド",
                "quantity": 1000,
                "is_fund": True,
            },
            {
                "code": "1234",
                "name": "株式会社",
                "quantity": 100,
                "is_fund": False,
            },
            {
                "code": "5678",
                "name": "別の株式",
                "quantity": 200,
                # is_fund がない場合はFalse扱い
            },
        ]

        mock_cursor.fetchone.side_effect = [None, None, None]
        mock_cursor.lastrowid = 1

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=mixed_data
        )

        # 投資信託1件のみ処理される
        assert updated == 0
        assert new == 1
        mock_conn.close.assert_called_once()

    def test_update_funds_average_price_none_handling(self, mock_connect):
        """平均取得価額がNoneの場合のハンドリングテスト"""
        _, mock_conn, mock_cursor = mock_connect

        fund_data = [
            {
                "fund_name": "テストファンド",
                "quantity": 1000,
                "average_price": None,  # None の場合
                "is_fund": True,
            }
        ]

        mock_cursor.fetchone.side_effect = [None, None, None]
        mock_cursor.lastrowid = 1

        updated, new = FundManager.update_funds_from_csv(
            user_id=1, holdings_data=fund_data
        )

        assert new == 1
        # execute呼び出しを確認して、average_priceが0になっていることを確認
        insert_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if call[0][0].strip().startswith("INSERT INTO fund_holdings")
        ]
        assert len(insert_calls) == 1
        # average_price は6番目のパラメータ（0ベースで5）
        assert insert_calls[0][0][1][5] == 0
        mock_conn.close.assert_called_once()
