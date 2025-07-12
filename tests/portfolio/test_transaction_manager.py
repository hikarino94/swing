"""src.portfolio.transaction_manager モジュールのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio.transaction_manager import TransactionManager


class TestTransactionManagerImportTransactionsFromCSV:
    """TransactionManager.import_transactions_from_csv のテスト"""

    @patch("src.portfolio.transaction_manager.Transaction.bulk_insert")
    def test_import_transactions_success(self, mock_bulk_insert):
        """取引履歴のインポート成功テスト"""
        # モックの設定
        mock_bulk_insert.return_value = 3  # 3件インポート成功

        # テストデータ
        transactions_data = [
            {
                "code": "1234",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 1000.0,
                "commission": 100,
            },
            {
                "code": "1234",
                "transaction_date": "2024-01-15",
                "transaction_type": "sell",
                "quantity": 50,
                "price": 1200.0,
                "commission": 100,
            },
            {
                "code": "5678",
                "transaction_date": "2024-01-20",
                "transaction_type": "buy",
                "quantity": 200,
                "price": 2000.0,
                "commission": 200,
            },
        ]

        # 実行
        imported_count = TransactionManager.import_transactions_from_csv(
            user_id=1, transactions_data=transactions_data
        )

        # 検証
        assert imported_count == 3
        # user_idが各取引データに追加されていることを確認
        assert all(trans["user_id"] == 1 for trans in transactions_data)
        mock_bulk_insert.assert_called_once_with(transactions_data)

    @patch("src.portfolio.transaction_manager.Transaction.bulk_insert")
    def test_import_transactions_empty_data(self, mock_bulk_insert):
        """空データのインポートテスト"""
        mock_bulk_insert.return_value = 0

        # 空のリスト
        imported_count = TransactionManager.import_transactions_from_csv(
            user_id=1, transactions_data=[]
        )

        # 検証
        assert imported_count == 0
        mock_bulk_insert.assert_called_once_with([])

    @patch("src.portfolio.transaction_manager.Transaction.bulk_insert")
    def test_import_transactions_database_error(self, mock_bulk_insert):
        """データベースエラーのテスト"""
        # エラーを発生させる
        mock_bulk_insert.side_effect = sqlite3.Error("Database error")

        transactions_data = [
            {
                "code": "1234",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 1000.0,
            }
        ]

        # エラーが発生した場合
        with pytest.raises(sqlite3.Error):
            TransactionManager.import_transactions_from_csv(
                user_id=1, transactions_data=transactions_data
            )

    @patch("src.portfolio.transaction_manager.Transaction.bulk_insert")
    def test_import_transactions_no_holding_recalculation(self, mock_bulk_insert):
        """保有銘柄の再計算が行われないことの確認テスト"""
        mock_bulk_insert.return_value = 5

        transactions_data = [
            {
                "code": "1234",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 1000.0,
            }
        ]

        # recalculate_holdings がパッチされていないことを確認
        # （コメントアウトされているため呼ばれない）
        with patch.object(
            TransactionManager, "recalculate_holdings"
        ) as mock_recalculate:
            TransactionManager.import_transactions_from_csv(
                user_id=1, transactions_data=transactions_data
            )
            # recalculate_holdings が呼ばれていないことを確認
            mock_recalculate.assert_not_called()


class TestTransactionManagerRecalculateHoldings:
    """TransactionManager.recalculate_holdings のテスト"""

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    @patch("src.portfolio.transaction_manager.Holding")
    def test_recalculate_holdings_single_stock(self, mock_holding_class, mock_connect):
        """単一銘柄の保有再計算テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # 銘柄コード一覧
        mock_cursor.fetchall.side_effect = [
            [("1234",)],  # 銘柄コード
            [  # 取引履歴
                (1, "2024-01-01", "buy", 100, 1000.0, 100),
                (2, "2024-01-15", "buy", 50, 1100.0, 50),
                (3, "2024-02-01", "sell", 30, 1200.0, 30),
            ],
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 既存の保有銘柄モック
        mock_holding = MagicMock()
        mock_holding_class.find_by_user_and_code.return_value = mock_holding

        # 実行
        TransactionManager.recalculate_holdings(user_id=1)

        # 検証
        # 平均法での計算確認
        # 買付1: 100株 × 1000円 + 手数料100円 = 100,100円
        # 買付2: 50株 × 1100円 + 手数料50円 = 55,050円
        # 合計: 150株、155,150円（平均単価: 1034.33円）
        # 売却: 30株売却後、120株保有
        assert mock_holding.quantity == 120
        assert mock_holding.average_price == pytest.approx(1034.33, rel=1e-2)

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    @patch("src.portfolio.transaction_manager.Holding")
    def test_recalculate_holdings_all_sold(self, mock_holding_class, mock_connect):
        """全株売却のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("5678",)],  # 銘柄コード
            [  # 取引履歴
                (1, "2024-01-01", "buy", 100, 2000.0, 200),
                (2, "2024-02-01", "sell", 100, 2500.0, 250),
            ],
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 既存の保有銘柄モック
        mock_holding = MagicMock()
        mock_holding.id = 1
        mock_holding_class.find_by_user_and_code.return_value = mock_holding

        # 実行
        TransactionManager.recalculate_holdings(user_id=1)

        # 検証 - 全株売却の場合、保有数量が0になるのでDELETE SQLが実行される
        # holding.save() は呼ばれない
        mock_holding.save.assert_not_called()
        # DELETE SQL が実行されることを確認
        delete_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "DELETE FROM holdings" in call[0][0]
        ]
        assert len(delete_calls) == 1

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    @patch("src.portfolio.transaction_manager.Holding")
    def test_recalculate_holdings_new_holding(self, mock_holding_class, mock_connect):
        """新規保有銘柄の作成テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("9999",)],  # 銘柄コード
            [  # 取引履歴（買付のみ）
                (1, "2024-01-01", "buy", 300, 3000.0, 300),
            ],
        ]
        # 最新株価取得のために追加
        mock_cursor.fetchone.return_value = None  # 株価データなし
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 既存の保有銘柄なし
        mock_holding_class.find_by_user_and_code.return_value = None

        # 新規作成用のモック
        new_holding = MagicMock()
        mock_holding_class.return_value = new_holding

        # 実行
        TransactionManager.recalculate_holdings(user_id=1)

        # 検証 - 新規保有銘柄が作成される（account_name/account_typeパラメータなし）
        mock_holding_class.assert_called_with(user_id=1, code="9999")
        assert new_holding.quantity == 300
        assert new_holding.average_price == pytest.approx(3001.0, rel=1e-2)
        new_holding.save.assert_called_once()

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    def test_recalculate_holdings_no_transactions(self, mock_connect):
        """取引履歴がない場合のテスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []  # 取引なし
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        TransactionManager.recalculate_holdings(user_id=1)

        # 検証 - 銘柄取得とDELETEクエリが実行される
        assert mock_cursor.execute.call_count >= 2  # 銘柄取得のクエリとDELETEクエリ

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    @patch("src.portfolio.transaction_manager.Holding")
    def test_recalculate_holdings_multiple_stocks(
        self, mock_holding_class, mock_connect
    ):
        """複数銘柄の保有再計算テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("1111",), ("2222",)],  # 2銘柄
            # 銘柄1111の取引
            [
                (1, "2024-01-01", "buy", 100, 1000.0, 100),
            ],
            # 銘柄2222の取引
            [
                (2, "2024-01-02", "buy", 200, 2000.0, 200),
            ],
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # モックの保有銘柄
        holdings = [MagicMock(), MagicMock()]
        mock_holding_class.find_by_user_and_code.side_effect = holdings

        # 実行
        TransactionManager.recalculate_holdings(user_id=1)

        # 検証 - 2銘柄とも処理される
        assert holdings[0].quantity == 100
        assert holdings[0].average_price == pytest.approx(1001.0, rel=1e-2)
        assert holdings[1].quantity == 200
        assert holdings[1].average_price == pytest.approx(2001.0, rel=1e-2)

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    def test_recalculate_holdings_database_error(self, mock_connect):
        """データベースエラーのハンドリングテスト"""
        # エラーを発生させる
        mock_connect.side_effect = sqlite3.Error("Database error")

        # エラーが発生してもクラッシュしないことを確認
        with pytest.raises(sqlite3.Error):
            TransactionManager.recalculate_holdings(user_id=1)

    @patch("src.portfolio.transaction_manager.sqlite3.connect")
    @patch("src.portfolio.transaction_manager.Holding")
    def test_recalculate_holdings_sell_more_than_owned(
        self, mock_holding_class, mock_connect
    ):
        """保有数以上の売却処理テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("7777",)],  # 銘柄コード
            [  # 取引履歴（売却数が購入数を超える）
                (1, "2024-01-01", "buy", 100, 1000.0, 100),
                (2, "2024-02-01", "sell", 150, 1200.0, 150),  # 150株売却（保有は100株）
            ],
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 既存の保有銘柄モック
        mock_holding = MagicMock()
        mock_holding.id = 1
        mock_holding_class.find_by_user_and_code.return_value = mock_holding

        # 実行
        TransactionManager.recalculate_holdings(user_id=1)

        # 検証 - 保有数が0になる場合、保存されずDELETE SQLが実行される
        mock_holding.save.assert_not_called()
        # DELETE SQL が実行されることを確認
        delete_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "DELETE FROM holdings" in call[0][0]
        ]
        assert len(delete_calls) == 1
