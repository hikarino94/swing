"""src.portfolio.models.transaction モジュールのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

from src.portfolio.models.transaction import Transaction


class TestTransactionInitialization:
    """Transaction クラスの初期化テスト"""

    def test_init_with_required_params(self):
        """必須パラメータでの初期化テスト"""
        transaction = Transaction(
            user_id=1,
            code="1234",
            transaction_date="2024-01-01",
            transaction_type="buy",
            quantity=100,
            price=1000.0,
        )

        assert transaction.id is None
        assert transaction.user_id == 1
        assert transaction.code == "1234"
        assert transaction.transaction_date == "2024-01-01"
        assert transaction.transaction_type == "buy"
        assert transaction.quantity == 100
        assert transaction.price == 1000.0
        assert transaction.commission == 0.0
        assert transaction.tax == 0.0
        assert transaction.total_amount == 100000.0  # quantity * price
        assert transaction.realized_profit is None
        assert transaction.remarks == ""
        assert transaction.detailed_type == ""


class TestTransactionFindMethods:
    """検索系メソッドのテスト"""

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_find_all_by_user_all_params(self, mock_connect):
        """全パラメータ指定での検索テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                1,
                10,
                "1234",
                "2024-01-01",
                "buy",
                100,
                1000.0,
                100,
                0,
                100100,
                "備考",
                "2024-01-01 10:00:00",
                "テスト会社",
                "新規買い",
                None,
            ),
            (
                2,
                10,
                "1234",
                "2024-01-15",
                "sell",
                50,
                1200.0,
                100,
                1000,
                61100,
                "",
                "2024-01-15 15:00:00",
                "テスト会社",
                "決済売り",
                10000.0,
            ),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        transactions = Transaction.find_all_by_user(
            user_id=10, code="1234", start_date="2024-01-01", end_date="2024-01-31"
        )

        # 検証
        assert len(transactions) == 2
        assert transactions[0].id == 1
        assert transactions[0].transaction_type == "buy"
        assert transactions[0].company_name == "テスト会社"
        assert transactions[0].detailed_type == "新規買い"
        assert transactions[1].id == 2
        assert transactions[1].transaction_type == "sell"
        assert transactions[1].realized_profit == 10000.0

        # SQLのパラメータを確認
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == [10, "1234", "2024-01-01", "2024-01-31"]

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_find_all_by_user_minimal_params(self, mock_connect):
        """最小パラメータでの検索テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        transactions = Transaction.find_all_by_user(user_id=10)

        # 検証
        assert transactions == []
        # SQLのパラメータを確認（user_idのみ）
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == [10]

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_find_by_id_found(self, mock_connect):
        """IDでの検索テスト（見つかる場合）"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            5,
            10,
            "5678",
            "2024-02-01",
            "buy",
            200,
            2000.0,
            200,
            0,
            400200,
            "メモ",
            "2024-02-01 09:00:00",
            "別会社",
            "新規買い",
            None,
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        transaction = Transaction.find_by_id(user_id=10, transaction_id=5)

        # 検証
        assert transaction is not None
        assert transaction.id == 5
        assert transaction.user_id == 10
        assert transaction.code == "5678"
        assert transaction.quantity == 200
        assert transaction.price == 2000.0
        assert transaction.remarks == "メモ"
        assert transaction.company_name == "別会社"

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_find_by_id_not_found(self, mock_connect):
        """IDでの検索テスト（見つからない場合）"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        transaction = Transaction.find_by_id(user_id=10, transaction_id=999)

        # 検証
        assert transaction is None


class TestTransactionSave:
    """save メソッドのテスト"""

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_save_new_transaction(self, mock_connect):
        """新規取引の保存テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 10
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 新規取引
        transaction = Transaction(
            user_id=1,
            code="1234",
            transaction_date="2024-01-01",
            transaction_type="buy",
            quantity=100,
            price=1000.0,
        )
        transaction.commission = 100
        transaction.remarks = "テスト取引"

        # 実行
        result = transaction.save()

        # 検証
        assert result is True
        assert transaction.id == 10
        # INSERT文が実行されたことを確認
        insert_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "INSERT INTO transactions" in call[0][0]
        ]
        assert len(insert_calls) == 1

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_save_existing_transaction(self, mock_connect):
        """既存取引の更新テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 既存取引
        transaction = Transaction(
            user_id=1,
            code="1234",
            transaction_date="2024-01-01",
            transaction_type="buy",
            quantity=100,
            price=1000.0,
        )
        transaction.id = 5  # 既存ID
        transaction.quantity = 150  # 数量を変更

        # 実行
        result = transaction.save()

        # 検証
        assert result is True
        # UPDATE文が実行されたことを確認
        update_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "UPDATE transactions" in call[0][0]
        ]
        assert len(update_calls) == 1
        # パラメータを確認
        update_params = update_calls[0][0][1]
        assert update_params[3] == 150  # quantity
        assert update_params[-2] == 5  # id
        assert update_params[-1] == 1  # user_id

    def test_save_database_error(self):
        """データベースエラー時のテスト"""
        transaction = Transaction(
            user_id=1,
            code="1234",
            transaction_date="2024-01-01",
            transaction_type="buy",
            quantity=100,
            price=1000.0,
        )

        # saveメソッド内でエラーが発生するようにパッチ
        with patch("src.portfolio.models.transaction.sqlite3.connect") as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = sqlite3.Error("Database error")
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            # 実行
            result = transaction.save()

            # 検証
            assert result is False


class TestTransactionBulkInsert:
    """bulk_insert メソッドのテスト"""

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_bulk_insert_success(self, mock_connect):
        """一括挿入成功テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # テストデータ
        transactions_data = [
            {
                "user_id": 1,
                "code": "1234",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 1000.0,
                "total_amount": 100000.0,
                "commission": 100,
                "tax": 0,
                "remarks": "買付1",
            },
            {
                "user_id": 1,
                "code": "5678",
                "transaction_date": "2024-01-02",
                "transaction_type": "buy",
                "quantity": 200,
                "price": 2000.0,
                "total_amount": 400000.0,
                "commission": 200,
                "detailed_type": "新規買い",
                "realized_profit": None,
            },
            {
                "user_id": 1,
                "code": "1234",
                "transaction_date": "2024-01-10",
                "transaction_type": "sell",
                "quantity": 50,
                "price": 1200.0,
                "total_amount": 60000.0,
                "commission": 100,
                "tax": 1000,
                "detailed_type": "決済売り",
                "realized_profit": 10000.0,
            },
        ]

        # 実行
        result = Transaction.bulk_insert(transactions_data)

        # 検証
        assert result == 3
        assert mock_cursor.execute.call_count == 3
        mock_conn.commit.assert_called_once()

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_bulk_insert_partial_failure(self, mock_connect):
        """一部失敗する一括挿入テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # 2回目の挿入でエラーを発生させる
        mock_cursor.execute.side_effect = [None, sqlite3.Error("Duplicate entry"), None]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # テストデータ
        transactions_data = [
            {
                "user_id": 1,
                "code": "1234",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 1000.0,
                "total_amount": 100000.0,
            },
            {  # このデータでエラーが発生
                "user_id": 1,
                "code": "5678",
                "transaction_date": "2024-01-02",
                "transaction_type": "buy",
                "quantity": 200,
                "price": 2000.0,
                "total_amount": 400000.0,
            },
            {
                "user_id": 1,
                "code": "9999",
                "transaction_date": "2024-01-03",
                "transaction_type": "buy",
                "quantity": 300,
                "price": 3000.0,
                "total_amount": 900000.0,
            },
        ]

        # 実行
        result = Transaction.bulk_insert(transactions_data)

        # 検証 - エラーが発生した1件を除いて2件が挿入される
        assert result == 2
        assert mock_cursor.execute.call_count == 3
        mock_conn.commit.assert_called_once()

    @patch("src.portfolio.models.transaction.sqlite3.connect")
    def test_bulk_insert_empty_data(self, mock_connect):
        """空データの一括挿入テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        result = Transaction.bulk_insert([])

        # 検証
        assert result == 0
        mock_cursor.execute.assert_not_called()
        mock_conn.commit.assert_called_once()

    def test_bulk_insert_database_error(self):
        """データベースエラー時のテスト"""
        transactions_data = [
            {
                "user_id": 1,
                "code": "1234",
                "transaction_date": "2024-01-01",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 1000.0,
                "total_amount": 100000.0,
            }
        ]

        # bulk_insertメソッド内でエラーが発生するようにパッチ
        with patch("src.portfolio.models.transaction.sqlite3.connect") as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = sqlite3.Error("Database error")
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            # エラーが発生してもクラッシュしないことを確認
            result = Transaction.bulk_insert(transactions_data)

            # 検証
            assert result == 0
