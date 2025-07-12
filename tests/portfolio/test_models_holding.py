"""src.portfolio.models.holding モジュールのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

from src.portfolio.models.holding import Holding


class TestHoldingInitialization:
    """Holding クラスの初期化テスト"""

    def test_init_with_minimum_params(self):
        """最小パラメータでの初期化テスト"""
        holding = Holding(user_id=1, code="1234")

        assert holding.user_id == 1
        assert holding.code == "1234"
        assert holding.account_name == "default"
        assert holding.account_type == "特定"
        assert holding.quantity == 0
        assert holding.average_price == 0.0
        assert holding.id is None

    def test_init_with_all_params(self):
        """全パラメータでの初期化テスト"""
        holding = Holding(
            user_id=1,
            code="5678",
            account_name="NISA",
            account_type="NISA",
            id=10,
            quantity=100,
            average_price=1000.0,
            market_value=110000.0,
            profit_loss=10000.0,
            profit_loss_ratio=10.0,
            expected_per=15.5,
            actual_pbr=1.2,
            dividend_yield=2.5,
            company_name="テスト会社",
        )

        assert holding.id == 10
        assert holding.user_id == 1
        assert holding.code == "5678"
        assert holding.account_name == "NISA"
        assert holding.account_type == "NISA"
        assert holding.quantity == 100
        assert holding.average_price == 1000.0
        assert holding.market_value == 110000.0
        assert holding.profit_loss == 10000.0
        assert holding.profit_loss_ratio == 10.0
        assert holding.expected_per == 15.5
        assert holding.actual_pbr == 1.2
        assert holding.dividend_yield == 2.5
        assert holding.company_name == "テスト会社"


class TestHoldingFromDbRow:
    """from_db_row メソッドのテスト"""

    def test_from_db_row_with_full_data(self):
        """完全なデータからの作成テスト"""
        row = (
            1,  # id
            10,  # user_id
            "1234",  # code
            "default",  # account_name
            "特定",  # account_type
            100,  # quantity
            1000.0,  # average_price
            110000.0,  # market_value
            10000.0,  # profit_loss
            10.0,  # profit_loss_ratio
            "2024-01-01 00:00:00",  # updated_at
            15.5,  # expected_per
            1.2,  # actual_pbr
            2.5,  # dividend_yield
            65.0,  # expected_eps
            833.0,  # actual_bps
            25.0,  # expected_dividend
            "一般",  # lending_type
        )

        description = [
            ("id",),
            ("user_id",),
            ("code",),
            ("account_name",),
            ("account_type",),
            ("quantity",),
            ("average_price",),
            ("market_value",),
            ("profit_loss",),
            ("profit_loss_ratio",),
            ("updated_at",),
            ("expected_per",),
            ("actual_pbr",),
            ("dividend_yield",),
            ("expected_eps",),
            ("actual_bps",),
            ("expected_dividend",),
            ("lending_type",),
        ]

        holding = Holding.from_db_row(row, description)

        assert holding.id == 1
        assert holding.user_id == 10
        assert holding.code == "1234"
        assert holding.account_name == "default"
        assert holding.account_type == "特定"
        assert holding.quantity == 100
        assert holding.average_price == 1000.0
        assert holding.expected_per == 15.5


class TestHoldingFindMethods:
    """検索系メソッドのテスト"""

    @patch("src.portfolio.models.holding.sqlite3.connect")
    def test_find_by_user_and_code(self, mock_connect):
        """ユーザーIDと銘柄コードでの検索テスト（後方互換性）"""
        # find_by_user_code_and_accountを呼び出すことを確認
        with patch.object(Holding, "find_by_user_code_and_account") as mock_find:
            mock_find.return_value = MagicMock(spec=Holding)

            result = Holding.find_by_user_and_code(user_id=1, code="1234")

            mock_find.assert_called_once_with(1, "1234", "default")
            assert result == mock_find.return_value

    @patch("src.portfolio.models.holding.sqlite3.connect")
    def test_find_by_user_code_and_account_found(self, mock_connect):
        """ユーザーID、銘柄コード、口座での検索テスト（見つかる場合）"""
        # モックの設定
        mock_cursor = MagicMock()
        # PRAGMA table_info の返り値: (cid, name, type, notnull, dflt_value, pk)
        mock_cursor.fetchall.return_value = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "user_id", "INTEGER", 1, None, 0),
            (2, "code", "TEXT", 1, None, 0),
            (3, "account_name", "TEXT", 1, None, 0),
            (4, "account_type", "TEXT", 0, None, 0),
        ]
        mock_cursor.fetchone.return_value = (
            1,
            10,
            "1234",
            "default",
            "特定",
            100,
            1000.0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        holding = Holding.find_by_user_code_and_account(
            user_id=10, code="1234", account_name="default", account_type="特定"
        )

        # 検証
        assert holding is not None
        assert holding.id == 1
        assert holding.user_id == 10
        assert holding.code == "1234"
        assert holding.account_type == "特定"

    @patch("src.portfolio.models.holding.sqlite3.connect")
    def test_find_by_user_code_and_account_not_found(self, mock_connect):
        """ユーザーID、銘柄コード、口座での検索テスト（見つからない場合）"""
        # モックの設定
        mock_cursor = MagicMock()
        # PRAGMA table_info の返り値
        mock_cursor.fetchall.return_value = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "user_id", "INTEGER", 1, None, 0),
            (2, "code", "TEXT", 1, None, 0),
            (3, "account_name", "TEXT", 1, None, 0),
            (4, "account_type", "TEXT", 0, None, 0),
        ]
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        holding = Holding.find_by_user_code_and_account(
            user_id=10, code="9999", account_name="default"
        )

        # 検証
        assert holding is None

    @patch("src.portfolio.models.holding.sqlite3.connect")
    def test_find_all_by_user(self, mock_connect):
        """ユーザーの全保有銘柄取得テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # カラム情報
        mock_cursor.fetchall.side_effect = [
            # PRAGMA table_info の返り値
            [
                (0, "id", "INTEGER", 0, None, 1),
                (1, "user_id", "INTEGER", 1, None, 0),
                (2, "code", "TEXT", 1, None, 0),
                (3, "account_name", "TEXT", 1, None, 0),
                (4, "account_type", "TEXT", 0, None, 0),
            ],
            [  # 保有銘柄データ
                (
                    1,
                    10,
                    "1234",
                    "default",
                    "特定",
                    100,
                    1000.0,
                    110000.0,
                    10000.0,
                    10.0,
                    None,
                    15.5,
                    1.2,
                    2.5,
                    None,
                    None,
                    None,
                    None,
                    "テスト会社",
                ),
                (
                    2,
                    10,
                    "5678",
                    "NISA",
                    "NISA",
                    200,
                    2000.0,
                    420000.0,
                    20000.0,
                    5.0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
            ],
        ]
        mock_cursor.fetchone.return_value = ("listed_info",)  # listed_infoテーブルあり
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 実行
        holdings = Holding.find_all_by_user(user_id=10)

        # 検証
        assert len(holdings) == 2
        assert holdings[0].code == "1234"
        assert holdings[0].account_type == "特定"
        assert holdings[0].company_name == "テスト会社"
        assert holdings[1].code == "5678"
        assert holdings[1].account_type == "NISA"


class TestHoldingSave:
    """save メソッドのテスト"""

    @patch("src.portfolio.models.holding.sqlite3.connect")
    def test_save_new_holding(self, mock_connect):
        """新規保有銘柄の保存テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # PRAGMA table_info の返り値
        mock_cursor.fetchall.return_value = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "user_id", "INTEGER", 1, None, 0),
            (2, "code", "TEXT", 1, None, 0),
            (3, "account_name", "TEXT", 1, None, 0),
            (4, "account_type", "TEXT", 0, None, 0),
        ]
        mock_cursor.lastrowid = 123
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # find_by_user_code_and_accountがNoneを返すようにモック
        with patch.object(Holding, "find_by_user_code_and_account", return_value=None):
            # 新規保有銘柄
            holding = Holding(
                user_id=1, code="1234", quantity=100, average_price=1000.0
            )

            # 実行
            result = holding.save()

            # 検証
            assert result is True
            assert holding.id == 123
            # INSERT文が実行されたことを確認
            insert_calls = [
                call
                for call in mock_cursor.execute.call_args_list
                if "INSERT INTO holdings" in call[0][0]
            ]
            assert len(insert_calls) == 1

    @patch("src.portfolio.models.holding.sqlite3.connect")
    def test_save_existing_holding(self, mock_connect):
        """既存保有銘柄の更新テスト"""
        # モックの設定
        mock_cursor = MagicMock()
        # PRAGMA table_info の返り値
        mock_cursor.fetchall.return_value = [
            (0, "id", "INTEGER", 0, None, 1),
            (1, "user_id", "INTEGER", 1, None, 0),
            (2, "code", "TEXT", 1, None, 0),
            (3, "account_name", "TEXT", 1, None, 0),
            (4, "account_type", "TEXT", 0, None, 0),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # 既存の保有銘柄をモック
        existing = MagicMock(spec=Holding)
        existing.id = 100

        with patch.object(
            Holding, "find_by_user_code_and_account", return_value=existing
        ):
            # 保有銘柄
            holding = Holding(
                user_id=1, code="1234", quantity=150, average_price=1100.0
            )

            # 実行
            result = holding.save()

            # 検証
            assert result is True
            assert holding.id == 100  # 既存のIDが設定される
            # UPDATE文が実行されたことを確認
            update_calls = [
                call
                for call in mock_cursor.execute.call_args_list
                if "UPDATE holdings" in call[0][0]
            ]
            assert len(update_calls) == 1

    def test_save_database_error(self):
        """データベースエラー時のテスト"""
        holding = Holding(user_id=1, code="1234")

        # saveメソッド内でエラーが発生するようにパッチ
        with patch("src.portfolio.models.holding.sqlite3.connect") as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = sqlite3.Error("Database error")
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            # 実行
            result = holding.save()

            # 検証
            assert result is False


class TestHoldingUpdateMarketValue:
    """update_market_value メソッドのテスト"""

    def test_update_market_value_normal(self):
        """通常の時価評価更新テスト"""
        holding = Holding(user_id=1, code="1234", quantity=100, average_price=1000.0)

        # 実行
        holding.update_market_value(1100.0)

        # 検証
        assert holding.market_value == 110000.0
        assert holding.profit_loss == 10000.0
        assert holding.profit_loss_ratio == 10.0

    def test_update_market_value_loss(self):
        """損失が出ている場合のテスト"""
        holding = Holding(user_id=1, code="1234", quantity=100, average_price=1000.0)

        # 実行
        holding.update_market_value(900.0)

        # 検証
        assert holding.market_value == 90000.0
        assert holding.profit_loss == -10000.0
        assert holding.profit_loss_ratio == -10.0

    def test_update_market_value_zero_quantity(self):
        """数量0の場合のテスト"""
        holding = Holding(user_id=1, code="1234", quantity=0, average_price=1000.0)

        # 実行
        holding.update_market_value(1100.0)

        # 検証
        assert holding.market_value is None
        assert holding.profit_loss is None
        assert holding.profit_loss_ratio is None

    def test_update_market_value_zero_price(self):
        """株価0の場合のテスト"""
        holding = Holding(user_id=1, code="1234", quantity=100, average_price=1000.0)

        # 実行
        holding.update_market_value(0.0)

        # 検証
        assert holding.market_value is None
        assert holding.profit_loss is None
        assert holding.profit_loss_ratio is None

    def test_update_market_value_zero_average_price(self):
        """平均取得価格0の場合のテスト"""
        holding = Holding(user_id=1, code="1234", quantity=100, average_price=0.0)

        # 実行
        holding.update_market_value(1100.0)

        # 検証
        assert holding.market_value == 110000.0
        assert holding.profit_loss == 110000.0
        assert holding.profit_loss_ratio == 0.0  # ゼロ除算を避ける
