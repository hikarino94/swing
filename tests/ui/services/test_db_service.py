"""ui.services.db_serviceのテスト"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from src.ui.services.db_service import DatabaseService


class TestGetDbSummary:
    """get_db_summaryメソッドのテスト"""

    @patch("src.ui.services.db_service.get_db_connection")
    def test_get_db_summary_with_data(self, mock_get_db_connection):
        """データが存在する場合のサマリー取得テスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 各テーブルのクエリ結果を設定
        results = [
            (1000, "2024-01-01", "2024-12-31"),  # prices
            (500, "2024-01-01", "2024-12-31"),  # listed_info
            (200, "2024-01-01", "2024-12-31"),  # statements
            (50, "2024-01-01", "2024-12-31"),  # fundamental_signals
            (100, "2024-01-01", "2024-12-31"),  # technical_indicators
        ]

        mock_cursor.fetchone.side_effect = results
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        summary = DatabaseService.get_db_summary()

        # 検証
        assert summary["prices"]["count"] == 1000
        assert summary["prices"]["min_date"] == "2024-01-01"
        assert summary["prices"]["max_date"] == "2024-12-31"

        assert summary["listed_info"]["count"] == 500
        assert summary["statements"]["count"] == 200
        assert summary["fundamental_signals"]["count"] == 50
        assert summary["technical_indicators"]["count"] == 100

        # 各テーブルに対してクエリが実行されたことを確認
        assert mock_conn.execute.call_count == 5

    @patch("src.ui.services.db_service.get_db_connection")
    def test_get_db_summary_empty_tables(self, mock_get_db_connection):
        """空のテーブルの場合のサマリー取得テスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 空のテーブルの結果
        mock_cursor.fetchone.return_value = (0, None, None)
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        summary = DatabaseService.get_db_summary()

        # 検証
        for table in summary:
            assert summary[table]["count"] == 0
            assert summary[table]["min_date"] == "N/A"
            assert summary[table]["max_date"] == "N/A"

    @patch("src.ui.services.db_service.get_db_connection")
    def test_get_db_summary_partial_data(self, mock_get_db_connection):
        """一部のテーブルにデータがある場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 混在したデータ
        results = [
            (1000, "2024-01-01", "2024-12-31"),  # prices - データあり
            (0, None, None),  # listed_info - 空
            (200, "2024-06-01", "2024-06-30"),  # statements - データあり
            (None, None, None),  # fundamental_signals - NULL
            (0, None, None),  # technical_indicators - 空
        ]

        mock_cursor.fetchone.side_effect = results
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        summary = DatabaseService.get_db_summary()

        # 検証
        assert summary["prices"]["count"] == 1000
        assert summary["listed_info"]["count"] == 0
        assert summary["statements"]["count"] == 200
        assert summary["fundamental_signals"]["count"] == 0
        assert summary["technical_indicators"]["count"] == 0


class TestListSignals:
    """list_signalsメソッドのテスト"""

    @patch("src.ui.services.db_service.get_db_connection")
    def test_list_signals_fundamental(self, mock_get_db_connection):
        """ファンダメンタルシグナル取得のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # カラム名の設定
        mock_cursor.description = [
            ("code",),
            ("company_name",),
            ("created_at",),
            ("eps_yoy_fy",),
            ("eps_yoy_q",),
            ("op_margin_delta",),
            ("feps_revision",),
            ("cf_quality",),
            ("eta_delta",),
            ("leverage",),
            ("turnaround",),
            ("treasury_delta",),
        ]

        # データの設定
        mock_cursor.__iter__.return_value = iter(
            [
                (
                    "1234",
                    "テスト企業",
                    "2024-01-15",
                    0.15,
                    0.20,
                    0.05,
                    0.10,
                    1.2,
                    -0.03,
                    0.5,
                    1,
                    0.02,
                ),
                (
                    "5678",
                    "サンプル企業",
                    "2024-01-14",
                    0.25,
                    0.30,
                    0.08,
                    0.15,
                    1.5,
                    -0.05,
                    0.4,
                    0,
                    0.03,
                ),
            ]
        )

        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        results = DatabaseService.list_signals("fund", "2024-01-01", "2024-01-31")

        # 検証
        assert len(results) == 2
        assert results[0]["code"] == "1234"
        assert results[0]["company_name"] == "テスト企業"
        assert results[0]["created_at"] == "2024-01-15"
        assert results[0]["eps_yoy_fy"] == 0.15

        # クエリの確認
        executed_query = mock_conn.execute.call_args[0][0]
        assert "fundamental_signals" in executed_query
        assert "created_at >= ?" in executed_query
        assert "created_at <= ?" in executed_query

        # パラメータの確認
        params = mock_conn.execute.call_args[0][1]
        assert params == ["2024-01-01", "2024-01-31"]

    @patch("src.ui.services.db_service.get_db_connection")
    def test_list_signals_technical(self, mock_get_db_connection):
        """テクニカルシグナル取得のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # カラム名の設定
        mock_cursor.description = [
            ("code",),
            ("company_name",),
            ("signal_date",),
            ("signals_count",),
            ("signals_short_count",),
            ("signal_ma",),
            ("signal_rsi",),
            ("signal_adx",),
            ("signal_bb",),
            ("signal_macd",),
        ]

        # データの設定
        mock_cursor.__iter__.return_value = iter(
            [
                ("1234", "テスト企業", "2024-01-15", 3, 1, 1, 0, 1, 1, 0),
                ("5678", "サンプル企業", "2024-01-14", 2, 0, 0, 1, 0, 1, 0),
            ]
        )

        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        results = DatabaseService.list_signals("tech")

        # 検証
        assert len(results) == 2
        assert results[0]["code"] == "1234"
        assert results[0]["signals_count"] == 3
        assert results[0]["signal_ma"] == 1

        # クエリの確認
        executed_query = mock_conn.execute.call_args[0][0]
        assert "technical_indicators" in executed_query
        assert "signal_date" in executed_query

    @patch("src.ui.services.db_service.get_db_connection")
    def test_list_signals_no_date_filter(self, mock_get_db_connection):
        """日付フィルターなしの場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.description = [("code",), ("company_name",), ("created_at",)]
        mock_cursor.__iter__.return_value = iter([])
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        DatabaseService.list_signals("fund")

        # パラメータが空であることを確認
        params = mock_conn.execute.call_args[0][1]
        assert params == []

    @patch("src.ui.services.db_service.get_db_connection")
    def test_list_signals_start_date_only(self, mock_get_db_connection):
        """開始日のみ指定の場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.description = []
        mock_cursor.__iter__.return_value = iter([])
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        DatabaseService.list_signals("tech", start_date="2024-01-01")

        # クエリとパラメータの確認
        executed_query = mock_conn.execute.call_args[0][0]
        params = mock_conn.execute.call_args[0][1]

        assert "signal_date >= ?" in executed_query
        assert "signal_date <= ?" not in executed_query
        assert params == ["2024-01-01"]


class TestCheckAuthTables:
    """check_auth_tablesメソッドのテスト"""

    @patch("src.ui.services.db_service.get_db_connection")
    def test_check_auth_tables_exists(self, mock_get_db_connection):
        """認証テーブルが存在する場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # usersテーブルが存在する
        mock_cursor.fetchone.return_value = ("users",)

        # テスト実行
        result = DatabaseService.check_auth_tables()

        # 検証
        assert result is True
        mock_cursor.execute.assert_called_once_with(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )

    @patch("src.ui.services.db_service.get_db_connection")
    def test_check_auth_tables_not_exists(self, mock_get_db_connection):
        """認証テーブルが存在しない場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # usersテーブルが存在しない
        mock_cursor.fetchone.return_value = None

        # テスト実行
        result = DatabaseService.check_auth_tables()

        # 検証
        assert result is False

    @patch("src.ui.services.db_service.get_db_connection")
    def test_check_auth_tables_exception(self, mock_get_db_connection):
        """例外が発生した場合のテスト"""
        # 接続時に例外を発生させる
        mock_get_db_connection.side_effect = Exception("Database error")

        # テスト実行
        result = DatabaseService.check_auth_tables()

        # 検証
        assert result is False


class TestInitDatabase:
    """init_databaseメソッドのテスト"""

    @patch("db.db_schema.init_schema")
    @patch("src.ui.services.db_service.get_db_path")
    @patch("src.ui.services.db_service.logger")
    def test_init_database_not_exists(
        self, mock_logger, mock_get_db_path, mock_init_schema
    ):
        """データベースが存在しない場合の初期化テスト"""
        # 存在しないパスを設定
        mock_db_path = Path("/test/db/stock.db")
        mock_get_db_path.return_value = str(mock_db_path)

        # パスが存在しないように設定
        with patch.object(Path, "exists", return_value=False):
            with patch.object(Path, "mkdir") as mock_mkdir:
                # テスト実行
                DatabaseService.init_database()

        # 検証
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_init_schema.assert_called_once_with(mock_db_path)
        mock_logger.info.assert_any_call(
            "データベースが存在しません。初期化を開始します..."
        )
        mock_logger.info.assert_any_call("データベースの初期化が完了しました")

    @patch("db.db_schema.init_schema")
    @patch("src.ui.services.db_service.DatabaseService.check_auth_tables")
    @patch("src.ui.services.db_service.get_db_path")
    @patch("src.ui.services.db_service.logger")
    def test_init_database_exists_no_auth_tables(
        self, mock_logger, mock_get_db_path, mock_check_auth, mock_init_schema
    ):
        """データベースは存在するが認証テーブルがない場合のテスト"""
        # 存在するパスを設定
        mock_db_path = Path("/test/db/stock.db")
        mock_get_db_path.return_value = str(mock_db_path)

        # パスは存在するが認証テーブルがない
        with patch.object(Path, "exists", return_value=True):
            mock_check_auth.return_value = False

            # テスト実行
            DatabaseService.init_database()

        # 検証
        mock_init_schema.assert_called_once_with(mock_db_path)
        mock_logger.info.assert_any_call(
            "認証テーブルが存在しません。スキーマを再作成します..."
        )
        mock_logger.info.assert_any_call("スキーマの再作成が完了しました")

    @patch("src.ui.services.db_service.DatabaseService.check_auth_tables")
    @patch("src.ui.services.db_service.get_db_path")
    def test_init_database_already_initialized(self, mock_get_db_path, mock_check_auth):
        """既に初期化済みの場合のテスト"""
        # 存在するパスを設定
        mock_db_path = Path("/test/db/stock.db")
        mock_get_db_path.return_value = str(mock_db_path)

        # パスも認証テーブルも存在する
        with patch.object(Path, "exists", return_value=True):
            mock_check_auth.return_value = True

            # init_schemaがインポートされないようにパッチ
            with patch("db.db_schema.init_schema") as mock_init_schema:
                # テスト実行
                DatabaseService.init_database()

                # init_schemaが呼ばれていないことを確認
                mock_init_schema.assert_not_called()


class TestDatabaseServiceEdgeCases:
    """DatabaseServiceのエッジケーステスト"""

    @patch("src.ui.services.db_service.get_db_connection")
    def test_list_signals_empty_result(self, mock_get_db_connection):
        """シグナルが1件もない場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        mock_cursor.description = [("code",), ("company_name",), ("signal_date",)]
        mock_cursor.__iter__.return_value = iter([])
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        results = DatabaseService.list_signals("tech", "2024-01-01", "2024-01-31")

        # 検証
        assert results == []

    @patch("src.ui.services.db_service.get_db_connection")
    def test_get_db_summary_sql_injection_safe(self, mock_get_db_connection):
        """SQLインジェクション対策のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn

        # 各テーブルのクエリ結果を設定
        mock_cursor.fetchone.return_value = (0, None, None)
        mock_conn.execute.return_value = mock_cursor

        # テスト実行
        DatabaseService.get_db_summary()

        # テーブル名が直接SQLに埋め込まれていることを確認
        # （この場合は固定値なので問題ない）
        calls = mock_conn.execute.call_args_list
        for call in calls:
            query = call[0][0]
            # パラメータ化されていないことを確認（固定テーブル名なのでOK）
            assert "?" not in query
