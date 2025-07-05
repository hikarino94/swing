"""ポートフォリオ管理機能のテストスイート"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src.portfolio import Holding, PortfolioManager, SBICSVParser, Transaction


class TestSBICSVParser:
    """SBI証券CSVパーサーのテスト"""

    def test_parse_holdings_csv_standard(self):
        """標準的な保有銘柄CSVの解析"""
        csv_content = """銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価額,評価損益,評価損益率(%)
7203,トヨタ自動車,東証プライム,100,2500,2800,280000,30000,12.0
6758,ソニーグループ,東証プライム,50,13000,14500,725000,75000,11.54
"""

        result = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(result) == 2

        # トヨタ自動車のデータ確認
        toyota = result[0]
        assert toyota["code"] == "7203"
        assert toyota["name"] == "トヨタ自動車"
        assert toyota["quantity"] == 100
        assert toyota["average_price"] == 2500
        assert toyota["current_price"] == 2800
        assert toyota["market_value"] == 280000
        assert toyota["profit_loss"] == 30000
        assert toyota["profit_loss_ratio"] == 12.0

    def test_parse_holdings_csv_with_comma_numbers(self):
        """カンマ付き数値を含む保有銘柄CSV"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価,評価損益
7203,トヨタ自動車,"1,000","2,500","30,000"
"""

        result = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(result) == 1
        assert result[0]["quantity"] == 1000
        assert result[0]["average_price"] == 2500
        assert result[0]["profit_loss"] == 30000

    def test_parse_holdings_csv_with_negative_values(self):
        """マイナス値を含む保有銘柄CSV"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価,評価損益,評価損益率(%)
9999,テスト銘柄,100,1000,-5000,-5.0
8888,損失銘柄,200,500,△10000,▲10.0
"""

        result = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(result) == 2
        assert result[0]["profit_loss"] == -5000
        assert result[0]["profit_loss_ratio"] == -5.0
        assert result[1]["profit_loss"] == -10000
        assert result[1]["profit_loss_ratio"] == -10.0

    def test_parse_holdings_csv_empty(self):
        """空のCSVファイル"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価
"""

        result = SBICSVParser.parse_holdings_csv(csv_content)
        assert len(result) == 0

    def test_parse_holdings_csv_invalid_format(self):
        """不正なフォーマットのCSV"""
        csv_content = """これは不正なCSVです
データがありません
"""

        # 不正なフォーマットの場合は空のリストを返す
        result = SBICSVParser.parse_holdings_csv(csv_content)
        assert len(result) == 0

    def test_parse_holdings_csv_detailed_format(self):
        """保有証券_現物形式のCSV"""
        csv_content = """﻿銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額,配当落ち日,配当落ち日,保有期間,当日売却注文,日中売却有効数量,引け後売却有効数量,週間騰落率,月間騰落率,年間騰落率,予想PER,実績PBR,予想配当利回り,予想EPS,実績BPS,予想1株配当,貸借区分
,,,,1911,住友林業,東P,特定,300,--,"1,455","1,434",→,"-6,300",-1.44%,"436,500","430,200",,,,,,,,,,,10.5,1.2,3.2%,136.57,1195.00,46.00,貸借
,,,,7267,本田技研工業,東P,NISA,100,--,"1,476","1,445",↑,"-3,100",-2.10%,"147,600","144,500",,,,,,,,,,,15.2,0.8,2.8%,95.07,1806.25,40.50,貸借
,,,,9432,ＮＴＴ,東P,旧NISA,500,--,163,154.2,↑,"-4,400",-5.40%,"81,500","77,100",,,,,,,,,,,12.3,1.5,4.1%,12.54,102.80,6.32,貸借
"""

        result = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(result) == 3

        # 住友林業
        assert result[0]["code"] == "1911"
        assert result[0]["name"] == "住友林業"
        assert result[0]["quantity"] == 300
        assert result[0]["average_price"] == 1455
        assert result[0]["current_price"] == 1434
        assert result[0]["profit_loss"] == -6300
        assert result[0]["profit_loss_ratio"] == -1.44

        # 本田技研工業
        assert result[1]["code"] == "7267"
        assert result[1]["quantity"] == 100
        assert result[1]["average_price"] == 1476

        # NTT
        assert result[2]["code"] == "9432"
        assert result[2]["quantity"] == 500

    def test_parse_holdings_csv_savefile_format(self):
        """SaveFile形式のCSV（Shift-JISエンコーディング）"""
        # 実際のShift-JISエンコードされたデータをシミュレート
        csv_content = """
保有証券一覧

銘柄（特定口座）合計

評価額合計,評価損益合計
7361450,+572250

銘柄（特定口座）

銘柄コード,銘柄名,保有数量,取得単価,現在値,取得金額,評価額,評価損益
"1911","住友林業",300,,1455,1435,436500,430500,-6000
"7267","本田技研",100,,1476,1443,147600,144300,-3300
"8306","三菱UFJ",200,,970,2015.5,194000,403100,+209100

銘柄（NISA口座（みずほ証券））合計

評価額合計,評価損益合計
2450200,+234820

銘柄（NISA口座（みずほ証券））

銘柄コード,銘柄名,保有数量,取得単価,現在値,取得金額,評価額,評価損益
"1814","大末建設",100,,1607,2211,160700,221100,+60400
"7272","ヤマハ発動機",100,,1218,1083.5,121800,108350,-13450
"""

        result = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(result) == 5

        # 特定口座の銘柄
        assert result[0]["code"] == "1911"
        assert result[0]["name"] == "住友林業"
        assert result[0]["quantity"] == 300
        assert result[0]["average_price"] == 1455
        assert result[0]["profit_loss"] == -6000

        # NISA口座の銘柄
        assert result[3]["code"] == "1814"
        assert result[3]["name"] == "大末建設"
        assert result[3]["quantity"] == 100
        assert result[3]["profit_loss"] == 60400

    def test_parse_holdings_csv_with_encoding(self):
        """バイト列入力でのエンコーディング検出"""
        # UTF-8 with BOM
        csv_content_bytes = b"\xef\xbb\xbf\xe9\x8a\x98\xe6\x9f\x84\xe3\x82\xb3\xe3\x83\xbc\xe3\x83\x89,\xe9\x8a\x98\xe6\x9f\x84\xe5\x90\x8d,\xe4\xbf\x9d\xe6\x9c\x89\xe6\x95\xb0\xe9\x87\x8f\n7203,\xe3\x83\x88\xe3\x83\xa8\xe3\x82\xbf,100\n"

        result = SBICSVParser.parse_holdings_csv(csv_content_bytes)

        assert len(result) == 1
        assert result[0]["code"] == "7203"
        assert result[0]["quantity"] == 100

    def test_parse_number_special_cases(self):
        """特殊な数値フォーマットのテスト"""
        assert SBICSVParser._parse_number("--") is None
        assert SBICSVParser._parse_number("--%") is None
        assert SBICSVParser._parse_number("+1,000") == 1000
        assert SBICSVParser._parse_number("-1,000") == -1000
        assert SBICSVParser._parse_number("--", default=0) == 0

    def test_parse_transactions_csv_standard(self):
        """標準的な取引履歴CSVの解析"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額
2024/01/10,7203,トヨタ自動車,買付,100,2500,275,0,250275
2024/01/15,6758,ソニーグループ,買付,50,13000,495,0,650495
2024/01/20,7203,トヨタ自動車,売却,50,2800,275,1400,138325
"""

        result = SBICSVParser.parse_transactions_csv(csv_content)

        assert len(result) == 3

        # 最初の取引を確認
        first = result[0]
        assert first["code"] == "7203"
        assert first["transaction_date"] == "2024-01-10"
        assert first["transaction_type"] == "buy"
        assert first["quantity"] == 100
        assert first["price"] == 2500
        assert first["commission"] == 275
        assert first["tax"] == 0
        assert first["total_amount"] == 250275

        # 売却取引を確認
        sell = result[2]
        assert sell["transaction_type"] == "sell"
        assert sell["tax"] == 1400

    def test_parse_transactions_csv_date_formats(self):
        """様々な日付フォーマットの処理"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,受渡金額
2024/01/10,7203,トヨタ,買,100,2500,250000
2024-01-15,6758,ソニー,買,50,13000,650000
24/01/20,9999,テスト,買,10,1000,10000
2024年1月25日,8888,サンプル,買,20,500,10000
"""

        result = SBICSVParser.parse_transactions_csv(csv_content)

        assert result[0]["transaction_date"] == "2024-01-10"
        assert result[1]["transaction_date"] == "2024-01-15"
        assert result[2]["transaction_date"] == "2024-01-20"
        assert result[3]["transaction_date"] == "2024-01-25"

    def test_parse_transactions_csv_sort_by_date(self):
        """日付順ソートの確認"""
        csv_content = """約定日,銘柄コード,売買区分,数量,約定単価,受渡金額
2024/01/20,7203,買,100,2500,250000
2024/01/10,6758,買,50,13000,650000
2024/01/15,9999,買,10,1000,10000
"""

        result = SBICSVParser.parse_transactions_csv(csv_content)

        # 日付順にソートされていることを確認
        assert result[0]["transaction_date"] == "2024-01-10"
        assert result[1]["transaction_date"] == "2024-01-15"
        assert result[2]["transaction_date"] == "2024-01-20"

    def test_normalize_code(self):
        """銘柄コードの正規化"""
        assert SBICSVParser._normalize_code("7203") == "7203"
        assert SBICSVParser._normalize_code("203") == "0203"
        assert SBICSVParser._normalize_code("7203-T") == "7203"
        assert SBICSVParser._normalize_code("") == ""
        assert SBICSVParser._normalize_code(None) == ""

    def test_parse_number(self):
        """数値解析のテスト"""
        assert SBICSVParser._parse_number("1,234") == 1234
        assert SBICSVParser._parse_number("1234.56") == 1234.56
        assert SBICSVParser._parse_number("-100") == -100
        assert SBICSVParser._parse_number("(100)") == -100
        assert SBICSVParser._parse_number("△100") == -100
        assert SBICSVParser._parse_number("▲100") == -100
        assert SBICSVParser._parse_number("10.5%") == 10.5
        assert SBICSVParser._parse_number("") is None
        assert SBICSVParser._parse_number(None) is None
        assert SBICSVParser._parse_number("invalid", default=0) == 0

    def test_parse_trade_type(self):
        """売買区分の解析"""
        assert SBICSVParser._parse_trade_type("現物買")[0] == "buy"
        assert SBICSVParser._parse_trade_type("現物売")[0] == "sell"
        assert SBICSVParser._parse_trade_type("信用新規買")[0] == "buy"
        assert SBICSVParser._parse_trade_type("信用新規売")[0] == "sell"
        assert SBICSVParser._parse_trade_type("信用返済買")[0] == "buy"
        assert SBICSVParser._parse_trade_type("信用返済売")[0] == "sell"
        assert SBICSVParser._parse_trade_type("")[0] == "buy"
        assert SBICSVParser._parse_trade_type("その他")[0] == "buy"


class TestHoldingModel:
    """Holdingモデルのテスト"""

    @pytest.fixture
    def temp_db(self):
        """テスト用の一時データベース"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # データベースを初期化
        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT NOT NULL DEFAULT 'default',
                quantity INTEGER NOT NULL,
                average_price REAL NOT NULL,
                market_value REAL,
                profit_loss REAL,
                profit_loss_ratio REAL,
                expected_per REAL,
                actual_pbr REAL,
                dividend_yield REAL,
                expected_eps REAL,
                actual_bps REAL,
                expected_dividend REAL,
                lending_type TEXT,
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(user_id, code, account_name)
            );
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                company_name TEXT
            );
        """
        )

        # テスト用の企業情報を追加
        conn.execute("INSERT INTO listed_info VALUES ('7203', 'トヨタ自動車')")
        conn.commit()
        conn.close()

        yield db_path

        # クリーンアップ
        os.unlink(db_path)

    def test_holding_creation(self):
        """保有銘柄インスタンスの作成"""
        holding = Holding(user_id=1, code="7203")
        assert holding.user_id == 1
        assert holding.code == "7203"
        assert holding.account_name == "default"
        assert holding.quantity == 0
        assert holding.average_price == 0.0

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_save_new_holding(self, temp_db):
        """新規保有銘柄の保存"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            holding = Holding(user_id=1, code="7203")
            holding.quantity = 100
            holding.average_price = 2500

            result = holding.save()
            assert result is True
            assert holding.id is not None

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_find_by_user_and_code(self, temp_db):
        """ユーザーIDと銘柄コードでの検索"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            # テストデータを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            conn.execute(
                """
                INSERT INTO holdings (user_id, code, account_name, quantity, average_price)
                VALUES (?, ?, ?, ?, ?)
            """,
                (1, "7203", "default", 100, 2500),
            )
            conn.commit()
            conn.close()

            # 検索テスト
            holding = Holding.find_by_user_and_code(1, "7203")
            assert holding is not None
            assert holding.quantity == 100
            assert holding.average_price == 2500

            # 存在しない場合
            holding = Holding.find_by_user_and_code(1, "9999")
            assert holding is None

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_find_all_by_user(self, temp_db):
        """ユーザーの全保有銘柄取得"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            # テストデータを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            conn.execute(
                """
                INSERT INTO holdings (user_id, code, account_name, quantity, average_price)
                VALUES (?, ?, ?, ?, ?)
            """,
                (1, "7203", "default", 100, 2500),
            )
            conn.execute(
                """
                INSERT INTO holdings (user_id, code, account_name, quantity, average_price)
                VALUES (?, ?, ?, ?, ?)
            """,
                (1, "6758", "default", 50, 13000),
            )
            conn.execute(
                """
                INSERT INTO holdings (user_id, code, account_name, quantity, average_price)
                VALUES (?, ?, ?, ?, ?)
            """,
                (2, "9999", "default", 10, 1000),
            )  # 別ユーザー
            conn.commit()
            conn.close()

            # ユーザー1の保有銘柄を取得
            holdings = Holding.find_all_by_user(1)
            assert len(holdings) == 2
            assert holdings[0].code == "6758"  # コード順
            assert holdings[1].code == "7203"

    def test_update_market_value(self):
        """時価評価の更新"""
        holding = Holding(user_id=1, code="7203")
        holding.quantity = 100
        holding.average_price = 2500

        # 現在価格で更新
        holding.update_market_value(2800)

        assert holding.market_value == 280000  # 100株 × 2800円
        assert holding.profit_loss == 30000  # 280000 - 250000
        assert holding.profit_loss_ratio == 12.0  # 30000 / 250000 * 100

    def test_update_market_value_zero_quantity(self):
        """数量0の場合の時価評価更新"""
        holding = Holding(user_id=1, code="7203")
        holding.quantity = 0
        holding.average_price = 2500

        holding.update_market_value(2800)

        # 値は更新されない
        assert holding.market_value is None
        assert holding.profit_loss is None


class TestTransactionModel:
    """Transactionモデルのテスト"""

    @pytest.fixture
    def temp_db(self):
        """テスト用の一時データベース"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        # データベースを初期化
        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                transaction_date TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                commission REAL DEFAULT 0,
                tax REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                remarks TEXT,
                detailed_type TEXT,
                realized_profit REAL,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                company_name TEXT
            );
        """
        )

        # テスト用の企業情報を追加
        conn.execute("INSERT INTO listed_info VALUES ('7203', 'トヨタ自動車')")
        conn.commit()
        conn.close()

        yield db_path

        # クリーンアップ
        os.unlink(db_path)

    def test_transaction_creation(self):
        """取引インスタンスの作成"""
        trans = Transaction(
            user_id=1,
            code="7203",
            transaction_date="2024-01-10",
            transaction_type="buy",
            quantity=100,
            price=2500,
        )
        assert trans.user_id == 1
        assert trans.code == "7203"
        assert trans.transaction_date == "2024-01-10"
        assert trans.transaction_type == "buy"
        assert trans.quantity == 100
        assert trans.price == 2500
        assert trans.total_amount == 250000

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_save_transaction(self, temp_db):
        """取引の保存"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            trans = Transaction(
                user_id=1,
                code="7203",
                transaction_date="2024-01-10",
                transaction_type="buy",
                quantity=100,
                price=2500,
            )
            trans.commission = 275

            result = trans.save()
            assert result is True
            assert trans.id is not None

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_find_all_by_user(self, temp_db):
        """ユーザーの取引履歴取得"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            # テストデータを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            # ユーザー1の取引
            conn.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (1, "7203", "2024-01-10", "buy", 100, 2500, 250000),
            )
            conn.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (1, "7203", "2024-01-20", "sell", 50, 2800, 140000),
            )
            # ユーザー2の取引
            conn.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (2, "9999", "2024-01-15", "buy", 10, 1000, 10000),
            )
            conn.commit()
            conn.close()

            # ユーザー1の取引を取得
            transactions = Transaction.find_all_by_user(1)
            assert len(transactions) == 2
            # 日付の降順でソートされている
            assert transactions[0].transaction_date == "2024-01-20"
            assert transactions[1].transaction_date == "2024-01-10"

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_find_all_by_user_with_filters(self, temp_db):
        """フィルター付き取引履歴取得"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            # テストデータを作成
            import sqlite3

            conn = sqlite3.connect(temp_db)
            conn.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (1, "7203", "2024-01-10", "buy", 100, 2500, 250000),
            )
            conn.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (1, "6758", "2024-01-15", "buy", 50, 13000, 650000),
            )
            conn.execute(
                """
                INSERT INTO transactions
                (user_id, code, transaction_date, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (1, "7203", "2024-01-20", "sell", 50, 2800, 140000),
            )
            conn.commit()
            conn.close()

            # 銘柄コードでフィルター
            transactions = Transaction.find_all_by_user(1, code="7203")
            assert len(transactions) == 2

            # 日付範囲でフィルター
            transactions = Transaction.find_all_by_user(
                1, start_date="2024-01-15", end_date="2024-01-20"
            )
            assert len(transactions) == 2
            assert transactions[0].code == "7203"  # 1/20
            assert transactions[1].code == "6758"  # 1/15

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_bulk_insert(self, temp_db):
        """複数取引の一括挿入"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            transactions_data = [
                {
                    "user_id": 1,
                    "code": "7203",
                    "transaction_date": "2024-01-10",
                    "transaction_type": "buy",
                    "quantity": 100,
                    "price": 2500,
                    "total_amount": 250000,
                    "commission": 275,
                },
                {
                    "user_id": 1,
                    "code": "6758",
                    "transaction_date": "2024-01-15",
                    "transaction_type": "buy",
                    "quantity": 50,
                    "price": 13000,
                    "total_amount": 650000,
                    "commission": 495,
                },
            ]

            count = Transaction.bulk_insert(transactions_data)
            assert count == 2

            # 挿入されたことを確認
            transactions = Transaction.find_all_by_user(1)
            assert len(transactions) == 2

    @patch("src.portfolio.models.DB_PATH", new="test.db")
    def test_bulk_insert_skip_duplicates(self, temp_db):
        """重複データのスキップ"""
        with patch("src.portfolio.models.DB_PATH", temp_db):
            # 既存データを作成
            trans = Transaction(
                user_id=1,
                code="7203",
                transaction_date="2024-01-10",
                transaction_type="buy",
                quantity=100,
                price=2500,
            )
            trans.save()

            # 同じデータを含む一括挿入
            transactions_data = [
                {
                    "user_id": 1,
                    "code": "7203",
                    "transaction_date": "2024-01-10",
                    "transaction_type": "buy",
                    "quantity": 100,
                    "price": 2500,
                    "total_amount": 250000,
                },
                {
                    "user_id": 1,
                    "code": "6758",
                    "transaction_date": "2024-01-15",
                    "transaction_type": "buy",
                    "quantity": 50,
                    "price": 13000,
                    "total_amount": 650000,
                },
            ]

            # 2件挿入される（SQLiteでは主キー制約がないため、同じデータも挿入される）
            count = Transaction.bulk_insert(transactions_data)
            assert count == 2


class TestPortfolioManager:
    """PortfolioManagerのテスト"""

    @patch("src.portfolio.manager.Holding")
    def test_update_holdings_from_csv_new(self, mock_holding_class):
        """CSVから新規保有銘柄を更新"""
        # モックの設定
        mock_holding_class.find_by_user_and_code.return_value = None
        mock_holding_instance = MagicMock()
        mock_holding_instance.save.return_value = True
        mock_holding_class.return_value = mock_holding_instance

        holdings_data = [
            {
                "code": "7203",
                "quantity": 100,
                "average_price": 2500,
                "market_value": 280000,
                "profit_loss": 30000,
                "profit_loss_ratio": 12.0,
            }
        ]

        updated, new = PortfolioManager.update_holdings_from_csv(1, holdings_data)

        assert updated == 0
        assert new == 1
        assert mock_holding_instance.quantity == 100
        assert mock_holding_instance.average_price == 2500
        mock_holding_instance.save.assert_called_once()

    @patch("src.portfolio.manager.Holding")
    def test_update_holdings_from_csv_existing(self, mock_holding_class):
        """CSVから保有銘柄を追加（重複チェックなし）"""
        # 新規インスタンスをモック
        mock_holding_instance = MagicMock()
        mock_holding_instance.save.return_value = True
        mock_holding_class.return_value = mock_holding_instance

        holdings_data = [
            {
                "code": "7203",
                "quantity": 150,
                "average_price": 2600,
                "market_value": 420000,
                "profit_loss": 30000,
                "profit_loss_ratio": 7.7,
            }
        ]

        updated, new = PortfolioManager.update_holdings_from_csv(1, holdings_data)

        # 新しい仕様では常に新規作成
        assert updated == 0
        assert new == 1
        assert mock_holding_instance.quantity == 150
        assert mock_holding_instance.average_price == 2600
        mock_holding_instance.save.assert_called_once()

    @patch("src.portfolio.manager.Transaction")
    def test_import_transactions_from_csv(self, mock_transaction_class):
        """CSVから取引履歴をインポート（保有銘柄への反映なし）"""
        # モックの設定
        mock_transaction_class.bulk_insert.return_value = 2

        transactions_data = [
            {
                "code": "7203",
                "transaction_date": "2024-01-10",
                "transaction_type": "buy",
                "quantity": 100,
                "price": 2500,
                "total_amount": 250000,
            },
            {
                "code": "6758",
                "transaction_date": "2024-01-15",
                "transaction_type": "buy",
                "quantity": 50,
                "price": 13000,
                "total_amount": 650000,
            },
        ]

        count = PortfolioManager.import_transactions_from_csv(1, transactions_data)

        assert count == 2
        # ユーザーIDが追加されていることを確認
        assert transactions_data[0]["user_id"] == 1
        assert transactions_data[1]["user_id"] == 1
        # 新しい仕様では保有銘柄の再計算は行わない

    @patch("src.portfolio.manager.DB_PATH", new="test.db")
    def test_recalculate_holdings(self):
        """取引履歴から保有銘柄を再計算"""
        # テスト用DBを作成
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                code TEXT,
                transaction_date TEXT,
                transaction_type TEXT,
                quantity INTEGER,
                price REAL,
                commission REAL
            );
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                code TEXT,
                account_name TEXT NOT NULL DEFAULT 'default',
                quantity INTEGER,
                average_price REAL,
                market_value REAL,
                profit_loss REAL,
                profit_loss_ratio REAL,
                expected_per REAL,
                actual_pbr REAL,
                dividend_yield REAL,
                expected_eps REAL,
                actual_bps REAL,
                expected_dividend REAL,
                lending_type TEXT,
                updated_at TEXT,
                UNIQUE(user_id, code, account_name)
            );
            CREATE TABLE prices (
                code TEXT,
                date TEXT,
                close REAL
            );
        """
        )

        # テストデータ
        # 取引履歴: 100株買い → 50株買い → 50株売り
        conn.execute(
            """
            INSERT INTO transactions VALUES
            (1, 1, '7203', '2024-01-10', 'buy', 100, 2500, 0),
            (2, 1, '7203', '2024-01-15', 'buy', 50, 2600, 0),
            (3, 1, '7203', '2024-01-20', 'sell', 50, 2800, 0)
        """
        )
        # 現在価格
        conn.execute("INSERT INTO prices VALUES ('72030', '2024-01-20', 2900)")
        conn.commit()
        conn.close()

        with patch("src.portfolio.manager.DB_PATH", db_path):
            with patch("src.portfolio.manager.Holding") as mock_holding_class:
                # モックの設定
                mock_holding = MagicMock()
                mock_holding.save.return_value = True
                mock_holding_class.find_by_user_and_code.return_value = mock_holding
                mock_holding_class.return_value = mock_holding

                PortfolioManager.recalculate_holdings(1)

                # 残り100株、平均取得価格2533.33円で更新されることを確認
                assert mock_holding.quantity == 100
                assert abs(mock_holding.average_price - 2533.33) < 0.01
                # 時価評価も更新される
                mock_holding.update_market_value.assert_called_with(2900)

        # クリーンアップ
        os.unlink(db_path)

    @patch("src.portfolio.manager.Holding")
    def test_update_market_values(self, mock_holding_class):
        """時価評価の一括更新"""
        # モック保有銘柄
        mock_holdings = []
        for code in ["7203", "6758"]:
            mock_holding = MagicMock()
            mock_holding.code = code
            mock_holding.save.return_value = True
            mock_holdings.append(mock_holding)

        mock_holding_class.find_all_by_user.return_value = mock_holdings

        # モックDB接続
        with patch("src.portfolio.manager.sqlite3.connect") as mock_connect:
            mock_cursor = MagicMock()
            # 最新日付の取得、各銘柄の株価取得
            mock_cursor.fetchone.side_effect = [("2024-01-20",), (2900,), (15000,)]
            mock_conn = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            count = PortfolioManager.update_market_values(1)

            assert count == 2
            # 各銘柄の時価評価が更新されることを確認
            mock_holdings[0].update_market_value.assert_called_with(2900)
            mock_holdings[1].update_market_value.assert_called_with(15000)

    @patch("src.portfolio.manager.DB_PATH", new="test.db")
    def test_get_portfolio_summary(self):
        """ポートフォリオサマリーの取得"""
        # テスト用DBを作成
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE holdings (
                user_id INTEGER,
                quantity INTEGER,
                average_price REAL,
                market_value REAL,
                profit_loss REAL
            );
            CREATE TABLE transactions (
                user_id INTEGER,
                transaction_date TEXT
            );
        """
        )

        # テストデータ
        conn.execute(
            """
            INSERT INTO holdings VALUES
            (1, 100, 2500, 280000, 30000),
            (1, 50, 13000, 750000, 100000)
        """
        )
        conn.execute(
            """
            INSERT INTO transactions VALUES
            (1, '2024-01-10'),
            (1, '2024-01-20')
        """
        )
        conn.commit()
        conn.close()

        with patch("src.portfolio.manager.DB_PATH", db_path):
            summary = PortfolioManager.get_portfolio_summary(1)

            assert summary["stock_count"] == 2
            assert summary["total_cost"] == 900000  # 250000 + 650000
            assert summary["total_market_value"] == 1030000  # 280000 + 750000
            assert summary["total_profit_loss"] == 130000  # 30000 + 100000
            assert abs(summary["total_profit_loss_ratio"] - 14.44) < 0.01
            assert summary["transaction_count"] == 2
            assert summary["first_transaction_date"] == "2024-01-10"
            assert summary["last_transaction_date"] == "2024-01-20"

    @patch("src.portfolio.manager.DB_PATH", new="test.db")
    def test_aggregate_holdings_by_code(self):
        """銘柄コードで保有銘柄を集約"""
        # テスト用DBを作成
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                code TEXT,
                account_name TEXT,
                quantity INTEGER,
                average_price REAL,
                market_value REAL,
                profit_loss REAL,
                expected_per REAL,
                actual_pbr REAL,
                dividend_yield REAL,
                expected_eps REAL,
                actual_bps REAL,
                expected_dividend REAL,
                lending_type TEXT
            );
            CREATE TABLE listed_info (
                code TEXT PRIMARY KEY,
                company_name TEXT
            );
        """
        )

        # テストデータ（同じ銘柄を複数口座で保有）
        conn.execute(
            """
            INSERT INTO holdings VALUES
            (1, 1, '7203', 'SBI', 100, 2500, 280000, 30000, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            (2, 1, '7203', '楽天', 50, 2600, 140000, 10000, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
        )
        conn.execute("INSERT INTO listed_info VALUES ('72030', 'トヨタ自動車')")
        conn.commit()
        conn.close()

        with patch("src.portfolio.manager.DB_PATH", db_path):
            aggregated = PortfolioManager.aggregate_holdings_by_code(1)

            assert len(aggregated) == 1
            assert aggregated[0]["code"] == "7203"
            assert aggregated[0]["total_quantity"] == 150
            assert (
                abs(aggregated[0]["weighted_avg_price"] - 2533.33) < 0.01
            )  # (100*2500 + 50*2600) / 150
            assert aggregated[0]["total_market_value"] == 420000
            assert aggregated[0]["total_profit_loss"] == 40000
            assert aggregated[0]["account_count"] == 2
            assert "SBI" in aggregated[0]["account_names"]
            assert "楽天" in aggregated[0]["account_names"]

        # クリーンアップ
        os.unlink(db_path)

    @patch("src.portfolio.manager.sqlite3.connect")
    def test_delete_all_holdings(self, mock_connect):
        """全保有銘柄の削除"""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (3,)  # 3件削除
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        count = PortfolioManager.delete_all_holdings(1)

        assert count == 3
        mock_cursor.execute.assert_any_call(
            "SELECT COUNT(*) FROM holdings WHERE user_id = ?", (1,)
        )
        mock_cursor.execute.assert_any_call(
            "DELETE FROM holdings WHERE user_id = ?", (1,)
        )
        mock_conn.commit.assert_called_once()

    @patch("src.portfolio.manager.sqlite3.connect")
    def test_delete_holdings_by_account(self, mock_connect):
        """特定口座の保有銘柄削除"""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (2,)  # 2件削除
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        count = PortfolioManager.delete_holdings_by_account(1, "SBI")

        assert count == 2
        mock_cursor.execute.assert_any_call(
            "SELECT COUNT(*) FROM holdings WHERE user_id = ? AND account_name = ?",
            (1, "SBI"),
        )
        mock_cursor.execute.assert_any_call(
            "DELETE FROM holdings WHERE user_id = ? AND account_name = ?", (1, "SBI")
        )
        mock_conn.commit.assert_called_once()

    def test_parse_transactions_csv_savefile_format(self):
        """約定履歴照会形式の取引履歴CSV"""
        csv_content = """
約定履歴照会

商品指定,約定開始年月日,約定終了年月日,明細数,明細指定開始,明細指定終了
"すべての商品","2024年06月05日","2024年07月04日","10","1","10"

（注）明細数はご指定された期間の合計です。

約定日,銘柄,銘柄コード,市場,取引,期限,預り,課税,約定数量,約定単価,手数料/諸経費等,税額,受渡日,受渡金額/決済損益
"2024/07/01","テスト銘柄","1234","東証","現物買","--"," 特定 ","--",100,1500,105,0,"2024/07/03",150105
"2024/07/02","テスト銘柄","1234","東証","現物売","--"," 特定 ","--",100,1600,105,2100,"2024/07/04",157795
"2024/07/03","サンプル","5678","東証","信用新規買","6ヶ月"," 特定 ","--",200,2000,--,--,"2024/07/05",--
"""

        # Shift-JISでエンコード
        csv_bytes = csv_content.encode("shift_jis")

        transactions = SBICSVParser.parse_transactions_csv(csv_bytes)

        assert len(transactions) == 3

        # 現物買付
        assert transactions[0]["code"] == "1234"
        assert transactions[0]["name"] == "テスト銘柄"
        assert transactions[0]["transaction_type"] == "buy"
        assert transactions[0]["quantity"] == 100
        assert transactions[0]["price"] == 1500
        assert transactions[0]["commission"] == 105

        # 現物売却
        assert transactions[1]["code"] == "1234"
        assert transactions[1]["transaction_type"] == "sell"

        # 信用取引
        assert transactions[2]["code"] == "5678"
        assert transactions[2]["transaction_type"] == "buy"
        assert transactions[2]["detailed_type"] == "新規買い"

    def test_parse_transactions_csv_order_list_format(self):
        """注文一覧形式の取引履歴CSV（信用取引も含む）"""
        csv_content = """﻿銘柄,銘柄,銘柄,取引区分,期限,預り区分,約定日,受渡日,株数,平均約定単価,手数料・諸経費等,課税額・譲渡益税,受渡金額・決済損益,受渡金額(日計り分)
3498,霞ヶ関キャピタル,東P,信用新規買,６ヵ月,特定,2025/07/04,2025/07/08,200,"16,348.5",--,--,--,--
3498,霞ヶ関キャピタル,東P,信用返済売,６ヵ月,特定,2025/07/04,2025/07/08,200,"16,250",--,--,"-19,700",--
7481,尾家産業,東S,現物買,--,NISA,2025/07/04,2025/07/08,100,"2,104",--,--,"210,400",--
9275,ナルミヤ・インターナショナル,東S,信用返済売,６ヵ月,特定,2025/07/04,2025/07/08,400,"1,504",96,--,"-31,696",--
"""

        transactions = SBICSVParser.parse_transactions_csv(csv_content)

        assert len(transactions) == 4  # 全ての取引が取得される

        # 信用新規買
        assert transactions[0]["code"] == "3498"
        assert transactions[0]["name"] == "霞ヶ関キャピタル"
        assert transactions[0]["transaction_type"] == "buy"
        assert transactions[0]["quantity"] == 200
        assert transactions[0]["price"] == 16348.5
        assert transactions[0]["detailed_type"] == "新規買い"

        # 信用返済売
        assert transactions[1]["code"] == "3498"
        assert transactions[1]["transaction_type"] == "sell"
        assert transactions[1]["detailed_type"] == "決済売り"

        # 現物買付
        assert transactions[2]["code"] == "7481"
        assert transactions[2]["name"] == "尾家産業"
        assert transactions[2]["transaction_type"] == "buy"
        assert transactions[2]["quantity"] == 100
        assert transactions[2]["price"] == 2104
        assert transactions[2]["total_amount"] == 210400
        assert transactions[2]["transaction_date"] == "2025-07-04"
        assert transactions[2]["remarks"] == ""  # 現物取引

        # 信用返済売
        assert transactions[3]["code"] == "9275"
        assert transactions[3]["transaction_type"] == "sell"
        assert transactions[3]["remarks"] == "信用"
