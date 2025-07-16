"""
CSVインポート機能のテスト
"""

import sqlite3
from unittest.mock import patch

import pytest

from src.portfolio.csv_importer import (
    import_holdings_csv,
    parse_margin_csv,
    parse_number,
    parse_percentage,
    parse_spot_csv,
)


def test_parse_number():
    """数値パースのテスト"""
    # 通常の数値
    assert parse_number("1,234") == 1234.0
    assert parse_number("1234.56") == 1234.56

    # 範囲表記
    assert parse_number("190 ~ 200") == 195.0
    assert parse_number("100~110") == 105.0

    # 無効な値
    assert parse_number("--") is None
    assert parse_number("") is None
    assert parse_number("nan") is None

    # カンマ付き
    assert parse_number("1,234,567") == 1234567.0


def test_parse_percentage():
    """パーセンテージパースのテスト"""
    # 通常のパーセンテージ
    assert parse_percentage("12.34%") == 12.34
    assert parse_percentage("5.0") == 5.0

    # 無効な値
    assert parse_percentage("--") is None
    assert parse_percentage("") is None


def test_parse_spot_csv(tmp_path):
    """現物CSVパースのテスト"""
    # テストCSVファイル作成
    csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額,前日終値,前日比,前日比(%),評価額前日比,評価額前日比(%),決算月,貸株金利,騰落チャート(5分足),始値,高値,安値,売買代金(千円),出来高,予想PER(倍),実績PBR(倍),予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分,騰落チャート(日足)
,,,,7203,トヨタ自動車,東P,特定,100,--,"2,100","2,500",↑,"+40,000",+19.05%,"210,000","250,000","2,450",+50,+2.04%,"+5,000",+2.04%,3月,0.10%,,"2,480","2,510","2,470","10,000,000","4,000,000",10.5,1.2,3.5,238.1,"2,083.33",87.5,貸借,
,,,,6758,ソニーグループ,東P,NISA,200,--,"10,000","12,000",↓,"+400,000",+20.00%,"2,000,000","2,400,000","12,100",-100,-0.83%,"-20,000",-0.83%,3月,0.10%,,"12,050","12,100","11,950","20,000,000","1,700,000",15.2,2.5,1.0,789.5,"4,800.00",120,貸借,
"""
    csv_path = tmp_path / "test_spot.csv"
    csv_path.write_text(csv_content, encoding="utf-8-sig")

    # パース実行
    holdings = parse_spot_csv(str(csv_path))

    # 結果確認
    assert len(holdings) == 2

    # トヨタ自動車
    toyota = holdings[0]
    assert toyota["code"] == "7203"
    assert toyota["name"] == "トヨタ自動車"
    assert toyota["account_type"] == "特定"
    assert toyota["stock_type"] == "現物"
    assert toyota["quantity"] == 100
    assert toyota["average_price"] == 2100.0
    assert toyota["current_price"] == 2500.0
    assert toyota["profit_loss"] == 40000.0
    assert toyota["profit_loss_ratio"] == 19.05
    assert toyota["expected_per"] == 10.5
    assert toyota["actual_pbr"] == 1.2
    assert toyota["dividend_yield"] == 3.5
    assert toyota["expected_dividend"] == 87.5

    # ソニーグループ
    sony = holdings[1]
    assert sony["code"] == "6758"
    assert sony["name"] == "ソニーグループ"
    assert sony["account_type"] == "NISA"


def test_parse_margin_csv(tmp_path):
    """信用CSVパースのテスト"""
    # テストCSVファイル作成
    csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,建区分,期限,預り区分,建株数,注文株数,現在値,現在値,評価額,評価損益,評価損益(%),諸経費等,前日終値,前日比,前日比(%),評価額前日比,評価額前日比(%),騰落チャート(5分足),決算月,始値,高値,安値,売買代金(千円),出来高,貸株金利,予想PER(倍),実績PBR(倍),予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分,建日,騰落チャート(日足),逆日歩(日数)
,,,,6526,ソシオネクスト,東P,買建,６ヵ月,特定,"1,000",--,"2,600",→,"2,600,000","-57,919",-2.18%,"1,169","2,575",25,0.97%,"25,000",0.97%,,3月,"2,563.50","2,605","2,537","29,068,211","11,318,700",0.10%,35.04,3.37,1.92,74.2,770.79,50,貸借,2025/7/4,,--
,,,,5724,アサカ理研,東S,売建,無期限,特定,500,--,"1,200",↓,"600,000","+50,000",+9.09%,550,"1,220",-20,-1.64%,"-10,000",-1.64%,,9月,"1,230","1,250","1,190","100,000","85,000",2.00%,20.0,1.5,1.5,60.0,800.00,18,信用,2025/7/1,,--
"""
    csv_path = tmp_path / "test_margin.csv"
    csv_path.write_text(csv_content, encoding="utf-8-sig")

    # パース実行
    holdings = parse_margin_csv(str(csv_path))

    # 結果確認
    assert len(holdings) == 2

    # ソシオネクスト（買建）
    socio = holdings[0]
    assert socio["code"] == "6526"
    assert socio["name"] == "ソシオネクスト"
    assert socio["account_type"] == "特定"
    assert socio["stock_type"] == "信用"
    assert socio["trade_position"] == "買建"
    assert socio["margin_term"] == "６ヵ月"
    assert socio["quantity"] == 1000
    assert socio["current_price"] == 2600.0
    assert socio["profit_loss"] == -57919.0
    assert socio["acquisition_date"] == "2025/7/4"

    # アサカ理研（売建）
    asaka = holdings[1]
    assert asaka["code"] == "5724"
    assert asaka["name"] == "アサカ理研"
    assert asaka["trade_position"] == "売建"
    assert asaka["margin_term"] == "無期限"


@pytest.fixture
def test_db(tmp_path):
    """テスト用データベースのセットアップ"""
    db_path = tmp_path / "test.db"

    # テーブル作成
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL,
                stock_type TEXT DEFAULT '現物',
                trade_position TEXT,
                margin_term TEXT,
                quantity INTEGER NOT NULL,
                average_price REAL,
                current_price REAL,
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
                acquisition_date TEXT,
                updated_at TEXT DEFAULT (datetime('now')),
                deleted_at TEXT DEFAULT NULL
            )
        """
        )

    return db_path


@patch("src.portfolio.csv_importer.get_db_path")
def test_import_holdings_csv(mock_db_path, test_db, tmp_path):
    """CSVインポート統合テスト"""
    mock_db_path.return_value = str(test_db)

    # テストCSVファイル作成
    csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額,前日終値,前日比,前日比(%),評価額前日比,評価額前日比(%),決算月,貸株金利,騰落チャート(5分足),始値,高値,安値,売買代金(千円),出来高,予想PER(倍),実績PBR(倍),予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分,騰落チャート(日足)
,,,,7203,トヨタ自動車,東P,特定,100,--,"2,100","2,500",↑,"+40,000",+19.05%,"210,000","250,000","2,450",+50,+2.04%,"+5,000",+2.04%,3月,0.10%,,"2,480","2,510","2,470","10,000,000","4,000,000",10.5,1.2,3.5,238.1,"2,083.33",87.5,貸借,
"""
    csv_path = tmp_path / "test_import.csv"
    csv_path.write_text(csv_content, encoding="utf-8-sig")

    # インポート実行
    imported_count = import_holdings_csv(
        user_id=1, account_name="test_account", file_path=str(csv_path), csv_type="spot"
    )

    assert imported_count == 1

    # データベース確認
    with sqlite3.connect(test_db) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM holdings WHERE deleted_at IS NULL")
        rows = cursor.fetchall()

        assert len(rows) == 1

        # カラム名を取得
        col_names = [desc[0] for desc in cursor.description]
        holding = dict(zip(col_names, rows[0], strict=False))

        assert holding["user_id"] == 1
        assert holding["code"] == "7203"
        assert holding["account_name"] == "test_account"
        assert holding["account_type"] == "特定"
        assert holding["stock_type"] == "現物"
        assert holding["quantity"] == 100
        assert holding["average_price"] == 2100.0
        assert holding["current_price"] == 2500.0


@patch("src.portfolio.csv_importer.get_db_path")
def test_import_duplicate_handling(mock_db_path, test_db, tmp_path):
    """重複データの処理テスト"""
    mock_db_path.return_value = str(test_db)

    # 既存データを追加
    with sqlite3.connect(test_db) as conn:
        conn.execute(
            """
            INSERT INTO holdings (user_id, code, account_name, account_type,
                                stock_type, quantity, average_price)
            VALUES (1, '7203', 'test_account', '特定', '現物', 50, 2000)
        """
        )

    # 同じ銘柄を含むCSVをインポート
    csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額,前日終値,前日比,前日比(%),評価額前日比,評価額前日比(%),決算月,貸株金利,騰落チャート(5分足),始値,高値,安値,売買代金(千円),出来高,予想PER(倍),実績PBR(倍),予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分,騰落チャート(日足)
,,,,7203,トヨタ自動車,東P,特定,100,--,"2,100","2,500",↑,"+40,000",+19.05%,"210,000","250,000","2,450",+50,+2.04%,"+5,000",+2.04%,3月,0.10%,,"2,480","2,510","2,470","10,000,000","4,000,000",10.5,1.2,3.5,238.1,"2,083.33",87.5,貸借,
"""
    csv_path = tmp_path / "test_duplicate.csv"
    csv_path.write_text(csv_content, encoding="utf-8-sig")

    # インポート実行
    imported_count = import_holdings_csv(
        user_id=1, account_name="test_account", file_path=str(csv_path), csv_type="spot"
    )

    assert imported_count == 1

    # データベース確認（古いデータは論理削除され、新しいデータのみ有効）
    with sqlite3.connect(test_db) as conn:
        cursor = conn.cursor()

        # 有効なデータは1件のみ
        cursor.execute("SELECT * FROM holdings WHERE deleted_at IS NULL")
        active_rows = cursor.fetchall()
        assert len(active_rows) == 1

        # 数量が新しい値に更新されている
        assert active_rows[0][8] == 100  # quantity

        # 論理削除されたデータも含めると2件
        cursor.execute("SELECT * FROM holdings")
        all_rows = cursor.fetchall()
        assert len(all_rows) == 2
