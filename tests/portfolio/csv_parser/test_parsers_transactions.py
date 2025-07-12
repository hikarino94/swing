"""取引履歴CSV解析モジュールのテスト"""

import pytest

from src.portfolio.csv_parser.parsers.transactions import (
    TransactionsParser,
    parse_trade_type,
)


class TestParseTradeType:
    """parse_trade_type関数のテスト"""

    def test_parse_trade_type_buy_patterns(self):
        """買い取引パターンのテスト"""
        assert parse_trade_type("現物買") == ("buy", "新規買い")
        assert parse_trade_type("株式現物買") == ("buy", "新規買い")
        assert parse_trade_type("信用新規買") == ("buy", "新規買い")
        assert parse_trade_type("買付") == ("buy", "新規買い")
        assert parse_trade_type("信用返済買") == ("buy", "決済買い")
        assert parse_trade_type("信用決済買") == ("buy", "決済買い")

    def test_parse_trade_type_sell_patterns(self):
        """売り取引パターンのテスト"""
        assert parse_trade_type("現物売") == ("sell", "決済売り")
        assert parse_trade_type("株式現物売") == ("sell", "決済売り")
        assert parse_trade_type("信用新規売") == ("sell", "新規売り")
        assert parse_trade_type("売付") == ("sell", "決済売り")
        assert parse_trade_type("信用返済売") == ("sell", "決済売り")
        assert parse_trade_type("信用決済売") == ("sell", "決済売り")

    def test_parse_trade_type_skip_patterns(self):
        """スキップパターンのテスト"""
        assert parse_trade_type("現引き") == ("skip", "現引き")
        assert parse_trade_type("投信買付") == ("skip", "投資信託")
        assert parse_trade_type("投資信託売却") == ("skip", "投資信託")

    def test_parse_trade_type_default(self):
        """デフォルト動作のテスト"""
        assert parse_trade_type("") == ("buy", "新規買い")
        assert parse_trade_type("不明な取引") == ("buy", "新規買い")


class TestTransactionsParser:
    """TransactionsParserのテスト"""

    def test_parse_standard_format(self):
        """標準形式の解析テスト"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額
2024-01-10,1234,テスト株式会社,買,100,1000,100,0,100100
2024-01-20,1234,テスト株式会社,売,50,1200,100,1000,58900
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 2

        # 買い取引
        assert result[0]["code"] == "1234"
        assert result[0]["transaction_date"] == "2024-01-10"
        assert result[0]["transaction_type"] == "buy"
        assert result[0]["detailed_type"] == "新規買い"
        assert result[0]["quantity"] == 100
        assert result[0]["price"] == 1000.0
        assert result[0]["commission"] == 100.0
        assert result[0]["tax"] == 0.0
        assert result[0]["total_amount"] == 100100.0

        # 売り取引
        assert result[1]["transaction_type"] == "sell"
        assert result[1]["detailed_type"] == "決済売り"
        assert result[1]["quantity"] == 50
        assert result[1]["tax"] == 1000.0

    def test_parse_order_list_format_new(self):
        """注文一覧形式（新フォーマット）の解析テスト"""
        csv_content = """銘柄（コード）,銘柄名,市場,取引区分,信用区分,弁済区分,約定日,受渡日,株数,平均約定単価,手数料,税額,受渡金額・決済損益,入金額
1234,テスト株式会社,東証,現物買,,,2024-01-10,2024-01-13,100,1000.00,100,0,100100,
1234,テスト株式会社,東証,現物売,,,2024-01-20,2024-01-23,50,1200.00,100,1000,58900,58900
5678,サンプル株式会社,東証,信用新規買,制度,,2024-01-15,2024-01-18,200,2000.00,200,0,400200,
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 3

        # 現物買い
        assert result[0]["code"] == "1234"
        assert result[0]["transaction_type"] == "buy"
        assert result[0]["remarks"] == ""

        # 現物売り
        assert result[1]["transaction_type"] == "sell"
        assert (
            result[1]["realized_profit"] is None
        )  # 注文一覧形式では決済損益は取得できない

        # 信用新規買い
        assert result[2]["code"] == "5678"
        assert result[2]["transaction_type"] == "buy"
        assert result[2]["detailed_type"] == "新規買い"
        assert result[2]["remarks"] == "信用"

    def test_parse_savefile_format(self):
        """SaveFile形式の解析テスト"""
        csv_content = """約定履歴照会
期間：2024/01/01～2024/01/31

約定日,銘柄,銘柄コード,市場,取引,期限,預り,課税,約定数量,約定単価,手数料,税額,受渡日,受渡金額/決済損益
2024/01/10,テスト株式会社,1234,東証,現物買,,,特定,100,1000.00,100,0,2024/01/13,100100
2024/01/20,テスト株式会社,1234,東証,信用決済売,,,特定,50,1200.00,100,1000,2024/01/23,10000
2024/01/15,現引き銘柄,9999,東証,現引き,,,特定,100,1500.00,0,0,2024/01/18,150000

(注)上記データは参考値です
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 2  # 現引きはスキップされる

        # 現物買い
        assert result[0]["code"] == "1234"
        assert result[0]["transaction_type"] == "buy"
        assert result[0]["detailed_type"] == "新規買い"

        # 信用決済売り（決済損益あり）
        assert result[1]["code"] == "1234"
        assert result[1]["transaction_type"] == "sell"
        assert result[1]["detailed_type"] == "決済売り"
        assert result[1]["realized_profit"] == 10000.0
        assert result[1]["remarks"] == "信用"

    def test_parse_order_list_format_old(self):
        """注文一覧形式（旧フォーマット）の解析テスト"""
        csv_content = """銘柄（コード）,銘柄名,市場,取引区分,信用区分,弁済区分,約定日,受渡日,株数,平均約定単価,手数料・諸経費等,課税額・譲渡益税,受渡金額・決済損益
1234,テスト株式会社,東証,現物買,,,2024-01-10,2024-01-13,100,1000.00,100,0,100100
1234,テスト株式会社,東証,信用決済売,制度,,2024-01-20,2024-01-23,50,1200.00,100,1000,5000
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 2

        # 現物買い
        assert result[0]["transaction_type"] == "buy"
        assert result[0]["commission"] == 100.0
        assert result[0]["tax"] == 0.0

        # 信用決済売り（決済損益あり）
        assert result[1]["transaction_type"] == "sell"
        assert result[1]["detailed_type"] == "決済売り"
        assert result[1]["realized_profit"] == 5000.0

    def test_parse_skip_fund_transactions(self):
        """投資信託取引のスキップテスト"""
        csv_content = """銘柄（コード）,銘柄名,市場,取引区分,信用区分,弁済区分,約定日,受渡日,株数,平均約定単価,手数料,税額,受渡金額・決済損益,入金額
,テスト投資信託,,投信買付,,,2024-01-10,2024-01-13,10000,1.5000,0,0,15000,
1234,テスト株式会社,東証,現物買,,,2024-01-10,2024-01-13,100,1000.00,100,0,100100,
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 1  # 投資信託はスキップ
        assert result[0]["code"] == "1234"

    def test_parse_empty_csv(self):
        """空のCSVの解析テスト"""
        csv_content = ""
        result = TransactionsParser.parse(csv_content)
        assert result == []

    def test_parse_header_only_csv(self):
        """ヘッダーのみのCSVの解析テスト"""
        csv_content = "約定日,銘柄コード,銘柄名,売買区分,数量\n"
        result = TransactionsParser.parse(csv_content)
        assert result == []

    def test_parse_unsupported_format(self):
        """未対応形式の解析テスト"""
        csv_content = """日付,商品,個数,価格
2024-01-10,商品A,10,1000
"""
        result = TransactionsParser.parse(csv_content)
        assert result == []

    def test_parse_numeric_values_with_comma(self):
        """カンマ区切り数値の解析テスト"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額
2024-01-10,1234,テスト株式会社,買,"1,000","2,000.50","1,100","2,200","2,003,800"
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 1
        assert result[0]["quantity"] == 1000
        assert result[0]["price"] == 2000.50
        assert result[0]["commission"] == 1100.0
        assert result[0]["tax"] == 2200.0
        assert result[0]["total_amount"] == 2003800.0

    def test_parse_calculated_total_amount(self):
        """受渡金額の計算テスト"""
        csv_content = """銘柄（コード）,銘柄名,市場,取引区分,信用区分,弁済区分,約定日,受渡日,株数,平均約定単価,手数料,税額,受渡金額・決済損益,入金額
1234,テスト株式会社,東証,現物買,,,2024-01-10,2024-01-13,100,1000.00,100,0,,
1234,テスト株式会社,東証,現物売,,,2024-01-20,2024-01-23,50,1200.00,100,1000,,58900
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 2
        # 買い: 100 * 1000 + 100 + 0 = 100100
        assert result[0]["total_amount"] == 100100.0
        # 売り: 50 * 1200 - 100 - 1000 = 58900
        assert result[1]["total_amount"] == 58900.0

    def test_parse_short_selling_transactions(self):
        """空売り取引の解析テスト"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額,備考
2024-01-10,1234,テスト株式会社,売,100,1000,100,0,99900,
2024-01-20,1234,テスト株式会社,買,100,900,100,0,90100,
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 2
        # 新規売り（空売り）
        assert result[0]["transaction_type"] == "sell"
        assert (
            result[0]["detailed_type"] == "決済売り"
        )  # 標準形式では新規売りと判定できない
        # 決済買い（空売りの決済）
        assert result[1]["transaction_type"] == "buy"
        assert (
            result[1]["detailed_type"] == "新規買い"
        )  # 標準形式では決済買いと判定できない

    def test_parse_invalid_csv_error(self):
        """不正なCSVフォーマットのエラーテスト"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量
2024-01-10,1234,"引用符が閉じていない,買,100
"""
        with pytest.raises(ValueError) as exc_info:
            TransactionsParser.parse(csv_content)
        assert "CSVファイルの解析に失敗しました" in str(exc_info.value)

    def test_parse_code_normalization(self):
        """銘柄コード正規化のテスト"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額
2024-01-10,123,短いコード,買,100,1000,0,0,100000
2024-01-20,12345,長いコード,売,200,2000,0,0,400000
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 2
        # 短いコードは4桁に正規化される
        assert result[0]["code"] == "0123"
        # 5桁コードはそのまま
        assert result[1]["code"] == "12345"

    def test_parse_date_sorting(self):
        """日付ソートのテスト（標準形式）"""
        csv_content = """約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額
2024-01-20,1234,テスト株式会社,売,50,1200,0,0,60000
2024-01-10,1234,テスト株式会社,買,100,1000,0,0,100000
2024-01-15,1234,テスト株式会社,買,50,1100,0,0,55000
"""
        result = TransactionsParser.parse(csv_content)

        assert len(result) == 3
        # 日付順にソートされている
        assert result[0]["transaction_date"] == "2024-01-10"
        assert result[1]["transaction_date"] == "2024-01-15"
        assert result[2]["transaction_date"] == "2024-01-20"
