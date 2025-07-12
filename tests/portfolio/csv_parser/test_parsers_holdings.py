"""保有銘柄CSV解析モジュールのテスト"""

from src.portfolio.csv_parser.parsers.holdings import HoldingsParser


class TestHoldingsParser:
    """HoldingsParserのテスト"""

    def test_parse_standard_format_basic(self):
        """標準形式の基本的な解析テスト"""
        csv_content = """銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価額,評価損益,評価損益率(%)
1234,テスト株式会社,東証,100,1000,1100,110000,10000,10.0
5678,サンプル株式会社,東証,200,2000,1900,380000,-20000,-5.0
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2

        # 1銘柄目の検証
        assert result[0]["code"] == "1234"
        assert result[0]["name"] == "テスト株式会社"
        assert result[0]["quantity"] == 100
        assert result[0]["average_price"] == 1000.0
        assert result[0]["current_price"] == 1100.0
        assert result[0]["profit_loss"] == 10000.0
        assert result[0]["profit_loss_ratio"] == 10.0
        assert result[0]["account_type"] == "特定"  # デフォルト

        # 2銘柄目の検証
        assert result[1]["code"] == "5678"
        assert result[1]["quantity"] == 200
        assert result[1]["profit_loss"] == -20000.0

    def test_parse_standard_format_with_account_type(self):
        """標準形式の口座区分ありの解析テスト"""
        csv_content = """銘柄コード,銘柄名,口座区分,保有数量,取得単価,現在値,評価額,評価損益,評価損益率(%)
1234,テスト株式会社,特定,100,1000,1100,110000,10000,10.0
5678,サンプル株式会社,NISA,200,2000,1900,380000,-20000,-5.0
9999,つみたて銘柄,つみたてNISA,300,3000,3100,930000,30000,3.33
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 3
        assert result[0]["account_type"] == "特定"
        assert result[1]["account_type"] == "NISA"
        assert result[2]["account_type"] == "つみたてNISA"

    def test_parse_standard_format_column_variations(self):
        """標準形式のカラム名バリエーションテスト"""
        csv_content = """コード,銘柄名,預り,数量,平均取得単価,株価,時価評価額,損益,損益率(%)
1234,テスト株式会社,一般,100,1000,1100,110000,10000,10.0
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 1
        assert result[0]["code"] == "1234"
        assert result[0]["quantity"] == 100
        assert result[0]["average_price"] == 1000.0
        assert result[0]["current_price"] == 1100.0
        assert result[0]["account_type"] == "一般"

    def test_parse_savefile_format_stocks(self):
        """SaveFile形式の株式解析テスト"""
        csv_content = """保有証券一覧
株式（特定預り）
"1234","テスト株式会社","100","株","1,000.00","1,100.00","1,100.00","110,000","10,000"
"5678","サンプル株式会社","200","株","2,000.00","1,900.00","1,900.00","380,000","-20,000"

株式（NISA預り（成長投資枠））
"9999","NISA銘柄","300","株","3,000.00","3,100.00","3,100.00","930,000","30,000"

評価額合計
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 3

        # 特定預りの銘柄
        assert result[0]["code"] == "1234"
        assert result[0]["account_type"] == "特定"
        assert result[0]["quantity"] == 100
        assert result[0]["average_price"] == 1000.0

        assert result[1]["code"] == "5678"
        assert result[1]["account_type"] == "特定"

        # NISA預りの銘柄
        assert result[2]["code"] == "9999"
        assert result[2]["account_type"] == "NISA"
        assert result[2]["quantity"] == 300

    def test_parse_savefile_format_funds(self):
        """SaveFile形式の投資信託解析テスト"""
        csv_content = """保有証券一覧
投資信託（口数/特定預り）
"テスト投資信託","10,000口","10,000","1.5000","1.6000","1.6000","16,000","1,000"

投資信託（口数/つみたてNISA預り）
"つみたて投信","20,000口","20,000","2.0000","2.1000","2.1000","42,000","2,000"

評価額合計
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2

        # 特定預りの投資信託
        assert result[0]["fund_name"] == "テスト投資信託"
        assert result[0]["account_type"] == "特定"
        assert result[0]["quantity"] == 10000
        assert result[0]["average_price"] == 1.5
        assert result[0]["is_fund"] is True
        assert result[0]["code"] is None

        # つみたてNISAの投資信託
        assert result[1]["fund_name"] == "つみたて投信"
        assert result[1]["account_type"] == "つみたてNISA"
        assert result[1]["quantity"] == 20000

    def test_parse_detailed_format_basic(self):
        """詳細形式の基本的な解析テスト"""
        # 特殊な形式：銘柄が複数回出現
        csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,取得単価,現在値,評価額,評価損益,評価損益(%)
,,,,"1234","テスト株式会社",,特定,100,1000,1100,110000,10000,10.0
,,,,"5678","サンプル株式会社",,NISA,200,2000,1900,380000,-20000,-5.0
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2
        assert result[0]["code"] == "1234"
        assert result[0]["name"] == "テスト株式会社"
        assert result[0]["account_type"] == "特定"
        assert result[0]["quantity"] == 100

        assert result[1]["code"] == "5678"
        assert result[1]["account_type"] == "NISA"

    def test_parse_detailed_format_with_indicators(self):
        """詳細形式の株価指標付き解析テスト"""
        csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,取得単価,現在値,評価額,評価損益,評価損益(%),予想PER,実績PBR,配当利回り,予想1株配当,予想EPS,実績BPS,貸借区分
,,,,"1234","テスト株式会社",,特定,100,1000,1100,110000,10000,10.0,15.5,1.2,2.5,25.0,71.0,917.0,一般
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 1
        assert result[0]["code"] == "1234"
        assert result[0]["expected_per"] == 15.5
        assert result[0]["actual_pbr"] == 1.2
        assert result[0]["dividend_yield"] == 2.5
        assert result[0]["expected_dividend"] == 25.0
        assert result[0]["expected_eps"] == 71.0
        assert result[0]["actual_bps"] == 917.0
        assert result[0]["lending_type"] == "一般"

    def test_parse_empty_csv(self):
        """空のCSVの解析テスト"""
        csv_content = ""
        result = HoldingsParser.parse(csv_content)
        assert result == []

    def test_parse_header_only_csv(self):
        """ヘッダーのみのCSVの解析テスト"""
        csv_content = "銘柄コード,銘柄名,保有数量,取得単価\n"
        result = HoldingsParser.parse(csv_content)
        assert result == []

    def test_parse_invalid_data_skip(self):
        """無効なデータ行のスキップテスト"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価
,名前なし銘柄,100,1000
1234,テスト株式会社,,1000
5678,サンプル株式会社,200,2000
"""
        result = HoldingsParser.parse(csv_content)

        # 銘柄コードなしと数量なしの行はスキップされる
        assert len(result) == 1
        assert result[0]["code"] == "5678"

    def test_parse_with_bom(self):
        """BOM付きCSVの解析テスト（詳細形式として判定）"""
        csv_content = """﻿銘柄コード,銘柄名,保有数量,取得単価
1234,テスト株式会社,100,1000
"""
        # BOMがあると詳細形式と判定される
        result = HoldingsParser.parse(csv_content)
        # 詳細形式として処理され、正しく解析される
        assert len(result) == 1
        assert result[0]["code"] == "1234"
        assert result[0]["name"] == "テスト株式会社"
        assert result[0]["quantity"] == 100
        assert result[0]["average_price"] == 1000

    def test_parse_numeric_values_with_comma(self):
        """カンマ区切り数値の解析テスト"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価,評価損益
1234,テスト株式会社,"1,000","2,000.50","10,000"
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 1
        assert result[0]["quantity"] == 1000
        assert result[0]["average_price"] == 2000.50
        assert result[0]["profit_loss"] == 10000

    def test_parse_mixed_format(self):
        """混在形式のテスト（セクション判定）"""
        csv_content = """保有証券一覧
株式（特定預り）
"1234","テスト株式会社","100","株","1,000.00","1,100.00","1,100.00","110,000","10,000"

投資信託（口数/特定預り）
"テスト投資信託","10,000口","10,000","1.5000","1.6000","1.6000","16,000","1,000"
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2
        # 株式
        assert result[0]["code"] == "1234"
        assert result[0]["is_fund"] is False
        # 投資信託
        assert result[1]["fund_name"] == "テスト投資信託"
        assert result[1]["is_fund"] is True

    def test_parse_malformed_csv_error(self):
        """不正なCSVフォーマットの処理テスト"""
        csv_content = """銘柄コード,銘柄名,保有数量
1234,"引用符が閉じていない,100
"""
        # 不正なCSVでも処理が継続される（エラーハンドリングが改善）
        result = HoldingsParser.parse(csv_content)
        # 不正な行はスキップされる
        assert len(result) == 0

    def test_parse_code_normalization(self):
        """銘柄コード正規化のテスト"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価
123,短いコード,100,1000
12345,長いコード,200,2000
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2
        # 短いコードは4桁に正規化される
        assert result[0]["code"] == "0123"
        # 5桁コードはそのまま
        assert result[1]["code"] == "12345"

    def test_parse_旧nisa_account_type(self):
        """旧NISA口座タイプの解析テスト"""
        csv_content = """銘柄コード,銘柄名,口座区分,保有数量,取得単価
1234,テスト株式会社,旧NISA,100,1000
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 1
        assert result[0]["account_type"] == "旧NISA"

    def test_parse_fund_quantity_without_unit(self):
        """投資信託の口数解析（単位なし）テスト"""
        csv_content = """保有証券一覧
投資信託（口数/特定預り）
"テスト投資信託","10000","10,000","1.5000","1.6000","1.6000","16,000","1,000"
"""
        result = HoldingsParser.parse(csv_content)

        assert len(result) == 1
        assert result[0]["quantity"] == 10000

    def test_parse_fund_invalid_quantity_skip(self):
        """投資信託の無効な口数のスキップテスト"""
        csv_content = """保有証券一覧
投資信託（口数/特定預り）
"無効な投資信託","","10,000","1.5000","1.6000","1.6000","16,000","1,000"
"ゼロ口投資信託","0口","10,000","1.5000","1.6000","1.6000","16,000","1,000"
"有効な投資信託","5000口","10,000","1.5000","1.6000","1.6000","16,000","1,000"
"""
        result = HoldingsParser.parse(csv_content)

        # 口数が空またはゼロの投資信託はスキップされる
        assert len(result) == 1
        assert result[0]["fund_name"] == "有効な投資信託"
        assert result[0]["quantity"] == 5000
