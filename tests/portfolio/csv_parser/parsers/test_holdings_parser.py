"""portfolio.csv_parser.parsers.holdingsのテスト"""

import sys
from pathlib import Path

# プロジェクトのパスを追加
sys.path.append(str(Path(__file__).resolve().parents[5]))

from src.portfolio.csv_parser.parsers.holdings import HoldingsParser


class TestHoldingsParser:
    """HoldingsParserのテスト"""

    def test_parse_standard_format(self):
        """標準形式のCSV解析"""
        csv_content = """銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価損益,評価損益率(%)
1234,テスト会社A,東証プライム,100,1000,1100,10000,10.0
5678,テスト会社B,東証スタンダード,200,2000,1950,-10000,-2.5"""

        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2

        # 1件目の確認
        holding1 = result[0]
        assert holding1["code"] == "1234"
        assert holding1["name"] == "テスト会社A"
        assert holding1["quantity"] == 100
        assert holding1["average_price"] == 1000.0
        assert holding1["current_price"] == 1100.0
        assert holding1["profit_loss"] == 10000.0
        assert holding1["profit_loss_ratio"] == 10.0
        assert holding1["account_type"] == "特定"  # デフォルト

        # 2件目の確認
        holding2 = result[1]
        assert holding2["code"] == "5678"
        assert holding2["quantity"] == 200
        assert holding2["profit_loss"] == -10000.0
        assert holding2["profit_loss_ratio"] == -2.5

    def test_parse_with_account_type(self):
        """口座区分付きのCSV解析"""
        csv_content = """銘柄コード,銘柄名,口座区分,保有数量,取得単価
1234,テスト会社A,NISA,100,1000
5678,テスト会社B,特定,200,2000
9012,テスト会社C,つみたてNISA,50,3000
3456,テスト会社D,一般,150,1500"""

        result = HoldingsParser.parse(csv_content)

        assert len(result) == 4
        assert result[0]["account_type"] == "NISA"
        assert result[1]["account_type"] == "特定"
        assert result[2]["account_type"] == "つみたてNISA"
        assert result[3]["account_type"] == "一般"

    def test_parse_detailed_format(self):
        """詳細形式（保有証券_現物）のCSV解析"""
        csv_content = """﻿銘柄,コード/ティッカー,市場,銘柄（カナ）,現在値,現在値(基準日),前日比,前日比(%),出来高,単元株数,売却/購入,現物売,信用売,現物買,信用買,品貸,逆日歩(日歩),貸株,金利(年利),現買,現売,信買,信売,代用,掛目
テスト会社A,1234,東証P,テストガイシャA,"1,100",2024/01/15,+100,+10.00%,"100,000",100,,売却,--,購入,購入,不可,--,可,0.20%,100,0,0,0,0,80%
テスト会社B,5678,東証S,テストガイシャB,"2,000",2024/01/15,-50,-2.44%,"50,000",100,,売却,--,購入,購入,可,0.05,可,0.50%,200,0,0,0,0,70%"""

        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2

        # 1件目の確認
        holding1 = result[0]
        assert holding1["code"] == "1234"
        assert holding1["name"] == "テスト会社A"
        assert holding1["current_price"] == 1100.0
        assert holding1["quantity"] == 100  # 現買列から

        # 2件目の確認
        holding2 = result[1]
        assert holding2["code"] == "5678"
        assert holding2["current_price"] == 2000.0
        assert holding2["quantity"] == 200

    def test_parse_savefile_format(self):
        """SaveFile形式のCSV解析"""
        csv_content = """作成日：2024年01月15日
保有証券一覧

証券コード,銘柄名,市場,株数,取得単価,現在値,評価額,評価損益,損益率(%)
1234,テスト会社A,東証プライム,100,"1,000","1,100","110,000","10,000",10.00%
5678,テスト会社B,東証スタンダード,200,"2,000","1,950","390,000","-10,000",-2.50%

評価額合計,500,000
評価損益合計,0"""

        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2

        holding1 = result[0]
        assert holding1["code"] == "1234"
        assert holding1["quantity"] == 100
        assert holding1["average_price"] == 1000.0
        assert holding1["profit_loss_ratio"] == 10.0

    def test_parse_empty_content(self):
        """空のコンテンツ"""
        result = HoldingsParser.parse("")
        assert result == []

    def test_parse_header_only(self):
        """ヘッダーのみ"""
        csv_content = "銘柄コード,銘柄名,保有数量,取得単価"
        result = HoldingsParser.parse(csv_content)
        assert result == []

    def test_parse_invalid_data(self):
        """無効なデータのスキップ"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価
,名前なし,100,1000
1234,テスト会社,200,2000
invalid,無効データ,abc,xyz"""

        result = HoldingsParser.parse(csv_content)

        # 有効なデータ1件のみ
        assert len(result) == 1
        assert result[0]["code"] == "1234"
        assert result[0]["quantity"] == 200

    def test_parse_with_total_section(self):
        """合計セクション付きのCSV"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価,評価額
1234,テスト会社A,100,1000,110000
5678,テスト会社B,200,2000,390000
合計,,,合計,500000"""

        result = HoldingsParser.parse(csv_content)

        # 合計行は除外される
        assert len(result) == 2
        assert all(h["code"] in ["1234", "5678"] for h in result)

    def test_parse_column_variations(self):
        """カラム名のバリエーション"""
        csv_content = """コード,銘柄名,保有株数,平均取得単価,株価,評価損益額,損益率
1234,テスト会社,100,1000,1100,10000,10.0"""

        result = HoldingsParser.parse(csv_content)

        assert len(result) == 1
        holding = result[0]
        assert holding["code"] == "1234"
        assert holding["quantity"] == 100  # 保有株数から
        assert holding["average_price"] == 1000.0  # 平均取得単価から
        assert holding["current_price"] == 1100.0  # 株価から

    def test_parse_with_lending_info(self):
        """貸株情報付きのCSV"""
        csv_content = """銘柄コード,銘柄名,保有数量,貸株数量,取得単価
1234,テスト会社A,100,50,1000
5678,テスト会社B,200,0,2000"""

        result = HoldingsParser.parse(csv_content)

        assert len(result) == 2
        assert result[0]["lending_quantity"] == 50
        assert result[1]["lending_quantity"] == 0

    def test_normalize_code(self):
        """銘柄コードの正規化"""
        csv_content = """銘柄コード,銘柄名,保有数量
01234,テスト会社A,100
5678T,テスト会社B,200
9012.T,テスト会社C,300"""

        result = HoldingsParser.parse(csv_content)

        # 先頭の0やサフィックスが除去される
        assert result[0]["code"] == "1234"
        assert result[1]["code"] == "5678"
        assert result[2]["code"] == "9012"

    def test_parse_percentage_values(self):
        """パーセンテージ値の処理"""
        csv_content = """銘柄コード,銘柄名,保有数量,評価損益率(%)
1234,テスト会社A,100,10.5%
5678,テスト会社B,200,-2.3%
9012,テスト会社C,300,0.0"""

        result = HoldingsParser.parse(csv_content)

        assert result[0]["profit_loss_ratio"] == 10.5
        assert result[1]["profit_loss_ratio"] == -2.3
        assert result[2]["profit_loss_ratio"] == 0.0

    def test_parse_with_commas_in_numbers(self):
        """数値にカンマが含まれる場合"""
        csv_content = '''銘柄コード,銘柄名,保有数量,取得単価,評価額
1234,テスト会社,100,"1,000","110,000"
5678,大量保有,"10,000",500,"5,000,000"'''

        result = HoldingsParser.parse(csv_content)

        assert result[0]["average_price"] == 1000.0
        assert result[0]["market_value"] == 110000.0
        assert result[1]["quantity"] == 10000
        assert result[1]["market_value"] == 5000000.0

    def test_edge_cases(self):
        """エッジケース"""
        # BOM付きCSV
        csv_content_bom = "\ufeff銘柄コード,銘柄名,保有数量\n1234,テスト,100"
        result = HoldingsParser.parse(csv_content_bom)
        assert len(result) == 1
        assert result[0]["code"] == "1234"

        # 空白行を含むCSV
        csv_content_blank = """銘柄コード,銘柄名,保有数量
1234,テスト会社A,100

5678,テスト会社B,200"""
        result = HoldingsParser.parse(csv_content_blank)
        assert len(result) == 2

        # 余分な空白
        csv_content_spaces = """銘柄コード,銘柄名,保有数量
  1234  ,  テスト会社  ,  100  """
        result = HoldingsParser.parse(csv_content_spaces)
        assert result[0]["code"] == "1234"
        assert result[0]["name"] == "テスト会社"
        assert result[0]["quantity"] == 100
