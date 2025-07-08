"""口座タイプ（NISA/特定）とヘッダー動的解析のテスト"""

from src.portfolio.csv_parser import SBICSVParser


class TestAccountTypeAndDynamicColumns:
    """口座タイプと動的カラム位置のテスト"""

    def test_parse_holdings_with_account_type(self):
        """標準形式で口座タイプを含むCSVの解析"""
        csv_content = """銘柄コード,銘柄名,口座区分,保有数量,取得単価,現在値,評価額,評価損益,評価損益率(%)
9984,ソフトバンクグループ,特定,100,1000,1500,150000,50000,50.0
9984,ソフトバンクグループ,NISA,50,1200,1500,75000,15000,25.0
7203,トヨタ自動車,つみたてNISA,200,2000,2500,500000,100000,25.0
"""
        results = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(results) == 3

        # 1つ目：特定口座のソフトバンク
        assert results[0]["code"] == "9984"
        assert results[0]["account_type"] == "特定"
        assert results[0]["quantity"] == 100

        # 2つ目：NISA口座のソフトバンク
        assert results[1]["code"] == "9984"
        assert results[1]["account_type"] == "NISA"
        assert results[1]["quantity"] == 50

        # 3つ目：つみたてNISA口座のトヨタ
        assert results[2]["code"] == "7203"
        assert results[2]["account_type"] == "つみたてNISA"
        assert results[2]["quantity"] == 200

    def test_parse_holdings_dynamic_column_order(self):
        """カラム順序が異なるCSVの解析"""
        csv_content = """保有株数,銘柄名,評価損益,銘柄コード,取得単価,評価額,現在値,預り区分
100,ソフトバンクグループ,50000,9984,1000,150000,1500,特定預り
50,ソフトバンクグループ,15000,9984,1200,75000,1500,NISA預り
"""
        results = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(results) == 2

        # カラム順序が変わってもデータが正しく取得されることを確認
        assert results[0]["code"] == "9984"
        assert results[0]["quantity"] == 100
        assert results[0]["account_type"] == "特定"
        assert results[0]["average_price"] == 1000

        assert results[1]["code"] == "9984"
        assert results[1]["quantity"] == 50
        assert results[1]["account_type"] == "NISA"
        assert results[1]["average_price"] == 1200

    def test_parse_holdings_detailed_format_with_headers(self):
        """詳細形式でヘッダーベースの解析"""
        csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄コード,銘柄名,市場,預り,業種,保有株数,参考単価,取得単価,現在値,前日比,前日比(%),評価損益,評価損益率(%),前日比騰落率,評価額,売却可能数量,買付余力,譲渡益税額概算,年初来高値,年初来高値日付,年初来安値,年初来安値日付,最終約定日時,予想PER,実績PBR,予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分
,,,,9984,ソフトバンクグループ,東証プライム,特定預り,情報・通信業,100,1000,1000,1500,50,3.45,50000,50.0,3.45,150000,100,0,10000,1600,2024/01/15,900,2024/03/20,2024/12/25,15.2,1.8,2.5,98.68,833.33,37.5,貸借
,,,,9984,ソフトバンクグループ,東証プライム,NISA預り,情報・通信業,50,1200,1200,1500,50,3.45,15000,25.0,3.45,75000,50,0,0,1600,2024/01/15,900,2024/03/20,2024/12/25,15.2,1.8,2.5,98.68,833.33,37.5,貸借
"""
        results = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(results) == 2

        # ヘッダーベースで正しく解析されることを確認
        assert results[0]["code"] == "9984"
        assert results[0]["account_type"] == "特定"
        assert results[0]["quantity"] == 100
        assert results[0]["average_price"] == 1000

        assert results[1]["code"] == "9984"
        assert results[1]["account_type"] == "NISA"
        assert results[1]["quantity"] == 50
        assert results[1]["average_price"] == 1200

    def test_parse_holdings_without_account_type(self):
        """口座タイプカラムがない場合のデフォルト処理"""
        csv_content = """銘柄コード,銘柄名,保有数量,取得単価,現在値,評価額,評価損益,評価損益率(%)
9984,ソフトバンクグループ,100,1000,1500,150000,50000,50.0
7203,トヨタ自動車,200,2000,2500,500000,100000,25.0
"""
        results = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(results) == 2

        # 口座タイプがない場合はデフォルトで「特定」になる
        assert results[0]["account_type"] == "特定"
        assert results[1]["account_type"] == "特定"

    def test_parse_holdings_various_account_types(self):
        """様々な口座タイプ表記の認識"""
        csv_content = """銘柄コード,銘柄名,預り,保有数量,取得単価
1001,銘柄A,特定,100,1000
1002,銘柄B,特定預り,100,1000
1003,銘柄C,NISA,100,1000
1004,銘柄D,NISA預り,100,1000
1005,銘柄E,つみたてNISA,100,1000
1006,銘柄F,一般,100,1000
1007,銘柄G,一般預り,100,1000
"""
        results = SBICSVParser.parse_holdings_csv(csv_content)

        assert len(results) == 7
        assert results[0]["account_type"] == "特定"
        assert results[1]["account_type"] == "特定"
        assert results[2]["account_type"] == "NISA"
        assert results[3]["account_type"] == "NISA"
        assert results[4]["account_type"] == "つみたてNISA"
        assert results[5]["account_type"] == "一般"
        assert results[6]["account_type"] == "一般"
