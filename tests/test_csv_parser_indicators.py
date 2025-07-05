"""CSVパーサーの株価指標読み込みテスト"""

import pytest

from src.portfolio.csv_parser import SBICSVParser


def test_parse_holdings_with_indicators():
    """保有証券_現物形式のCSVから株価指標を正しく読み込めることを確認"""

    # サンプルCSVデータ（実際のフォーマットに基づく）
    csv_content = """銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,銘柄,預り区分,保有株数,注文株数,取得単価,現在値,現在値,評価損益,評価損益(%),買付金額,評価額,基準値,基準値比,基準値比(%),決算月,貸株金利,始値,高値,安値,売買代金(千円),出来高,予想PER(倍),実績PBR(倍),予想配当利回り(%),予想EPS,実績BPS,予想1株配当,貸借区分,騰落チャート(日足)
,,,,2914,日本たばこ産業,東P,旧NISA,35,--,"3,155","4,210",↑,"+36,925",+33.44%,"110,425","147,350","4,218",-8,-0.19%,12月,0.10%,"4,198","4,210","4,198",840,200,16.07,1.98,4.61,261.9,"2,121.33",194 ~ 200,貸借,
,,,,7267,本田技研工業,東P,NISA,100,--,"1,476","1,445",↑,"-3,100",-2.10%,"147,600","144,500","1,443",+2,+0.14%,3月,0.10%,"1,445","1,445","1,445",144,100,13.85,0.51,4.84,104.3,"2,835.96",70 ~ 80,貸借,"""

    # パーサーを実行
    holdings = SBICSVParser.parse_holdings_csv(csv_content)

    # 基本的な解析結果の確認
    assert len(holdings) == 2

    # 1つ目の銘柄（JT）の確認
    jt = holdings[0]
    assert jt["code"] == "2914"
    assert jt["name"] == "日本たばこ産業"
    assert jt["quantity"] == 35
    assert jt["average_price"] == 3155
    assert jt["current_price"] == 4210

    # 株価指標の確認
    assert jt["expected_per"] == 16.07
    assert jt["actual_pbr"] == 1.98
    assert jt["dividend_yield"] == 4.61
    assert jt["expected_eps"] == 261.9
    assert jt["actual_bps"] == 2121.33
    assert (
        jt["expected_dividend"] is None
    )  # "194 ~ 200"という形式は数値として解析できない
    assert jt["lending_type"] == "貸借"

    # 2つ目の銘柄（ホンダ）の確認
    honda = holdings[1]
    assert honda["code"] == "7267"
    assert honda["expected_per"] == 13.85
    assert honda["actual_pbr"] == 0.51
    assert honda["dividend_yield"] == 4.84


def test_parse_holdings_without_indicators():
    """株価指標が含まれない標準形式のCSVも正しく読み込めることを確認"""

    csv_content = """銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価損益,評価損益率(%)
7203,トヨタ自動車,東証プライム,100,2500,2800,30000,12.0
9984,ソフトバンクグループ,東証プライム,50,8000,7500,-25000,-6.25"""

    holdings = SBICSVParser.parse_holdings_csv(csv_content)

    assert len(holdings) == 2
    assert holdings[0]["code"] == "7203"
    assert holdings[0]["quantity"] == 100
    # 株価指標は含まれていないのでNoneまたは空
    assert "expected_per" not in holdings[0]


def test_parse_number_with_range():
    """範囲表記（例: "194 ~ 200"）の数値解析"""

    # 通常の数値
    assert SBICSVParser._parse_number("16.07") == 16.07
    assert SBICSVParser._parse_number("1.98") == 1.98

    # カンマ付き数値
    assert SBICSVParser._parse_number("2,121.33") == 2121.33

    # 範囲表記は現在の実装ではNoneを返す
    assert SBICSVParser._parse_number("194 ~ 200") is None

    # 空値
    assert SBICSVParser._parse_number("--") is None
    assert SBICSVParser._parse_number("") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
