"""標準形式の取引履歴CSVパーサーのテスト"""

import pytest

from src.portfolio.csv_parser import SBICSVParser


def test_parse_transactions_standard_format_with_detailed_types():
    """標準形式の取引履歴CSVで詳細タイプが正しく判定されることを確認"""
    csv_content = """銘柄コード,銘柄名,売買区分,約定日,数量,約定単価,手数料,税金,受渡金額,備考
7203,トヨタ自動車,買付,2024/01/15,100,2500.00,250,25,250275,
7201,日畓自動車,売却,2024/01/16,200,1100.00,100,10,219890,
7267,ホンダ,現物買,2024/01/17,50,3000.00,150,15,150165,
7203,トヨタ自動車,現物売,2024/01/18,100,2600.00,250,25,259725,
7268,スズキ,信用新規買,2024/01/19,300,1500.00,100,10,450110,
7268,スズキ,信用返済売,2024/01/20,300,1600.00,100,10,479890,
"""

    transactions = SBICSVParser.parse_transactions_csv(csv_content)

    assert len(transactions) == 6

    # 買付 = 新規買い
    assert transactions[0]["code"] == "7203"
    assert transactions[0]["transaction_type"] == "buy"
    assert transactions[0]["detailed_type"] == "新規買い"

    # 売却 = 決済売り
    assert transactions[1]["code"] == "7201"
    assert transactions[1]["transaction_type"] == "sell"
    assert transactions[1]["detailed_type"] == "決済売り"

    # 現物買 = 新規買い
    assert transactions[2]["code"] == "7267"
    assert transactions[2]["transaction_type"] == "buy"
    assert transactions[2]["detailed_type"] == "新規買い"

    # 現物売 = 決済売り
    assert transactions[3]["code"] == "7203"
    assert transactions[3]["transaction_type"] == "sell"
    assert transactions[3]["detailed_type"] == "決済売り"

    # 信用新規買 = 新規買い
    assert transactions[4]["code"] == "7268"
    assert transactions[4]["transaction_type"] == "buy"
    assert transactions[4]["detailed_type"] == "新規買い"

    # 信用返済売 = 決済売り
    assert transactions[5]["code"] == "7268"
    assert transactions[5]["transaction_type"] == "sell"
    assert transactions[5]["detailed_type"] == "決済売り"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
