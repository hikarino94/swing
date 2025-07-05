"""取引タイプの詳細判定のテスト"""

import pytest

from src.portfolio.csv_parser import SBICSVParser


def test_parse_transactions_savefile_format_detailed_types():
    """SaveFile形式の取引履歴CSVで詳細タイプが正しく判定されることを確認"""
    csv_content = """
"約定履歴照会"

"約定日","銘柄","銘柄コード","市場","取引","約定価格","約定数量","手数料/諸経費等","税額","受渡日","受渡金額/決済損益","約定番号","備考1","備考2"
"2024/01/15","トヨタ自動車","7203","東証","現物買","2,500.00","100","250","25","2024/01/18","250,275","12345","",""
"2024/01/16","日産自動車","7201","東証","信用新規買","1,000.00","200","100","10","2024/01/19","200,110","12346","",""
"2024/01/17","ホンダ","7267","東証","信用新規売","3,000.00","100","150","15","2024/01/20","-299,835","12347","",""
"2024/01/18","トヨタ自動車","7203","東証","現物売","2,600.00","100","250","25","2024/01/21","259,725","12348","",""
"2024/01/19","日産自動車","7201","東証","信用返済売","1,100.00","200","100","10","2024/01/22","219,890","12349","",""
"2024/01/20","ホンダ","7267","東証","信用返済買","2,900.00","100","150","15","2024/01/23","290,165","12350","",""
"""

    transactions = SBICSVParser.parse_transactions_csv(csv_content)

    assert len(transactions) == 6

    # 現物買い = 新規買い
    assert transactions[0]["code"] == "7203"
    assert transactions[0]["transaction_type"] == "buy"
    assert transactions[0]["detailed_type"] == "新規買い"

    # 信用新規買い = 新規買い
    assert transactions[1]["code"] == "7201"
    assert transactions[1]["transaction_type"] == "buy"
    assert transactions[1]["detailed_type"] == "新規買い"

    # 信用新規売り = 新規売り（空売り）
    assert transactions[2]["code"] == "7267"
    assert transactions[2]["transaction_type"] == "sell"
    assert transactions[2]["detailed_type"] == "新規売り"

    # 現物売り = 決済売り
    assert transactions[3]["code"] == "7203"
    assert transactions[3]["transaction_type"] == "sell"
    assert transactions[3]["detailed_type"] == "決済売り"

    # 信用返済売り = 決済売り
    assert transactions[4]["code"] == "7201"
    assert transactions[4]["transaction_type"] == "sell"
    assert transactions[4]["detailed_type"] == "決済売り"
    # SaveFile形式では現在の実装では決済損益は取得できない

    # 信用返済買い = 決済買い（空売りの決済）
    assert transactions[5]["code"] == "7267"
    assert transactions[5]["transaction_type"] == "buy"
    assert transactions[5]["detailed_type"] == "決済買い"


def test_parse_transactions_order_list_format_detailed_types():
    """注文一覧形式の取引履歴CSVで詳細タイプが正しく判定されることを確認"""
    csv_content = """
"銘柄（コード）","銘柄（名前）","銘柄（市場）","取引区分","期限","預り区分","約定日","注文株数","約定株数","約定単価","手数料","消費税","約定代金","入金額"
"7203","トヨタ自動車","東証プライム","現物買","2024/01/15","特定預り","2024/01/15 09:00:00","100","100","2500.00","250","25","250275",""
"7201","日産自動車","東証プライム","信用新規買","2024/01/16","信用","2024/01/16 10:00:00","200","200","1000.00","100","10","200110",""
"7267","ホンダ","東証プライム","信用新規売","2024/01/17","信用","2024/01/17 11:00:00","100","100","3000.00","150","15","299835",""
"7203","トヨタ自動車","東証プライム","現物売","2024/01/18","特定預り","2024/01/18 13:00:00","100","100","2600.00","250","25","","259725"
"7201","日産自動車","東証プライム","信用返済売","2024/01/19","信用","2024/01/19 14:00:00","200","200","1100.00","100","10","","219890"
"""

    transactions = SBICSVParser.parse_transactions_csv(csv_content)

    assert len(transactions) == 5

    # 現物買い = 新規買い
    assert transactions[0]["code"] == "7203"
    assert transactions[0]["transaction_type"] == "buy"
    assert transactions[0]["detailed_type"] == "新規買い"

    # 信用新規買い = 新規買い
    assert transactions[1]["code"] == "7201"
    assert transactions[1]["transaction_type"] == "buy"
    assert transactions[1]["detailed_type"] == "新規買い"

    # 信用新規売り = 新規売り
    assert transactions[2]["code"] == "7267"
    assert transactions[2]["transaction_type"] == "sell"
    assert transactions[2]["detailed_type"] == "新規売り"

    # 現物売り = 決済売り
    assert transactions[3]["code"] == "7203"
    assert transactions[3]["transaction_type"] == "sell"
    assert transactions[3]["detailed_type"] == "決済売り"

    # 信用返済売り = 決済売り
    assert transactions[4]["code"] == "7201"
    assert transactions[4]["transaction_type"] == "sell"
    assert transactions[4]["detailed_type"] == "決済売り"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
