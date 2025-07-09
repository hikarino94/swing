#!/usr/bin/env python
"""PER計算問題の調査スクリプト"""
import sqlite3


def check_per_issue():
    """18140（大末建設）と19110（住友林業）のPER計算問題を調査"""

    conn = sqlite3.connect("stock.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    target_codes = ["18140", "19110"]

    print("=" * 80)
    print("PER計算問題の調査")
    print("=" * 80)

    # まずテーブル一覧を確認
    print("\nデータベース内のテーブル一覧:")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    for table in tables:
        print(f"  - {table['name']}")

    # 1. portfolio_holdingsテーブルの確認（holdingsではなくportfolio_holdingsかもしれない）
    print("\n1. portfolio_holdingsテーブルの確認:")
    print("-" * 60)

    # テーブル名を確認
    holdings_table = None
    for table in tables:
        if "holding" in table["name"].lower():
            holdings_table = table["name"]
            break

    if holdings_table:
        print(f"  使用するテーブル: {holdings_table}")

        # カラム情報を取得
        cur.execute(f"PRAGMA table_info({holdings_table})")
        columns = cur.fetchall()
        print("\n  カラム一覧:")
        for col in columns:
            print(f"    - {col['name']} ({col['type']})")

        # データを確認
        cur.execute(
            f"""
            SELECT * FROM {holdings_table}
            WHERE code IN (?, ?)
        """,
            target_codes,
        )

        holdings = cur.fetchall()
        for row in holdings:
            print(f"\nコード: {row['code']}")
            for key in row.keys():
                print(f"  {key}: {row[key]}")
    else:
        print("  *** holdingsテーブルが見つかりません ***")

    # 2. pricesテーブルの確認（0埋めされたコードで検索）
    print("\n\n2. pricesテーブルの最新データ確認:")
    print("-" * 60)
    padded_codes = ["0" + code for code in target_codes]

    for i, code in enumerate(target_codes):
        padded_code = padded_codes[i]
        print(f"\nコード: {code} (DB内: {padded_code})")

        # 最新の株価データを取得
        cur.execute(
            """
            SELECT Code, Date, Close, Volume
            FROM prices
            WHERE Code = ?
            ORDER BY Date DESC
            LIMIT 5
        """,
            (padded_code,),
        )

        prices = cur.fetchall()
        if prices:
            print("  最新5件の株価データ:")
            for price in prices:
                print(
                    f"    {price['Date']}: 終値={price['Close']}, 出来高={price['Volume']}"
                )
        else:
            print("  *** 株価データが見つかりません ***")

    # 3. statementsテーブルの確認
    print("\n\n3. statementsテーブルのEPS関連データ確認:")
    print("-" * 60)

    for code in target_codes:
        print(f"\nコード: {code}")

        # 最新の財務データを取得
        cur.execute(
            """
            SELECT
                LocalCode,
                DisclosedDate,
                ForecastDividendPerShareAnnual,
                ForecastNetSales,
                ForecastOperatingProfit,
                ForecastOrdinaryProfit,
                ForecastProfit,
                ForecastEarningsPerShare,
                ResultDividendPerShareAnnual,
                ResultNetSales,
                ResultOperatingProfit,
                ResultOrdinaryProfit,
                ResultProfit,
                ResultEarningsPerShare,
                NextYearForecastDividendPerShareAnnual,
                NextYearForecastNetSales,
                NextYearForecastOperatingProfit,
                NextYearForecastOrdinaryProfit,
                NextYearForecastProfit,
                NextYearForecastEarningsPerShare
            FROM statements
            WHERE LocalCode = ?
            ORDER BY DisclosedDate DESC
            LIMIT 3
        """,
            (code,),
        )

        statements = cur.fetchall()
        if statements:
            print(f"  最新{len(statements)}件の財務データ:")
            for stmt in statements:
                print(f"\n    開示日: {stmt['DisclosedDate']}")
                print(f"    今期予想EPS: {stmt['ForecastEarningsPerShare']}")
                print(f"    実績EPS: {stmt['ResultEarningsPerShare']}")
                print(f"    来期予想EPS: {stmt['NextYearForecastEarningsPerShare']}")
                print(f"    今期予想利益: {stmt['ForecastProfit']}")
                print(f"    実績利益: {stmt['ResultProfit']}")
                print(f"    来期予想利益: {stmt['NextYearForecastProfit']}")
        else:
            print("  *** 財務データが見つかりません ***")

    # 4. listed_infoテーブルの確認
    print("\n\n4. listed_infoテーブルの確認:")
    print("-" * 60)

    for code in target_codes:
        cur.execute(
            """
            SELECT Code, CompanyName, IssuedShareEquityQuote, delete_flag
            FROM listed_info
            WHERE Code = ?
        """,
            (code,),
        )

        info = cur.fetchone()
        if info:
            print(f"\nコード: {code}")
            print(f"  会社名: {info['CompanyName']}")
            print(f"  発行済株式数: {info['IssuedShareEquityQuote']}")
            print(f"  削除フラグ: {info['delete_flag']}")
        else:
            print(f"\nコード: {code} - *** 企業情報が見つかりません ***")

    conn.close()


if __name__ == "__main__":
    check_per_issue()
