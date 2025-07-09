#!/usr/bin/env python
"""18140（大末建設）と19110（住友林業）のPER計算問題の詳細調査"""
import sqlite3
from pathlib import Path


def check_per_issue_detailed():
    """PER計算問題を詳細に調査"""

    # データベースパスを正しく設定
    db_path = Path(__file__).parent / "db" / "stock.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    target_codes = ["18140", "19110"]
    target_codes_5digit = ["18140", "19110"]  # 既に5桁

    print("=" * 80)
    print("18140（大末建設）と19110（住友林業）のPER計算問題の詳細調査")
    print("=" * 80)

    # 1. holdingsテーブルの確認
    print("\n1. holdingsテーブルの確認:")
    print("-" * 60)

    for code in target_codes:
        cur.execute(
            """
            SELECT code, expected_per, expected_eps, actual_pbr, actual_bps,
                   dividend_yield, expected_dividend, average_price, quantity,
                   market_value, profit_loss
            FROM holdings
            WHERE code = ? AND deleted_at IS NULL
        """,
            (code,),
        )

        rows = cur.fetchall()
        if rows:
            print(f"\nコード: {code}")
            for row in rows:
                print(f"  expected_per: {row['expected_per']}")
                print(f"  expected_eps: {row['expected_eps']}")
                print(f"  actual_pbr: {row['actual_pbr']}")
                print(f"  actual_bps: {row['actual_bps']}")
                print(f"  dividend_yield: {row['dividend_yield']}")
                print(f"  expected_dividend: {row['expected_dividend']}")
                print(f"  取得単価: {row['average_price']}")
                print(f"  保有株数: {row['quantity']}")
                print(f"  評価額: {row['market_value']}")
                print(f"  損益: {row['profit_loss']}")
        else:
            print(f"\nコード: {code} - *** holdingsテーブルにデータがありません ***")

    # 2. pricesテーブルの最新株価確認
    print("\n\n2. pricesテーブルの最新株価確認:")
    print("-" * 60)

    for code in target_codes_5digit:
        print(f"\nコード: {code}")

        # 最新の株価データを取得
        cur.execute(
            """
            SELECT code, date, close
            FROM prices
            WHERE code = ?
            ORDER BY date DESC
            LIMIT 5
        """,
            (code,),
        )

        prices = cur.fetchall()
        if prices:
            print("  最新5件の株価データ:")
            for price in prices:
                print(f"    {price['date']}: 終値={price['close']}円")
        else:
            print("  *** 株価データが見つかりません ***")

    # 3. statementsテーブルのEPS/BPS確認
    print("\n\n3. statementsテーブルのEPS/BPS確認:")
    print("-" * 60)

    for code in target_codes_5digit:
        print(f"\nコード: {code}")

        # 最新の財務データを取得
        cur.execute(
            """
            SELECT
                code,
                DisclosedDate,
                EarningsPerShare,
                BookValuePerShare,
                ForecastEarningsPerShare,
                NextYearForecastEarningsPerShare,
                ForecastDividendPerShareAnnual,
                NextYearForecastDividendPerShareAnnual,
                ResultDividendPerShareAnnual
            FROM statements
            WHERE code = ?
            ORDER BY DisclosedDate DESC
            LIMIT 3
        """,
            (code,),
        )

        statements = cur.fetchall()
        if statements:
            print(f"  最新{len(statements)}件の財務データ:")
            for i, stmt in enumerate(statements):
                print(f"\n  [{i+1}] 開示日: {stmt['DisclosedDate']}")
                print(f"      実績EPS: {stmt['EarningsPerShare']}")
                print(f"      実績BPS: {stmt['BookValuePerShare']}")
                print(f"      今期予想EPS: {stmt['ForecastEarningsPerShare']}")
                print(f"      来期予想EPS: {stmt['NextYearForecastEarningsPerShare']}")
                print(f"      今期予想配当: {stmt['ForecastDividendPerShareAnnual']}")
                print(
                    f"      来期予想配当: {stmt['NextYearForecastDividendPerShareAnnual']}"
                )
                print(f"      実績配当: {stmt['ResultDividendPerShareAnnual']}")
        else:
            print("  *** 財務データが見つかりません ***")

    # 4. PER計算シミュレーション
    print("\n\n4. PER計算シミュレーション:")
    print("-" * 60)

    for code, code_5digit in zip(target_codes, target_codes_5digit, strict=False):
        print(f"\nコード: {code}")

        # 最新株価を取得
        cur.execute(
            """
            SELECT close FROM prices
            WHERE code = ?
            ORDER BY date DESC
            LIMIT 1
        """,
            (code_5digit,),
        )
        price_row = cur.fetchone()

        if not price_row:
            print("  *** 株価データがないためPER計算不可 ***")
            continue

        current_price = price_row["close"]
        print(f"  現在株価: {current_price}円")

        # 最新のstatementデータを取得
        cur.execute(
            """
            SELECT
                EarningsPerShare,
                ForecastEarningsPerShare,
                NextYearForecastEarningsPerShare,
                BookValuePerShare
            FROM statements
            WHERE code = ?
            ORDER BY DisclosedDate DESC
            LIMIT 1
        """,
            (code_5digit,),
        )
        stmt_row = cur.fetchone()

        if not stmt_row:
            print("  *** 財務データがないためPER計算不可 ***")
            continue

        eps = stmt_row["EarningsPerShare"]
        forecast_eps = stmt_row["ForecastEarningsPerShare"]
        next_year_eps = stmt_row["NextYearForecastEarningsPerShare"]
        bps = stmt_row["BookValuePerShare"]

        # 予想EPSを優先的に使用
        expected_eps = forecast_eps or next_year_eps or eps

        print("  EPS関連データ:")
        print(f"    実績EPS: {eps}")
        print(f"    今期予想EPS: {forecast_eps}")
        print(f"    来期予想EPS: {next_year_eps}")
        print(f"    使用するEPS: {expected_eps}")

        # PER計算
        if expected_eps and expected_eps > 0:
            expected_per = round(current_price / expected_eps, 2)
            print(
                f"  計算されるPER: {expected_per} (= {current_price} / {expected_eps})"
            )
        else:
            print("  *** EPSが0または未設定のためPER計算不可 ***")

        # PBR計算
        if bps and bps > 0:
            actual_pbr = round(current_price / bps, 2)
            print(f"  計算されるPBR: {actual_pbr} (= {current_price} / {bps})")
        else:
            print("  *** BPSが0または未設定のためPBR計算不可 ***")

    conn.close()


if __name__ == "__main__":
    check_per_issue_detailed()
