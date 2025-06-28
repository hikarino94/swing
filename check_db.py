#!/usr/bin/env python3
"""データベース確認ツール（読み取り専用）"""

import sqlite3
import sys


def main():
    try:
        # 読み取り専用で接続
        conn = sqlite3.connect("file:data/db/stock.db?mode=ro", uri=True)
        cur = conn.cursor()

        print("📊 最近のテクニカルシグナル (買いシグナル上位10件)")
        print("=" * 80)

        # 修正されたカラム名を使用
        signals = cur.execute(
            """
            SELECT
                ti.signal_date,
                ti.code,
                li.company_name,
                ti.signals_count,
                ti.signals_short_count,
                ti.signals_overheating,
                ti.signals_oversold
            FROM technical_indicators ti
            LEFT JOIN listed_info li ON ti.code = li.code
            WHERE ti.signal_date = (SELECT MAX(signal_date) FROM technical_indicators)
              AND ti.signals_count >= 3
            ORDER BY ti.signals_count DESC
            LIMIT 10
        """
        ).fetchall()

        headers = ["日付", "コード", "会社名", "買い", "売り", "過熱", "売られ"]
        print(
            f"{headers[0]:<12} {headers[1]:<6} {headers[2]:<25} {headers[3]:>4} {headers[4]:>4} {headers[5]:>4} {headers[6]:>6}"
        )
        print("-" * 80)

        for row in signals:
            date, code, name, buy, sell, heat, sold = row
            name = (name[:25] if name else "N/A") if name else "N/A"
            print(f"{date} {code:<6} {name:<25} {buy:>4} {sell:>4} {heat:>4} {sold:>6}")

        # シグナル統計
        stats = cur.execute(
            """
            SELECT
                signal_date,
                COUNT(*) as total,
                SUM(CASE WHEN signals_count >= 3 THEN 1 ELSE 0 END) as buy_signals,
                SUM(CASE WHEN signals_short_count >= 4 THEN 1 ELSE 0 END) as sell_signals,
                SUM(CASE WHEN signals_overheating > 0 THEN 1 ELSE 0 END) as overheat,
                SUM(CASE WHEN signals_oversold > 0 THEN 1 ELSE 0 END) as oversold
            FROM technical_indicators
            WHERE signal_date = (SELECT MAX(signal_date) FROM technical_indicators)
            GROUP BY signal_date
        """
        ).fetchone()

        if stats:
            print(f"\n📈 シグナル統計 ({stats[0]}):")
            print(f"  総シグナル数: {stats[1]:,} 件")
            print(f"  買いシグナル (3個以上): {stats[2]:,} 件")
            print(f"  売りシグナル (4個以上): {stats[3]:,} 件")
            print(f"  過熱シグナル: {stats[4]:,} 件")
            print(f"  売られ過ぎシグナル: {stats[5]:,} 件")

        # 最近の処理日付
        print("\n📅 最近の処理日:")
        recent_dates = cur.execute(
            """
            SELECT signal_date, COUNT(*) as count
            FROM technical_indicators
            GROUP BY signal_date
            ORDER BY signal_date DESC
            LIMIT 5
        """
        ).fetchall()

        for date, count in recent_dates:
            print(f"  {date}: {count:,} 件")

        # データベース全体の統計
        print("\n💾 データベース統計:")
        tables = ["prices", "listed_info", "technical_indicators", "fundamental_signals", "statements"]
        for table in tables:
            try:
                count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count:,} 件")
            except sqlite3.OperationalError:
                pass

        conn.close()

    except Exception as e:
        print(f"エラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
