"""
デイトレード記録管理のビジネスロジック
"""

import csv
import io
import sqlite3
from calendar import monthrange
from datetime import datetime
from typing import Any

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("daytrade_service")


class DaytradeService:
    """デイトレードサービスクラス"""

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.db_path = get_db_path()

    def import_futures_csv(self, file) -> dict[str, Any]:
        """先物取引CSVのインポート"""
        try:
            # まずバイナリとして読み込む
            content_bytes = file.read()

            # エンコーディングを自動検出
            try:
                # UTF-8 with BOMを試す
                if content_bytes.startswith(b"\xef\xbb\xbf"):
                    content = content_bytes.decode("utf-8-sig")
                else:
                    # Shift-JISを試す
                    content = content_bytes.decode("shift-jis")
            except UnicodeDecodeError:
                # UTF-8を試す
                try:
                    content = content_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    # CP932を試す
                    content = content_bytes.decode("cp932")

            lines = content.split("\n")

            # デバッグ：最初の10行を出力
            logger.info("CSVファイルの最初の10行:")
            for i, line in enumerate(lines[:10]):
                logger.info(f"行{i}: {line[:100]}...")  # 最初の100文字のみ

            # CSVヘッダーを探す（最初の10行以内）
            header_line = None
            data_start_index = 0

            # ヘッダー行を探す（特定のキーワードを含む行）
            header_keywords = ["約定番号", "取引日", "銘柄", "取引", "約定価格"]
            for i, line in enumerate(lines[:15]):  # 最初の15行を検索
                # 複数のキーワードが含まれているか確認
                keyword_count = sum(1 for keyword in header_keywords if keyword in line)
                if keyword_count >= 3:  # 3つ以上のキーワードが含まれていればヘッダー
                    header_line = line
                    data_start_index = i + 1
                    logger.info(
                        f"ヘッダー行が{i}行目で見つかりました（{keyword_count}個のキーワード）"
                    )
                    break

            if not header_line:
                # ヘッダーが見つからない場合、5行目を試す（従来の方法）
                if len(lines) > 5:
                    header_line = lines[5]
                    data_start_index = 6
                    logger.info("デフォルトで5行目をヘッダーとして使用")
                else:
                    # ファイルの内容をログに出力してエラーを報告
                    logger.error(f"CSVファイルの行数: {len(lines)}")
                    logger.error("期待されるキーワード: " + ", ".join(header_keywords))
                    raise ValueError(
                        f"CSVヘッダーが見つかりません。ファイルに{len(lines)}行しかありません。"
                    )

            # データ行を収集
            data_lines = []
            for i in range(data_start_index, len(lines)):
                if lines[i].strip():
                    data_lines.append(lines[i])

            # ヘッダーとデータを結合してCSVとして読み込む
            csv_content = header_line + "\n" + "\n".join(data_lines)
            csv_reader = csv.DictReader(io.StringIO(csv_content))

            imported_count = 0
            skipped_count = 0
            errors = []

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                for row in csv_reader:
                    try:
                        # デバッグ：最初の行のキーを表示
                        if imported_count == 0 and skipped_count == 0:
                            logger.info(f"CSVカラム: {list(row.keys())}")

                        # 取引区分をチェック（決済取引のみインポート）
                        trade_type = row.get("取引", "")
                        if "決済" not in trade_type:
                            skipped_count += 1
                            continue

                        # 約定日時から取引日を取得（日付部分のみ）
                        trade_datetime = row["約定日時"]
                        # 日付部分を抽出（YYYY/MM/DD HH:MM:SS形式から）
                        trade_date = datetime.strptime(
                            trade_datetime.split(" ")[0], "%Y/%m/%d"
                        ).strftime("%Y-%m-%d")

                        # 決済損益の処理（+記号を除去）
                        profit_loss_str = row.get("決済損益", "0")
                        if profit_loss_str.startswith("+"):
                            profit_loss_str = profit_loss_str[1:]
                        profit_loss = (
                            float(profit_loss_str.replace(",", ""))
                            if profit_loss_str != "--"
                            else 0
                        )

                        # 受渡金額の処理
                        delivery_amount_str = row.get("受渡金額", "0")
                        if delivery_amount_str.startswith("+"):
                            delivery_amount_str = delivery_amount_str[1:]
                        delivery_amount = (
                            float(delivery_amount_str.replace(",", ""))
                            if delivery_amount_str != "--"
                            else 0
                        )

                        # データの挿入（利用可能なカラムのみ使用）
                        cursor.execute(
                            """
                            INSERT INTO daytrade_futures (
                                user_id, trade_date, trade_number, trade_datetime,
                                market, symbol, trade_type, price, quantity,
                                commission, tax, settlement_amount, delivery_amount,
                                delivery_date, open_date, open_price, open_commission,
                                open_tax, profit_loss, sq_date
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                self.user_id,
                                trade_date,
                                row["約定番号"],
                                row["約定日時"],
                                row.get("市場", ""),
                                row["銘柄"],
                                row["取引"],
                                float(row["約定単価"].replace(",", "")),
                                int(row["約定数量"]),
                                float(row.get("手数料", "0").replace(",", "")),
                                0,  # 消費税（CSVにない）
                                float(row["約定金額"].replace(",", "")),
                                delivery_amount,
                                (
                                    datetime.strptime(
                                        row["受渡日"], "%Y/%m/%d"
                                    ).strftime("%Y-%m-%d")
                                    if row.get("受渡日") and row["受渡日"].strip()
                                    else None
                                ),
                                None,  # 新規建日（CSVにない）
                                0,  # 新規建単価（CSVにない）
                                0,  # 新規建手数料（CSVにない）
                                0,  # 新規建消費税（CSVにない）
                                profit_loss,
                                row.get("SQ日", "").strip() if row.get("SQ日") else "",
                            ),
                        )
                        imported_count += 1

                    except Exception as e:
                        error_msg = f"行 {csv_reader.line_num}: {str(e)}"
                        if imported_count == 0:
                            error_msg += f" | Row data: {row}"
                        errors.append(error_msg)
                        logger.error(
                            f"先物データインポートエラー (行 {csv_reader.line_num}): {e}"
                        )
                        logger.error(f"Row data: {row}")

                conn.commit()

            return {
                "imported": imported_count,
                "skipped": skipped_count,
                "errors": errors,
            }

        except Exception as e:
            logger.error(f"先物CSVインポートエラー: {e}")
            raise

    def import_stocks_csv(self, file) -> dict[str, Any]:
        """株式取引CSVのインポート"""
        try:
            # まずバイナリとして読み込む
            content_bytes = file.read()

            # エンコーディングを自動検出
            try:
                # UTF-8 with BOMを試す
                if content_bytes.startswith(b"\xef\xbb\xbf"):
                    content = content_bytes.decode("utf-8-sig")
                else:
                    # UTF-8を試す
                    content = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                # Shift-JISを試す
                try:
                    content = content_bytes.decode("shift-jis")
                except UnicodeDecodeError:
                    # CP932を試す
                    content = content_bytes.decode("cp932")

            lines = content.strip().split("\n")

            # ヘッダー行を取得
            header_line = lines[0]
            headers = header_line.split(",")

            # データ行を処理
            csv_reader = csv.reader(lines[1:])

            imported_count = 0
            skipped_count = 0
            errors = []

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                for row_num, row in enumerate(
                    csv_reader, start=2
                ):  # 行番号は2から開始（ヘッダーが1行目）
                    try:
                        # 空行のスキップ
                        if len(row) < len(headers) or not row[0]:
                            continue

                        # 約定日の取得と変換（インデックス6）
                        trade_date = datetime.strptime(row[6], "%Y/%m/%d").strftime(
                            "%Y-%m-%d"
                        )

                        # 銘柄情報の取得（最初の3カラム）
                        code = row[0]  # 銘柄コード
                        name = row[1]  # 銘柄名
                        market = row[2]  # 市場

                        # 数値データの変換
                        def parse_number(value):
                            if not value or value == "--":
                                return None
                            return float(value.replace(",", ""))

                        # データの挿入
                        cursor.execute(
                            """
                            INSERT INTO daytrade_stocks (
                                user_id, code, name, market, trade_type, term,
                                custody_type, trade_date, delivery_date, quantity,
                                average_price, commission_tax, capital_gains_tax,
                                settlement_amount, day_trade_amount
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                self.user_id,
                                code,
                                name,
                                market,
                                row[3],  # 取引区分
                                row[4],  # 期限
                                row[5],  # 預り区分
                                trade_date,
                                (
                                    datetime.strptime(row[7], "%Y/%m/%d").strftime(
                                        "%Y-%m-%d"
                                    )
                                    if row[7]
                                    else None
                                ),  # 受渡日
                                int(row[8].replace(",", "")),  # 株数
                                parse_number(row[9]),  # 平均約定単価
                                parse_number(row[10]),  # 手数料・諸経費等
                                parse_number(row[11]),  # 課税額・譲渡益税
                                parse_number(row[12]),  # 受渡金額・決済損益
                                parse_number(row[13]),  # 受渡金額(日計り分)
                            ),
                        )
                        imported_count += 1

                    except Exception as e:
                        errors.append(f"行 {row_num}: {str(e)}")
                        logger.error(f"株式データインポートエラー (行 {row_num}): {e}")

                conn.commit()

            return {
                "imported": imported_count,
                "skipped": skipped_count,
                "errors": errors,
            }

        except Exception as e:
            logger.error(f"株式CSVインポートエラー: {e}")
            raise

    def get_calendar_data(self, year: int, month: int) -> dict[str, Any]:
        """指定月のカレンダーデータを取得"""
        start_date = f"{year:04d}-{month:02d}-01"
        last_day = monthrange(year, month)[1]
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"

        # 月の最初の日の曜日を取得（0=月曜日, 6=日曜日）
        first_weekday = datetime(year, month, 1).weekday()
        # 日曜日始まりに変換（0=日曜日, 6=土曜日）
        first_weekday = (first_weekday + 1) % 7

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 先物の日別損益
            cursor.execute(
                """
                SELECT trade_date, SUM(profit_loss) as daily_profit
                FROM daytrade_futures
                WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                GROUP BY trade_date
            """,
                (self.user_id, start_date, end_date),
            )

            futures_data = {row[0]: row[1] for row in cursor.fetchall()}

            # 株式の日別損益（決済損益と税金のみ）
            cursor.execute(
                """
                SELECT trade_date,
                       SUM(COALESCE(capital_gains_tax, 0) +
                           COALESCE(settlement_amount, 0)) as total_profit_loss,
                       SUM(COALESCE(day_trade_amount, 0)) as day_trade
                FROM daytrade_stocks
                WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                  AND trade_type LIKE '%返済%'
                GROUP BY trade_date
            """,
                (self.user_id, start_date, end_date),
            )

            stocks_data = {
                row[0]: {"settlement": row[1], "day_trade": row[2]}
                for row in cursor.fetchall()
            }

            # カレンダーデータの構築
            calendar_days: list[dict[str, Any]] = []

            # 月の最初の日の前の空白セルを追加
            for _ in range(first_weekday):
                calendar_days.append(
                    {
                        "date": None,
                        "day": None,
                        "futures_profit": 0,
                        "stocks_profit": 0,
                        "stocks_settlement": 0,
                        "stocks_day_trade": 0,
                        "total_profit": 0,
                        "has_trades": False,
                    }
                )

            # 実際の日付データを追加
            for day in range(1, last_day + 1):
                date_str = f"{year:04d}-{month:02d}-{day:02d}"

                futures_profit = futures_data.get(date_str, 0)
                stocks_total = stocks_data.get(date_str, {}).get("settlement", 0)
                stocks_day_trade = stocks_data.get(date_str, {}).get("day_trade", 0)

                # 株式の合計損益（税金・決済損益の合計 + 日計り分）※手数料は除外
                stocks_profit = (stocks_total or 0) + (stocks_day_trade or 0)
                total_profit = futures_profit + stocks_profit

                calendar_days.append(
                    {
                        "date": date_str,
                        "day": day,
                        "futures_profit": futures_profit,
                        "stocks_profit": stocks_profit,
                        "stocks_settlement": stocks_total,
                        "stocks_day_trade": stocks_day_trade,
                        "total_profit": total_profit,
                        "has_trades": total_profit != 0,
                    }
                )

            return {"year": year, "month": month, "days": calendar_days}

    def get_monthly_summary(self, year: int, month: int) -> dict[str, Any]:
        """月別サマリーデータを取得"""
        start_date = f"{year:04d}-{month:02d}-01"
        last_day = monthrange(year, month)[1]
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 先物の月間統計
            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT trade_date) as trading_days,
                    COUNT(*) as total_trades,
                    SUM(profit_loss) as total_profit,
                    SUM(CASE WHEN profit_loss > 0 THEN profit_loss ELSE 0 END) as total_win,
                    SUM(CASE WHEN profit_loss < 0 THEN profit_loss ELSE 0 END) as total_loss,
                    COUNT(CASE WHEN profit_loss > 0 THEN 1 END) as win_count,
                    COUNT(CASE WHEN profit_loss < 0 THEN 1 END) as loss_count
                FROM daytrade_futures
                WHERE user_id = ? AND trade_date BETWEEN ? AND ?
            """,
                (self.user_id, start_date, end_date),
            )

            futures_stats = cursor.fetchone()

            # 株式の月間統計
            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT trade_date) as trading_days,
                    COUNT(*) as total_trades,
                    SUM(COALESCE(capital_gains_tax, 0) +
                        COALESCE(settlement_amount, 0)) as total_profit_loss,
                    SUM(COALESCE(day_trade_amount, 0)) as total_day_trade,
                    COUNT(DISTINCT code) as unique_stocks,
                    SUM(CASE WHEN (COALESCE(capital_gains_tax, 0) +
                                   COALESCE(settlement_amount, 0)) > 0 THEN 1 ELSE 0 END) as win_count,
                    SUM(CASE WHEN (COALESCE(capital_gains_tax, 0) +
                                   COALESCE(settlement_amount, 0)) < 0 THEN 1 ELSE 0 END) as loss_count
                FROM daytrade_stocks
                WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                  AND trade_type LIKE '%返済%'
            """,
                (self.user_id, start_date, end_date),
            )

            stocks_stats = cursor.fetchone()

            # 日別の損益データを取得して追加統計を計算
            cursor.execute(
                """
                SELECT
                    trade_date,
                    SUM(profit_loss) as daily_futures_profit
                FROM daytrade_futures
                WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                GROUP BY trade_date
                ORDER BY trade_date
            """,
                (self.user_id, start_date, end_date),
            )

            futures_daily = cursor.fetchall()

            # 先物の追加統計計算
            futures_max_profit = 0
            futures_max_loss = 0
            futures_cumulative = 0
            futures_max_drawdown = 0
            futures_peak = 0

            for _date, profit in futures_daily:
                futures_cumulative += profit
                if profit > futures_max_profit:
                    futures_max_profit = profit
                if profit < futures_max_loss:
                    futures_max_loss = profit

                # ドローダウン計算
                if futures_cumulative > futures_peak:
                    futures_peak = futures_cumulative
                drawdown = futures_peak - futures_cumulative
                if drawdown > futures_max_drawdown:
                    futures_max_drawdown = drawdown

            # 株式の日別データ
            cursor.execute(
                """
                SELECT
                    trade_date,
                    SUM(COALESCE(capital_gains_tax, 0) +
                        COALESCE(settlement_amount, 0)) as daily_stocks_profit
                FROM daytrade_stocks
                WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                  AND trade_type LIKE '%返済%'
                GROUP BY trade_date
                ORDER BY trade_date
            """,
                (self.user_id, start_date, end_date),
            )

            stocks_daily = cursor.fetchall()

            # 株式の追加統計計算
            stocks_max_profit = 0
            stocks_max_loss = 0
            stocks_cumulative = 0
            stocks_max_drawdown = 0
            stocks_peak = 0

            for _date, profit in stocks_daily:
                stocks_cumulative += profit
                if profit > stocks_max_profit:
                    stocks_max_profit = profit
                if profit < stocks_max_loss:
                    stocks_max_loss = profit

                # ドローダウン計算
                if stocks_cumulative > stocks_peak:
                    stocks_peak = stocks_cumulative
                drawdown = stocks_peak - stocks_cumulative
                if drawdown > stocks_max_drawdown:
                    stocks_max_drawdown = drawdown

            return {
                "year": year,
                "month": month,
                "futures": {
                    "trading_days": futures_stats[0] or 0,
                    "total_trades": futures_stats[1] or 0,
                    "total_profit": futures_stats[2] or 0,
                    "total_win": futures_stats[3] or 0,
                    "total_loss": futures_stats[4] or 0,
                    "win_count": futures_stats[5] or 0,
                    "loss_count": futures_stats[6] or 0,
                    "win_rate": (
                        (futures_stats[5] / (futures_stats[5] + futures_stats[6]) * 100)
                        if (futures_stats[5] + futures_stats[6]) > 0
                        else 0
                    ),
                    "max_daily_profit": futures_max_profit,
                    "max_daily_loss": futures_max_loss,
                    "max_drawdown": futures_max_drawdown,
                    "avg_win": (
                        futures_stats[3] / futures_stats[5]
                        if futures_stats[5] > 0
                        else 0
                    ),
                    "avg_loss": (
                        futures_stats[4] / futures_stats[6]
                        if futures_stats[6] > 0
                        else 0
                    ),
                },
                "stocks": {
                    "trading_days": stocks_stats[0] or 0,
                    "total_trades": stocks_stats[1] or 0,
                    "total_settlement": stocks_stats[2] or 0,
                    "total_day_trade": stocks_stats[3] or 0,
                    "unique_stocks": stocks_stats[4] or 0,
                    "win_count": stocks_stats[5] or 0,
                    "loss_count": stocks_stats[6] or 0,
                    "win_rate": (
                        (stocks_stats[5] / (stocks_stats[5] + stocks_stats[6]) * 100)
                        if (stocks_stats[5] + stocks_stats[6]) > 0
                        else 0
                    ),
                    "max_daily_profit": stocks_max_profit,
                    "max_daily_loss": stocks_max_loss,
                    "max_drawdown": stocks_max_drawdown,
                },
            }

    def get_daily_details(self, date: str) -> dict[str, Any]:
        """指定日の取引詳細を取得"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 先物取引
            cursor.execute(
                """
                SELECT * FROM daytrade_futures
                WHERE user_id = ? AND trade_date = ?
                ORDER BY trade_datetime
            """,
                (self.user_id, date),
            )

            futures_trades = [dict(row) for row in cursor.fetchall()]

            # 株式取引
            cursor.execute(
                """
                SELECT * FROM daytrade_stocks
                WHERE user_id = ? AND trade_date = ?
                ORDER BY id
            """,
                (self.user_id, date),
            )

            stocks_trades = [dict(row) for row in cursor.fetchall()]

            return {"date": date, "futures": futures_trades, "stocks": stocks_trades}
