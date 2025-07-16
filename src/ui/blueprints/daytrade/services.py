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

    def get_cumulative_profit_data(
        self, start_date: str, end_date: str
    ) -> dict[str, Any]:
        """指定期間の累積損益データを取得"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 日別の損益データを取得
            cursor.execute(
                """
                SELECT
                    d.date,
                    COALESCE(f.daily_profit, 0) as futures_profit,
                    COALESCE(s.daily_profit, 0) as stocks_profit,
                    COALESCE(s.day_trade_profit, 0) as stocks_day_trade
                FROM (
                    SELECT DISTINCT date FROM (
                        SELECT trade_date as date FROM daytrade_futures
                        WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                        UNION
                        SELECT trade_date as date FROM daytrade_stocks
                        WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                    )
                ) d
                LEFT JOIN (
                    SELECT trade_date, SUM(profit_loss) as daily_profit
                    FROM daytrade_futures
                    WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                    GROUP BY trade_date
                ) f ON d.date = f.trade_date
                LEFT JOIN (
                    SELECT
                        trade_date,
                        SUM(COALESCE(capital_gains_tax, 0) +
                            COALESCE(settlement_amount, 0)) as daily_profit,
                        SUM(COALESCE(day_trade_amount, 0)) as day_trade_profit
                    FROM daytrade_stocks
                    WHERE user_id = ? AND trade_date BETWEEN ? AND ?
                      AND trade_type LIKE '%返済%'
                    GROUP BY trade_date
                ) s ON d.date = s.trade_date
                ORDER BY d.date
            """,
                (
                    self.user_id,
                    start_date,
                    end_date,
                    self.user_id,
                    start_date,
                    end_date,
                    self.user_id,
                    start_date,
                    end_date,
                    self.user_id,
                    start_date,
                    end_date,
                ),
            )

            daily_data = cursor.fetchall()

            # 累積計算
            cumulative_data = []
            futures_cumulative = 0
            stocks_cumulative = 0
            total_cumulative = 0

            for date, futures_profit, stocks_profit, stocks_day_trade in daily_data:
                # 株式の合計損益
                stocks_total = stocks_profit + stocks_day_trade

                # 累積更新
                futures_cumulative += futures_profit
                stocks_cumulative += stocks_total
                total_cumulative += futures_profit + stocks_total

                cumulative_data.append(
                    {
                        "date": date,
                        "futures_daily": futures_profit,
                        "stocks_daily": stocks_total,
                        "total_daily": futures_profit + stocks_total,
                        "futures_cumulative": futures_cumulative,
                        "stocks_cumulative": stocks_cumulative,
                        "total_cumulative": total_cumulative,
                    }
                )

            return {
                "start_date": start_date,
                "end_date": end_date,
                "data": cumulative_data,
                "summary": {
                    "futures_total": futures_cumulative,
                    "stocks_total": stocks_cumulative,
                    "total": total_cumulative,
                },
            }

    def get_trade_list(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        page: int = 1,
        per_page: int = 50,
    ) -> dict[str, Any]:
        """取引履歴一覧を取得"""
        offset = (page - 1) * per_page

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 条件設定
            where_clause = "WHERE user_id = ?"
            params: list[Any] = [self.user_id]

            if start_date:
                where_clause += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                where_clause += " AND trade_date <= ?"
                params.append(end_date)

            # 総件数を取得
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT id FROM daytrade_futures {where_clause}
                    UNION ALL
                    SELECT id FROM daytrade_stocks {where_clause}
                ) t
            """,
                params * 2,
            )
            total_count = cursor.fetchone()[0]

            # 取引データを取得
            cursor.execute(
                f"""
                SELECT * FROM (
                    SELECT
                        'futures' as trade_category,
                        id,
                        trade_date,
                        trade_datetime as datetime,
                        symbol as name,
                        trade_type,
                        price,
                        quantity,
                        profit_loss,
                        NULL as settlement_amount,
                        NULL as code
                    FROM daytrade_futures
                    {where_clause}

                    UNION ALL

                    SELECT
                        'stocks' as trade_category,
                        id,
                        trade_date,
                        trade_date || ' 00:00:00' as datetime,
                        name,
                        trade_type,
                        average_price as price,
                        quantity,
                        NULL as profit_loss,
                        settlement_amount,
                        code
                    FROM daytrade_stocks
                    {where_clause}
                ) t
                ORDER BY trade_date DESC, datetime DESC
                LIMIT ? OFFSET ?
            """,
                params * 2 + [per_page, offset],
            )

            trades = [dict(row) for row in cursor.fetchall()]

            return {
                "trades": trades,
                "total": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page,
            }

    def create_trade(self, data: dict[str, Any]) -> dict[str, Any]:
        """取引を手動登録"""
        trade_type = data.get("trade_category")
        if trade_type not in ["futures", "stocks"]:
            raise ValueError("Invalid trade category")

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            if trade_type == "futures":
                # 先物取引の登録
                cursor.execute(
                    """
                    INSERT INTO daytrade_futures (
                        user_id, trade_date, trade_number, trade_datetime,
                        market, symbol, trade_type, price, quantity,
                        commission, tax, settlement_amount, delivery_amount,
                        delivery_date, profit_loss
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        self.user_id,
                        data["trade_date"],
                        data.get("trade_number", ""),
                        data.get("trade_datetime", data["trade_date"] + " 00:00:00"),
                        data.get("market", ""),
                        data["symbol"],
                        data["trade_type"],
                        float(data["price"]),
                        int(data["quantity"]),
                        float(data.get("commission", 0)),
                        float(data.get("tax", 0)),
                        float(data["settlement_amount"]),
                        float(data.get("delivery_amount", 0)),
                        data.get("delivery_date"),
                        float(data["profit_loss"]),
                    ),
                )
            else:
                # 株式取引の登録
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
                        data["code"],
                        data["name"],
                        data.get("market", ""),
                        data["trade_type"],
                        data.get("term", ""),
                        data.get("custody_type", ""),
                        data["trade_date"],
                        data.get("delivery_date"),
                        int(data["quantity"]),
                        float(data["average_price"]),
                        float(data.get("commission_tax", 0)),
                        float(data.get("capital_gains_tax", 0)),
                        float(data.get("settlement_amount", 0)),
                        float(data.get("day_trade_amount", 0)),
                    ),
                )

            conn.commit()
            return {"id": cursor.lastrowid}

    def update_trade(self, trade_id: int, data: dict[str, Any]) -> None:
        """取引を編集"""
        trade_type = data.get("trade_category")
        if trade_type not in ["futures", "stocks"]:
            raise ValueError("Invalid trade category")

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            if trade_type == "futures":
                cursor.execute(
                    """
                    UPDATE daytrade_futures SET
                        trade_date = ?, trade_datetime = ?, symbol = ?,
                        trade_type = ?, price = ?, quantity = ?,
                        commission = ?, tax = ?, settlement_amount = ?,
                        delivery_amount = ?, delivery_date = ?, profit_loss = ?
                    WHERE id = ? AND user_id = ?
                """,
                    (
                        data["trade_date"],
                        data.get("trade_datetime", data["trade_date"] + " 00:00:00"),
                        data["symbol"],
                        data["trade_type"],
                        float(data["price"]),
                        int(data["quantity"]),
                        float(data.get("commission", 0)),
                        float(data.get("tax", 0)),
                        float(data["settlement_amount"]),
                        float(data.get("delivery_amount", 0)),
                        data.get("delivery_date"),
                        float(data["profit_loss"]),
                        trade_id,
                        self.user_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE daytrade_stocks SET
                        code = ?, name = ?, trade_type = ?,
                        trade_date = ?, delivery_date = ?, quantity = ?,
                        average_price = ?, commission_tax = ?,
                        capital_gains_tax = ?, settlement_amount = ?,
                        day_trade_amount = ?
                    WHERE id = ? AND user_id = ?
                """,
                    (
                        data["code"],
                        data["name"],
                        data["trade_type"],
                        data["trade_date"],
                        data.get("delivery_date"),
                        int(data["quantity"]),
                        float(data["average_price"]),
                        float(data.get("commission_tax", 0)),
                        float(data.get("capital_gains_tax", 0)),
                        float(data.get("settlement_amount", 0)),
                        float(data.get("day_trade_amount", 0)),
                        trade_id,
                        self.user_id,
                    ),
                )

            if cursor.rowcount == 0:
                raise ValueError("Trade not found or unauthorized")

            conn.commit()

    def delete_trade(self, trade_id: int) -> None:
        """取引を削除"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # 先物から削除試行
            cursor.execute(
                "DELETE FROM daytrade_futures WHERE id = ? AND user_id = ?",
                (trade_id, self.user_id),
            )

            if cursor.rowcount == 0:
                # 株式から削除試行
                cursor.execute(
                    "DELETE FROM daytrade_stocks WHERE id = ? AND user_id = ?",
                    (trade_id, self.user_id),
                )

                if cursor.rowcount == 0:
                    raise ValueError("Trade not found or unauthorized")

            conn.commit()

    def get_monthly_profit_data(self, year: int) -> dict[str, Any]:
        """年間の月別損益データを取得"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            monthly_data = []

            for month in range(1, 13):
                start_date = f"{year:04d}-{month:02d}-01"
                if month == 12:
                    end_date = f"{year:04d}-12-31"
                else:
                    end_date = f"{year:04d}-{month+1:02d}-01"

                # 先物の月間損益
                cursor.execute(
                    """
                    SELECT
                        COALESCE(SUM(profit_loss), 0) as total_profit,
                        COUNT(DISTINCT trade_date) as trading_days,
                        COUNT(*) as total_trades
                    FROM daytrade_futures
                    WHERE user_id = ? AND trade_date >= ? AND trade_date < ?
                """,
                    (self.user_id, start_date, end_date),
                )
                futures_data = cursor.fetchone()

                # 株式の月間損益
                cursor.execute(
                    """
                    SELECT
                        COALESCE(SUM(COALESCE(capital_gains_tax, 0) +
                                     COALESCE(settlement_amount, 0)), 0) as total_profit,
                        COALESCE(SUM(COALESCE(day_trade_amount, 0)), 0) as day_trade_profit,
                        COUNT(DISTINCT trade_date) as trading_days,
                        COUNT(*) as total_trades
                    FROM daytrade_stocks
                    WHERE user_id = ? AND trade_date >= ? AND trade_date < ?
                      AND trade_type LIKE '%返済%'
                """,
                    (self.user_id, start_date, end_date),
                )
                stocks_data = cursor.fetchone()

                futures_profit = futures_data[0] if futures_data else 0
                stocks_profit = (stocks_data[0] + stocks_data[1]) if stocks_data else 0

                monthly_data.append(
                    {
                        "month": month,
                        "futures_profit": futures_profit,
                        "stocks_profit": stocks_profit,
                        "total_profit": futures_profit + stocks_profit,
                        "futures_days": futures_data[1] if futures_data else 0,
                        "stocks_days": stocks_data[2] if stocks_data else 0,
                        "futures_trades": futures_data[2] if futures_data else 0,
                        "stocks_trades": stocks_data[3] if stocks_data else 0,
                    }
                )

            # 年間合計
            total_futures = sum(m["futures_profit"] for m in monthly_data)
            total_stocks = sum(m["stocks_profit"] for m in monthly_data)

            return {
                "year": year,
                "months": monthly_data,
                "yearly_total": {
                    "futures": total_futures,
                    "stocks": total_stocks,
                    "total": total_futures + total_stocks,
                },
            }
