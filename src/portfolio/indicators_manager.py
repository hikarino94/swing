"""株価指標管理ロジック"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("portfolio.indicators_manager")


class IndicatorsManager:
    """株価指標管理クラス"""

    @staticmethod
    def update_stock_indicators(user_id: int, codes: list[str] | None = None) -> int:
        """
        statementsテーブルのデータを使用して保有銘柄の株価指標を更新

        Args:
            user_id: ユーザーID
            codes: 更新対象の銘柄コードリスト（Noneの場合は全保有銘柄）

        Returns:
            更新件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        updated_count = 0

        try:
            # 更新対象の保有銘柄を取得
            if codes:
                placeholders = ",".join("?" * len(codes))
                cursor.execute(
                    f"""
                    SELECT DISTINCT h.code
                    FROM holdings h
                    WHERE h.user_id = ? AND h.code IN ({placeholders}) AND h.deleted_at IS NULL
                    """,
                    [user_id, *codes],
                )
            else:
                cursor.execute(
                    """
                    SELECT DISTINCT code
                    FROM holdings
                    WHERE user_id = ? AND deleted_at IS NULL
                    """,
                    (user_id,),
                )

            target_codes = [row[0] for row in cursor.fetchall()]

            for code in target_codes:
                # 最新の株価を取得（5桁変換）
                code_5digit = code.ljust(5, "0")
                cursor.execute(
                    """
                    SELECT close, date FROM prices
                    WHERE code = ?
                    ORDER BY date DESC
                    LIMIT 1
                    """,
                    (code_5digit,),
                )
                price_row = cursor.fetchone()

                if not price_row:
                    logger.warning(f"No price data for {code}")
                    continue

                current_price = price_row[0]

                # 最新のstatementデータを取得（5桁変換）
                # まず最新のデータを取得（BPSや配当データも含む）
                cursor.execute(
                    """
                    SELECT
                        EarningsPerShare,
                        BookValuePerShare,
                        ForecastDividendPerShareAnnual,
                        NextYearForecastDividendPerShareAnnual,
                        ForecastEarningsPerShare,
                        NextYearForecastEarningsPerShare,
                        ResultDividendPerShareAnnual
                    FROM statements
                    WHERE code = ?
                    ORDER BY CurrentFiscalYearStartDate DESC, DisclosedDate DESC
                    LIMIT 1
                    """,
                    (code_5digit,),
                )
                stmt_row = cursor.fetchone()

                if not stmt_row:
                    logger.warning(f"No statement data for {code}")
                    continue

                # 株価指標を計算
                eps = stmt_row[0]  # 実績EPS
                bps = stmt_row[1]  # 実績BPS
                forecast_dividend = stmt_row[2]  # 今期予想配当
                next_year_dividend = stmt_row[3]  # 来期予想配当
                forecast_eps = stmt_row[4]  # 今期予想EPS
                next_year_eps = stmt_row[5]  # 来期予想EPS
                result_dividend = stmt_row[6]  # 実績配当

                # 予想EPSを優先的に使用（なければ実績EPSを使用）
                expected_eps = forecast_eps or next_year_eps or eps

                # 予想EPSがない場合、別のレコードから取得を試みる
                if not expected_eps:
                    cursor.execute(
                        """
                        SELECT
                            ForecastEarningsPerShare,
                            NextYearForecastEarningsPerShare,
                            EarningsPerShare
                        FROM statements
                        WHERE code = ?
                          AND (ForecastEarningsPerShare IS NOT NULL
                               AND ForecastEarningsPerShare != ''
                               AND ForecastEarningsPerShare > 0
                               OR NextYearForecastEarningsPerShare IS NOT NULL
                               AND NextYearForecastEarningsPerShare != ''
                               AND NextYearForecastEarningsPerShare > 0
                               OR EarningsPerShare IS NOT NULL
                               AND EarningsPerShare != ''
                               AND EarningsPerShare > 0)
                        ORDER BY CurrentFiscalYearStartDate DESC, DisclosedDate DESC
                        LIMIT 1
                        """,
                        (code_5digit,),
                    )
                    eps_row = cursor.fetchone()
                    if eps_row:
                        forecast_eps = eps_row[0]
                        next_year_eps = eps_row[1]
                        eps = eps_row[2]
                        expected_eps = forecast_eps or next_year_eps or eps
                    else:
                        logger.debug(f"No EPS data found for {code}")

                # PER計算（予想EPSを優先、なければ実績EPS）
                expected_per = None
                if expected_eps and expected_eps > 0:
                    expected_per = round(current_price / expected_eps, 2)

                # PBR計算（実績BPSベース）
                actual_pbr = None
                if bps and bps > 0:
                    actual_pbr = round(current_price / bps, 2)

                # BPSがない場合、別のレコードから取得を試みる
                if not bps:
                    cursor.execute(
                        """
                        SELECT BookValuePerShare
                        FROM statements
                        WHERE code = ?
                          AND BookValuePerShare IS NOT NULL
                          AND BookValuePerShare != ''
                          AND BookValuePerShare > 0
                        ORDER BY CurrentFiscalYearStartDate DESC, DisclosedDate DESC
                        LIMIT 1
                        """,
                        (code_5digit,),
                    )
                    bps_row = cursor.fetchone()
                    if bps_row and bps_row[0]:
                        bps = bps_row[0]
                        if bps > 0:
                            actual_pbr = round(current_price / bps, 2)

                # 配当利回り計算（予想配当ベース）
                dividend_yield = None
                expected_dividend = (
                    next_year_dividend or forecast_dividend or result_dividend
                )

                # 配当データがない場合、別のレコードから取得を試みる
                if not expected_dividend:
                    cursor.execute(
                        """
                        SELECT
                            CASE
                                WHEN NextYearForecastDividendPerShareAnnual IS NOT NULL
                                     AND NextYearForecastDividendPerShareAnnual != ''
                                     AND NextYearForecastDividendPerShareAnnual > 0
                                THEN NextYearForecastDividendPerShareAnnual
                                WHEN ForecastDividendPerShareAnnual IS NOT NULL
                                     AND ForecastDividendPerShareAnnual != ''
                                     AND ForecastDividendPerShareAnnual > 0
                                THEN ForecastDividendPerShareAnnual
                                WHEN ResultDividendPerShareAnnual IS NOT NULL
                                     AND ResultDividendPerShareAnnual != ''
                                     AND ResultDividendPerShareAnnual > 0
                                THEN ResultDividendPerShareAnnual
                                ELSE NULL
                            END as dividend
                        FROM statements
                        WHERE code = ?
                          AND (
                               (NextYearForecastDividendPerShareAnnual IS NOT NULL AND NextYearForecastDividendPerShareAnnual != '')
                               OR (ForecastDividendPerShareAnnual IS NOT NULL AND ForecastDividendPerShareAnnual != '')
                               OR (ResultDividendPerShareAnnual IS NOT NULL AND ResultDividendPerShareAnnual != '')
                          )
                        ORDER BY CurrentFiscalYearStartDate DESC, DisclosedDate DESC
                        LIMIT 1
                        """,
                        (code_5digit,),
                    )
                    div_row = cursor.fetchone()
                    if div_row and div_row[0]:
                        expected_dividend = div_row[0]

                if expected_dividend and expected_dividend > 0:
                    dividend_yield = round((expected_dividend / current_price) * 100, 2)

                # 保有銘柄を更新（全ての該当レコードを更新）
                cursor.execute(
                    """
                    UPDATE holdings
                    SET
                        expected_per = ?,
                        actual_pbr = ?,
                        dividend_yield = ?,
                        expected_dividend = ?,
                        expected_eps = ?,
                        actual_bps = ?
                    WHERE user_id = ? AND code = ? AND deleted_at IS NULL
                    """,
                    (
                        expected_per,
                        actual_pbr,
                        dividend_yield,
                        expected_dividend,
                        expected_eps,
                        bps,
                        user_id,
                        code,
                    ),
                )

                if cursor.rowcount > 0:
                    updated_count += cursor.rowcount
                    if expected_per is not None:
                        logger.info(
                            f"Updated indicators for {code}: PER={expected_per}, "
                            f"PBR={actual_pbr}, Yield={dividend_yield}%"
                        )
                    else:
                        logger.info(
                            f"Updated indicators for {code}: PER=N/A (no EPS data), "
                            f"PBR={actual_pbr}, Yield={dividend_yield}%"
                        )

            conn.commit()
            logger.info(f"株価指標更新完了: {updated_count}件")
            return updated_count

        except sqlite3.Error as e:
            logger.error(f"株価指標更新エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @staticmethod
    def get_codes_needing_update(user_id: int) -> list[str]:
        """
        PER等が未設定の銘柄コードを取得

        Args:
            user_id: ユーザーID

        Returns:
            更新が必要な銘柄コードのリスト
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT DISTINCT code FROM holdings
                WHERE user_id = ?
                  AND deleted_at IS NULL
                  AND (expected_per IS NULL
                       OR actual_pbr IS NULL
                       OR dividend_yield IS NULL
                       OR expected_dividend IS NULL)
                """,
                (user_id,),
            )
            return [row[0] for row in cursor.fetchall()]

        finally:
            conn.close()
