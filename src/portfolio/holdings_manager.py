"""株式の保有銘柄管理ロジック"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

from .models import Holding

logger = get_logger("portfolio.holdings_manager")


class HoldingsManager:
    """株式の保有銘柄管理クラス"""

    @staticmethod
    def update_holdings_from_csv(
        user_id: int, holdings_data: list[dict], account_name: str = "default"
    ) -> tuple[int, int, bool]:
        """
        CSVデータから株式の保有銘柄を追加・更新

        Args:
            user_id: ユーザーID
            holdings_data: 保有銘柄データのリスト
            account_name: 口座名

        Returns:
            (更新件数, 新規件数, 標準形式フラグ)のタプル
        """
        updated_count = 0
        new_count = 0

        # CSVタイプの判定（最初のデータでチェック）
        is_standard_format = False
        if holdings_data:
            first_data = holdings_data[0]
            # 標準形式の特徴: PER、PBR、配当利回りなどの株価指標が含まれている
            if any(
                key in first_data
                for key in ["expected_per", "actual_pbr", "dividend_yield"]
            ):
                is_standard_format = True
                logger.info("標準形式CSVとして処理します")
            else:
                logger.info("SaveFile形式CSVとして処理します")

        # 株式データのみをフィルタリング
        stock_data = [data for data in holdings_data if not data.get("is_fund", False)]

        for data in stock_data:
            # 口座タイプを取得（デフォルトは"特定"）
            account_type = data.get("account_type", "特定")

            # 既存の保有銘柄を検索
            existing = Holding.find_by_user_code_and_account(
                user_id, data["code"], account_name, account_type
            )

            if is_standard_format:
                # 標準形式の処理: 重複時は上書き、PER等の再計算なし
                if existing:
                    holding = existing
                    updated_count += 1
                    logger.debug(
                        f"標準形式: 既存銘柄を上書き更新 {data['code']} ({account_type})"
                    )
                else:
                    holding = Holding(
                        user_id=user_id,
                        code=data["code"],
                        account_name=account_name,
                        account_type=account_type,
                    )
                    new_count += 1
                    logger.debug(
                        f"標準形式: 新規銘柄追加 {data['code']} ({account_type})"
                    )

                # データを設定（標準形式は全て上書き）
                holding.quantity = data["quantity"]
                holding.average_price = data.get("average_price", 0)

                # 標準形式はPER等のデータを持っているのでそのまま設定
                holding.expected_per = data.get("expected_per")
                holding.actual_pbr = data.get("actual_pbr")
                holding.dividend_yield = data.get("dividend_yield")
                holding.expected_eps = data.get("expected_eps")
                holding.actual_bps = data.get("actual_bps")
                holding.expected_dividend = data.get("expected_dividend")
                holding.lending_type = data.get("lending_type")

            else:
                # SaveFile形式の処理: 条件付き更新、PER等の再計算
                if existing:
                    holding = existing
                    updated_count += 1
                    logger.debug(
                        f"SaveFile形式: 既存銘柄を更新 {data['code']} ({account_type})"
                    )

                    # 保有株数と取得単価を更新
                    holding.quantity = data["quantity"]
                    holding.average_price = data.get("average_price", 0)

                    # PER等の指標が未設定の場合のみ、後で再計算するためNoneを設定
                    if holding.expected_per is None:
                        holding.expected_per = None
                    if holding.actual_pbr is None:
                        holding.actual_pbr = None
                    if holding.dividend_yield is None:
                        holding.dividend_yield = None
                    if holding.expected_dividend is None:
                        holding.expected_dividend = None
                    # 既存の値がある場合は更新しない（要件通り）

                else:
                    holding = Holding(
                        user_id=user_id,
                        code=data["code"],
                        account_name=account_name,
                        account_type=account_type,
                    )
                    new_count += 1
                    logger.debug(
                        f"SaveFile形式: 新規銘柄追加 {data['code']} ({account_type})"
                    )

                    # データを設定
                    holding.quantity = data["quantity"]
                    holding.average_price = data.get("average_price", 0)

                    # SaveFile形式はPER等のデータを持っていないのでNoneを設定（後で再計算）
                    holding.expected_per = None
                    holding.actual_pbr = None
                    holding.dividend_yield = None
                    holding.expected_dividend = None
                    holding.expected_eps = None
                    holding.actual_bps = None
                    holding.lending_type = None

            # 市場価値と損益は取得価格と数量から再計算
            # CSVからの値はデバッグ用にログ出力のみ
            csv_market_value = data.get("market_value")
            csv_profit_loss = data.get("profit_loss")
            csv_profit_loss_ratio = data.get("profit_loss_ratio")

            if csv_market_value is not None and csv_profit_loss is not None:
                logger.debug(
                    f"CSV損益データ: {data['code']} ({account_type}) - "
                    f"評価額: {csv_market_value}, 損益: {csv_profit_loss}, "
                    f"損益率: {csv_profit_loss_ratio}%"
                )

            # 一旦NULLに設定（後でupdate_market_valuesで更新）
            holding.market_value = None
            holding.profit_loss = None
            holding.profit_loss_ratio = None

            # 保存
            if not holding.save():
                logger.error(f"保有銘柄の保存に失敗: {data['code']} ({account_type})")

        logger.info(
            f"株式保有銘柄更新完了: 更新{updated_count}件, 新規{new_count}件（口座: {account_name}）"
        )

        return updated_count, new_count, is_standard_format

    @staticmethod
    def delete_stocks_not_in_csv(
        user_id: int, holdings_data: list[dict], account_name: str
    ) -> int:
        """
        CSVに含まれていない株式を物理削除

        Args:
            user_id: ユーザーID
            holdings_data: CSVの保有銘柄データ
            account_name: 口座名

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        stock_deleted_count = 0

        try:
            # 株式データのみフィルタリング
            stock_data = [
                data for data in holdings_data if not data.get("is_fund", False)
            ]

            if stock_data:
                csv_codes = [data["code"] for data in stock_data if data.get("code")]
                if csv_codes:
                    placeholders = ",".join("?" * len(csv_codes))
                    cursor.execute(
                        f"""
                        DELETE FROM holdings
                        WHERE user_id = ?
                          AND account_name = ?
                          AND code NOT IN ({placeholders})
                        """,
                        [user_id, account_name, *csv_codes],
                    )
                    stock_deleted_count = cursor.rowcount
                else:
                    # CSVに株式データがない場合は全ての株式を削除
                    cursor.execute(
                        """
                        DELETE FROM holdings
                        WHERE user_id = ?
                          AND account_name = ?
                        """,
                        (user_id, account_name),
                    )
                    stock_deleted_count = cursor.rowcount
            else:
                # CSVに株式データがない場合は全ての株式を削除
                cursor.execute(
                    """
                    DELETE FROM holdings
                    WHERE user_id = ?
                      AND account_name = ?
                    """,
                    (user_id, account_name),
                )
                stock_deleted_count = cursor.rowcount

            if stock_deleted_count > 0:
                logger.info(
                    f"CSVに存在しない株式を物理削除しました: {stock_deleted_count}件"
                )

            conn.commit()

        except sqlite3.Error as e:
            logger.error(f"株式物理削除エラー: {e}")
            conn.rollback()
        finally:
            conn.close()

        return stock_deleted_count

    @staticmethod
    def update_market_values(user_id: int) -> int:
        """
        保有銘柄の時価評価を最新の株価で更新

        Args:
            user_id: ユーザーID

        Returns:
            更新件数
        """
        holdings = Holding.find_all_by_user(user_id)
        updated_count = 0

        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # データベースから最新の日付を取得
            cursor.execute("SELECT MAX(date) as latest_date FROM prices")
            latest_date_row = cursor.fetchone()
            latest_date = latest_date_row[0] if latest_date_row else None

            if not latest_date:
                logger.warning("No price data available in database")
                return 0

            logger.info(f"Using latest price date: {latest_date}")

            for holding in holdings:
                # pricesテーブルは5桁（末尾0埋め）なので変換
                code_5digit = holding.code.ljust(5, "0")

                # 最新日付の株価を取得
                cursor.execute(
                    """
                    SELECT close FROM prices
                    WHERE code = ? AND date = ?
                """,
                    (code_5digit, latest_date),
                )
                row = cursor.fetchone()

                if row:
                    current_price = row[0]
                    holding.update_market_value(current_price)
                    if holding.save():
                        updated_count += 1
                else:
                    # 最新日付にデータがない場合は、その銘柄の最新データを取得
                    cursor.execute(
                        """
                        SELECT close FROM prices
                        WHERE code = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """,
                        (code_5digit,),
                    )
                    row = cursor.fetchone()

                    if row:
                        current_price = row[0]
                        holding.update_market_value(current_price)
                        if holding.save():
                            updated_count += 1
                    else:
                        logger.warning(
                            f"No price data found for code {holding.code} (tried {code_5digit})"
                        )

            logger.info(f"時価評価更新完了: {updated_count}件")
            return updated_count

        finally:
            conn.close()

    @staticmethod
    def delete_all_holdings(user_id: int) -> int:
        """
        ユーザーの全株式保有銘柄を削除

        Args:
            user_id: ユーザーID

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 株式の削除対象件数を取得
            cursor.execute(
                "SELECT COUNT(*) FROM holdings WHERE user_id = ?",
                (user_id,),
            )
            stock_count = cursor.fetchone()[0]

            # 株式を物理削除
            cursor.execute(
                "DELETE FROM holdings WHERE user_id = ?",
                (user_id,),
            )

            conn.commit()

            logger.info(f"株式保有銘柄削除完了: {stock_count}件")
            return int(stock_count)

        except sqlite3.Error as e:
            logger.error(f"株式保有銘柄削除エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @staticmethod
    def delete_holdings_by_account(user_id: int, account_name: str) -> int:
        """
        特定口座の株式保有銘柄を削除

        Args:
            user_id: ユーザーID
            account_name: 口座名

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 株式の削除対象件数を取得
            cursor.execute(
                "SELECT COUNT(*) FROM holdings WHERE user_id = ? AND account_name = ?",
                (user_id, account_name),
            )
            stock_count = cursor.fetchone()[0]

            # 株式を物理削除
            cursor.execute(
                "DELETE FROM holdings WHERE user_id = ? AND account_name = ?",
                (user_id, account_name),
            )

            conn.commit()

            logger.info(
                f"株式保有銘柄削除完了: {stock_count}件（口座: {account_name}）"
            )
            return int(stock_count)

        except sqlite3.Error as e:
            logger.error(f"株式保有銘柄削除エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
