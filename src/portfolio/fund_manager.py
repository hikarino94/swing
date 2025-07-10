"""投資信託の保有管理ロジック"""

import sqlite3
from datetime import datetime

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("portfolio.fund_manager")


class FundManager:
    """投資信託の保有管理クラス"""

    @staticmethod
    def update_funds_from_csv(
        user_id: int, holdings_data: list[dict], account_name: str = "default"
    ) -> tuple[int, int]:
        """
        CSVデータから投資信託の保有を追加・更新

        Args:
            user_id: ユーザーID
            holdings_data: 保有データのリスト
            account_name: 口座名

        Returns:
            (更新件数, 新規件数)のタプル
        """
        updated_count = 0
        new_count = 0

        # 投資信託データのみをフィルタリング
        fund_data = [data for data in holdings_data if data.get("is_fund", False)]

        for data in fund_data:
            fund_name = data.get("fund_name", "")
            if not fund_name:
                logger.warning("投資信託名が空のデータをスキップ")
                continue

            # 必須項目のチェック
            quantity = data.get("quantity")
            if quantity is None or quantity == 0:
                logger.warning(
                    f"投資信託の口数が無効: {fund_name} (quantity={quantity})"
                )
                continue

            average_price = data.get("average_price", 0)
            if average_price is None:
                average_price = 0

            # 口座タイプを取得（デフォルトは"特定"）
            account_type = data.get("account_type", "特定")

            # fund_masterからfund_idを取得または作成
            conn = sqlite3.connect(get_db_path())
            cursor = conn.cursor()
            try:
                # 既存のファンドを検索
                cursor.execute(
                    "SELECT fund_id FROM fund_master WHERE fund_name = ?",
                    (fund_name,),
                )
                fund_row = cursor.fetchone()

                if fund_row:
                    fund_id = fund_row[0]
                else:
                    # 新規ファンドとして登録
                    cursor.execute(
                        """
                        INSERT INTO fund_master (fund_name, is_active)
                        VALUES (?, 1)
                        """,
                        (fund_name,),
                    )
                    fund_id = cursor.lastrowid
                    conn.commit()
                    logger.info(f"新規ファンドを登録: {fund_name} (ID: {fund_id})")

                # 既存の投資信託保有情報を検索
                cursor.execute(
                    """
                    SELECT id, deleted_at FROM fund_holdings
                    WHERE user_id = ? AND fund_id = ? AND account_name = ? AND account_type = ?
                    """,
                    (user_id, fund_id, account_name, account_type),
                )
                existing_fund_row = cursor.fetchone()

                if existing_fund_row:
                    # 既存データを更新
                    cursor.execute(
                        """
                        UPDATE fund_holdings
                        SET quantity = ?, average_price = ?,
                            market_value = ?, profit_loss = ?,
                            profit_loss_ratio = ?,
                            updated_at = datetime('now'),
                            deleted_at = NULL
                        WHERE id = ?
                        """,
                        (
                            quantity,
                            average_price,
                            data.get("market_value"),
                            data.get("profit_loss"),
                            data.get("profit_loss_ratio"),
                            existing_fund_row[0],
                        ),
                    )
                    updated_count += 1
                    logger.info(f"投資信託更新: {fund_name} ({account_type})")
                else:
                    # 新規データを挿入
                    cursor.execute(
                        """
                        INSERT INTO fund_holdings
                        (user_id, fund_id, account_name, account_type,
                         quantity, average_price, market_value, profit_loss,
                         profit_loss_ratio)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            user_id,
                            fund_id,
                            account_name,
                            account_type,
                            quantity,
                            average_price,
                            data.get("market_value"),
                            data.get("profit_loss"),
                            data.get("profit_loss_ratio"),
                        ),
                    )
                    new_count += 1
                    logger.info(f"投資信託追加: {fund_name} ({account_type})")

                # 基準価額履歴を更新（現在の基準価額がある場合）
                current_price = data.get("current_price")
                if current_price:
                    today = datetime.now().strftime("%Y-%m-%d")

                    # 今日のデータがあるか確認
                    cursor.execute(
                        """
                        SELECT nav FROM fund_prices
                        WHERE fund_id = ? AND date = ?
                        """,
                        (fund_id, today),
                    )
                    price_row = cursor.fetchone()

                    if price_row:
                        # 既存データを更新
                        cursor.execute(
                            """
                            UPDATE fund_prices
                            SET nav = ?
                            WHERE fund_id = ? AND date = ?
                            """,
                            (current_price, fund_id, today),
                        )
                    else:
                        # 新規データを挿入
                        cursor.execute(
                            """
                            INSERT INTO fund_prices (fund_id, date, nav)
                            VALUES (?, ?, ?)
                            """,
                            (fund_id, today, current_price),
                        )

                conn.commit()

            except sqlite3.Error as e:
                logger.error(f"投資信託データ処理エラー: {e}")
                conn.rollback()
            finally:
                conn.close()

        logger.info(
            f"投資信託保有更新完了: 更新{updated_count}件, 新規{new_count}件（口座: {account_name}）"
        )

        return updated_count, new_count

    @staticmethod
    def delete_funds_not_in_csv(
        user_id: int, holdings_data: list[dict], account_name: str
    ) -> int:
        """
        CSVに含まれていない投資信託を論理削除

        Args:
            user_id: ユーザーID
            holdings_data: CSVの保有データ
            account_name: 口座名

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        fund_deleted_count = 0

        try:
            # 投資信託データのみフィルタリング
            fund_data = [data for data in holdings_data if data.get("is_fund", False)]

            if fund_data:
                csv_fund_names = [
                    data["fund_name"] for data in fund_data if data.get("fund_name")
                ]
                if csv_fund_names:
                    # CSVに含まれるファンド名からfund_idを取得
                    fund_placeholders = ",".join("?" * len(csv_fund_names))
                    cursor.execute(
                        f"""
                        SELECT fund_id FROM fund_master
                        WHERE fund_name IN ({fund_placeholders})
                        """,
                        csv_fund_names,
                    )
                    csv_fund_ids = [row[0] for row in cursor.fetchall()]

                    if csv_fund_ids:
                        fund_id_placeholders = ",".join("?" * len(csv_fund_ids))
                        cursor.execute(
                            f"""
                            UPDATE fund_holdings
                            SET deleted_at = datetime('now')
                            WHERE user_id = ?
                              AND account_name = ?
                              AND fund_id NOT IN ({fund_id_placeholders})
                              AND deleted_at IS NULL
                            """,
                            [user_id, account_name, *csv_fund_ids],
                        )
                        fund_deleted_count = cursor.rowcount
                    else:
                        fund_deleted_count = 0
                else:
                    # CSVに投資信託データがない場合は全ての投資信託を削除
                    cursor.execute(
                        """
                        UPDATE fund_holdings
                        SET deleted_at = datetime('now')
                        WHERE user_id = ?
                          AND account_name = ?
                          AND deleted_at IS NULL
                        """,
                        (user_id, account_name),
                    )
                    fund_deleted_count = cursor.rowcount
            else:
                # CSVに投資信託データがない場合は全ての投資信託を削除
                cursor.execute(
                    """
                    UPDATE fund_holdings
                    SET deleted_at = datetime('now')
                    WHERE user_id = ?
                      AND account_name = ?
                      AND deleted_at IS NULL
                    """,
                    (user_id, account_name),
                )
                fund_deleted_count = cursor.rowcount

            if fund_deleted_count > 0:
                logger.info(
                    f"CSVに存在しない投資信託を論理削除しました: {fund_deleted_count}件"
                )

            conn.commit()

        except sqlite3.Error as e:
            logger.error(f"投資信託論理削除エラー: {e}")
            conn.rollback()
        finally:
            conn.close()

        return fund_deleted_count

    @staticmethod
    def delete_all_funds(user_id: int) -> int:
        """
        ユーザーの全投資信託保有を削除

        Args:
            user_id: ユーザーID

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 投資信託の削除対象件数を取得
            cursor.execute(
                "SELECT COUNT(*) FROM fund_holdings WHERE user_id = ? AND deleted_at IS NULL",
                (user_id,),
            )
            fund_count = cursor.fetchone()[0]

            # 投資信託を論理削除
            cursor.execute(
                "UPDATE fund_holdings SET deleted_at = datetime('now') WHERE user_id = ? AND deleted_at IS NULL",
                (user_id,),
            )

            conn.commit()

            logger.info(f"投資信託保有削除完了: {fund_count}件")
            return int(fund_count)

        except sqlite3.Error as e:
            logger.error(f"投資信託保有削除エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @staticmethod
    def delete_funds_by_account(user_id: int, account_name: str) -> int:
        """
        特定口座の投資信託保有を削除

        Args:
            user_id: ユーザーID
            account_name: 口座名

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 投資信託の削除対象件数を取得
            cursor.execute(
                "SELECT COUNT(*) FROM fund_holdings WHERE user_id = ? AND account_name = ? AND deleted_at IS NULL",
                (user_id, account_name),
            )
            fund_count = cursor.fetchone()[0]

            # 投資信託を論理削除
            cursor.execute(
                "UPDATE fund_holdings SET deleted_at = datetime('now') WHERE user_id = ? AND account_name = ? AND deleted_at IS NULL",
                (user_id, account_name),
            )

            conn.commit()

            logger.info(f"投資信託保有削除完了: {fund_count}件（口座: {account_name}）")
            return int(fund_count)

        except sqlite3.Error as e:
            logger.error(f"投資信託保有削除エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
