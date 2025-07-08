"""ポートフォリオ管理ロジック"""

import sqlite3

from src.config import get_db_path
from src.utils.logging_config import get_logger

from .models import Holding, Transaction

logger = get_logger("portfolio.manager")


class PortfolioManager:
    """ポートフォリオ管理クラス"""

    @staticmethod
    def update_holdings_from_csv(
        user_id: int, holdings_data: list[dict], account_name: str = "default"
    ) -> tuple[int, int]:
        """
        CSVデータから保有銘柄を追加・更新

        Args:
            user_id: ユーザーID
            holdings_data: 保有銘柄データのリスト
            account_name: 口座名

        Returns:
            (更新件数, 新規件数)のタプル
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

        for data in holdings_data:
            # 投資信託かどうかをチェック
            is_fund = data.get("is_fund", False)

            # 口座タイプを取得（デフォルトは"特定"）
            account_type = data.get("account_type", "特定")

            if is_fund:
                # 投資信託の場合
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
                        from datetime import datetime

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

                # 投資信託の場合は次のデータへ
                continue

            # 以下は株式の処理
            # 既存の保有銘柄を検索（論理削除されたものも含めて検索）
            conn = sqlite3.connect(get_db_path())
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT id, deleted_at FROM holdings
                    WHERE user_id = ? AND code = ? AND account_name = ? AND account_type = ?
                    """,
                    (user_id, data["code"], account_name, account_type),
                )
                existing_row = cursor.fetchone()
                existing_id = existing_row[0] if existing_row else None
                is_deleted = existing_row[1] is not None if existing_row else False
            finally:
                conn.close()

            existing = None
            if existing_id and not is_deleted:
                # 論理削除されていない場合のみ取得
                existing = Holding.find_by_user_code_and_account(
                    user_id, data["code"], account_name, account_type
                )
            elif existing_id and is_deleted:
                # 論理削除されている場合は、復活させる処理を行う
                conn = sqlite3.connect(get_db_path())
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        UPDATE holdings
                        SET deleted_at = NULL, updated_at = datetime('now')
                        WHERE id = ?
                        """,
                        (existing_id,),
                    )
                    conn.commit()
                    # 復活させた後、既存レコードとして取得
                    existing = Holding(
                        user_id=user_id,
                        code=data["code"],
                        account_name=account_name,
                        account_type=account_type,
                    )
                    existing.id = existing_id
                    logger.debug(
                        f"論理削除されていた銘柄を復活: {data['code']} ({account_type})"
                    )
                except sqlite3.Error as e:
                    logger.error(f"論理削除銘柄の復活エラー: {e}")
                    conn.rollback()
                finally:
                    conn.close()

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
            f"保有銘柄更新完了: 更新{updated_count}件, 新規{new_count}件（口座: {account_name}）"
        )

        # CSVに含まれていない銘柄を論理削除
        if holdings_data:  # CSVにデータがある場合のみ処理
            conn = sqlite3.connect(get_db_path())
            cursor = conn.cursor()
            try:
                # CSVに含まれる銘柄コードのリスト
                csv_codes = [data["code"] for data in holdings_data]
                placeholders = ",".join("?" * len(csv_codes))

                # 論理削除の実行
                cursor.execute(
                    f"""
                    UPDATE holdings
                    SET deleted_at = datetime('now')
                    WHERE user_id = ?
                      AND account_name = ?
                      AND code NOT IN ({placeholders})
                      AND deleted_at IS NULL
                    """,
                    [user_id, account_name, *csv_codes],
                )

                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(
                        f"CSVに存在しない銘柄を論理削除しました: {deleted_count}件"
                    )

                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"論理削除エラー: {e}")
                conn.rollback()
            finally:
                conn.close()

        # SaveFile形式の場合、株価指標の再計算が必要
        if not is_standard_format and (updated_count > 0 or new_count > 0):
            # PER等が未設定の銘柄のみ更新
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
                codes_to_update = [row[0] for row in cursor.fetchall()]
            finally:
                conn.close()

            if codes_to_update:
                indicator_count = PortfolioManager.update_stock_indicators(
                    user_id, codes_to_update
                )
                logger.info(
                    f"SaveFile形式: 株価指標を再計算しました（{indicator_count}件）"
                )

        return updated_count, new_count

    @staticmethod
    def import_transactions_from_csv(
        user_id: int, transactions_data: list[dict]
    ) -> int:
        """
        CSVデータから取引履歴をインポート（保有銘柄への反映なし）

        Args:
            user_id: ユーザーID
            transactions_data: 取引データのリスト

        Returns:
            インポート件数
        """
        # ユーザーIDを追加
        for trans in transactions_data:
            trans["user_id"] = user_id

        # 一括挿入
        imported_count = Transaction.bulk_insert(transactions_data)

        # 保有銘柄の再計算は行わない（ユーザーの要望により）
        # if imported_count > 0:
        #     PortfolioManager.recalculate_holdings(user_id)

        return imported_count

    @staticmethod
    def recalculate_holdings(user_id: int) -> None:
        """
        取引履歴から保有銘柄を再計算（平均法）

        Args:
            user_id: ユーザーID
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 各銘柄の取引履歴を時系列で取得
            cursor.execute(
                """
                SELECT code
                FROM transactions
                WHERE user_id = ?
                GROUP BY code
            """,
                (user_id,),
            )

            codes = [row[0] for row in cursor.fetchall()]

            for code in codes:
                # 時系列で取引を取得（同日の場合はIDで順序を保証）
                cursor.execute(
                    """
                    SELECT id, transaction_date, transaction_type, quantity, price, commission
                    FROM transactions
                    WHERE user_id = ? AND code = ?
                    ORDER BY transaction_date ASC, id ASC
                """,
                    (user_id, code),
                )

                transactions = cursor.fetchall()

                # 平均法で計算
                total_quantity = 0
                total_cost = 0.0

                for (
                    trans_id,
                    _date,
                    trans_type,
                    quantity,
                    price,
                    commission,
                ) in transactions:
                    if trans_type == "buy":
                        # 買付時：総数量と総コストを増やす（手数料込み）
                        total_quantity += quantity
                        total_cost += quantity * price + (commission or 0)
                    else:  # sell
                        # 売却時：平均単価で総コストを減らす
                        if total_quantity > 0:
                            # 現在の平均取得価格を計算
                            avg_price = (
                                total_cost / total_quantity if total_quantity > 0 else 0
                            )

                            # 売却数量が保有数量を超える場合の処理（デイトレードなど）
                            sell_quantity = min(quantity, total_quantity)

                            total_quantity -= sell_quantity
                            total_cost -= sell_quantity * avg_price

                            # 売却数量が保有数量を超えていた場合の警告
                            if quantity > sell_quantity:
                                logger.warning(
                                    f"売却数量が保有数量を超過: {code} - 売却{quantity}株, 保有{sell_quantity}株（取引ID: {trans_id}）"
                                )
                        else:
                            logger.warning(
                                f"保有していない銘柄の売却: {code} - {quantity}株（取引ID: {trans_id}）"
                            )

                # 保有数量が残っている場合のみ保存
                if total_quantity > 0:
                    average_price = (
                        total_cost / total_quantity if total_quantity > 0 else 0
                    )

                    # 保有銘柄を更新
                    holding = Holding.find_by_user_and_code(user_id, code)
                    if holding is None:
                        holding = Holding(user_id=user_id, code=code)

                    holding.quantity = int(total_quantity)
                    holding.average_price = average_price

                    # 現在の株価を取得して時価評価を更新
                    # pricesテーブルは5桁（末尾0埋め）なので変換
                    code_5digit = code.ljust(5, "0")
                    cursor.execute(
                        """
                        SELECT close FROM prices
                        WHERE code = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """,
                        (code_5digit,),
                    )
                    price_row = cursor.fetchone()

                    if price_row:
                        current_price = price_row[0]
                        holding.update_market_value(current_price)

                    holding.save()
                    logger.info(
                        f"保有銘柄更新: {code} - {total_quantity}株 @ {average_price:.2f}円"
                    )

            # 保有数量が0になった銘柄を削除
            cursor.execute(
                """
                DELETE FROM holdings
                WHERE user_id = ? AND code NOT IN (
                    SELECT code FROM transactions
                    WHERE user_id = ?
                    GROUP BY code
                    HAVING SUM(CASE WHEN transaction_type = 'buy' THEN quantity
                                    WHEN transaction_type = 'sell' THEN -quantity
                                    ELSE 0 END) > 0
                )
                AND deleted_at IS NULL
            """,
                (user_id, user_id),
            )

            conn.commit()
            logger.info(f"保有銘柄再計算完了: ユーザーID {user_id}")

        except sqlite3.Error as e:
            logger.error(f"保有銘柄再計算エラー: {e}")
            conn.rollback()
        finally:
            conn.close()

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
    def aggregate_holdings_by_code(user_id: int) -> list[dict]:
        """
        ユーザーの保有銘柄を銘柄コードで集約（複数口座の合算）

        Args:
            user_id: ユーザーID

        Returns:
            集約された保有銘柄のリスト
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT
                    h.code,
                    li.company_name,
                    SUM(h.quantity) as total_quantity,
                    SUM(h.quantity * h.average_price) / NULLIF(SUM(h.quantity), 0) as weighted_avg_price,
                    SUM(h.market_value) as total_market_value,
                    SUM(h.profit_loss) as total_profit_loss,
                    COUNT(DISTINCT h.account_name) as account_count,
                    GROUP_CONCAT(DISTINCT h.account_name) as account_names,
                    GROUP_CONCAT(DISTINCT h.account_type) as account_types,
                    -- 株価指標は最初の値を使用（通常、同じ銘柄なら同じ値のはず）
                    MAX(h.expected_per) as expected_per,
                    MAX(h.actual_pbr) as actual_pbr,
                    MAX(h.dividend_yield) as dividend_yield,
                    MAX(h.expected_eps) as expected_eps,
                    MAX(h.actual_bps) as actual_bps,
                    MAX(h.expected_dividend) as expected_dividend,
                    MAX(h.lending_type) as lending_type
                FROM holdings h
                LEFT JOIN listed_info li ON (h.code || '0') = li.code
                WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                GROUP BY h.code, li.company_name
                ORDER BY h.code
            """,
                (user_id,),
            )

            aggregated_holdings = []
            for row in cursor.fetchall():
                holding = {
                    "type": "stock",  # 株式であることを示す
                    "code": row[0],
                    "company_name": row[1],
                    "total_quantity": row[2],
                    "weighted_avg_price": row[3] or 0,
                    "total_market_value": row[4],
                    "total_profit_loss": row[5],
                    "account_count": row[6],
                    "account_names": row[7],
                    "account_types": row[8],
                    "profit_loss_ratio": 0,
                    # 株価指標データ
                    "expected_per": row[9],
                    "actual_pbr": row[10],
                    "dividend_yield": row[11],
                    "expected_eps": row[12],
                    "actual_bps": row[13],
                    "expected_dividend": row[14],
                    "lending_type": row[15],
                }

                # 損益率の計算
                total_cost = holding["total_quantity"] * holding["weighted_avg_price"]
                if total_cost > 0:
                    holding["profit_loss_ratio"] = (
                        holding["total_profit_loss"] / total_cost
                    ) * 100

                aggregated_holdings.append(holding)

            logger.info(f"保有銘柄集約完了: {len(aggregated_holdings)}銘柄")
            return aggregated_holdings

        finally:
            conn.close()

    @staticmethod
    def delete_all_holdings(user_id: int) -> int:
        """
        ユーザーの全保有銘柄を削除

        Args:
            user_id: ユーザーID

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT COUNT(*) FROM holdings WHERE user_id = ? AND deleted_at IS NULL",
                (user_id,),
            )
            count = cursor.fetchone()[0]

            cursor.execute(
                "UPDATE holdings SET deleted_at = datetime('now') WHERE user_id = ? AND deleted_at IS NULL",
                (user_id,),
            )
            conn.commit()

            logger.info(f"保有銘柄削除完了: {count}件")
            return int(count)

        except sqlite3.Error as e:
            logger.error(f"保有銘柄削除エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @staticmethod
    def delete_holdings_by_account(user_id: int, account_name: str) -> int:
        """
        特定口座の保有銘柄を削除

        Args:
            user_id: ユーザーID
            account_name: 口座名

        Returns:
            削除件数
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT COUNT(*) FROM holdings WHERE user_id = ? AND account_name = ? AND deleted_at IS NULL",
                (user_id, account_name),
            )
            count = cursor.fetchone()[0]

            cursor.execute(
                "UPDATE holdings SET deleted_at = datetime('now') WHERE user_id = ? AND account_name = ? AND deleted_at IS NULL",
                (user_id, account_name),
            )
            conn.commit()

            logger.info(f"保有銘柄削除完了: {count}件（口座: {account_name}）")
            return int(count)

        except sqlite3.Error as e:
            logger.error(f"保有銘柄削除エラー: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

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
    def get_portfolio_summary(user_id: int) -> dict:
        """
        ポートフォリオのサマリー情報を取得

        Args:
            user_id: ユーザーID

        Returns:
            サマリー情報の辞書
        """
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()

        try:
            # 保有銘柄の集計
            cursor.execute(
                """
                SELECT
                    COUNT(*) as stock_count,
                    SUM(quantity * average_price) as total_cost,
                    SUM(market_value) as total_market_value,
                    SUM(profit_loss) as total_profit_loss
                FROM holdings
                WHERE user_id = ? AND quantity > 0 AND deleted_at IS NULL
            """,
                (user_id,),
            )

            row = cursor.fetchone()

            summary = {
                "stock_count": row[0] or 0,
                "total_cost": row[1] or 0,
                "total_market_value": row[2] or 0,
                "total_profit_loss": row[3] or 0,
                "total_profit_loss_ratio": 0,
            }

            # 損益率の計算
            if summary["total_cost"] > 0:
                summary["total_profit_loss_ratio"] = (
                    summary["total_profit_loss"] / summary["total_cost"] * 100
                )

            # 取引履歴の集計
            cursor.execute(
                """
                SELECT
                    COUNT(*) as transaction_count,
                    MIN(transaction_date) as first_transaction_date,
                    MAX(transaction_date) as last_transaction_date
                FROM transactions
                WHERE user_id = ?
            """,
                (user_id,),
            )

            trans_row = cursor.fetchone()
            summary["transaction_count"] = trans_row[0] or 0
            summary["first_transaction_date"] = trans_row[1]
            summary["last_transaction_date"] = trans_row[2]

            return summary

        finally:
            conn.close()
