"""保有銘柄モデル"""

import sqlite3
from typing import Optional

from src.config import get_db_path
from src.utils.logging_config import get_logger

logger = get_logger("portfolio.models.holding")


class Holding:
    """保有銘柄モデル"""

    def __init__(
        self,
        user_id: int,
        code: str,
        account_name: str = "default",
        account_type: str = "特定",
        **kwargs,
    ):
        self.id: int | None = kwargs.get("id")
        self.user_id: int = user_id
        self.code: str = code
        self.account_name: str = account_name
        self.account_type: str = account_type  # 特定/NISA/つみたてNISA等
        self.quantity: int = kwargs.get("quantity", 0)
        self.average_price: float = kwargs.get("average_price", 0.0)
        self.market_value: float | None = kwargs.get("market_value")
        self.profit_loss: float | None = kwargs.get("profit_loss")
        self.profit_loss_ratio: float | None = kwargs.get("profit_loss_ratio")
        self.updated_at: str | None = kwargs.get("updated_at")
        # 株価指標データ
        self.expected_per: float | None = kwargs.get("expected_per")
        self.actual_pbr: float | None = kwargs.get("actual_pbr")
        self.dividend_yield: float | None = kwargs.get("dividend_yield")
        self.expected_eps: float | None = kwargs.get("expected_eps")
        self.actual_bps: float | None = kwargs.get("actual_bps")
        self.expected_dividend: float | None = kwargs.get("expected_dividend")
        self.lending_type: str | None = kwargs.get("lending_type")
        # 追加情報（DBには保存しない）
        self.company_name: str | None = kwargs.get("company_name")

    @classmethod
    def from_db_row(cls, row: tuple, description: list) -> "Holding":
        """データベースの行データからHoldingオブジェクトを作成"""
        data = {}
        for i, desc in enumerate(description):
            data[desc[0]] = row[i]

        # 必須フィールドを取得
        user_id = data.pop("user_id")
        code = data.pop("code")
        account_name = data.pop("account_name", "default")
        account_type = data.pop("account_type", "特定")

        return cls(user_id, code, account_name, account_type, **data)

    @classmethod
    def find_by_user_and_code(cls, user_id: int, code: str) -> Optional["Holding"]:
        """ユーザーIDと銘柄コードで保有銘柄を検索（後方互換性のため残す）"""
        return cls.find_by_user_code_and_account(user_id, code, "default")

    @classmethod
    def find_by_user_code_and_account(
        cls, user_id: int, code: str, account_name: str, account_type: str | None = None
    ) -> Optional["Holding"]:
        """ユーザーID、銘柄コード、口座名（、口座タイプ）で保有銘柄を検索"""
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        try:
            # account_typeカラムが存在するか確認
            cursor.execute("PRAGMA table_info(holdings)")
            columns = [col[1] for col in cursor.fetchall()]
            has_account_type = "account_type" in columns

            if has_account_type and account_type:
                cursor.execute(
                    """
                    SELECT id, user_id, code, account_name, account_type, quantity, average_price,
                           market_value, profit_loss, profit_loss_ratio, updated_at,
                           expected_per, actual_pbr, dividend_yield, expected_eps,
                           actual_bps, expected_dividend, lending_type
                    FROM holdings
                    WHERE user_id = ? AND code = ? AND account_name = ? AND account_type = ? AND deleted_at IS NULL
                """,
                    (user_id, code, account_name, account_type),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, user_id, code, account_name, quantity, average_price,
                           market_value, profit_loss, profit_loss_ratio, updated_at,
                           expected_per, actual_pbr, dividend_yield, expected_eps,
                           actual_bps, expected_dividend, lending_type
                    FROM holdings
                    WHERE user_id = ? AND code = ? AND account_name = ? AND deleted_at IS NULL
                """,
                    (user_id, code, account_name),
                )
            row = cursor.fetchone()
            if row:
                if has_account_type and account_type:
                    holding = cls(
                        user_id=row[1],
                        code=row[2],
                        account_name=row[3],
                        account_type=row[4],
                    )
                    holding.id = row[0]
                    holding.quantity = row[5]
                    holding.average_price = row[6]
                    holding.market_value = row[7]
                    holding.profit_loss = row[8]
                    holding.profit_loss_ratio = row[9]
                    holding.updated_at = row[10]
                    # 株価指標データ
                    holding.expected_per = row[11]
                    holding.actual_pbr = row[12]
                    holding.dividend_yield = row[13]
                    holding.expected_eps = row[14]
                    holding.actual_bps = row[15]
                    holding.expected_dividend = row[16]
                    holding.lending_type = row[17]
                else:
                    holding = cls(user_id=row[1], code=row[2], account_name=row[3])
                    holding.id = row[0]
                    holding.quantity = row[4]
                    holding.average_price = row[5]
                    holding.market_value = row[6]
                    holding.profit_loss = row[7]
                    holding.profit_loss_ratio = row[8]
                    holding.updated_at = row[9]
                    # 株価指標データ
                    holding.expected_per = row[10]
                    holding.actual_pbr = row[11]
                    holding.dividend_yield = row[12]
                    holding.expected_eps = row[13]
                    holding.actual_bps = row[14]
                    holding.expected_dividend = row[15]
                    holding.lending_type = row[16]
                return holding
            return None
        finally:
            conn.close()

    @classmethod
    def find_all_by_user(cls, user_id: int) -> list["Holding"]:
        """ユーザーの全保有銘柄を取得"""
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        try:
            # account_typeカラムが存在するか確認
            cursor.execute("PRAGMA table_info(holdings)")
            columns = [col[1] for col in cursor.fetchall()]
            has_account_type = "account_type" in columns

            # listed_infoテーブルが存在するか確認
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='listed_info'"
            )
            has_listed_info = cursor.fetchone() is not None

            if has_account_type:
                if has_listed_info:
                    cursor.execute(
                        """
                        SELECT h.id, h.user_id, h.code, h.account_name, h.account_type, h.quantity, h.average_price,
                               h.market_value, h.profit_loss, h.profit_loss_ratio, h.updated_at,
                               h.expected_per, h.actual_pbr, h.dividend_yield, h.expected_eps,
                               h.actual_bps, h.expected_dividend, h.lending_type,
                               li.company_name
                        FROM holdings h
                        LEFT JOIN listed_info li ON (h.code || '0') = li.code
                        WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                        ORDER BY h.code, h.account_name, h.account_type
                    """,
                        (user_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT h.id, h.user_id, h.code, h.account_name, h.account_type, h.quantity, h.average_price,
                               h.market_value, h.profit_loss, h.profit_loss_ratio, h.updated_at,
                               h.expected_per, h.actual_pbr, h.dividend_yield, h.expected_eps,
                               h.actual_bps, h.expected_dividend, h.lending_type,
                               NULL as company_name
                        FROM holdings h
                        WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                        ORDER BY h.code, h.account_name, h.account_type
                    """,
                        (user_id,),
                    )
            else:
                if has_listed_info:
                    cursor.execute(
                        """
                        SELECT h.id, h.user_id, h.code, h.account_name, h.quantity, h.average_price,
                               h.market_value, h.profit_loss, h.profit_loss_ratio, h.updated_at,
                               h.expected_per, h.actual_pbr, h.dividend_yield, h.expected_eps,
                               h.actual_bps, h.expected_dividend, h.lending_type,
                               li.company_name
                        FROM holdings h
                        LEFT JOIN listed_info li ON (h.code || '0') = li.code
                        WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                        ORDER BY h.code, h.account_name
                    """,
                        (user_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT h.id, h.user_id, h.code, h.account_name, h.quantity, h.average_price,
                               h.market_value, h.profit_loss, h.profit_loss_ratio, h.updated_at,
                               h.expected_per, h.actual_pbr, h.dividend_yield, h.expected_eps,
                               h.actual_bps, h.expected_dividend, h.lending_type,
                               NULL as company_name
                        FROM holdings h
                        WHERE h.user_id = ? AND h.quantity > 0 AND h.deleted_at IS NULL
                        ORDER BY h.code, h.account_name
                    """,
                        (user_id,),
                    )

            holdings = []
            for row in cursor.fetchall():
                if has_account_type:
                    holding = cls(
                        user_id=row[1],
                        code=row[2],
                        account_name=row[3],
                        account_type=row[4],
                    )
                    holding.id = row[0]
                    holding.quantity = row[5]
                    holding.average_price = row[6]
                    holding.market_value = row[7]
                    holding.profit_loss = row[8]
                    holding.profit_loss_ratio = row[9]
                    holding.updated_at = row[10]
                    # 株価指標データ
                    holding.expected_per = row[11]
                    holding.actual_pbr = row[12]
                    holding.dividend_yield = row[13]
                    holding.expected_eps = row[14]
                    holding.actual_bps = row[15]
                    holding.expected_dividend = row[16]
                    holding.lending_type = row[17]
                    holding.company_name = row[18]  # 追加情報
                else:
                    holding = cls(user_id=row[1], code=row[2], account_name=row[3])
                    holding.id = row[0]
                    holding.quantity = row[4]
                    holding.average_price = row[5]
                    holding.market_value = row[6]
                    holding.profit_loss = row[7]
                    holding.profit_loss_ratio = row[8]
                    holding.updated_at = row[9]
                    # 株価指標データ
                    holding.expected_per = row[10]
                    holding.actual_pbr = row[11]
                    holding.dividend_yield = row[12]
                    holding.expected_eps = row[13]
                    holding.actual_bps = row[14]
                    holding.expected_dividend = row[15]
                    holding.lending_type = row[16]
                    holding.company_name = row[17]  # 追加情報
                holdings.append(holding)

            return holdings
        finally:
            conn.close()

    def save(self) -> bool:
        """保有銘柄情報を保存"""
        conn = sqlite3.connect(get_db_path())
        cursor = conn.cursor()
        try:
            # 既存のレコードをチェック
            existing = self.find_by_user_code_and_account(
                self.user_id, self.code, self.account_name, self.account_type
            )

            if existing:
                # 既存レコードがある場合は更新
                self.id = existing.id
                # account_typeカラムが存在するか確認
                cursor.execute("PRAGMA table_info(holdings)")
                columns = [col[1] for col in cursor.fetchall()]
                has_account_type = "account_type" in columns

                if has_account_type:
                    # 財務情報の上書き防止：既存値を保持する場合
                    cursor.execute(
                        """
                        UPDATE holdings
                        SET quantity = ?, average_price = ?, market_value = ?,
                            profit_loss = ?, profit_loss_ratio = ?, account_type = ?,
                            expected_per = COALESCE(?, expected_per),
                            actual_pbr = COALESCE(?, actual_pbr),
                            dividend_yield = COALESCE(?, dividend_yield),
                            expected_eps = COALESCE(?, expected_eps),
                            actual_bps = COALESCE(?, actual_bps),
                            expected_dividend = COALESCE(?, expected_dividend),
                            lending_type = COALESCE(?, lending_type),
                            updated_at = datetime('now')
                        WHERE id = ?
                    """,
                        (
                            self.quantity,
                            self.average_price,
                            self.market_value,
                            self.profit_loss,
                            self.profit_loss_ratio,
                            self.account_type,
                            self.expected_per,
                            self.actual_pbr,
                            self.dividend_yield,
                            self.expected_eps,
                            self.actual_bps,
                            self.expected_dividend,
                            self.lending_type,
                            self.id,
                        ),
                    )
                else:
                    # 財務情報の上書き防止：既存値を保持する場合
                    cursor.execute(
                        """
                        UPDATE holdings
                        SET quantity = ?, average_price = ?, market_value = ?,
                            profit_loss = ?, profit_loss_ratio = ?,
                            expected_per = COALESCE(?, expected_per),
                            actual_pbr = COALESCE(?, actual_pbr),
                            dividend_yield = COALESCE(?, dividend_yield),
                            expected_eps = COALESCE(?, expected_eps),
                            actual_bps = COALESCE(?, actual_bps),
                            expected_dividend = COALESCE(?, expected_dividend),
                            lending_type = COALESCE(?, lending_type),
                            updated_at = datetime('now')
                        WHERE id = ?
                    """,
                        (
                            self.quantity,
                            self.average_price,
                            self.market_value,
                            self.profit_loss,
                            self.profit_loss_ratio,
                            self.expected_per,
                            self.actual_pbr,
                            self.dividend_yield,
                            self.expected_eps,
                            self.actual_bps,
                            self.expected_dividend,
                            self.lending_type,
                            self.id,
                        ),
                    )
            elif self.id is None:
                # 新規作成
                # account_typeカラムが存在するか確認
                cursor.execute("PRAGMA table_info(holdings)")
                columns = [col[1] for col in cursor.fetchall()]
                has_account_type = "account_type" in columns

                if has_account_type:
                    cursor.execute(
                        """
                        INSERT INTO holdings
                        (user_id, code, account_name, account_type, quantity, average_price, market_value,
                         profit_loss, profit_loss_ratio, expected_per, actual_pbr,
                         dividend_yield, expected_eps, actual_bps, expected_dividend,
                         lending_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            self.user_id,
                            self.code,
                            self.account_name,
                            self.account_type,
                            self.quantity,
                            self.average_price,
                            self.market_value,
                            self.profit_loss,
                            self.profit_loss_ratio,
                            self.expected_per,
                            self.actual_pbr,
                            self.dividend_yield,
                            self.expected_eps,
                            self.actual_bps,
                            self.expected_dividend,
                            self.lending_type,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO holdings
                        (user_id, code, account_name, quantity, average_price, market_value,
                         profit_loss, profit_loss_ratio, expected_per, actual_pbr,
                         dividend_yield, expected_eps, actual_bps, expected_dividend,
                         lending_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            self.user_id,
                            self.code,
                            self.account_name,
                            self.quantity,
                            self.average_price,
                            self.market_value,
                            self.profit_loss,
                            self.profit_loss_ratio,
                            self.expected_per,
                            self.actual_pbr,
                            self.dividend_yield,
                            self.expected_eps,
                            self.actual_bps,
                            self.expected_dividend,
                            self.lending_type,
                        ),
                    )
                self.id = cursor.lastrowid
            else:
                # 更新
                cursor.execute(
                    """
                    UPDATE holdings
                    SET quantity = ?, average_price = ?, market_value = ?,
                        profit_loss = ?, profit_loss_ratio = ?,
                        expected_per = ?, actual_pbr = ?, dividend_yield = ?,
                        expected_eps = ?, actual_bps = ?, expected_dividend = ?,
                        lending_type = ?, updated_at = datetime('now')
                    WHERE id = ?
                """,
                    (
                        self.quantity,
                        self.average_price,
                        self.market_value,
                        self.profit_loss,
                        self.profit_loss_ratio,
                        self.expected_per,
                        self.actual_pbr,
                        self.dividend_yield,
                        self.expected_eps,
                        self.actual_bps,
                        self.expected_dividend,
                        self.lending_type,
                        self.id,
                    ),
                )

            conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"保有銘柄保存エラー: {e}")
            return False
        finally:
            conn.close()

    def update_market_value(self, current_price: float) -> None:
        """時価評価額と損益を更新"""
        if self.quantity > 0 and current_price > 0:
            self.market_value = float(self.quantity * current_price)
            total_cost = self.quantity * self.average_price
            self.profit_loss = float(self.market_value - total_cost)
            if total_cost > 0:
                self.profit_loss_ratio = float((self.profit_loss / total_cost) * 100)
            else:
                self.profit_loss_ratio = 0.0
