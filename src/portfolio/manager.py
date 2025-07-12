"""ポートフォリオ管理ファサード"""

from src.utils.logging_config import get_logger

from .fund_manager import FundManager
from .holdings_manager import HoldingsManager
from .indicators_manager import IndicatorsManager
from .portfolio_aggregator import PortfolioAggregator
from .transaction_manager import TransactionManager

logger = get_logger("portfolio.manager")


class PortfolioManager:
    """ポートフォリオ管理のファサードクラス"""

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
        total_updated = 0
        total_new = 0

        # 株式の更新
        stock_updated, stock_new, is_standard_format = (
            HoldingsManager.update_holdings_from_csv(
                user_id, holdings_data, account_name
            )
        )
        total_updated += stock_updated
        total_new += stock_new

        # 投資信託の更新
        fund_updated, fund_new = FundManager.update_funds_from_csv(
            user_id, holdings_data, account_name
        )
        total_updated += fund_updated
        total_new += fund_new

        # CSVに含まれていない銘柄を論理削除
        if holdings_data:
            stock_deleted = HoldingsManager.delete_stocks_not_in_csv(
                user_id, holdings_data, account_name
            )
            fund_deleted = FundManager.delete_funds_not_in_csv(
                user_id, holdings_data, account_name
            )

            if stock_deleted > 0 or fund_deleted > 0:
                logger.info(
                    f"CSVに存在しない銘柄を論理削除しました: "
                    f"株式{stock_deleted}件、投資信託{fund_deleted}件"
                )

        # SaveFile形式の場合、株価指標の再計算が必要
        if not is_standard_format and (stock_updated > 0 or stock_new > 0):
            codes_to_update = IndicatorsManager.get_codes_needing_update(user_id)
            if codes_to_update:
                indicator_count = IndicatorsManager.update_stock_indicators(
                    user_id, codes_to_update
                )
                logger.info(
                    f"SaveFile形式: 株価指標を再計算しました（{indicator_count}件）"
                )

        return total_updated, total_new

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
        return TransactionManager.import_transactions_from_csv(
            user_id, transactions_data
        )

    @staticmethod
    def recalculate_holdings(user_id: int) -> None:
        """
        取引履歴から保有銘柄を再計算（平均法）

        Args:
            user_id: ユーザーID
        """
        TransactionManager.recalculate_holdings(user_id)

    @staticmethod
    def update_market_values(user_id: int) -> int:
        """
        保有銘柄の時価評価を最新の株価で更新

        Args:
            user_id: ユーザーID

        Returns:
            更新件数
        """
        return HoldingsManager.update_market_values(user_id)

    @staticmethod
    def aggregate_holdings_by_code(user_id: int) -> list[dict]:
        """
        ユーザーの保有銘柄を銘柄コードで集約（複数口座の合算）

        Args:
            user_id: ユーザーID

        Returns:
            集約された保有銘柄のリスト
        """
        return PortfolioAggregator.aggregate_holdings_by_code(user_id)

    @staticmethod
    def delete_all_holdings(user_id: int) -> int:
        """
        ユーザーの全保有銘柄を削除（株式と投資信託の両方）

        Args:
            user_id: ユーザーID

        Returns:
            削除件数
        """
        stock_count = HoldingsManager.delete_all_holdings(user_id)
        fund_count = FundManager.delete_all_funds(user_id)

        total_count = stock_count + fund_count
        logger.info(
            f"保有銘柄削除完了: 株式{stock_count}件、投資信託{fund_count}件（合計{total_count}件）"
        )
        return total_count

    @staticmethod
    def delete_holdings_by_account(user_id: int, account_name: str) -> int:
        """
        特定口座の保有銘柄を削除（株式と投資信託の両方）

        Args:
            user_id: ユーザーID
            account_name: 口座名

        Returns:
            削除件数
        """
        stock_count = HoldingsManager.delete_holdings_by_account(user_id, account_name)
        fund_count = FundManager.delete_funds_by_account(user_id, account_name)

        total_count = stock_count + fund_count
        logger.info(
            f"保有銘柄削除完了: 株式{stock_count}件、投資信託{fund_count}件（合計{total_count}件）（口座: {account_name}）"
        )
        return total_count

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
        return IndicatorsManager.update_stock_indicators(user_id, codes)

    @staticmethod
    def get_portfolio_summary(user_id: int) -> dict:
        """
        ポートフォリオのサマリー情報を取得

        Args:
            user_id: ユーザーID

        Returns:
            サマリー情報の辞書
        """
        return PortfolioAggregator.get_portfolio_summary(user_id)
