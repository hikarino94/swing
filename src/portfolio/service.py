"""ポートフォリオビジネスロジック層"""

from typing import Any

from src.utils.logging_config import get_logger

from .models import Holding
from .repository import PortfolioRepository

logger = get_logger("portfolio.service")


class PortfolioService:
    """ポートフォリオのビジネスロジックを管理"""

    def __init__(self):
        self.repo = PortfolioRepository()

    def update_holdings_from_csv(
        self, user_id: int, holdings_data: list[dict], account_name: str = "default"
    ) -> tuple[int, int]:
        """CSVデータから保有銘柄を追加・更新"""
        updated_count = 0
        new_count = 0

        # CSVタイプの判定
        is_standard_format = self._is_standard_format(holdings_data)
        logger.info(
            f"{'標準形式' if is_standard_format else 'SaveFile形式'}CSVとして処理します"
        )

        for data in holdings_data:
            holding = self._create_holding_from_csv_data(
                user_id, data, account_name, is_standard_format
            )

            if holding:
                is_updated, is_new = self.repo.upsert_holding(holding)
                if is_updated:
                    updated_count += 1
                elif is_new:
                    new_count += 1

        logger.info(f"保有銘柄更新完了: 更新={updated_count}件, 新規={new_count}件")
        return updated_count, new_count

    def recalculate_holdings(self, user_id: int) -> None:
        """取引履歴から保有銘柄を再計算"""
        logger.info(f"ユーザー {user_id} の保有銘柄を再計算開始")

        # 既存の保有銘柄を全て論理削除
        deleted_count = self.repo.delete_all_holdings(user_id)
        logger.info(f"{deleted_count}件の既存保有銘柄を論理削除")

        # 取引履歴を取得
        transactions = self.repo.get_transactions(user_id)

        # 銘柄・口座別に集計
        holdings_map = {}

        for trans in transactions:
            key = (
                trans["code"],
                trans.get("account_name", "default"),
                trans.get("account_type", "特定"),
            )

            if key not in holdings_map:
                holdings_map[key] = {
                    "quantity": 0,
                    "total_cost": 0,
                    "commission": 0,
                    "tax": 0,
                }

            holding_data = holdings_map[key]

            if trans["transaction_type"] == "buy":
                holding_data["quantity"] += trans["quantity"]
                holding_data["total_cost"] += trans["total_amount"]
                holding_data["commission"] += trans.get("commission", 0)
                holding_data["tax"] += trans.get("tax", 0)
            else:  # sell
                if holding_data["quantity"] > 0:
                    # 売却分を減算
                    sell_ratio = trans["quantity"] / holding_data["quantity"]
                    holding_data["quantity"] -= trans["quantity"]
                    holding_data["total_cost"] *= 1 - sell_ratio
                    holding_data["commission"] *= 1 - sell_ratio
                    holding_data["tax"] *= 1 - sell_ratio

        # 保有銘柄として登録
        for (code, account_name, account_type), holding_data in holdings_map.items():
            if holding_data["quantity"] > 0:
                average_price = holding_data["total_cost"] / holding_data["quantity"]

                holding = Holding(
                    user_id=user_id,
                    code=code,
                    account_name=account_name,
                    account_type=account_type,
                    quantity=holding_data["quantity"],
                    average_price=average_price,
                )

                self.repo.upsert_holding(holding)

        # 市場価値を更新
        self.update_market_values(user_id)

        logger.info("保有銘柄の再計算が完了しました")

    def update_market_values(self, user_id: int) -> int:
        """保有銘柄の市場価値を更新"""
        holdings = self.repo.get_holdings(user_id)
        if not holdings:
            return 0

        # 銘柄コードリストを取得
        codes = list({h["code"] for h in holdings})

        # 最新株価を取得
        latest_prices = self.repo.get_latest_prices(codes)

        # 更新処理
        updated_count = 0
        for holding_dict in holdings:
            code = holding_dict["code"]
            if code in latest_prices:
                current_price = latest_prices[code]
                quantity = holding_dict["quantity"]
                average_price = holding_dict["average_price"]

                # 市場価値と損益を計算
                market_value = current_price * quantity
                profit_loss = (current_price - average_price) * quantity
                profit_loss_ratio = (
                    ((current_price - average_price) / average_price * 100)
                    if average_price > 0
                    else 0
                )

                # Holdingオブジェクトを作成して更新
                holding = Holding(
                    user_id=user_id,
                    code=code,
                    account_name=holding_dict["account_name"],
                    account_type=holding_dict["account_type"],
                    quantity=quantity,
                    average_price=average_price,
                    market_value=market_value,
                    profit_loss=profit_loss,
                    profit_loss_ratio=profit_loss_ratio,
                )

                self.repo.upsert_holding(holding)
                updated_count += 1

        logger.info(f"{updated_count}件の市場価値を更新しました")
        return updated_count

    def update_stock_indicators(
        self, user_id: int, codes: list[str] | None = None
    ) -> int:
        """株価指標を更新"""
        # 保有銘柄を取得
        holdings = self.repo.get_holdings(user_id)

        if codes:
            # 指定された銘柄のみフィルタ
            holdings = [h for h in holdings if h["code"] in codes]

        if not holdings:
            return 0

        # 財務データを取得して指標を計算
        updated_count = 0
        for holding_dict in holdings:
            indicators = self._calculate_stock_indicators(holding_dict["code"])

            if indicators:
                # Holdingオブジェクトを作成して指標を更新
                holding = Holding(
                    user_id=user_id,
                    code=holding_dict["code"],
                    account_name=holding_dict["account_name"],
                    account_type=holding_dict["account_type"],
                    quantity=holding_dict["quantity"],
                    average_price=holding_dict["average_price"],
                    market_value=holding_dict.get("market_value"),
                    profit_loss=holding_dict.get("profit_loss"),
                    profit_loss_ratio=holding_dict.get("profit_loss_ratio"),
                    **indicators,
                )

                self.repo.upsert_holding(holding)
                updated_count += 1

        logger.info(f"{updated_count}件の株価指標を更新しました")
        return updated_count

    def aggregate_holdings_by_code(self, user_id: int) -> list[dict[str, Any]]:
        """銘柄別に保有情報を集約"""
        holdings = self.repo.get_holdings(user_id)

        # 銘柄別に集計
        aggregated = {}
        for holding in holdings:
            code = holding["code"]
            if code not in aggregated:
                aggregated[code] = {
                    "code": code,
                    "company_name": holding.get("company_name", ""),
                    "total_quantity": 0,
                    "total_value": 0,
                    "total_cost": 0,
                    "accounts": [],
                }

            agg = aggregated[code]
            quantity = holding["quantity"]
            average_price = holding["average_price"]

            agg["total_quantity"] += quantity
            agg["total_cost"] += quantity * average_price
            agg["total_value"] += holding.get("market_value", 0)

            agg["accounts"].append(
                {
                    "account_name": holding["account_name"],
                    "account_type": holding["account_type"],
                    "quantity": quantity,
                    "average_price": average_price,
                    "market_value": holding.get("market_value"),
                    "profit_loss": holding.get("profit_loss"),
                    "profit_loss_ratio": holding.get("profit_loss_ratio"),
                }
            )

        # 集約後の計算
        result = []
        for _code, agg in aggregated.items():
            if agg["total_quantity"] > 0:
                agg["average_price"] = agg["total_cost"] / agg["total_quantity"]
                agg["profit_loss"] = agg["total_value"] - agg["total_cost"]
                agg["profit_loss_ratio"] = (
                    (agg["profit_loss"] / agg["total_cost"] * 100)
                    if agg["total_cost"] > 0
                    else 0
                )
                result.append(agg)

        # 評価額の降順でソート
        result.sort(key=lambda x: x["total_value"], reverse=True)
        return result

    def _is_standard_format(self, holdings_data: list[dict]) -> bool:
        """標準形式のCSVかどうかを判定"""
        if not holdings_data:
            return False

        first_data = holdings_data[0]
        # 標準形式の特徴: PER、PBR、配当利回りなどの株価指標が含まれている
        return any(
            key in first_data
            for key in ["expected_per", "actual_pbr", "dividend_yield"]
        )

    def _create_holding_from_csv_data(
        self,
        user_id: int,
        data: dict,
        account_name: str,
        is_standard_format: bool,
    ) -> Holding | None:
        """CSVデータからHoldingオブジェクトを作成"""
        try:
            # 共通フィールド
            code = str(data.get("code", "")).strip()
            if not code:
                return None

            code = code.zfill(4)
            quantity = int(data.get("quantity", 0))

            if quantity == 0:
                return None

            # 口座タイプの取得（デフォルト: 特定）
            account_type = data.get("account_type", "特定")

            if is_standard_format:
                # 標準形式の場合
                holding = Holding(
                    user_id=user_id,
                    code=code,
                    account_name=account_name,
                    account_type=account_type,
                    quantity=quantity,
                    average_price=float(data.get("average_price", 0)),
                    market_value=float(data.get("market_value", 0)),
                    profit_loss=float(data.get("profit_loss", 0)),
                    profit_loss_ratio=float(data.get("profit_loss_ratio", 0)),
                    expected_per=(
                        float(data.get("expected_per", 0))
                        if data.get("expected_per") not in [None, "", "-", "N/A"]
                        else None
                    ),
                    actual_pbr=(
                        float(data.get("actual_pbr", 0))
                        if data.get("actual_pbr") not in [None, "", "-", "N/A"]
                        else None
                    ),
                    dividend_yield=(
                        float(data.get("dividend_yield", 0))
                        if data.get("dividend_yield") not in [None, "", "-", "N/A"]
                        else None
                    ),
                    expected_eps=(
                        float(data.get("expected_eps", 0))
                        if data.get("expected_eps") not in [None, "", "-", "N/A"]
                        else None
                    ),
                    actual_bps=(
                        float(data.get("actual_bps", 0))
                        if data.get("actual_bps") not in [None, "", "-", "N/A"]
                        else None
                    ),
                    expected_dividend=(
                        float(data.get("expected_dividend", 0))
                        if data.get("expected_dividend") not in [None, "", "-", "N/A"]
                        else None
                    ),
                    lending_type=data.get("lending_type"),
                )
            else:
                # SaveFile形式の場合
                holding = Holding(
                    user_id=user_id,
                    code=code,
                    account_name=account_name,
                    account_type=account_type,
                    quantity=quantity,
                    average_price=float(data.get("average_cost", 0)),
                )

            return holding

        except (ValueError, KeyError) as e:
            logger.error(f"保有銘柄データの変換エラー: {e}, data={data}")
            return None

    def _calculate_stock_indicators(self, code: str) -> dict[str, Any]:
        """株価指標を計算"""
        # TODO: 実装が必要な場合は、財務データから計算
        # 現在は空の辞書を返す
        return {}
