"""取引履歴サービス

ビジネスロジックを担当するサービス層
"""

from datetime import datetime
from typing import Any

import pandas as pd

from src.portfolio import PortfolioManager, SBICSVParser
from src.portfolio.models.transaction import Transaction
from src.utils.logging_config import get_logger

from .repositories import TransactionRepository

logger = get_logger("web.portfolio.transactions.services")


class TransactionService:
    """取引履歴サービス"""

    def __init__(self, repository: TransactionRepository):
        self.repository = repository

    def get_transactions(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        取引履歴一覧を取得

        Args:
            params: 検索パラメータ

        Returns:
            取引データとページネーション情報
        """
        trans_data, total_count = self.repository.get_transactions_with_pagination(
            params
        )

        return {
            "success": True,
            "transactions": trans_data,
            "pagination": {
                "page": params["page"],
                "per_page": params["per_page"],
                "total": total_count,
                "pages": (total_count + params["per_page"] - 1) // params["per_page"],
            },
        }

    def import_transactions_csv(
        self, user_id: int, csv_content: bytes
    ) -> dict[str, Any]:
        """
        CSVファイルから取引履歴をインポート

        Args:
            user_id: ユーザーID
            csv_content: CSVファイルの内容

        Returns:
            インポート結果
        """
        try:
            # CSV解析
            transactions_data = SBICSVParser.parse_transactions_csv(csv_content)
            logger.info(f"CSV解析完了: {len(transactions_data)}件の取引を検出")

            if not transactions_data:
                return {
                    "success": False,
                    "error": "取引データが見つかりませんでした",
                }

            # 取引履歴をインポート
            imported = PortfolioManager.import_transactions_from_csv(
                user_id, transactions_data
            )
            logger.info(f"取引履歴インポート完了: {imported}件")

            # 部分的な成功も成功として扱う
            if imported > 0:
                total_count = len(transactions_data)
                if imported < total_count:
                    message = f"取引履歴を部分的にインポートしました（{imported}/{total_count}件）"
                    logger.warning(
                        f"一部の取引がインポートされませんでした: {total_count - imported}件"
                    )
                else:
                    message = f"取引履歴をインポートしました（{imported}件）"

                return {
                    "success": True,
                    "message": message,
                    "imported": imported,
                    "total": total_count,
                    "partial": imported < total_count,
                }
            else:
                return {
                    "success": False,
                    "error": "取引をインポートできませんでした。データの形式を確認してください。",
                    "imported": 0,
                    "total": len(transactions_data),
                }

        except Exception as e:
            logger.error(f"CSV解析エラー: {str(e)}")
            return {
                "success": False,
                "error": f"CSVファイルの解析に失敗しました: {str(e)}",
            }

    def add_transaction(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        取引を追加

        Args:
            data: 取引データ

        Returns:
            追加結果
        """
        # 証券コードが5桁の場合は末尾1桁を削除
        code = data["code"]
        if len(code) == 5 and code.isdigit():
            code = code[:4]

        # バリデーション
        if not code:
            return {"success": False, "error": "銘柄コードは必須です"}
        if not data["transaction_date"]:
            return {"success": False, "error": "取引日は必須です"}
        if data["transaction_type"] not in ["buy", "sell"]:
            return {
                "success": False,
                "error": "取引種別は buy または sell を指定してください",
            }
        if data["quantity"] is None or data["quantity"] <= 0:
            return {"success": False, "error": "数量は正の数を入力してください"}
        if data["price"] is None or data["price"] <= 0:
            return {"success": False, "error": "価格は正の数を入力してください"}

        # 取引を作成
        transaction = Transaction(
            user_id=data["user_id"],
            code=code,
            transaction_date=data["transaction_date"],
            transaction_type=data["transaction_type"],
            quantity=data["quantity"],
            price=data["price"],
        )
        transaction.commission = data["commission"]
        transaction.tax = data["tax"]
        transaction.total_amount = data["quantity"] * data["price"]

        # 信用取引の場合、remarksに追記
        if data["is_margin"]:
            if data["remarks"]:
                transaction.remarks = f"{data['remarks']} (信用)"
            else:
                transaction.remarks = "信用"
        else:
            transaction.remarks = data["remarks"]

        # 詳細タイプを設定
        if data["detailed_type"]:
            transaction.detailed_type = {
                "new_buy": "新規買い",
                "new_sell": "新規売り",
                "close_buy": "決済買い",
                "close_sell": "決済売り",
            }.get(data["detailed_type"], data["detailed_type"])
        else:
            # フォールバック
            transaction.detailed_type = (
                "新規買い" if data["transaction_type"] == "buy" else "新規売り"
            )

        # 実現損益を設定（決済取引時かつ手動入力がある場合）
        if (
            data["detailed_type"] in ["close_sell", "close_buy"]
            and data["realized_profit"] is not None
        ):
            transaction.realized_profit = data["realized_profit"]

        # 保存
        if transaction.save():
            logger.info(
                f"取引追加成功: {data['transaction_date']} {code} "
                f"{data['transaction_type']} {data['quantity']}株 @{data['price']}円"
            )
            return {"success": True, "message": "取引を追加しました"}
        else:
            return {"success": False, "error": "取引の保存に失敗しました"}

    def update_transaction(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        取引を更新

        Args:
            data: 更新データ

        Returns:
            更新結果
        """
        # 取引を取得
        transaction = Transaction.find_by_id(data["user_id"], data["transaction_id"])
        if not transaction:
            return {"success": False, "error": "取引が見つかりません"}

        # 更新
        if data.get("transaction_date"):
            transaction.transaction_date = data["transaction_date"]
        if data.get("transaction_type"):
            transaction.transaction_type = data["transaction_type"]
        if data.get("detailed_type"):
            transaction.detailed_type = {
                "new_buy": "新規買い",
                "new_sell": "新規売り",
                "close_buy": "決済買い",
                "close_sell": "決済売り",
            }.get(data["detailed_type"], data["detailed_type"])
        if data.get("quantity") is not None:
            transaction.quantity = data["quantity"]
        if data.get("price") is not None:
            transaction.price = data["price"]
        if data.get("commission") is not None:
            transaction.commission = data["commission"]
        if data.get("tax") is not None:
            transaction.tax = data["tax"]
        if data.get("realized_profit") is not None:
            transaction.realized_profit = data["realized_profit"]
        if "remarks" in data:
            transaction.remarks = data["remarks"]

        # 受渡金額を再計算
        if transaction.quantity and transaction.price:
            transaction.total_amount = transaction.quantity * transaction.price

        # 保存
        if transaction.save():
            logger.info(f"取引更新成功: ID {data['transaction_id']}")
            return {"success": True, "message": "取引を更新しました"}
        else:
            return {"success": False, "error": "取引の更新に失敗しました"}

    def delete_transaction(self, user_id: int, transaction_id: int) -> dict[str, Any]:
        """
        取引を削除

        Args:
            user_id: ユーザーID
            transaction_id: 取引ID

        Returns:
            削除結果
        """
        # 取引が存在するか確認
        transaction = self.repository.find_transaction(user_id, transaction_id)
        if not transaction:
            return {"success": False, "error": "取引が見つかりません"}

        # 削除
        if self.repository.delete_transaction(user_id, transaction_id):
            logger.info(f"取引削除成功: ID {transaction_id}")
            return {"success": True, "message": "取引を削除しました"}
        else:
            return {"success": False, "error": "取引の削除に失敗しました"}

    def calculate_performance(
        self, user_id: int, period: str, include_holdings: bool
    ) -> dict[str, Any]:
        """
        取引パフォーマンスを計算

        Args:
            user_id: ユーザーID
            period: 期間（all, 1y, 6m, 3m, 1m）
            include_holdings: 保有銘柄を含むか

        Returns:
            パフォーマンスデータ
        """
        # 期間に応じて開始日を設定
        end_date = datetime.now().strftime("%Y-%m-%d")
        if period == "1y":
            start_date = (datetime.now() - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
        elif period == "6m":
            start_date = (datetime.now() - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
        elif period == "3m":
            start_date = (datetime.now() - pd.DateOffset(months=3)).strftime("%Y-%m-%d")
        elif period == "1m":
            start_date = (datetime.now() - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
        else:
            start_date = None

        # 取引データを取得
        transactions = self.repository.get_transactions_for_performance(
            user_id, start_date
        )

        # 銘柄ごとのパフォーマンスを計算（シンプルな総額ベース）
        stock_performance = {}

        # 取引データを銘柄ごとに集計
        for trans in transactions:
            code = trans["code"]

            # 現物売り（決済売りでremarksが信用でない）はパフォーマンス集計から除外
            # 新規売りは全て信用取引（空売り）なので含める
            if (
                trans["transaction_type"] == "sell"
                and trans.get("detailed_type") == "決済売り"
                and trans.get("remarks", "") != "信用"
            ):
                continue

            if code not in stock_performance:
                stock_performance[code] = {
                    "code": code,
                    "company_name": trans.get("company_name", ""),
                    "total_buy_amount": 0,
                    "total_sell_amount": 0,
                    "total_buy_quantity": 0,
                    "total_sell_quantity": 0,
                    "realized_profit": 0,
                    "net_quantity": 0,
                    "average_buy_price": 0,
                    "transactions": [],
                }

            sp = stock_performance[code]
            sp["transactions"].append(trans)

            if trans["transaction_type"] == "buy":
                # 買付金額（手数料込み）
                buy_amount = trans["quantity"] * trans["price"] + (
                    trans["commission"] or 0
                )
                sp["total_buy_amount"] += buy_amount
                sp["total_buy_quantity"] += trans["quantity"]
                sp["net_quantity"] += trans["quantity"]

            else:  # sell
                # 売却金額（手数料・税金控除後）
                sell_amount = (
                    trans["quantity"] * trans["price"]
                    - (trans["commission"] or 0)
                    - (trans["tax"] or 0)
                )
                sp["total_sell_amount"] += sell_amount
                sp["total_sell_quantity"] += trans["quantity"]
                sp["net_quantity"] -= trans["quantity"]

            # 実現損益はtransactionsテーブルの値を使用（NULLは0として扱う）
            # 買い・売り両方で実現損益が記録されている場合があるため、全ての取引で加算
            realized_profit = trans.get("realized_profit") or 0
            sp["realized_profit"] += realized_profit

        # 銘柄ごとに平均買付価格を計算
        for _code, sp in stock_performance.items():
            if sp["total_buy_quantity"] > 0:
                sp["average_buy_price"] = (
                    sp["total_buy_amount"] / sp["total_buy_quantity"]
                )

        # 含み損益の計算は行わない
        for _code, sp in stock_performance.items():
            sp["unrealized_profit"] = 0
            sp["current_price"] = None
            sp["market_value"] = None

        # 全体のパフォーマンスサマリー
        total_realized_profit = sum(
            sp["realized_profit"] for sp in stock_performance.values()
        )
        total_buy_amount = sum(
            sp["total_buy_amount"] for sp in stock_performance.values()
        )
        total_sell_amount = sum(
            sp["total_sell_amount"] for sp in stock_performance.values()
        )

        summary = {
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "total_realized_profit": total_realized_profit,
            "total_profit": total_realized_profit,  # 実現損益のみ
            "total_buy_amount": total_buy_amount,
            "total_sell_amount": total_sell_amount,
            "profit_rate": (
                (total_realized_profit / total_buy_amount * 100)
                if total_buy_amount > 0
                else 0
            ),
            "transaction_count": len(transactions),
            "stock_count": len(stock_performance),
        }

        # 月別損益の計算
        monthly_pnl = {}
        for trans in transactions:
            # 現物売りは除外
            if (
                trans["transaction_type"] == "sell"
                and trans.get("detailed_type") == "決済売り"
                and trans.get("remarks", "") != "信用"
            ):
                continue

            if (
                trans["transaction_type"] == "sell"
                and trans.get("realized_profit") is not None
            ):
                month = trans["transaction_date"][:7]  # YYYY-MM
                if month not in monthly_pnl:
                    monthly_pnl[month] = 0
                monthly_pnl[month] += trans["realized_profit"]

        # 累積損益の計算
        cumulative_pnl = []
        cumulative_profit = 0
        for month in sorted(monthly_pnl.keys()):
            cumulative_profit += monthly_pnl[month]
            cumulative_pnl.append(
                {
                    "month": month,
                    "monthly_profit": monthly_pnl[month],
                    "cumulative_profit": cumulative_profit,
                }
            )

        # 取引時間帯分布（仮データ - 実際の時間データがある場合は使用）
        trading_hours = {
            "9-10": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "10-11": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "11-12": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "13-14": len([t for t in transactions if t["transaction_type"] == "buy"])
            // 4,
            "14-15": len([t for t in transactions if t["transaction_type"] == "sell"])
            // 2,
        }

        # 保有期間分布の計算
        holding_periods = {
            "1日以内": 0,
            "1週間以内": 0,
            "1ヶ月以内": 0,
            "3ヶ月以内": 0,
            "3ヶ月超": 0,
        }

        # 保有銘柄を含める場合の処理
        if include_holdings:
            holdings = self.repository.get_holdings_for_performance(user_id)

            for holding in holdings:
                code = holding["code"]

                if code in stock_performance:
                    # 取引履歴にある銘柄の場合、sourceを'both'に更新
                    stock_performance[code]["source"] = "both"
                else:
                    # 取引履歴にない銘柄の場合、新規追加
                    stock_performance[code] = {
                        "code": code,
                        "company_name": holding["company_name"],
                        "total_buy_amount": holding["quantity"]
                        * holding["average_price"],
                        "total_sell_amount": 0,
                        "total_buy_quantity": holding["quantity"],
                        "total_sell_quantity": 0,
                        "realized_profit": 0,
                        "net_quantity": holding["quantity"],
                        "average_buy_price": holding["average_price"],
                        "unrealized_profit": holding["profit_loss"] or 0,
                        "current_price": (
                            holding["market_value"] / holding["quantity"]
                            if holding["quantity"] > 0
                            else None
                        ),
                        "market_value": holding["market_value"],
                        "transactions": [],
                        "source": "holdings",  # 保有のみ
                    }

            # ソース情報を既存の取引履歴銘柄に追加
            for _code, sp in stock_performance.items():
                if "source" not in sp:
                    sp["source"] = "transaction"  # 取引履歴のみ

            # サマリーを再計算（保有銘柄を含める場合）
            total_realized_profit = sum(
                sp["realized_profit"] for sp in stock_performance.values()
            )
            total_unrealized_profit = sum(
                sp["unrealized_profit"] for sp in stock_performance.values()
            )
            total_buy_amount = sum(
                sp["total_buy_amount"] for sp in stock_performance.values()
            )
            total_sell_amount = sum(
                sp["total_sell_amount"] for sp in stock_performance.values()
            )
            total_profit = total_realized_profit + total_unrealized_profit

            summary.update(
                {
                    "total_realized_profit": total_realized_profit,
                    "total_unrealized_profit": total_unrealized_profit,
                    "total_profit": total_profit,
                    "total_buy_amount": total_buy_amount,
                    "total_sell_amount": total_sell_amount,
                    "profit_rate": (
                        (total_profit / total_buy_amount * 100)
                        if total_buy_amount > 0
                        else 0
                    ),
                    "stock_count": len(stock_performance),
                }
            )

        # 結果を返す
        return {
            "summary": summary,
            "stock_performance": list(stock_performance.values()),
            "monthly_pnl": monthly_pnl,
            "cumulative_pnl": cumulative_pnl,
            "trading_hours": trading_hours,
            "holding_periods": holding_periods,
        }
