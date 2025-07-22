"""
ポートフォリオ分析機能
"""

from collections import defaultdict
from typing import Any

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def calculate_portfolio_analysis(holdings: list[dict[str, Any]]) -> dict[str, Any]:
    """ポートフォリオの分析データを計算"""

    # 基本統計
    total_value = sum(h.get("market_value", 0) for h in holdings)
    total_profit_loss = sum(h.get("profit_loss", 0) for h in holdings)
    total_cost = total_value - total_profit_loss
    total_profit_loss_ratio = (
        (total_profit_loss / total_cost * 100) if total_cost > 0 else 0
    )

    # セクター別集計（セクター情報がない場合は「その他」とする）
    sector17_data: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"value": 0, "profit_loss": 0, "count": 0}
    )
    sector33_data: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"value": 0, "profit_loss": 0, "count": 0}
    )

    # 市場別集計
    market_data: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"value": 0, "profit_loss": 0, "count": 0}
    )

    # 配当利回り別集計
    dividend_ranges: list[dict[str, str | float]] = [
        {"range": "0%", "min": 0.0, "max": 0.01},
        {"range": "0-1%", "min": 0.01, "max": 1.0},
        {"range": "1-2%", "min": 1.0, "max": 2.0},
        {"range": "2-3%", "min": 2.0, "max": 3.0},
        {"range": "3-4%", "min": 3.0, "max": 4.0},
        {"range": "4-5%", "min": 4.0, "max": 5.0},
        {"range": "5%以上", "min": 5.0, "max": float("inf")},
    ]
    dividend_data = {r["range"]: {"value": 0, "count": 0} for r in dividend_ranges}

    # 配当金額別集計（評価額に対する配当金額）
    dividend_amount_data: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"value": 0, "count": 0, "total_dividend": 0}
    )

    for holding in holdings:
        # 17業種セクター別
        sector17 = holding.get("sector17", "その他")
        if not sector17:
            sector17 = "その他"

        sector17_data[sector17]["value"] += holding.get("market_value", 0)
        sector17_data[sector17]["profit_loss"] += holding.get("profit_loss", 0)
        sector17_data[sector17]["count"] += 1

        # 33業種セクター別
        sector33 = holding.get("sector33", "その他")
        if not sector33:
            sector33 = "その他"

        sector33_data[sector33]["value"] += holding.get("market_value", 0)
        sector33_data[sector33]["profit_loss"] += holding.get("profit_loss", 0)
        sector33_data[sector33]["count"] += 1

        # 市場別
        market = holding.get("market", "その他")
        if not market:
            market = "その他"

        market_data[market]["value"] += holding.get("market_value", 0)
        market_data[market]["profit_loss"] += holding.get("profit_loss", 0)
        market_data[market]["count"] += 1

        # 配当利回り別
        dividend_yield = holding.get("dividend_yield", 0) or 0
        for range_info in dividend_ranges:
            range_min = float(range_info["min"])
            range_max = float(range_info["max"])
            if range_min <= dividend_yield < range_max:
                dividend_data[range_info["range"]]["value"] += holding.get(
                    "market_value", 0
                )
                dividend_data[range_info["range"]]["count"] += 1
                break

        # 配当金額別（年間配当額を計算）
        if dividend_yield > 0:
            current_price = holding.get("current_price", 0)
            quantity = holding.get("quantity", holding.get("total_quantity", 0))
            if current_price > 0 and quantity > 0:
                annual_dividend = current_price * quantity * dividend_yield / 100
                # 配当金額のレンジを決定
                if annual_dividend < 10000:
                    range_key = "1万円未満"
                elif annual_dividend < 50000:
                    range_key = "1-5万円"
                elif annual_dividend < 100000:
                    range_key = "5-10万円"
                elif annual_dividend < 300000:
                    range_key = "10-30万円"
                elif annual_dividend < 500000:
                    range_key = "30-50万円"
                else:
                    range_key = "50万円以上"

                dividend_amount_data[range_key]["value"] += holding.get(
                    "market_value", 0
                )
                dividend_amount_data[range_key]["count"] += 1
                dividend_amount_data[range_key]["total_dividend"] += annual_dividend

    # 17業種セクター別データを配列形式に変換
    sector17_breakdown = []
    for sector, data in sector17_data.items():
        sector17_breakdown.append(
            {
                "sector": sector,
                "value": data["value"],
                "profit_loss": data["profit_loss"],
                "ratio": (data["value"] / total_value * 100) if total_value > 0 else 0,
                "count": data["count"],
            }
        )

    # 評価額でソート
    sector17_breakdown.sort(key=lambda x: x["value"], reverse=True)

    # 33業種セクター別データを配列形式に変換
    sector33_breakdown = []
    for sector, data in sector33_data.items():
        sector33_breakdown.append(
            {
                "sector": sector,
                "value": data["value"],
                "profit_loss": data["profit_loss"],
                "ratio": (data["value"] / total_value * 100) if total_value > 0 else 0,
                "count": data["count"],
            }
        )

    # 評価額でソート
    sector33_breakdown.sort(key=lambda x: x["value"], reverse=True)

    # 市場別データを配列形式に変換
    market_breakdown = []
    for market, data in market_data.items():
        market_breakdown.append(
            {
                "market": market,
                "value": data["value"],
                "profit_loss": data["profit_loss"],
                "ratio": (data["value"] / total_value * 100) if total_value > 0 else 0,
                "count": data["count"],
            }
        )

    # 評価額でソート
    market_breakdown.sort(key=lambda x: x["value"], reverse=True)

    # 配当利回り分布を配列形式に変換
    dividend_distribution = []
    for range_info in dividend_ranges:
        range_name = range_info["range"]
        if dividend_data[range_name]["count"] > 0:
            dividend_distribution.append(
                {
                    "range": range_name,
                    "value": dividend_data[range_name]["value"],
                    "count": dividend_data[range_name]["count"],
                    "ratio": (
                        (dividend_data[range_name]["value"] / total_value * 100)
                        if total_value > 0
                        else 0
                    ),
                }
            )

    # 配当金額分布を配列形式に変換
    dividend_amount_distribution = []
    # 順序を定義
    amount_order = [
        "1万円未満",
        "1-5万円",
        "5-10万円",
        "10-30万円",
        "30-50万円",
        "50万円以上",
    ]
    for range_key in amount_order:
        if (
            range_key in dividend_amount_data
            and dividend_amount_data[range_key]["count"] > 0
        ):
            dividend_amount_distribution.append(
                {
                    "range": range_key,
                    "value": dividend_amount_data[range_key]["value"],
                    "count": dividend_amount_data[range_key]["count"],
                    "total_dividend": dividend_amount_data[range_key]["total_dividend"],
                    "ratio": (
                        (dividend_amount_data[range_key]["value"] / total_value * 100)
                        if total_value > 0
                        else 0
                    ),
                }
            )

    # パフォーマンス分布（損益率の範囲別）
    performance_ranges: list[dict[str, str | float]] = [
        {"range": "-50%以下", "min": -float("inf"), "max": -50.0},
        {"range": "-50%～-20%", "min": -50.0, "max": -20.0},
        {"range": "-20%～-10%", "min": -20.0, "max": -10.0},
        {"range": "-10%～0%", "min": -10.0, "max": 0.0},
        {"range": "0%～10%", "min": 0.0, "max": 10.0},
        {"range": "10%～20%", "min": 10.0, "max": 20.0},
        {"range": "20%～50%", "min": 20.0, "max": 50.0},
        {"range": "50%以上", "min": 50.0, "max": float("inf")},
    ]

    performance_distribution = []
    for range_info in performance_ranges:
        count = 0
        value = 0
        for holding in holdings:
            ratio = holding.get("profit_loss_ratio", 0)
            range_min = float(range_info["min"])
            range_max = float(range_info["max"])
            if range_min <= ratio < range_max:
                count += 1
                value += holding.get("market_value", 0)

        if count > 0:
            performance_distribution.append(
                {
                    "range": range_info["range"],
                    "count": count,
                    "value": value,
                    "ratio": (value / total_value * 100) if total_value > 0 else 0,
                }
            )

    # 上位・下位銘柄
    sorted_by_profit = sorted(
        holdings, key=lambda x: x.get("profit_loss", 0), reverse=True
    )
    top_gainers = []
    top_losers = []

    for holding in sorted_by_profit[:5]:
        if holding.get("profit_loss", 0) > 0:
            top_gainers.append(
                {
                    "code": holding.get("code"),
                    "name": holding.get("name"),
                    "profit_loss": holding.get("profit_loss", 0),
                    "profit_loss_ratio": holding.get("profit_loss_ratio", 0),
                    "market_value": holding.get("market_value", 0),
                }
            )

    for holding in sorted_by_profit[-5:]:
        if holding.get("profit_loss", 0) < 0:
            top_losers.append(
                {
                    "code": holding.get("code"),
                    "name": holding.get("name"),
                    "profit_loss": holding.get("profit_loss", 0),
                    "profit_loss_ratio": holding.get("profit_loss_ratio", 0),
                    "market_value": holding.get("market_value", 0),
                }
            )

    top_losers.reverse()  # 損失が大きい順に並べ替え

    # 評価額上位銘柄
    sorted_by_value = sorted(
        holdings, key=lambda x: x.get("market_value", 0), reverse=True
    )
    holdings_by_value = []

    for holding in sorted_by_value[:10]:
        holdings_by_value.append(
            {
                "code": holding.get("code"),
                "name": holding.get("name"),
                "market_value": holding.get("market_value", 0),
                "ratio": (
                    (holding.get("market_value", 0) / total_value * 100)
                    if total_value > 0
                    else 0
                ),
                "profit_loss": holding.get("profit_loss", 0),
                "profit_loss_ratio": holding.get("profit_loss_ratio", 0),
            }
        )

    # 銘柄別配当情報（配当がある銘柄のみ）
    dividend_by_stock = []
    total_dividend_amount = 0

    for holding in holdings:
        dividend_yield = holding.get("dividend_yield", 0) or 0
        if dividend_yield > 0:
            current_price = holding.get("current_price", 0)
            quantity = holding.get("quantity", holding.get("total_quantity", 0))
            if current_price > 0 and quantity > 0:
                annual_dividend = current_price * quantity * dividend_yield / 100
                total_dividend_amount += annual_dividend
                dividend_by_stock.append(
                    {
                        "code": holding.get("code"),
                        "name": holding.get("name"),
                        "annual_dividend": annual_dividend,
                        "dividend_yield": dividend_yield,
                        "market_value": holding.get("market_value", 0),
                    }
                )

    # 配当額でソート
    dividend_by_stock.sort(key=lambda x: x["annual_dividend"], reverse=True)

    # 配当額の比率を計算
    for stock in dividend_by_stock:
        stock["ratio"] = (
            (stock["annual_dividend"] / total_dividend_amount * 100)
            if total_dividend_amount > 0
            else 0
        )

    return {
        "total_value": total_value,
        "total_profit_loss": total_profit_loss,
        "total_profit_loss_ratio": total_profit_loss_ratio,
        "total_holdings": len(holdings),
        "sector17_breakdown": sector17_breakdown,
        "sector33_breakdown": sector33_breakdown,
        "market_breakdown": market_breakdown,
        "dividend_distribution": dividend_distribution,
        "dividend_amount_distribution": dividend_amount_distribution,
        "dividend_by_stock": dividend_by_stock,
        "total_dividend_amount": total_dividend_amount,
        "performance_distribution": performance_distribution,
        "top_gainers": top_gainers,
        "top_losers": top_losers,
        "holdings_by_value": holdings_by_value,
    }
