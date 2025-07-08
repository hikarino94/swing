#!/usr/bin/env python
"""Advanced JSON backtest analysis with visual outputs.

This module extends the basic analyze_backtest_json.py with:
- Visual charts (cumulative returns, drawdown, monthly performance)
- Additional metrics (max drawdown, Calmar ratio, monthly stats)
- Export capabilities (Excel, PDF reports)
- Multi-strategy comparison
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

# Reuse profit/return column definitions
PROFIT_COLUMNS = ["profit_jpy", "pnl_yen"]
RET_COLUMNS = ["ret_pct", "pnl_pct"]


def load_trades(paths: list[str]) -> pd.DataFrame:
    """Load trade records from JSON files with enhanced path resolution."""
    frames = []

    for p in paths:
        path = Path(p)

        # Try multiple locations
        if not path.exists():
            candidates = [
                path,  # Original path
                Path("data/output/backtest") / path.name,  # Default backtest dir
                Path("data/output/backtest") / p,  # Full path under backtest
            ]

            found = False
            for candidate in candidates:
                if candidate.exists():
                    path = candidate
                    found = True
                    break

            if not found:
                print(f"Error: File '{p}' not found in any of the expected locations")
                continue

        with open(path) as f:
            data = json.load(f)

        # Handle different JSON structures
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and "trades" in data:
            df = pd.DataFrame(data["trades"])
            # Add metadata as columns
            for key, value in data.items():
                if key != "trades" and not isinstance(value, list | dict):
                    df[f"meta_{key}"] = value
        else:
            df = pd.DataFrame([data])

        # Add source file info
        df["source_file"] = path.name
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    # Convert date columns to datetime
    date_cols = ["entry_date", "exit_date", "date"]
    for col in date_cols:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col])

    return result


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    """Find the first existing column from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"Required column not found. Candidates: {candidates}")


def calculate_metrics(trades: pd.DataFrame) -> dict[str, Any]:
    """Calculate comprehensive performance metrics."""
    if trades.empty:
        return {}

    profit_col = _find_col(trades, PROFIT_COLUMNS)
    ret_col = _find_col(trades, RET_COLUMNS)

    # Basic metrics
    total_trades = len(trades)
    total_profit = trades[profit_col].sum()
    wins = trades[profit_col] > 0
    win_rate = wins.mean()

    # Return statistics
    returns = trades[ret_col]
    mean_ret = returns.mean()
    std_ret = returns.std()
    sharpe = mean_ret / std_ret if std_ret > 0 else 0

    # Win/Loss analysis
    avg_win = trades.loc[wins, profit_col].mean() if wins.any() else 0
    avg_loss = trades.loc[~wins, profit_col].mean() if (~wins).any() else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    # Drawdown calculation
    cumulative_profit = trades[profit_col].cumsum()
    running_max = cumulative_profit.expanding().max()
    drawdown = cumulative_profit - running_max
    max_drawdown = drawdown.min()
    max_drawdown_pct = (
        max_drawdown / running_max[drawdown.idxmin()] * 100
        if running_max[drawdown.idxmin()] != 0
        else 0
    )

    # Calmar ratio (annualized return / max drawdown)
    annualized_return = mean_ret * 252  # Assuming daily returns
    calmar = annualized_return / abs(max_drawdown_pct) if max_drawdown_pct != 0 else 0

    # Duration analysis
    if "entry_date" in trades.columns and "exit_date" in trades.columns:
        trades["holding_days"] = (trades["exit_date"] - trades["entry_date"]).dt.days
        avg_holding = trades["holding_days"].mean()
    else:
        avg_holding = None

    return {
        "total_trades": total_trades,
        "total_profit": total_profit,
        "win_rate": win_rate * 100,
        "avg_return": mean_ret * 100,
        "sharpe_ratio": sharpe,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "max_drawdown_pct": max_drawdown_pct,
        "calmar_ratio": calmar,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "avg_holding_days": avg_holding,
        "best_trade": trades[profit_col].max(),
        "worst_trade": trades[profit_col].min(),
    }


def create_visual_report(trades: pd.DataFrame, output_path: Path | None = None) -> None:
    """Create comprehensive visual analysis report."""
    if trades.empty:
        print("No trades to visualize")
        return

    profit_col = _find_col(trades, PROFIT_COLUMNS)
    ret_col = _find_col(trades, RET_COLUMNS)

    # Set style
    plt.style.use("seaborn-v0_8-darkgrid")
    plt.figure(figsize=(16, 12))

    # 1. Cumulative Returns
    ax1 = plt.subplot(3, 2, 1)
    cumulative_returns = (1 + trades[ret_col] / 100).cumprod()  # type: ignore[var-annotated]
    cumulative_returns.plot(ax=ax1, linewidth=2)
    ax1.set_title("Cumulative Returns", fontsize=14, fontweight="bold")
    ax1.set_xlabel("Trade Number")
    ax1.set_ylabel("Cumulative Return")
    ax1.axhline(y=1, color="black", linestyle="--", alpha=0.5)

    # 2. Drawdown
    ax2 = plt.subplot(3, 2, 2)
    cumulative_profit = trades[profit_col].cumsum()
    running_max = cumulative_profit.expanding().max()
    drawdown_pct = ((cumulative_profit - running_max) / running_max * 100).fillna(0)
    drawdown_pct.plot(ax=ax2, color="red", linewidth=2, fill=True, alpha=0.3)
    ax2.set_title("Drawdown %", fontsize=14, fontweight="bold")
    ax2.set_xlabel("Trade Number")
    ax2.set_ylabel("Drawdown %")
    ax2.fill_between(drawdown_pct.index, drawdown_pct, 0, color="red", alpha=0.3)

    # 3. Return Distribution
    ax3 = plt.subplot(3, 2, 3)
    trades[ret_col].hist(ax=ax3, bins=30, alpha=0.7, color="blue", edgecolor="black")
    ax3.axvline(x=0, color="red", linestyle="--", linewidth=2)
    ax3.set_title("Return Distribution", fontsize=14, fontweight="bold")
    ax3.set_xlabel("Return %")
    ax3.set_ylabel("Frequency")

    # Add normal distribution overlay
    mean_ret = trades[ret_col].mean()
    std_ret = trades[ret_col].std()
    x = np.linspace(trades[ret_col].min(), trades[ret_col].max(), 100)
    from scipy import stats

    ax3_twin = ax3.twinx()
    ax3_twin.plot(
        x, stats.norm.pdf(x, mean_ret, std_ret), "r-", linewidth=2, label="Normal"
    )
    ax3_twin.set_ylabel("Density")

    # 4. Monthly Performance
    ax4 = plt.subplot(3, 2, 4)
    if "exit_date" in trades.columns:
        trades["month"] = trades["exit_date"].dt.to_period("M")
        monthly_returns = trades.groupby("month")[profit_col].sum()
        monthly_returns.plot(
            kind="bar",
            ax=ax4,
            color=["green" if x > 0 else "red" for x in monthly_returns],
        )
        ax4.set_title("Monthly Performance", fontsize=14, fontweight="bold")
        ax4.set_xlabel("Month")
        ax4.set_ylabel("Profit (JPY)")
        plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)

    # 5. Win/Loss Scatter
    ax5 = plt.subplot(3, 2, 5)
    wins = trades[trades[profit_col] > 0]
    losses = trades[trades[profit_col] <= 0]

    if len(wins) > 0:
        ax5.scatter(
            range(len(wins)), wins[profit_col], color="green", alpha=0.6, label="Wins"
        )
    if len(losses) > 0:
        ax5.scatter(
            range(len(losses)),
            losses[profit_col],
            color="red",
            alpha=0.6,
            label="Losses",
        )

    ax5.set_title("Win/Loss Pattern", fontsize=14, fontweight="bold")
    ax5.set_xlabel("Trade Sequence")
    ax5.set_ylabel("Profit (JPY)")
    ax5.legend()
    ax5.axhline(y=0, color="black", linestyle="-", alpha=0.3)

    # 6. Performance Summary Table
    ax6 = plt.subplot(3, 2, 6)
    ax6.axis("off")

    metrics = calculate_metrics(trades)
    table_data = []
    for key, value in metrics.items():
        if value is not None:
            if isinstance(value, float):
                if "pct" in key or "rate" in key or "return" in key:
                    formatted = f"{value:.2f}%"
                elif "profit" in key or "drawdown" in key and "pct" not in key:
                    formatted = f"¥{value:,.0f}"
                else:
                    formatted = f"{value:.2f}"
            else:
                formatted = str(value)
            table_data.append([key.replace("_", " ").title(), formatted])

    table = ax6.table(
        cellText=table_data, cellLoc="left", loc="center", colWidths=[0.6, 0.4]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
    else:
        plt.show()

    plt.close()


def export_to_excel(
    trades: pd.DataFrame, metrics: dict[str, Any], output_path: Path
) -> None:
    """Export analysis results to Excel with formatting."""
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        # Summary sheet
        summary_df = pd.DataFrame([metrics]).T.reset_index()
        summary_df.columns = ["Metric", "Value"]
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        # Trades sheet
        trades.to_excel(writer, sheet_name="Trades", index=False)

        # Monthly analysis
        if "exit_date" in trades.columns:
            trades["month"] = trades["exit_date"].dt.to_period("M")
            profit_col = _find_col(trades, PROFIT_COLUMNS)

            monthly = (
                trades.groupby("month")
                .agg(
                    {
                        profit_col: ["sum", "mean", "count"],
                        _find_col(trades, RET_COLUMNS): ["mean", "std"],
                    }
                )
                .round(2)
            )

            monthly.columns = ["_".join(col).strip() for col in monthly.columns]
            monthly.to_excel(writer, sheet_name="Monthly Analysis")

        # Format summary sheet
        worksheet = writer.sheets["Summary"]
        worksheet.set_column("A:A", 25)
        worksheet.set_column("B:B", 20)


def compare_strategies(
    files_by_strategy: dict[str, list[str]], output_dir: Path
) -> None:
    """Compare multiple strategies side by side."""
    strategy_metrics: dict[str, dict[str, Any]] = {}
    all_trades = []

    for strategy, files in files_by_strategy.items():
        trades = load_trades(files)
        if not trades.empty:
            trades["strategy"] = strategy
            all_trades.append(trades)
            strategy_metrics[strategy] = calculate_metrics(trades)

    if not all_trades:
        print("No trades loaded for comparison")
        return

    # Combine all trades (for potential future use)
    # combined_trades = pd.concat(all_trades, ignore_index=True)

    # Create comparison DataFrame
    comparison_df = pd.DataFrame(strategy_metrics).T

    # Visual comparison
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # 1. Strategy Returns Comparison
    ax1 = axes[0, 0]
    comparison_df[["avg_return", "sharpe_ratio"]].plot(kind="bar", ax=ax1)
    ax1.set_title("Return & Risk Metrics")
    ax1.set_ylabel("Value")

    # 2. Win Rate & Profit Factor
    ax2 = axes[0, 1]
    comparison_df[["win_rate", "profit_factor"]].plot(kind="bar", ax=ax2)
    ax2.set_title("Win Rate & Profit Factor")

    # 3. Drawdown Comparison
    ax3 = axes[1, 0]
    comparison_df["max_drawdown_pct"].abs().plot(kind="bar", ax=ax3, color="red")
    ax3.set_title("Maximum Drawdown %")
    ax3.set_ylabel("Drawdown %")

    # 4. Total Profit
    ax4 = axes[1, 1]
    comparison_df["total_profit"].plot(kind="bar", ax=ax4, color="green")
    ax4.set_title("Total Profit")
    ax4.set_ylabel("Profit (JPY)")

    plt.tight_layout()
    plt.savefig(output_dir / "strategy_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Export comparison to Excel
    comparison_df.to_excel(output_dir / "strategy_comparison.xlsx")

    print(f"\nStrategy comparison saved to {output_dir}")


def main(argv: list[str] | None = None) -> None:
    """Enhanced backtest analyzer with visual outputs."""
    ap = argparse.ArgumentParser(
        description="Advanced backtest JSON analyzer with visual reports"
    )
    ap.add_argument("files", nargs="+", help="JSON files to analyze")
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/output/analysis"),
        help="Output directory for reports (default: data/output/analysis)",
    )
    ap.add_argument(
        "--show-trades",
        action="store_true",
        help="Include detailed trade table in console output",
    )
    ap.add_argument(
        "--side",
        choices=["long", "short", "all"],
        default="all",
        help="Analyze only long or short trades (default: all)",
    )
    ap.add_argument(
        "--export-excel",
        action="store_true",
        help="Export results to Excel file",
    )
    ap.add_argument(
        "--export-pdf",
        action="store_true",
        help="Export visual report as PDF",
    )
    ap.add_argument(
        "--compare",
        action="store_true",
        help="Compare multiple strategies (group files by strategy name)",
    )

    args = ap.parse_args(argv)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load trades
    trades = load_trades(args.files)
    if trades.empty:
        print("No trades loaded.")
        return

    # Filter by side if requested
    if args.side != "all":
        if "side" in trades.columns:
            trades = trades[trades["side"] == args.side]
            print(f"Analyzing {args.side} trades only")
        else:
            print("Warning: 'side' column not found. Analyzing all trades.")

    # Calculate metrics
    metrics = calculate_metrics(trades)

    # Console output
    print("\n" + "=" * 60)
    print("BACKTEST ANALYSIS REPORT".center(60))
    print("=" * 60)
    print(f"\nAnalyzed files: {len(args.files)}")
    print(f"Total trades: {metrics['total_trades']}")
    print(
        f"Date range: {trades.iloc[0]['entry_date'] if 'entry_date' in trades.columns else 'N/A'} to "
        f"{trades.iloc[-1]['exit_date'] if 'exit_date' in trades.columns else 'N/A'}"
    )

    print("\n" + "-" * 60)
    print("PERFORMANCE METRICS".center(60))
    print("-" * 60)

    for key, value in metrics.items():
        if value is not None:
            label = key.replace("_", " ").title()
            if isinstance(value, float):
                if "pct" in key or "rate" in key or "return" in key:
                    print(f"{label:.<30} {value:>20.2f}%")
                elif "profit" in key or "drawdown" in key and "pct" not in key:
                    print(f"{label:.<30} ¥{value:>19,.0f}")
                else:
                    print(f"{label:.<30} {value:>20.2f}")
            else:
                print(f"{label:.<30} {value:>20}")

    # Show trades if requested
    if args.show_trades:
        print("\n" + "=" * 60)
        print("TRADE DETAILS".center(60))
        print("=" * 60)
        print(trades.to_string(max_rows=50))

    # Generate visual report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Visual charts
    chart_path = args.output_dir / f"backtest_analysis_{timestamp}.png"
    create_visual_report(trades, chart_path)
    print(f"\nVisual report saved to: {chart_path}")

    # Excel export
    if args.export_excel:
        excel_path = args.output_dir / f"backtest_analysis_{timestamp}.xlsx"
        export_to_excel(trades, metrics, excel_path)
        print(f"Excel report saved to: {excel_path}")

    # PDF export
    if args.export_pdf:
        pdf_path = args.output_dir / f"backtest_analysis_{timestamp}.pdf"
        with PdfPages(pdf_path) as pdf:
            # Create report pages
            create_visual_report(trades, None)
            pdf.savefig(bbox_inches="tight")
            plt.close()

            # Add metadata
            d = pdf.infodict()
            d["Title"] = "Backtest Analysis Report"
            d["Author"] = "Swing Trading System"
            d["Subject"] = "Backtest Performance Analysis"
            d["Keywords"] = "Trading, Backtest, Performance"
            d["CreationDate"] = datetime.now()

        print(f"PDF report saved to: {pdf_path}")

    # Strategy comparison
    if args.compare:
        # Group files by strategy name (assuming filename contains strategy)
        strategies: dict[str, list[str]] = {}
        for f in args.files:
            # Extract strategy name from filename
            filename = Path(f).stem
            strategy = filename.split("_")[
                0
            ]  # Assuming format: strategy_YYYYMMDD_HHMMSS.json
            if strategy not in strategies:
                strategies[strategy] = []
            strategies[strategy].append(f)

        if len(strategies) > 1:
            compare_strategies(strategies, args.output_dir)
        else:
            print(
                "\nNote: Only one strategy found. Comparison requires multiple strategies."
            )


if __name__ == "__main__":
    main()
