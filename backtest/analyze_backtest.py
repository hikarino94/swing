#!/usr/bin/env python
"""Unified backtest analysis tool.

This module combines the functionality of analyze_backtest_json.py and
analyze_json_advanced.py into a single, comprehensive analysis tool.

Basic usage:
    python -m backtest.analyze_backtest result.json

Advanced usage with visual reports:
    python -m backtest.analyze_backtest result.json --advanced --export-excel --export-pdf
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# Constants
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
                print(f"Error: File '{p}' not found.")
                print("\nAvailable backtest files:")
                backtest_dir = Path("data/output/backtest")
                if backtest_dir.exists():
                    for f in sorted(backtest_dir.glob("*.json")):
                        print(f"  {f.name}")
                raise FileNotFoundError(f"File {p} not found")

        # Store filename before opening
        source_filename = path.name

        with open(str(path)) as f:  # type: ignore[assignment]
            data = json.load(f)  # type: ignore[arg-type]

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
        df["source_file"] = source_filename
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


def format_summary(metrics: dict[str, Any]) -> None:
    """Print formatted summary to console."""
    print("\n" + "=" * 60)
    print("BACKTEST ANALYSIS SUMMARY".center(60))
    print("=" * 60)

    # Basic metrics
    print(f"\nTotal Trades: {metrics.get('total_trades', 0)}")
    print(f"Total Profit: ¥{metrics.get('total_profit', 0):,.0f}")
    print(f"Win Rate: {metrics.get('win_rate', 0):.2f}%")
    print(f"Average Return: {metrics.get('avg_return', 0):.2f}%")
    print(f"Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}")

    # Risk metrics
    print(f"\nMax Drawdown: ¥{metrics.get('max_drawdown', 0):,.0f}")
    print(f"Max Drawdown %: {metrics.get('max_drawdown_pct', 0):.2f}%")
    print(f"Calmar Ratio: {metrics.get('calmar_ratio', 0):.2f}")

    # Trade analysis
    print(f"\nProfit Factor: {metrics.get('profit_factor', 0):.2f}")
    print(f"Average Win: ¥{metrics.get('avg_win', 0):,.0f}")
    print(f"Average Loss: ¥{metrics.get('avg_loss', 0):,.0f}")

    if metrics.get("avg_holding_days") is not None:
        print(f"Average Holding Days: {metrics.get('avg_holding_days'):.1f}")

    print(f"\nBest Trade: ¥{metrics.get('best_trade', 0):,.0f}")
    print(f"Worst Trade: ¥{metrics.get('worst_trade', 0):,.0f}")


def _ascii_table(df: pd.DataFrame, heavy: bool = False) -> str:
    """Return a simple ASCII table."""
    cols = list(df.columns)
    widths = [max(len(str(v)) for v in [c] + df[c].astype(str).tolist()) for c in cols]

    # Determine which characters to use for drawing
    h: str = "-"
    v: str = "|"
    c: str = "+"
    if heavy:
        try:
            "═╬║".encode(sys.stdout.encoding or "utf-8")
            h, v, c = "═", "║", "╬"
        except Exception:
            heavy = False

    if not heavy:
        h, v, c = "-", "|", "+"

    def border() -> str:
        return c + c.join(str(h * (w + 2)) for w in widths) + c

    lines = [border()]
    header = (
        v
        + v.join(f" {cname.ljust(w)} " for cname, w in zip(cols, widths, strict=False))
        + v
    )
    lines.append(header)
    lines.append(border())

    for idx in range(len(df)):
        row_values = [
            str(df.iloc[idx][cname]).rjust(w)
            for cname, w in zip(cols, widths, strict=False)
        ]
        line = v + v.join(f" {val} " for val in row_values) + v
        lines.append(line)
        lines.append(border())

    return "\n".join(lines)


def create_visual_report(
    trades: pd.DataFrame, metrics: dict[str, Any], output_path: Path
) -> None:
    """Create comprehensive visual analysis report."""
    try:
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.backends.backend_pdf import PdfPages
    except ImportError:
        print("Warning: matplotlib not available. Skipping visual report generation.")
        return

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
    cumulative_returns: pd.Series = (1 + trades[ret_col] / 100).cumprod()
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
    drawdown_pct.plot(ax=ax2, color="red", linewidth=2)
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

    table_data = []
    for key, value in metrics.items():
        if value is not None and not isinstance(value, float) or not np.isinf(value):
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

    if output_path.suffix == ".pdf":
        with PdfPages(output_path) as pdf:
            pdf.savefig(bbox_inches="tight")
            plt.close()

            # Add metadata
            d = pdf.infodict()
            d["Title"] = "Backtest Analysis Report"
            d["Author"] = "Swing Trading System"
            d["Subject"] = "Backtest Performance Analysis"
            d["Keywords"] = "Trading, Backtest, Performance"
            d["CreationDate"] = datetime.now()
    else:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()


def export_to_excel(
    trades: pd.DataFrame, metrics: dict[str, Any], output_path: Path
) -> None:
    """Export analysis results to Excel with formatting."""
    try:
        import xlsxwriter  # noqa: F401
    except ImportError:
        print("Warning: xlsxwriter not available. Skipping Excel export.")
        return

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


def main(argv: list[str] | None = None) -> None:
    """Unified backtest analyzer."""
    ap = argparse.ArgumentParser(
        description="Analyze backtest results from JSON files",
        epilog="Example: python -m backtest.analyze_backtest result.json --advanced",
    )
    ap.add_argument("files", nargs="+", help="JSON files to analyze")
    ap.add_argument(
        "--show-trades",
        action="store_true",
        help="Display detailed trade table",
    )
    ap.add_argument(
        "--side",
        choices=["long", "short", "all"],
        default="all",
        help="Analyze only long or short trades (default: all)",
    )
    ap.add_argument(
        "--advanced",
        action="store_true",
        help="Enable advanced analysis features (visual reports, Excel export)",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/output/analysis"),
        help="Output directory for reports (default: data/output/analysis)",
    )
    ap.add_argument(
        "--export-excel",
        action="store_true",
        help="Export results to Excel file (requires --advanced)",
    )
    ap.add_argument(
        "--export-pdf",
        action="store_true",
        help="Export visual report as PDF (requires --advanced)",
    )

    args = ap.parse_args(argv)

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

    # Basic console output
    format_summary(metrics)

    # Show trades if requested
    if args.show_trades:
        print("\n" + "=" * 60)
        print("TRADE DETAILS".center(60))
        print("=" * 60)
        print(_ascii_table(trades.head(50), heavy=True))
        if len(trades) > 50:
            print(f"\n... and {len(trades) - 50} more trades")

    # Advanced features
    if args.advanced or args.export_excel or args.export_pdf:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Visual report
        if args.advanced or args.export_pdf:
            output_format = "pdf" if args.export_pdf else "png"
            chart_path = (
                args.output_dir / f"backtest_analysis_{timestamp}.{output_format}"
            )
            create_visual_report(trades, metrics, chart_path)
            print(f"\nVisual report saved to: {chart_path}")

        # Excel export
        if args.export_excel:
            excel_path = args.output_dir / f"backtest_analysis_{timestamp}.xlsx"
            export_to_excel(trades, metrics, excel_path)
            print(f"Excel report saved to: {excel_path}")


if __name__ == "__main__":
    main()
