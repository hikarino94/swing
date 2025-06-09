import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import shlex
import threading


def run_command(cmd, output_widget):
    """Execute *cmd* in a background thread and stream output to *output_widget*."""

    def _worker():
        output_widget.delete(1.0, tk.END)
        try:
            proc = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            assert proc.stdout is not None  # for type checkers
            for line in proc.stdout:
                output_widget.insert(tk.END, line)
                output_widget.see(tk.END)
            proc.wait()
            if proc.returncode:
                output_widget.insert(
                    tk.END,
                    f"\nコマンドが終了コード {proc.returncode} で終了しました",
                )
        except Exception as exc:  # pylint: disable=broad-except
            output_widget.insert(tk.END, f"\nエラー: {exc}")

    threading.Thread(target=_worker, daemon=True).start()


def build_fetch_quotes_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="株価取得")
    desc = (
        "J-Quants から株価を取得し prices テーブルに保存します。\n"
        "日付は任意で YYYY-MM-DD 形式です。"
    )
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    arg_frame = ttk.Frame(frame)
    arg_frame.pack(anchor="w", padx=5)
    ttk.Label(arg_frame, text="開始日:").grid(row=0, column=0, sticky="e")
    start_var = tk.StringVar()
    ttk.Entry(arg_frame, textvariable=start_var, width=15).grid(row=0, column=1)
    ttk.Label(arg_frame, text="終了日:").grid(row=0, column=2, sticky="e")
    end_var = tk.StringVar()
    ttk.Entry(arg_frame, textvariable=end_var, width=15).grid(row=0, column=3)

    def _run():
        cmd = "python fetch/daily_quotes.py"
        if start_var.get():
            cmd += f" --start {start_var.get()}"
        if end_var.get():
            cmd += f" --end {end_var.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_listed_info_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="上場情報取得")
    desc = "J-Quants の /listed/info を取得し listed_info テーブルを更新します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )

    def _run():
        cmd = "python fetch/listed_info.py"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_statements_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="財務諸表取得")
    desc = (
        "決算データを取得します。モード1: 銘柄ごとに一括取得。"
        "モード2: 指定日または期間を取得。"
    )
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    ttk.Label(arg, text="モード (1 または 2):").grid(row=0, column=0, sticky="e")
    mode_var = tk.StringVar(value="1")
    ttk.Entry(arg, textvariable=mode_var, width=5).grid(row=0, column=1)
    ttk.Label(arg, text="開始日:").grid(row=1, column=0, sticky="e")
    start_var = tk.StringVar()
    ttk.Entry(arg, textvariable=start_var, width=12).grid(row=1, column=1)
    ttk.Label(arg, text="終了日:").grid(row=1, column=2, sticky="e")
    end_var = tk.StringVar()
    ttk.Entry(arg, textvariable=end_var, width=12).grid(row=1, column=3)

    def _run():
        cmd = f"python fetch/statements.py {mode_var.get()}"
        if start_var.get():
            cmd += f" --start {start_var.get()}"
        if end_var.get():
            cmd += f" --end {end_var.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_screen_fund_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="財務スクリーニング")
    desc = "財務データをスクリーニングし、シグナルを fundamental_signals に保存します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    ttk.Label(arg, text="参照期間:").grid(row=0, column=0)
    lookback = tk.StringVar(value="1095")
    ttk.Entry(arg, textvariable=lookback, width=8).grid(row=0, column=1)
    ttk.Label(arg, text="開示閾値:").grid(row=0, column=2)
    recent = tk.StringVar(value="7")
    ttk.Entry(arg, textvariable=recent, width=5).grid(row=0, column=3)

    def _run():
        cmd = f"python screening/screen_statements.py --lookback {lookback.get()} --recent {recent.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_screen_tech_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="テクニカルスクリーニング")
    desc = "テクニカル指標を計算するか、当日のシグナルを表示します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    cmd_var = tk.StringVar(value="indicators")
    ttk.Label(frame, text="コマンド (indicators/screen):").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=cmd_var, width=12).pack(anchor="w", padx=5)
    asof_var = tk.StringVar()
    ttk.Label(frame, text="対象日 YYYY-MM-DD:").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=asof_var, width=12).pack(anchor="w", padx=5)

    def _run():
        cmd = f"python screening/screen_technical.py {cmd_var.get()}"
        if asof_var.get():
            cmd += f" --as-of {asof_var.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_backtest_stmt_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="ファンダメンタルバックテスト")
    desc = "財務シグナルでバックテストを実行し、結果を Excel に出力します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    hold = tk.StringVar(value="40")
    offset = tk.StringVar(value="1")
    cap = tk.StringVar(value="1000000")
    xlsx = tk.StringVar(value="trades.xlsx")
    ttk.Label(arg, text="保有日数:").grid(row=0, column=0)
    ttk.Entry(arg, textvariable=hold, width=6).grid(row=0, column=1)
    ttk.Label(arg, text="エントリーオフセット:").grid(row=0, column=2)
    ttk.Entry(arg, textvariable=offset, width=6).grid(row=0, column=3)
    ttk.Label(arg, text="資金:").grid(row=1, column=0)
    ttk.Entry(arg, textvariable=cap, width=10).grid(row=1, column=1)
    ttk.Label(arg, text="出力ファイル:").grid(row=1, column=2)
    ttk.Entry(arg, textvariable=xlsx, width=15).grid(row=1, column=3)

    def _run():
        cmd = (
            f"python backtest/backtest_statements.py --hold {hold.get()} "
            f"--entry-offset {offset.get()} --capital {cap.get()} --xlsx {xlsx.get()}"
        )
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_backtest_tech_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="テクニカルバックテスト")
    desc = "テクニカル指標を用いたスイングトレードのバックテストを実行します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    as_of = tk.StringVar()
    ttk.Label(arg, text="エントリー日 YYYY-MM-DD:").grid(row=0, column=0)
    ttk.Entry(arg, textvariable=as_of, width=12).grid(row=0, column=1)
    hold = tk.StringVar(value="60")
    stop = tk.StringVar(value="0.05")
    cap = tk.StringVar(value="1000000")
    out = tk.StringVar(value="backtest_results.xlsx")
    ttk.Label(arg, text="保有日数:").grid(row=1, column=0)
    ttk.Entry(arg, textvariable=hold, width=6).grid(row=1, column=1)
    ttk.Label(arg, text="損切り率:").grid(row=1, column=2)
    ttk.Entry(arg, textvariable=stop, width=6).grid(row=1, column=3)
    ttk.Label(arg, text="資金:").grid(row=2, column=0)
    ttk.Entry(arg, textvariable=cap, width=10).grid(row=2, column=1)
    ttk.Label(arg, text="出力ファイル:").grid(row=2, column=2)
    ttk.Entry(arg, textvariable=out, width=20).grid(row=2, column=3)

    def _run():
        if not as_of.get():
            messagebox.showerror("エラー", "エントリー日を入力してください")
            return
        cmd = (
            f"python backtest/backtest_technical.py --as-of {as_of.get()} "
            f"--hold-days {hold.get()} --stop-loss {stop.get()} "
            f"--capital {cap.get()} --outfile {out.get()}"
        )
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def main():
    root = tk.Tk()
    root.title("スイングトレードGUI")
    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True)

    output = scrolledtext.ScrolledText(root, width=80, height=20)
    output.pack(fill="both", expand=True)

    build_fetch_quotes_tab(nb, output)
    build_listed_info_tab(nb, output)
    build_statements_tab(nb, output)
    build_screen_fund_tab(nb, output)
    build_screen_tech_tab(nb, output)
    build_backtest_stmt_tab(nb, output)
    build_backtest_tech_tab(nb, output)

    root.mainloop()


if __name__ == "__main__":
    main()
