import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import shlex


def run_command(cmd, output_widget):
    output_widget.delete(1.0, tk.END)
    try:
        result = subprocess.run(
            shlex.split(cmd), capture_output=True, text=True, check=True
        )
        output_widget.insert(tk.END, result.stdout)
        if result.stderr:
            output_widget.insert(tk.END, "\n" + result.stderr)
    except subprocess.CalledProcessError as e:
        output_widget.insert(tk.END, e.stdout)
        output_widget.insert(tk.END, "\n" + str(e))
        if e.stderr:
            output_widget.insert(tk.END, "\n" + e.stderr)


def build_fetch_quotes_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Fetch Quotes")
    desc = (
        "Download daily quotes from J-Quants and upsert into the prices table.\n"
        "Dates are optional; format YYYYMMDD.")
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    arg_frame = ttk.Frame(frame)
    arg_frame.pack(anchor="w", padx=5)
    ttk.Label(arg_frame, text="start:").grid(row=0, column=0, sticky="e")
    start_var = tk.StringVar()
    ttk.Entry(arg_frame, textvariable=start_var, width=15).grid(row=0, column=1)
    ttk.Label(arg_frame, text="end:").grid(row=0, column=2, sticky="e")
    end_var = tk.StringVar()
    ttk.Entry(arg_frame, textvariable=end_var, width=15).grid(row=0, column=3)
    def _run():
        cmd = "python fetch/daily_quotes.py"
        if start_var.get():
            cmd += f" --start {start_var.get()}"
        if end_var.get():
            cmd += f" --end {end_var.get()}"
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def build_listed_info_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Fetch Listed Info")
    desc = "Fetch /listed/info snapshot and update the listed_info table."
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    def _run():
        cmd = "python fetch/listed_info.py"
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def build_statements_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Fetch Statements")
    desc = (
        "Fetch /fins/statements data. Mode 1: bulk by code. Mode 2: today only." )
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    ttk.Label(frame, text="mode (1 or 2):").pack(anchor="w", padx=5)
    mode_var = tk.StringVar(value="1")
    ttk.Entry(frame, textvariable=mode_var, width=5).pack(anchor="w", padx=5)
    def _run():
        cmd = f"python fetch/statements.py {mode_var.get()}"
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def build_screen_fund_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Screen Fundamentals")
    desc = (
        "Screen statements for fundamental signals and insert into fundamental_signals." )
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    ttk.Label(arg, text="lookback:").grid(row=0, column=0)
    lookback = tk.StringVar(value="1095")
    ttk.Entry(arg, textvariable=lookback, width=8).grid(row=0, column=1)
    ttk.Label(arg, text="recent:").grid(row=0, column=2)
    recent = tk.StringVar(value="7")
    ttk.Entry(arg, textvariable=recent, width=5).grid(row=0, column=3)
    def _run():
        cmd = f"python screening/screen_statements.py --lookback {lookback.get()} --recent {recent.get()}"
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def build_screen_tech_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Screen Technical")
    desc = (
        "Run technical indicator computation or display today's signals." )
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    cmd_var = tk.StringVar(value="indicators")
    ttk.Label(frame, text="command (indicators/screen):").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=cmd_var, width=12).pack(anchor="w", padx=5)
    asof_var = tk.StringVar()
    ttk.Label(frame, text="as_of YYYYMMDD:").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=asof_var, width=12).pack(anchor="w", padx=5)
    def _run():
        cmd = f"python screening/screen_technical.py {cmd_var.get()}"
        if asof_var.get():
            cmd += f" --as-of {asof_var.get()}"
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def build_backtest_stmt_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Backtest Fundamentals")
    desc = "Run fundamental signal backtest and export Excel results."
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    hold = tk.StringVar(value="40")
    offset = tk.StringVar(value="1")
    cap = tk.StringVar(value="1000000")
    xlsx = tk.StringVar(value="trades.xlsx")
    ttk.Label(arg, text="hold:").grid(row=0, column=0)
    ttk.Entry(arg, textvariable=hold, width=6).grid(row=0, column=1)
    ttk.Label(arg, text="entry offset:").grid(row=0, column=2)
    ttk.Entry(arg, textvariable=offset, width=6).grid(row=0, column=3)
    ttk.Label(arg, text="capital:").grid(row=1, column=0)
    ttk.Entry(arg, textvariable=cap, width=10).grid(row=1, column=1)
    ttk.Label(arg, text="xlsx:").grid(row=1, column=2)
    ttk.Entry(arg, textvariable=xlsx, width=15).grid(row=1, column=3)
    def _run():
        cmd = (
            f"python backtest/backtest_statements.py --hold {hold.get()} "
            f"--entry-offset {offset.get()} --capital {cap.get()} --xlsx {xlsx.get()}"
        )
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def build_backtest_tech_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="Backtest Technical")
    desc = "Run swing-trade backtest using technical indicators."
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(anchor="w", padx=5, pady=5)
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    as_of = tk.StringVar()
    ttk.Label(arg, text="as_of YYYYMMDD:").grid(row=0, column=0)
    ttk.Entry(arg, textvariable=as_of, width=12).grid(row=0, column=1)
    hold = tk.StringVar(value="60")
    stop = tk.StringVar(value="0.05")
    cap = tk.StringVar(value="1000000")
    out = tk.StringVar(value="backtest_results.xlsx")
    ttk.Label(arg, text="hold days:").grid(row=1, column=0)
    ttk.Entry(arg, textvariable=hold, width=6).grid(row=1, column=1)
    ttk.Label(arg, text="stop loss:").grid(row=1, column=2)
    ttk.Entry(arg, textvariable=stop, width=6).grid(row=1, column=3)
    ttk.Label(arg, text="capital:").grid(row=2, column=0)
    ttk.Entry(arg, textvariable=cap, width=10).grid(row=2, column=1)
    ttk.Label(arg, text="outfile:").grid(row=2, column=2)
    ttk.Entry(arg, textvariable=out, width=20).grid(row=2, column=3)
    def _run():
        if not as_of.get():
            messagebox.showerror("Error", "as_of date is required")
            return
        cmd = (
            f"python backtest/backtest_technical.py --as-of {as_of.get()} "
            f"--hold-days {hold.get()} --stop-loss {stop.get()} "
            f"--capital {cap.get()} --outfile {out.get()}"
        )
        run_command(cmd, output)
    ttk.Button(frame, text="Run", command=_run).pack(pady=5)


def main():
    root = tk.Tk()
    root.title("Swing Trading GUI")
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
