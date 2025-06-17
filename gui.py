import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import shlex
import threading
import json
from pathlib import Path


def run_command(cmd, output_widget, on_finish=None):
    """Execute ``cmd`` and stream output to ``output_widget``.

    If ``on_finish`` is provided it will be called with the collected output
    once the command finishes.
    """

    def _worker():
        output_widget.delete(1.0, tk.END)
        output_widget.insert(tk.END, f"$ {cmd}\n")
        collected = []
        try:
            proc = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            assert proc.stdout is not None  # for type checkers
            for line in proc.stdout:
                collected.append(line)
                output_widget.insert(tk.END, line)
                output_widget.see(tk.END)
            proc.wait()
            if proc.returncode:
                msg = f"\nコマンドが終了コード {proc.returncode} で終了しました"
                collected.append(msg)
                output_widget.insert(tk.END, msg)
        except Exception as exc:  # pylint: disable=broad-except
            msg = f"\nエラー: {exc}"
            collected.append(msg)
            output_widget.insert(tk.END, msg)
        finally:
            if on_finish:
                on_finish("".join(collected))

    threading.Thread(target=_worker, daemon=True).start()


def build_fetch_quotes_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="株価取得")
    desc = (
        "J-Quants から株価を取得し prices テーブルに保存します。\n"
        "日付は任意で YYYY-MM-DD 形式です。\n"
        "日付の指定がない場合は当日日付のデータを取得します。\n"
        "開始日のみ入力した場合は開始日から当日まで、終了日のみを入力した場合は当日のみ"
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

        def _finish(out):
            def _show():
                msg = "\n".join(out.strip().splitlines()[-10:])
                messagebox.showinfo("バックテスト結果", msg)

            output.after(0, _show)

        run_command(cmd, output, on_finish=_finish)

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
        "決算データを取得します。モード1: 銘柄ごとに一括取得。\n"
        "モード2: 指定がない場合は本日日付データ、開始日のみの場合は開始日から本日までのデータ\n"
        "モード2: 終了日のみの場合は指定された日付のみのデータ、期間指定であれば対象期間のデータ"
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

        def _finish(out):
            def _show():
                msg = "\n".join(out.strip().splitlines()[-10:])
                messagebox.showinfo("バックテスト結果", msg)

            output.after(0, _show)

        run_command(cmd, output, on_finish=_finish)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_screen_fund_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="財務スクリーニング")
    desc = (
        "財務データをスクリーニングし、シグナルを fundamental_signals に保存します。\n"
        "開示閾値で指定された期間の間に発表された決算をもとにscreeningします。\n"
        "テストのために日付を設定する場合は日付から開示閾値の間で検知を行います \n"
        "実施する場合は開示閾値＋1095してください"
    )
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
    ttk.Label(arg, text="基準日 (省略可):").grid(row=1, column=0, sticky="e")
    as_of = tk.StringVar()
    ttk.Entry(arg, textvariable=as_of, width=12).grid(row=1, column=1)

    def _run():
        cmd = f"python screening/screen_statements.py --lookback {lookback.get()} --recent {recent.get()}"
        if as_of.get():
            cmd += f" --as-of {as_of.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_screen_tech_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="テクニカルスクリーニング")
    desc = (
        "テクニカル指標を計算するか、当日のシグナルを表示します。\n"
        "対象日付を入力する場合はテストとみなし、過去参照日数分のデータを処理します。"
    )
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    cmd_var = tk.StringVar(value="indicators")
    ttk.Label(frame, text="コマンド (indicators/screen):").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=cmd_var, width=12).pack(anchor="w", padx=5)
    asof_var = tk.StringVar()
    ttk.Label(frame, text="対象日 YYYY-MM-DD:").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=asof_var, width=12).pack(anchor="w", padx=5)
    back_var = tk.StringVar(value="50")
    ttk.Label(frame, text="過去参照日数:").pack(anchor="w", padx=5)
    ttk.Entry(frame, textvariable=back_var, width=12).pack(anchor="w", padx=5)

    def _run():
        cmd = f"python screening/screen_technical.py {cmd_var.get()}"
        if asof_var.get():
            cmd += f" --as-of {asof_var.get()}"
        if back_var.get():
            cmd += f" --lookback {back_var.get()}"
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
    start_var = tk.StringVar()
    end_var = tk.StringVar()
    xlsx = tk.StringVar(value="trades.xlsx")
    ttk.Label(arg, text="保有日数:").grid(row=0, column=0)
    ttk.Entry(arg, textvariable=hold, width=6).grid(row=0, column=1)
    ttk.Label(arg, text="エントリーオフセット:").grid(row=0, column=2)
    ttk.Entry(arg, textvariable=offset, width=6).grid(row=0, column=3)
    ttk.Label(arg, text="資金:").grid(row=1, column=0)
    ttk.Entry(arg, textvariable=cap, width=10).grid(row=1, column=1)
    ttk.Label(arg, text="開始日:").grid(row=1, column=2)
    ttk.Entry(arg, textvariable=start_var, width=12).grid(row=1, column=3)
    ttk.Label(arg, text="終了日:").grid(row=2, column=0)
    ttk.Entry(arg, textvariable=end_var, width=12).grid(row=2, column=1)
    ttk.Label(arg, text="出力ファイル:").grid(row=2, column=2)
    ttk.Entry(arg, textvariable=xlsx, width=15).grid(row=2, column=3)

    def _run():
        cmd = (
            f"python backtest/backtest_statements.py --hold {hold.get()} "
            f"--entry-offset {offset.get()} --capital {cap.get()} --xlsx {xlsx.get()}"
        )
        if start_var.get():
            cmd += f" --start {start_var.get()}"
        if end_var.get():
            cmd += f" --end {end_var.get()}"

        def _finish(out):
            def _show():
                msg = "\n".join(out.strip().splitlines()[-10:])
                messagebox.showinfo("バックテスト結果", msg)

            output.after(0, _show)

        run_command(cmd, output, on_finish=_finish)

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
    start_var = tk.StringVar()
    ttk.Label(arg, text="エントリー開始日:").grid(row=0, column=0, sticky="e")
    ttk.Entry(arg, textvariable=start_var, width=12).grid(row=0, column=1)
    end_var = tk.StringVar()
    ttk.Label(arg, text="エントリー終了日:").grid(row=0, column=2, sticky="e")
    ttk.Entry(arg, textvariable=end_var, width=12).grid(row=0, column=3)
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
        if not start_var.get():
            messagebox.showerror("エラー", "開始日を入力してください")
            return
        cmd = (
            f"python backtest/backtest_technical.py --start {start_var.get()} "
            f"--hold-days {hold.get()} --stop-loss {stop.get()} "
            f"--capital {cap.get()} --outfile {out.get()}"
        )
        if end_var.get():
            cmd += f" --end {end_var.get()}"

        def _finish(out):
            def _show():
                msg = "\n".join(out.strip().splitlines()[-10:])
                messagebox.showinfo("バックテスト結果", msg)

            output.after(0, _show)

        run_command(cmd, output, on_finish=_finish)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_update_token_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="IDトークン更新")
    desc = "メールアドレスとパスワードから idtoken.json を更新します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )
    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)
    cred = {}
    path = Path(__file__).resolve().parent / "account.json"
    if path.is_file():
        try:
            with path.open("r", encoding="utf-8") as f:
                cred = json.load(f)
        except Exception:
            cred = {}
    mail = tk.StringVar(value=cred.get("mail", ""))
    pwd = tk.StringVar(value=cred.get("password", ""))
    ttk.Label(arg, text="メールアドレス:").grid(row=0, column=0, sticky="e")
    ttk.Entry(arg, textvariable=mail, width=25).grid(row=0, column=1)
    ttk.Label(arg, text="パスワード:").grid(row=1, column=0, sticky="e")
    ttk.Entry(arg, textvariable=pwd, width=25, show="*").grid(row=1, column=1)

    def _run():
        cmd = "python update_idtoken.py"
        if mail.get():
            cmd += f" --mail {mail.get()}"
        if pwd.get():
            cmd += f" --password {pwd.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_output_controls(root, output_widget):
    """Create buttons to manage the output widget."""
    frame = ttk.Frame(root)
    ttk.Button(
        frame,
        text="クリア",
        command=lambda: output_widget.delete(1.0, tk.END),
    ).pack(side="right")
    frame.pack(fill="x", padx=5, pady=(0, 5))


def main():
    root = tk.Tk()
    root.title("スイングトレードGUI")
    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True)

    output = scrolledtext.ScrolledText(root, width=80, height=20)
    output.pack(fill="both", expand=True)
    build_output_controls(root, output)

    build_fetch_quotes_tab(nb, output)
    build_listed_info_tab(nb, output)
    build_statements_tab(nb, output)
    build_screen_fund_tab(nb, output)
    build_screen_tech_tab(nb, output)
    build_backtest_stmt_tab(nb, output)
    build_backtest_tech_tab(nb, output)
    build_update_token_tab(nb, output)

    root.mainloop()


if __name__ == "__main__":
    main()
