import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import os
import sys
import shlex
import threading
import json
import datetime as dt
from pathlib import Path

from screening import thresholds


def timestamped_path(path: str, ext: str = ".xlsx") -> str:
    """Return *path* with current timestamp appended before the extension."""

    p = Path(path)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = p.suffix if p.suffix else ext
    return p.with_name(f"{p.stem}_{ts}{suffix}").as_posix()


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
        "開始日のみ入力した場合は開始日から当日まで、終了日のみを入力した場合は当日のみ取得します。\n"
        "取得期間は最大10年です。2025/06/19→2015-06-19"
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
    desc = "テクニカル指標を計算するか、当日のシグナルを表示します。\n" "対象日付を入力する場合はテストとみなし、過去参照日数分のデータを処理します。"
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
        path = timestamped_path(xlsx.get())
        xlsx.set(path)
        cmd = (
            f"python backtest/backtest_statements.py --hold {hold.get()} "
            f"--entry-offset {offset.get()} --capital {cap.get()} --xlsx {path}"
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
        path = timestamped_path(out.get())
        out.set(path)
        cmd = (
            f"python backtest/backtest_technical.py --start {start_var.get()} "
            f"--hold-days {hold.get()} --stop-loss {stop.get()} "
            f"--capital {cap.get()} --outfile {path}"
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


def build_thresholds_tab(nb):
    """Display and edit screening threshold values."""

    frame = ttk.Frame(nb)
    nb.add(frame, text="閾値設定")

    path = Path(thresholds.__file__).with_suffix(".json")
    vals = thresholds.load_thresholds(path)
    entries = {}

    for idx, (key, val) in enumerate(vals.items()):
        ttk.Label(frame, text=key).grid(row=idx, column=0, sticky="e")
        var = tk.StringVar(value=str(val))
        ttk.Entry(frame, textvariable=var, width=10).grid(row=idx, column=1)
        entries[key] = var

    def _save():
        try:
            data = {k: float(v.get()) for k, v in entries.items()}
        except ValueError:
            messagebox.showerror("エラー", "数値を入力してください")
            return
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        messagebox.showinfo("保存", f"{path} を更新しました")

    def _reload():
        vals = thresholds.load_thresholds(path)
        for k, v in vals.items():
            if k in entries:
                entries[k].set(str(v))

    ttk.Button(frame, text="保存", command=_save).grid(row=len(vals), column=0, pady=5)
    ttk.Button(frame, text="再読込", command=_reload).grid(row=len(vals), column=1, pady=5)


def build_db_summary_tab(nb, output):
    frame = ttk.Frame(nb)
    nb.add(frame, text="DBサマリー")
    desc = "データベースの件数と日付範囲を表示します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )

    def _run():
        cmd = "python db/db_summary.py"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_analyze_json_tab(nb, output):
    """Run backtest JSON analyzer from the GUI."""

    frame = ttk.Frame(nb)
    nb.add(frame, text="JSON分析")

    desc = "バックテスト結果 JSON を読み込み統計を表示します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )

    lb = tk.Listbox(frame, selectmode="extended", height=10)
    lb.pack(side="left", fill="both", expand=True, padx=5, pady=5)
    sb = ttk.Scrollbar(frame, orient="vertical", command=lb.yview)
    sb.pack(side="left", fill="y")
    lb.configure(yscrollcommand=sb.set)

    def refresh():
        lb.delete(0, tk.END)
        for p in sorted(Path(".").glob("*.json")):
            lb.insert(tk.END, p.name)

    show_var = tk.BooleanVar()
    ttk.Checkbutton(frame, text="トレード一覧も表示", variable=show_var).pack(anchor="w", padx=5)

    def _run():
        sel = lb.curselection()
        if not sel:
            messagebox.showerror("エラー", "ファイルを選択してください")
            return
        files = [lb.get(i) for i in sel]
        cmd = "python backtest/analyze_backtest_json.py " + " ".join(files)
        if show_var.get():
            cmd += " --show-trades"
        run_command(cmd, output)

    btn_frame = ttk.Frame(frame)
    btn_frame.pack(anchor="e", padx=5, pady=5)
    ttk.Button(btn_frame, text="更新", command=refresh).pack(side="left", padx=(0, 5))
    ttk.Button(btn_frame, text="実行", command=_run).pack(side="left")
    refresh()


def build_signals_tab(nb, output):
    """Display screening signals stored in the DB."""

    frame = ttk.Frame(nb)
    nb.add(frame, text="シグナル確認")

    desc = "DB に保存されたシグナルを表示します。開始日と終了日を指定しない" "場合は当日分を抽出します。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )

    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)

    ttk.Label(arg, text="種類 (fund/tech):").grid(row=0, column=0)
    kind_var = tk.StringVar(value="fund")
    ttk.Entry(arg, textvariable=kind_var, width=8).grid(row=0, column=1)

    ttk.Label(arg, text="開始日:").grid(row=1, column=0)
    start_var = tk.StringVar()
    ttk.Entry(arg, textvariable=start_var, width=12).grid(row=1, column=1)

    ttk.Label(arg, text="終了日:").grid(row=2, column=0)
    end_var = tk.StringVar()
    ttk.Entry(arg, textvariable=end_var, width=12).grid(row=2, column=1)

    ttk.Label(arg, text="表示件数:").grid(row=3, column=0)
    limit_var = tk.StringVar(value="20")
    ttk.Entry(arg, textvariable=limit_var, width=6).grid(row=3, column=1)

    def _run():
        cmd = (
            f"python db/list_signals.py {kind_var.get()} " f"--limit {limit_var.get()}"
        )
        if start_var.get():
            cmd += f" --start {start_var.get()}"
        if end_var.get():
            cmd += f" --end {end_var.get()}"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


def build_results_tab(nb):
    """List Excel files and open them."""

    frame = ttk.Frame(nb)
    nb.add(frame, text="結果閲覧")

    lb = tk.Listbox(frame, height=15)
    lb.pack(side="left", fill="both", expand=True, padx=5, pady=5)
    sb = ttk.Scrollbar(frame, orient="vertical", command=lb.yview)
    sb.pack(side="left", fill="y")
    lb.configure(yscrollcommand=sb.set)

    def refresh():
        lb.delete(0, tk.END)
        for p in sorted(Path(".").glob("*.xlsx")):
            lb.insert(tk.END, p.name)

    def open_selected(_event=None):
        sel = lb.curselection()
        if not sel:
            return
        path = Path(lb.get(sel[0]))
        try:
            if sys.platform.startswith("darwin"):
                subprocess.Popen(["open", path])
            elif os.name == "nt":
                os.startfile(path)  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", path])
        except Exception as exc:  # pylint: disable=broad-except
            messagebox.showerror("エラー", str(exc))

    ttk.Button(frame, text="更新", command=refresh).pack(anchor="e", padx=5, pady=(5, 2))
    ttk.Button(frame, text="開く", command=open_selected).pack(
        anchor="e", padx=5, pady=(0, 5)
    )
    lb.bind("<Double-1>", open_selected)
    refresh()


def build_output_controls(root, output_widget):
    """Create buttons to manage the output widget."""
    frame = ttk.Frame(root)
    ttk.Button(
        frame,
        text="クリア",
        command=lambda: output_widget.delete(1.0, tk.END),
    ).pack(side="right")
    frame.pack(fill="x", padx=5, pady=(0, 5))


###############################################################################
# 新規: ML スクリーニングタブ
###############################################################################


def build_screen_ml_tab(nb: ttk.Notebook, output: tk.Text):
    """Add tab to run ML screening (screen_ml.py)."""

    frame = ttk.Frame(nb)
    nb.add(frame, text="MLスクリーニング")

    desc = "機械学習モデルで1か月先の株価上昇確率を推定し、\n" "上位銘柄を抽出します。必要に応じて再学習 (--retrain) も実施可能。"
    ttk.Label(frame, text=desc, wraplength=400, justify="left").pack(
        anchor="w", padx=5, pady=5
    )

    arg = ttk.Frame(frame)
    arg.pack(anchor="w", padx=5)

    # ── 引数入力フィールド ─────────────────────────────────────────
    ttk.Label(arg, text="上位件数:").grid(row=0, column=0, sticky="e")
    top_var = tk.StringVar(value="30")
    ttk.Entry(arg, textvariable=top_var, width=6).grid(row=0, column=1)

    ttk.Label(arg, text="学習参照日数:").grid(row=0, column=2, sticky="e")
    lookback_var = tk.StringVar(value="1095")
    ttk.Entry(arg, textvariable=lookback_var, width=8).grid(row=0, column=3)

    retrain_var = tk.BooleanVar()
    ttk.Checkbutton(arg, text="強制再学習", variable=retrain_var).grid(
        row=0, column=4, padx=(10, 0)
    )

    # ── コマンド実行 ──────────────────────────────────────────────
    def _run():
        cmd = (
            "python screening/screen_ml.py screen "
            f"--top {top_var.get()} --lookback {lookback_var.get()}"
        )
        if retrain_var.get():
            cmd += " --retrain"
        run_command(cmd, output)

    ttk.Button(frame, text="実行", command=_run).pack(pady=5)


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
    build_screen_ml_tab(nb, output)
    build_backtest_stmt_tab(nb, output)
    build_backtest_tech_tab(nb, output)
    build_update_token_tab(nb, output)
    build_thresholds_tab(nb)
    build_db_summary_tab(nb, output)
    build_signals_tab(nb, output)
    build_analyze_json_tab(nb, output)
    build_results_tab(nb)

    root.mainloop()


if __name__ == "__main__":
    main()
