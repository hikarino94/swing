#!/usr/bin/env python3
"""
SQLite データベース閲覧ユーティリティ

SQLite BrowserがGUIで開けない場合の代替手段として、
コマンドラインまたはGUIでデータベースの内容を確認できます。
"""

import sys
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, scrolledtext, ttk
from typing import Optional

import pandas as pd

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.utils.db_utils import get_db_manager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class DBViewer:
    """データベース閲覧クラス"""

    def __init__(self, db_path: Optional[Path] = None):
        """
        Args:
            db_path: データベースファイルパス
        """
        self.db_manager = get_db_manager() if db_path is None else get_db_manager()
        if db_path:
            self.db_manager.db_path = Path(db_path)

    def show_tables(self) -> None:
        """テーブル一覧を表示"""
        print("📊 データベーステーブル一覧:")
        print("-" * 50)

        sql = """
        SELECT name, sql FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """

        try:
            tables = self.db_manager.execute_query(sql)

            for table in tables:
                print(f"テーブル名: {table['name']}")
                print(f"作成SQL: {table['sql'][:100]}...")
                print()

        except Exception as e:
            logger.error(f"テーブル一覧取得エラー: {e}")

    def show_table_info(self, table_name: str) -> None:
        """テーブルの詳細情報を表示"""
        print(f"📋 テーブル '{table_name}' の詳細:")
        print("-" * 50)

        try:
            # カラム情報
            pragma_sql = f"PRAGMA table_info({table_name})"
            columns = self.db_manager.execute_query(pragma_sql)

            print("カラム情報:")
            for col in columns:
                print(f"  {col['name']} ({col['type']}) {'NOT NULL' if col['notnull'] else 'NULL可'}")
            print()

            # 行数
            count_sql = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.db_manager.execute_query(count_sql)
            row_count = count_result[0]["count"] if count_result else 0
            print(f"総行数: {row_count:,} 行")
            print()

        except Exception as e:
            logger.error(f"テーブル情報取得エラー: {e}")

    def show_sample_data(self, table_name: str, limit: int = 10) -> None:
        """サンプルデータを表示"""
        print(f"📄 テーブル '{table_name}' のサンプルデータ（最初の{limit}行）:")
        print("-" * 80)

        try:
            sql = f"SELECT * FROM {table_name} LIMIT {limit}"
            rows = self.db_manager.execute_query(sql)

            if not rows:
                print("データがありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            print(df.to_string(index=False))

        except Exception as e:
            logger.error(f"サンプルデータ取得エラー: {e}")

    def execute_query(self, query: str) -> None:
        """任意のクエリを実行"""
        print(f"🔍 クエリ実行: {query}")
        print("-" * 80)

        try:
            rows = self.db_manager.execute_query(query)

            if not rows:
                print("結果がありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            print(df.to_string(index=False))

        except Exception as e:
            logger.error(f"クエリ実行エラー: {e}")

    def show_recent_data(self, table_name: str, date_column: str = "date", limit: int = 10) -> None:
        """最新のデータを表示"""
        print(f"📅 テーブル '{table_name}' の最新データ（{limit}行）:")
        print("-" * 80)

        try:
            sql = f"""
            SELECT * FROM {table_name}
            ORDER BY {date_column} DESC
            LIMIT {limit}
            """
            rows = self.db_manager.execute_query(sql)

            if not rows:
                print("データがありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            print(df.to_string(index=False))

        except Exception as e:
            logger.error(f"最新データ取得エラー: {e}")


class DBViewerGUI:
    """GUIデータベース閲覧アプリケーション"""

    def __init__(self, db_path: Optional[Path] = None):
        """
        Args:
            db_path: データベースファイルパス
        """
        self.db_viewer = DBViewer(db_path)

        # GUIセットアップ
        self.root = tk.Tk()
        self.root.title("🗃️ SQLite データベースビューアー")
        self.root.geometry("1200x800")
        self.root.configure(bg="#f0f0f0")

        # スタイル設定
        style = ttk.Style()
        style.theme_use("clam")

        self.setup_gui()

    def setup_gui(self):
        """GUI要素をセットアップ"""
        # 日本語フォントの設定
        # 利用可能なフォントを優先順位付きで試す
        font_families = [
            "Noto Sans CJK JP",
            "MS Gothic",
            "Yu Gothic",
            "Hiragino Kaku Gothic Pro",
            "TakaoGothic",
            "IPAGothic",
            "Arial Unicode MS",
            "Arial",
        ]

        mono_font_families = ["Noto Sans Mono CJK JP", "MS Gothic", "Consolas", "Courier New", "Courier"]

        # 利用可能なフォントを検索
        import tkinter.font as tkFont

        available_fonts = tkFont.families()

        # 通常フォント
        normal_font = None
        for font in font_families:
            if font in available_fonts:
                normal_font = font
                break
        if not normal_font:
            normal_font = "TkDefaultFont"

        # 等幅フォント
        mono_font = None
        for font in mono_font_families:
            if font in available_fonts:
                mono_font = font
                break
        if not mono_font:
            mono_font = "TkFixedFont"

        # フォント設定を保存
        self.normal_font = normal_font
        self.mono_font = mono_font

        # メインフレーム
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # タイトル
        title_label = ttk.Label(main_frame, text="🗃️ SQLite データベースビューアー", font=(self.normal_font, 16, "bold"))
        title_label.pack(pady=(0, 20))

        # 左側フレーム（テーブル一覧）
        left_frame = ttk.LabelFrame(main_frame, text="📊 テーブル一覧", padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))

        # テーブルリストボックス
        self.table_listbox = tk.Listbox(left_frame, width=25, height=20, font=(self.mono_font, 10))
        self.table_listbox.pack(fill=tk.BOTH, expand=True)
        self.table_listbox.bind("<<ListboxSelect>>", self.on_table_select)

        # ボタンフレーム
        button_frame = ttk.Frame(left_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(button_frame, text="🔄 更新", command=self.load_tables).pack(fill=tk.X, pady=(0, 5))

        ttk.Button(button_frame, text="ℹ️ テーブル詳細", command=self.show_table_details).pack(fill=tk.X, pady=(0, 5))

        # 右側フレーム（データ表示）
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # タブコントロール
        self.notebook = ttk.Notebook(right_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # データタブ
        self.data_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.data_frame, text="📄 データ表示")

        # データ表示用テキストエリア
        self.data_text = scrolledtext.ScrolledText(
            self.data_frame, wrap=tk.NONE, font=(self.mono_font, 9), width=80, height=30
        )
        self.data_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # データ制御フレーム
        data_control_frame = ttk.Frame(self.data_frame)
        data_control_frame.pack(fill=tk.X, padx=5, pady=(0, 5))

        ttk.Label(data_control_frame, text="表示行数:").pack(side=tk.LEFT)
        self.limit_var = tk.StringVar(value="100")
        limit_entry = ttk.Entry(data_control_frame, textvariable=self.limit_var, width=10)
        limit_entry.pack(side=tk.LEFT, padx=(5, 10))

        ttk.Button(data_control_frame, text="📄 サンプル表示", command=lambda: self.show_sample_data()).pack(
            side=tk.LEFT, padx=(0, 5)
        )

        ttk.Button(data_control_frame, text="📅 最新データ", command=self.show_recent_data).pack(side=tk.LEFT, padx=(0, 5))

        # SQLクエリタブ
        self.sql_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.sql_frame, text="🔍 SQL実行")

        # SQLクエリ入力
        sql_input_frame = ttk.LabelFrame(self.sql_frame, text="SQLクエリ", padding=5)
        sql_input_frame.pack(fill=tk.X, padx=5, pady=5)

        self.sql_text = tk.Text(sql_input_frame, height=4, font=("Courier", 10))
        self.sql_text.pack(fill=tk.X)

        # SQL実行ボタン
        sql_button_frame = ttk.Frame(sql_input_frame)
        sql_button_frame.pack(fill=tk.X, pady=(5, 0))

        ttk.Button(sql_button_frame, text="▶️ 実行", command=self.execute_sql).pack(side=tk.LEFT)

        ttk.Button(sql_button_frame, text="🗑️ クリア", command=lambda: self.sql_text.delete("1.0", tk.END)).pack(
            side=tk.LEFT, padx=(5, 0)
        )

        # SQL結果表示
        sql_result_frame = ttk.LabelFrame(self.sql_frame, text="実行結果", padding=5)
        sql_result_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        self.sql_result_text = scrolledtext.ScrolledText(sql_result_frame, wrap=tk.NONE, font=(self.mono_font, 9))
        self.sql_result_text.pack(fill=tk.BOTH, expand=True)

        # 初期化
        self.load_tables()

    def load_tables(self):
        """テーブル一覧を読み込み"""
        try:
            self.table_listbox.delete(0, tk.END)

            sql = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """

            tables = self.db_viewer.db_manager.execute_query(sql)

            for table in tables:
                self.table_listbox.insert(tk.END, table["name"])

        except Exception as e:
            messagebox.showerror("エラー", f"テーブル一覧の取得に失敗しました:\n{e}")

    def on_table_select(self, event):
        """テーブル選択時の処理"""
        selection = self.table_listbox.curselection()
        if selection:
            table_name = self.table_listbox.get(selection[0])
            self.show_sample_data(table_name)

    def show_table_details(self):
        """選択されたテーブルの詳細情報を表示"""
        selection = self.table_listbox.curselection()
        if not selection:
            messagebox.showwarning("警告", "テーブルを選択してください")
            return

        table_name = self.table_listbox.get(selection[0])

        try:
            # カラム情報取得
            pragma_sql = f"PRAGMA table_info({table_name})"
            columns = self.db_viewer.db_manager.execute_query(pragma_sql)

            # 行数取得
            count_sql = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.db_viewer.db_manager.execute_query(count_sql)
            row_count = count_result[0]["count"] if count_result else 0

            # 詳細情報表示
            details = f"📋 テーブル '{table_name}' の詳細情報\n"
            details += "=" * 50 + "\n\n"
            details += f"総行数: {row_count:,} 行\n\n"
            details += "カラム情報:\n"
            details += "-" * 30 + "\n"

            for col in columns:
                null_info = "NOT NULL" if col["notnull"] else "NULL可"
                details += f"  {col['name']} ({col['type']}) {null_info}\n"

            self.data_text.delete("1.0", tk.END)
            self.data_text.insert("1.0", details)

        except Exception as e:
            messagebox.showerror("エラー", f"テーブル詳細の取得に失敗しました:\n{e}")

    def show_sample_data(self, table_name: str | None = None):
        """サンプルデータを表示"""
        if table_name is None:
            selection = self.table_listbox.curselection()
            if not selection:
                messagebox.showwarning("警告", "テーブルを選択してください")
                return
            table_name = self.table_listbox.get(selection[0])

        try:
            limit = int(self.limit_var.get())
            sql = f"SELECT * FROM {table_name} LIMIT {limit}"
            rows = self.db_viewer.db_manager.execute_query(sql)

            if not rows:
                self.data_text.delete("1.0", tk.END)
                self.data_text.insert("1.0", "データがありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            data_str = f"📄 テーブル '{table_name}' のサンプルデータ（最初の{limit}行）\n"
            data_str += "=" * 60 + "\n\n"
            data_str += df.to_string(index=False)

            self.data_text.delete("1.0", tk.END)
            self.data_text.insert("1.0", data_str)

        except ValueError:
            messagebox.showerror("エラー", "行数は数値で入力してください")
        except Exception as e:
            messagebox.showerror("エラー", f"データの取得に失敗しました:\n{e}")

    def show_recent_data(self):
        """最新データを表示"""
        selection = self.table_listbox.curselection()
        if not selection:
            messagebox.showwarning("警告", "テーブルを選択してください")
            return

        table_name = self.table_listbox.get(selection[0])

        try:
            limit = int(self.limit_var.get())
            # 日付カラムを推測
            date_columns = ["date", "created_at", "updated_at", "timestamp"]

            # テーブルのカラム情報を取得
            pragma_sql = f"PRAGMA table_info({table_name})"
            columns = self.db_viewer.db_manager.execute_query(pragma_sql)
            column_names = [col["name"] for col in columns]

            # 日付カラムを探す
            date_column = None
            for col in date_columns:
                if col in column_names:
                    date_column = col
                    break

            if date_column:
                sql = f"""
                SELECT * FROM {table_name}
                ORDER BY {date_column} DESC
                LIMIT {limit}
                """
            else:
                sql = f"SELECT * FROM {table_name} LIMIT {limit}"

            rows = self.db_viewer.db_manager.execute_query(sql)

            if not rows:
                self.data_text.delete("1.0", tk.END)
                self.data_text.insert("1.0", "データがありません")
                return

            # DataFrameに変換して表示
            df = pd.DataFrame([dict(row) for row in rows])
            sort_info = f"（{date_column}で降順ソート）" if date_column else "（ソート無し）"
            data_str = f"📅 テーブル '{table_name}' の最新データ（{limit}行）{sort_info}\n"
            data_str += "=" * 60 + "\n\n"
            data_str += df.to_string(index=False)

            self.data_text.delete("1.0", tk.END)
            self.data_text.insert("1.0", data_str)

        except ValueError:
            messagebox.showerror("エラー", "行数は数値で入力してください")
        except Exception as e:
            messagebox.showerror("エラー", f"データの取得に失敗しました:\n{e}")

    def execute_sql(self):
        """SQLクエリを実行"""
        sql = self.sql_text.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "SQLクエリを入力してください")
            return

        try:
            rows = self.db_viewer.db_manager.execute_query(sql)

            if not rows:
                result_str = "結果がありません"
            else:
                # DataFrameに変換して表示
                df = pd.DataFrame([dict(row) for row in rows])
                result_str = f"🔍 クエリ実行結果（{len(rows)}行）\n"
                result_str += "=" * 50 + "\n\n"
                result_str += df.to_string(index=False)

            self.sql_result_text.delete("1.0", tk.END)
            self.sql_result_text.insert("1.0", result_str)

        except Exception as e:
            messagebox.showerror("エラー", f"SQLクエリの実行に失敗しました:\n{e}")

    def run(self):
        """GUIを開始"""
        self.root.mainloop()


def main():
    """メイン関数"""
    import argparse

    parser = argparse.ArgumentParser(description="SQLite データベース閲覧ツール")
    parser.add_argument("--db", help="データベースファイルパス")
    parser.add_argument("--gui", action="store_true", help="GUIモードで起動")
    parser.add_argument("--tables", action="store_true", help="テーブル一覧を表示")
    parser.add_argument("--info", help="指定テーブルの詳細情報を表示")
    parser.add_argument("--sample", help="指定テーブルのサンプルデータを表示")
    parser.add_argument("--recent", help="指定テーブルの最新データを表示")
    parser.add_argument("--limit", type=int, default=10, help="表示行数制限")
    parser.add_argument("--query", help="任意のSQLクエリを実行")

    args = parser.parse_args()

    # GUIモードの場合
    if args.gui:
        try:
            app = DBViewerGUI(args.db)
            app.run()
        except Exception as e:
            print(f"GUI起動エラー: {e}")
            print("コマンドラインモードを使用してください")
        return

    # コマンドラインモード
    viewer = DBViewer(args.db)

    if args.tables:
        viewer.show_tables()
    elif args.info:
        viewer.show_table_info(args.info)
    elif args.sample:
        viewer.show_sample_data(args.sample, args.limit)
    elif args.recent:
        viewer.show_recent_data(args.recent, limit=args.limit)
    elif args.query:
        viewer.execute_query(args.query)
    else:
        # デフォルト: テーブル一覧を表示
        viewer.show_tables()
        print("\n💡 使用例:")
        print("  python db_viewer.py --gui                 # GUI版起動")
        print("  python db_viewer.py --tables              # テーブル一覧")
        print("  python db_viewer.py --info prices         # pricesテーブル詳細")
        print("  python db_viewer.py --sample prices       # pricesテーブルサンプル")
        print("  python db_viewer.py --recent prices       # prices最新データ")
        print("  python db_viewer.py --query 'SELECT * FROM prices WHERE code=\"1301\" LIMIT 5'")


if __name__ == "__main__":
    main()
