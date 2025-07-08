#!/usr/bin/env python3
"""
コマンドラインログビューアー

ログファイルの一覧表示、閲覧、検索機能を提供します。
"""

import argparse
import sys
import time
from pathlib import Path

# プロジェクトルートをPYTHONPATHに追加
sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.config import LOG_DIR


def list_log_files() -> list[Path]:
    """ログファイル一覧を取得"""
    if not LOG_DIR.exists():
        return []

    log_files = []
    for log_file in LOG_DIR.glob("*.log"):
        log_files.append(log_file)

    return sorted(log_files, key=lambda x: x.stat().st_mtime, reverse=True)


def view_log_file(log_path: Path, lines: int | None = None) -> None:
    """ログファイルを表示"""
    if not log_path.exists():
        print(f"エラー: ログファイルが見つかりません: {log_path}")
        return

    with open(log_path, encoding="utf-8") as f:
        if lines:
            # 指定行数分読む
            content = f.readlines()
            for line in content[:lines]:
                print(line, end="")
        else:
            # 全て表示
            print(f.read())


def tail_log_file(log_path: Path, lines: int = 10, follow: bool = False) -> None:
    """ログファイルの末尾を表示"""
    if not log_path.exists():
        print(f"エラー: ログファイルが見つかりません: {log_path}")
        return

    with open(log_path, encoding="utf-8") as f:
        # 末尾のn行を取得
        content = f.readlines()
        tail_lines = content[-lines:]
        for line in tail_lines:
            print(line, end="")

        if follow:
            # リアルタイム追跡モード
            print("\n--- リアルタイム追跡中 (Ctrl+Cで終了) ---")
            f.seek(0, 2)  # ファイル末尾へ移動
            try:
                while True:
                    line = f.readline()
                    if line:
                        print(line, end="")
                    else:
                        time.sleep(0.1)
            except KeyboardInterrupt:
                print("\n--- 追跡終了 ---")


def search_logs(pattern: str, log_files: list[Path] | None = None) -> None:
    """ログファイル内を検索"""
    if log_files is None:
        log_files = list_log_files()

    found_count = 0
    for log_file in log_files:
        try:
            with open(log_file, encoding="utf-8") as f:
                line_number = 0
                for line in f:
                    line_number += 1
                    if pattern.lower() in line.lower():
                        if found_count == 0:
                            print(f"\n{log_file.name}:")
                        print(f"  {line_number}: {line.strip()}")
                        found_count += 1
        except Exception as e:
            print(f"エラー: {log_file}の読み込みに失敗: {e}")

    if found_count == 0:
        print(f"'{pattern}' は見つかりませんでした。")
    else:
        print(f"\n合計 {found_count} 件見つかりました。")


def main():
    parser = argparse.ArgumentParser(description="ログファイルビューアー")
    subparsers = parser.add_subparsers(dest="command", help="コマンド")

    # list コマンド
    subparsers.add_parser("list", help="ログファイル一覧を表示")

    # view コマンド
    view_parser = subparsers.add_parser("view", help="ログファイルを表示")
    view_parser.add_argument("file", help="ログファイル名")
    view_parser.add_argument("-n", "--lines", type=int, help="表示行数")

    # tail コマンド
    tail_parser = subparsers.add_parser("tail", help="ログファイルの末尾を表示")
    tail_parser.add_argument("file", help="ログファイル名")
    tail_parser.add_argument(
        "-n", "--lines", type=int, default=10, help="表示行数（デフォルト: 10）"
    )
    tail_parser.add_argument(
        "-f", "--follow", action="store_true", help="リアルタイム追跡"
    )

    # search コマンド
    search_parser = subparsers.add_parser("search", help="ログを検索")
    search_parser.add_argument("pattern", help="検索パターン")
    search_parser.add_argument("--file", help="特定のログファイルのみ検索")

    args = parser.parse_args()

    if args.command == "list":
        log_files = list_log_files()
        if not log_files:
            print("ログファイルがありません。")
        else:
            print("ログファイル一覧:")
            for log_file in log_files:
                size = log_file.stat().st_size / 1024  # KB
                mtime = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(log_file.stat().st_mtime)
                )
                print(f"  {log_file.name:<30} {size:>10.1f} KB  {mtime}")

    elif args.command == "view":
        log_path = LOG_DIR / args.file
        view_log_file(log_path, args.lines)

    elif args.command == "tail":
        log_path = LOG_DIR / args.file
        tail_log_file(log_path, args.lines, args.follow)

    elif args.command == "search":
        if args.file:
            log_files = [LOG_DIR / args.file]
        else:
            log_files = None
        search_logs(args.pattern, log_files)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
