#!/usr/bin/env python
"""
テクニカルスクリーニングの結果検証スクリプト
通常版と高速版の結果を比較して一致することを確認
"""
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import DB_PATH


def compare_results(date_str, db_path=DB_PATH):
    """指定日の結果を比較"""
    conn = sqlite3.connect(db_path)

    # 指定日のデータを取得
    query = """
        SELECT code, signal_ma, signal_rsi, signal_adx, signal_bb, signal_macd,
               signal_ma_short, signal_rsi_short, signal_bb_short, signal_macd_short,
               signals_count, signals_short_count, signals_overheating, signals_oversold,
               signals_first, signals_short_first
        FROM technical_indicators
        WHERE signal_date = ?
        ORDER BY code
    """

    df = pd.read_sql(query, conn, params=[date_str])
    conn.close()

    return df


def verify_results(test_date=None):
    """結果の検証"""
    if not test_date:
        test_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"検証日: {test_date}")

    # データベースをバックアップ
    import shutil

    backup_path = f"{DB_PATH}.backup"
    shutil.copy2(DB_PATH, backup_path)
    print(f"データベースをバックアップしました: {backup_path}")

    # 通常版を実行
    print("\n通常版を実行中...")
    import subprocess
    import time

    start_time = time.time()
    result = subprocess.run(
        [
            sys.executable,
            "screening/screen_technical.py",
            "indicators",
            "--as-of",
            test_date,
            "--lookback",
            "0",
        ],
        capture_output=True,
        text=True,
    )
    normal_time = time.time() - start_time

    if result.returncode != 0:
        print(f"通常版でエラー: {result.stderr}")
        return False

    print(f"通常版の実行時間: {normal_time:.2f}秒")

    # 通常版の結果を保存
    normal_results = compare_results(test_date)
    print(f"通常版の結果: {len(normal_results)} レコード")

    # 高速版を実行
    print("\n高速版を実行中...")
    start_time = time.time()
    result = subprocess.run(
        [
            sys.executable,
            "screening/screen_technical_fast.py",
            "indicators",
            "--as-of",
            test_date,
            "--lookback",
            "0",
        ],
        capture_output=True,
        text=True,
    )
    fast_time = time.time() - start_time

    if result.returncode != 0:
        print(f"高速版でエラー: {result.stderr}")
        return False

    print(f"高速版の実行時間: {fast_time:.2f}秒")

    # 高速版の結果を取得
    fast_results = compare_results(test_date)
    print(f"高速版の結果: {len(fast_results)} レコード")

    # 結果を比較
    print("\n結果の比較...")

    # レコード数の確認
    if len(normal_results) != len(fast_results):
        print(
            f"❌ レコード数が一致しません: 通常版={len(normal_results)}, 高速版={len(fast_results)}"
        )
        return False

    # データフレームの内容を比較
    if normal_results.empty and fast_results.empty:
        print("✓ 両方とも結果が空です")
        return True

    # codeでソートして比較
    normal_results = normal_results.sort_values("code").reset_index(drop=True)
    fast_results = fast_results.sort_values("code").reset_index(drop=True)

    # 各カラムを比較
    differences = []
    for col in normal_results.columns:
        if col == "code":
            continue

        # 数値の比較（float32の精度を考慮）
        if normal_results[col].dtype in ["float64", "float32", "int64", "int32"]:
            # 絶対誤差が0.001以下なら一致とみなす
            diff_mask = (normal_results[col] - fast_results[col]).abs() > 0.001
        else:
            diff_mask = normal_results[col] != fast_results[col]

        if diff_mask.any():
            diff_count = diff_mask.sum()
            differences.append(f"{col}: {diff_count} 件の差異")

            # 差異の詳細を表示（最初の5件）
            diff_indices = diff_mask[diff_mask].index[:5]
            for idx in diff_indices:
                code = normal_results.loc[idx, "code"]
                normal_val = normal_results.loc[idx, col]
                fast_val = fast_results.loc[idx, col]
                print(f"  {code}: 通常版={normal_val}, 高速版={fast_val}")

    if differences:
        print("❌ 差異が見つかりました:")
        for diff in differences:
            print(f"  - {diff}")
        return False

    print("✓ 結果が完全に一致しました")

    # パフォーマンス改善率
    speedup = normal_time / fast_time
    print(
        f"\nパフォーマンス改善: {speedup:.1f}倍速 ({normal_time:.2f}秒 → {fast_time:.2f}秒)"
    )

    # データベースを復元
    shutil.copy2(backup_path, DB_PATH)
    print("\nデータベースを復元しました")

    return True


if __name__ == "__main__":
    # 昨日の日付で検証
    verify_results()
