"""
保有銘柄CSVインポート処理
"""

import csv
import logging
import sqlite3
from typing import Any

from src.config import get_db_path

logger = logging.getLogger(__name__)


def parse_number(value: str) -> float | None:
    """数値文字列をfloatに変換（カンマや不要な文字を除去）"""
    if not value or value.strip() in ["--", "---", "nan", ""]:
        return None

    # カンマと空白を除去
    cleaned = value.strip().replace(",", "").replace(" ", "")

    # "~"を含む場合は範囲の中央値を取る（例："190 ~ 200" -> 195）
    if "~" in cleaned:
        parts = cleaned.split("~")
        if len(parts) == 2:
            try:
                low = float(parts[0].strip())
                high = float(parts[1].strip())
                return (low + high) / 2
            except ValueError:
                return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_percentage(value: str) -> float | None:
    """パーセンテージ文字列をfloatに変換（%記号を除去）"""
    if not value or value.strip() in ["--", "---", "nan", ""]:
        return None

    cleaned = value.strip().replace(",", "").replace(" ", "").replace("%", "")

    try:
        return float(cleaned)
    except ValueError:
        return None


def calculate_average_price_from_profit_loss(
    current_price: float | None,
    profit_loss: float | None,
    quantity: int,
    trade_position: str,
) -> float | None:
    """評価損益から平均取得価格を逆算"""
    if current_price is None or profit_loss is None or quantity == 0:
        # 計算できない場合は現在値を返す
        return current_price if current_price is not None else 0.0

    if trade_position == "買建":
        # 買建の場合: 評価損益 = (現在値 - 建単価) * 数量
        # 建単価 = 現在値 - (評価損益 / 数量)
        return current_price - (profit_loss / quantity)
    elif trade_position == "売建":
        # 売建の場合: 評価損益 = (建単価 - 現在値) * 数量
        # 建単価 = 現在値 + (評価損益 / 数量)
        return current_price + (profit_loss / quantity)
    else:
        # 建区分が不明な場合は現在値を返す
        return current_price


def parse_spot_csv(file_path: str) -> list[dict[str, Any]]:
    """現物CSVファイルを解析"""
    holdings = []

    with open(file_path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader)  # ヘッダー行を取得

        # ヘッダーのインデックスを取得
        預り区分_idx = headers.index("預り区分")
        保有株数_idx = headers.index("保有株数")
        取得単価_idx = headers.index("取得単価")
        現在値_idx = headers.index("現在値")
        評価額_idx = headers.index("評価額")
        評価損益_idx = headers.index("評価損益")
        評価損益率_idx = headers.index("評価損益(%)")
        per_idx = headers.index("予想PER(倍)")
        pbr_idx = headers.index("実績PBR(倍)")
        配当利回り_idx = headers.index("予想配当利回り(%)")
        eps_idx = headers.index("予想EPS")
        bps_idx = headers.index("実績BPS")
        配当_idx = headers.index("予想1株配当")
        貸借_idx = headers.index("貸借区分")

        for row in reader:
            # 銘柄コードと銘柄名を探す（「銘柄」列の値から）
            code = None
            name = None

            # 最初の7つの列が銘柄関連
            for i in range(min(7, len(row))):
                value = row[i].strip()
                if value:
                    if value.isdigit() and len(value) == 4:
                        code = value
                    elif not value.isdigit() and len(value) > 1:
                        # 市場コードを除外（東P、東Sなど）
                        if not (len(value) <= 3 and value[0] in "東名札福"):
                            name = value

            if not code or not name:
                continue

            holding = {
                "code": code,
                "name": name,
                "account_type": (
                    row[預り区分_idx].strip() if 預り区分_idx < len(row) else "特定"
                ),
                "stock_type": "現物",
                "trade_position": None,
                "margin_term": None,
                "quantity": (
                    int(parse_number(row[保有株数_idx]) or 0)
                    if 保有株数_idx < len(row)
                    else 0
                ),
                "average_price": (
                    parse_number(row[取得単価_idx]) if 取得単価_idx < len(row) else None
                ),
                "current_price": (
                    parse_number(row[現在値_idx]) if 現在値_idx < len(row) else None
                ),
                "market_value": (
                    parse_number(row[評価額_idx]) if 評価額_idx < len(row) else None
                ),
                "profit_loss": (
                    parse_number(row[評価損益_idx]) if 評価損益_idx < len(row) else None
                ),
                "profit_loss_ratio": (
                    parse_percentage(row[評価損益率_idx])
                    if 評価損益率_idx < len(row)
                    else None
                ),
                "expected_per": (
                    parse_number(row[per_idx]) if per_idx < len(row) else None
                ),
                "actual_pbr": (
                    parse_number(row[pbr_idx]) if pbr_idx < len(row) else None
                ),
                "dividend_yield": (
                    parse_percentage(row[配当利回り_idx])
                    if 配当利回り_idx < len(row)
                    else None
                ),
                "expected_eps": (
                    parse_number(row[eps_idx]) if eps_idx < len(row) else None
                ),
                "actual_bps": (
                    parse_number(row[bps_idx]) if bps_idx < len(row) else None
                ),
                "expected_dividend": (
                    parse_number(row[配当_idx]) if 配当_idx < len(row) else None
                ),
                "lending_type": (
                    row[貸借_idx].strip()
                    if 貸借_idx < len(row) and row[貸借_idx].strip()
                    else None
                ),
                "acquisition_date": None,  # 現物では取得日情報なし
            }

            holdings.append(holding)

    return holdings


def parse_margin_csv(file_path: str) -> list[dict[str, Any]]:
    """信用CSVファイルを解析"""
    holdings = []

    with open(file_path, encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader)  # ヘッダー行を取得

        # ヘッダーのインデックスを取得
        建区分_idx = headers.index("建区分")
        期限_idx = headers.index("期限")
        預り区分_idx = headers.index("預り区分")
        建株数_idx = headers.index("建株数")
        現在値_idx = headers.index("現在値")
        評価額_idx = headers.index("評価額")
        評価損益_idx = headers.index("評価損益")
        評価損益率_idx = headers.index("評価損益(%)")
        per_idx = headers.index("予想PER(倍)")
        pbr_idx = headers.index("実績PBR(倍)")
        配当利回り_idx = headers.index("予想配当利回り(%)")
        eps_idx = headers.index("予想EPS")
        bps_idx = headers.index("実績BPS")
        配当_idx = headers.index("予想1株配当")
        貸借_idx = headers.index("貸借区分")
        建日_idx = headers.index("建日")

        for row in reader:
            # 銘柄コードと銘柄名を探す（「銘柄」列の値から）
            code = None
            name = None

            # 最初の7つの列が銘柄関連
            for i in range(min(7, len(row))):
                value = row[i].strip()
                if value:
                    if value.isdigit() and len(value) == 4:
                        code = value
                    elif not value.isdigit() and len(value) > 1:
                        # 市場コードを除外（東P、東Sなど）
                        if not (len(value) <= 3 and value[0] in "東名札福"):
                            name = value

            if not code or not name:
                continue

            holding = {
                "code": code,
                "name": name,
                "account_type": (
                    row[預り区分_idx].strip() if 預り区分_idx < len(row) else "特定"
                ),
                "stock_type": "信用",
                "trade_position": (
                    row[建区分_idx].strip() if 建区分_idx < len(row) else ""
                ),
                "margin_term": row[期限_idx].strip() if 期限_idx < len(row) else "",
                "quantity": (
                    int(parse_number(row[建株数_idx]) or 0)
                    if 建株数_idx < len(row)
                    else 0
                ),
                # 信用取引では建単価情報がCSVにないため、評価損益から逆算
                "average_price": calculate_average_price_from_profit_loss(
                    parse_number(row[現在値_idx]) if 現在値_idx < len(row) else None,
                    (
                        parse_number(row[評価損益_idx])
                        if 評価損益_idx < len(row)
                        else None
                    ),
                    (
                        int(parse_number(row[建株数_idx]) or 0)
                        if 建株数_idx < len(row)
                        else 0
                    ),
                    row[建区分_idx].strip() if 建区分_idx < len(row) else "",
                ),
                "current_price": (
                    parse_number(row[現在値_idx]) if 現在値_idx < len(row) else None
                ),
                "market_value": (
                    parse_number(row[評価額_idx]) if 評価額_idx < len(row) else None
                ),
                "profit_loss": (
                    parse_number(row[評価損益_idx]) if 評価損益_idx < len(row) else None
                ),
                "profit_loss_ratio": (
                    parse_percentage(row[評価損益率_idx])
                    if 評価損益率_idx < len(row)
                    else None
                ),
                "expected_per": (
                    parse_number(row[per_idx]) if per_idx < len(row) else None
                ),
                "actual_pbr": (
                    parse_number(row[pbr_idx]) if pbr_idx < len(row) else None
                ),
                "dividend_yield": (
                    parse_percentage(row[配当利回り_idx])
                    if 配当利回り_idx < len(row)
                    else None
                ),
                "expected_eps": (
                    parse_number(row[eps_idx]) if eps_idx < len(row) else None
                ),
                "actual_bps": (
                    parse_number(row[bps_idx]) if bps_idx < len(row) else None
                ),
                "expected_dividend": (
                    parse_number(row[配当_idx]) if 配当_idx < len(row) else None
                ),
                "lending_type": (
                    row[貸借_idx].strip()
                    if 貸借_idx < len(row) and row[貸借_idx].strip()
                    else None
                ),
                "acquisition_date": (
                    row[建日_idx].strip()
                    if 建日_idx < len(row) and row[建日_idx].strip()
                    else None
                ),
            }

            holdings.append(holding)

    return holdings


def import_holdings_csv(
    user_id: int, account_name: str, file_path: str, csv_type: str = "spot"
) -> int:
    """
    CSVファイルから保有銘柄をインポート

    Args:
        user_id: ユーザーID
        account_name: 口座名
        file_path: CSVファイルパス
        csv_type: CSVタイプ ("spot": 現物, "margin": 信用)

    Returns:
        インポートした件数
    """
    logger.info(
        f"Starting import: file={file_path}, csv_type={csv_type}, account={account_name}"
    )

    if csv_type == "spot":
        holdings = parse_spot_csv(file_path)
    elif csv_type == "margin":
        holdings = parse_margin_csv(file_path)
    else:
        raise ValueError(f"Unknown csv_type: {csv_type}")

    db_path = get_db_path()

    with sqlite3.connect(db_path, isolation_level=None) as conn:  # 自動コミットモード
        cursor = conn.cursor()

        # 洗い替え処理：口座名とstock_type（現物/信用）をキーに既存データを論理削除
        stock_type = "現物" if csv_type == "spot" else "信用"

        # デバッグ：削除前の銘柄リストを取得
        cursor.execute(
            """
            SELECT h.code, li.company_name as name FROM holdings h
            LEFT JOIN listed_info li ON h.code = li.code
            WHERE h.user_id = ? AND h.account_name = ? AND h.stock_type = ?
              AND h.deleted_at IS NULL
            ORDER BY h.code
            """,
            (user_id, account_name, stock_type),
        )
        before_holdings = cursor.fetchall()
        before_codes = {row[0] for row in before_holdings}
        logger.info(
            f"Before delete: {len(before_codes)} active holdings for account '{account_name}' and type '{stock_type}'"
        )

        # 物理削除に変更（洗い替え処理のため）
        cursor.execute(
            """
            DELETE FROM holdings
            WHERE user_id = ? AND account_name = ? AND stock_type = ?
              AND deleted_at IS NULL
        """,
            (user_id, account_name, stock_type),
        )

        deleted_count = cursor.rowcount
        logger.info(
            f"Physically deleted {deleted_count} existing holdings for account '{account_name}' and type '{stock_type}'"
        )

        # 削除を即座にコミット
        conn.commit()

        # デバッグ：削除後の件数を確認
        cursor.execute(
            """
            SELECT COUNT(*) FROM holdings
            WHERE user_id = ? AND account_name = ? AND stock_type = ?
            """,
            (user_id, account_name, stock_type),
        )
        after_count = cursor.fetchone()[0]
        logger.info(f"After delete: {after_count} holdings remain (should be 0)")

        imported_count = 0
        for holding in holdings:
            try:
                # 新しいレコードを挿入
                cursor.execute(
                    """
                INSERT INTO holdings (
                    user_id, code, account_name, account_type, stock_type,
                    trade_position, margin_term, quantity, average_price, current_price,
                    market_value, profit_loss, profit_loss_ratio, expected_per,
                    actual_pbr, dividend_yield, expected_eps, actual_bps,
                    expected_dividend, lending_type, acquisition_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                    (
                        user_id,
                        holding["code"],
                        account_name,
                        holding["account_type"],
                        holding["stock_type"],
                        holding["trade_position"],
                        holding["margin_term"],
                        holding["quantity"],
                        holding["average_price"],
                        holding["current_price"],
                        holding["market_value"],
                        holding["profit_loss"],
                        holding["profit_loss_ratio"],
                        holding["expected_per"],
                        holding["actual_pbr"],
                        holding["dividend_yield"],
                        holding["expected_eps"],
                        holding["actual_bps"],
                        holding["expected_dividend"],
                        holding["lending_type"],
                        holding["acquisition_date"],
                    ),
                )

                imported_count += 1

            except sqlite3.IntegrityError as e:
                logger.error(
                    f"Integrity error for holding {holding['code']} - {holding['name']}: {e}"
                )
                logger.error(
                    f"Details: account_type={holding['account_type']}, stock_type={holding['stock_type']}, trade_position={holding['trade_position']}"
                )
                # エラーが発生した場合はスキップして続行
                continue
            except Exception as e:
                logger.error(f"Unexpected error for holding {holding['code']}: {e}")
                raise

        conn.commit()

        # デバッグ：CSVに含まれていなかった銘柄を確認
        csv_codes = {holding["code"] for holding in holdings}
        deleted_codes = before_codes - csv_codes
        if deleted_codes:
            logger.info(
                f"Following codes were deleted (not in CSV): {sorted(deleted_codes)}"
            )

        logger.info(
            f"Imported {imported_count} holdings from {file_path} (skipped {len(holdings) - imported_count} duplicates)"
        )

    return imported_count
