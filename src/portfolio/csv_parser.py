"""SBI証券のCSVファイル解析モジュール"""

import csv
import io
import re
from datetime import datetime
from typing import Any

import chardet

from src.utils.logging_config import get_logger

logger = get_logger("portfolio.csv_parser")


class SBICSVParser:
    """SBI証券のCSVファイルパーサー"""

    @staticmethod
    def detect_encoding(content: bytes) -> str:
        """バイト列のエンコーディングを検出"""
        # BOMをチェック
        if content.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"

        # chardetで検出
        result = chardet.detect(content)
        encoding = result["encoding"]

        # 日本語のエンコーディングを優先
        if encoding and encoding.lower() in ["shift_jis", "euc-jp", "iso-2022-jp"]:
            return str(encoding)
        elif encoding and "utf" in encoding.lower():
            return str(encoding)
        else:
            # デフォルトでShift-JISとUTF-8を試す
            return "shift_jis"

    @staticmethod
    def parse_holdings_csv(csv_content: str | bytes) -> list[dict[str, Any]]:
        """
        保有銘柄CSVを解析

        複数のSBI証券CSVフォーマットに対応:
        1. 標準形式: 銘柄コード,銘柄名,市場,保有数量,取得単価,現在値,評価損益,評価損益率(%),...
        2. 保有証券_現物形式: 複雑なヘッダー構造、位置ベースの列
        3. SaveFile形式: セクション分割された形式

        Args:
            csv_content: CSVファイルの内容（文字列またはバイト列）

        Returns:
            保有銘柄情報のリスト
        """
        # バイト列の場合はエンコーディングを検出してデコード
        if isinstance(csv_content, bytes):
            csv_bytes = csv_content  # 型アサーションのために変数に代入
            encoding = SBICSVParser.detect_encoding(csv_bytes)
            logger.info(f"検出されたエンコーディング: {encoding}")
            try:
                csv_content = csv_bytes.decode(encoding)
            except UnicodeDecodeError:
                logger.warning(
                    f"エンコーディング {encoding} でのデコードに失敗、フォールバックを試行"
                )
                # フォールバック
                for enc in ["utf-8-sig", "shift_jis", "cp932", "utf-8"]:
                    try:
                        csv_content = csv_bytes.decode(enc)
                        logger.info(
                            f"フォールバックエンコーディング {enc} でデコード成功"
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    logger.error("全てのエンコーディング試行が失敗しました")
                    raise ValueError("CSVファイルのエンコーディングを判定できません")

        # BOMを除去
        if csv_content.startswith("\ufeff"):
            csv_content = csv_content[1:]

        # フォーマットを判定
        lines = csv_content.strip().split("\n")
        if not lines:
            return []

        # 最初の数行でフォーマットを判定
        header_line = lines[0] if lines else ""

        # SaveFile形式の判定（セクション分割された形式）
        if (
            "株式（特定預り）" in csv_content
            or "株式（NISA預り）" in csv_content
            or "株式（旧NISA預り）" in csv_content
            or "保有証券一覧" in csv_content
        ):
            return SBICSVParser._parse_sbi_savefile_format(csv_content)

        # 保有証券_現物形式の判定（重複する「銘柄」ヘッダー）
        elif header_line.count("銘柄") >= 4:
            return SBICSVParser._parse_sbi_detailed_format(csv_content)

        # 標準形式
        else:
            return SBICSVParser._parse_standard_format(csv_content)

    @staticmethod
    def _parse_standard_format(csv_content: str) -> list[dict[str, Any]]:
        """標準形式のCSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            # CSVを読み込み
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            rows = list(csv_reader)

            # ヘッダーの検証
            if not rows:
                return holdings

            # 必須カラムの存在確認
            if rows:
                first_row = rows[0]
                if not any(key in first_row for key in ["銘柄コード", "コード"]):
                    raise ValueError("銘柄コードの列が見つかりません")

            for row in rows:
                # カラム名のバリエーションに対応
                code = row.get("銘柄コード") or row.get("コード") or ""
                code = SBICSVParser._normalize_code(code)

                if not code:
                    continue

                holding = {
                    "code": code,
                    "name": row.get("銘柄名", "").strip(),
                    "quantity": SBICSVParser._parse_number(
                        row.get("保有数量") or row.get("保有株数") or row.get("数量")
                    ),
                    "average_price": SBICSVParser._parse_number(
                        row.get("取得単価") or row.get("平均取得単価")
                    ),
                    "current_price": SBICSVParser._parse_number(
                        row.get("現在値") or row.get("株価")
                    ),
                    "market_value": SBICSVParser._parse_number(
                        row.get("評価額") or row.get("時価評価額")
                    ),
                    "profit_loss": SBICSVParser._parse_number(
                        row.get("評価損益") or row.get("損益")
                    ),
                    "profit_loss_ratio": SBICSVParser._parse_number(
                        row.get("評価損益率(%)") or row.get("損益率(%)")
                    ),
                }

                # 必須フィールドのチェック
                if holding["code"] and holding["quantity"] is not None:
                    holdings.append(holding)
                    logger.debug(
                        f"保有銘柄解析: {holding['code']} - {holding['quantity']}株"
                    )

            logger.info(f"保有銘柄CSV解析完了（標準形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（標準形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def _parse_sbi_detailed_format(csv_content: str) -> list[dict[str, Any]]:
        """保有証券_現物形式のCSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")
            if len(lines) < 2:
                return holdings

            # データ行をパース（ヘッダー行をスキップ）
            for line in lines[1:]:
                if not line.strip():
                    continue

                # CSVとして解析
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if not row or len(row) < 15:
                    continue

                # 位置ベースで値を取得
                code = SBICSVParser._normalize_code(row[4])  # 5列目
                if not code:
                    continue

                holding = {
                    "code": code,
                    "name": row[5].strip() if len(row) > 5 else "",  # 6列目
                    "quantity": (
                        SBICSVParser._parse_number(row[8]) if len(row) > 8 else None
                    ),  # 9列目
                    "average_price": (
                        SBICSVParser._parse_number(row[10]) if len(row) > 10 else None
                    ),  # 11列目
                    "current_price": (
                        SBICSVParser._parse_number(row[11]) if len(row) > 11 else None
                    ),  # 12列目
                    "market_value": (
                        SBICSVParser._parse_number(row[15]) if len(row) > 15 else None
                    ),  # 16列目（評価額）
                    "profit_loss": (
                        SBICSVParser._parse_number(row[13]) if len(row) > 13 else None
                    ),  # 14列目
                    "profit_loss_ratio": (
                        SBICSVParser._parse_number(row[14]) if len(row) > 14 else None
                    ),  # 15列目
                }

                # 必須フィールドのチェック
                if holding["code"] and holding["quantity"] is not None:
                    holdings.append(holding)
                    logger.debug(
                        f"保有銘柄解析（詳細形式）: {holding['code']} - {holding['quantity']}株"
                    )

            logger.info(f"保有銘柄CSV解析完了（詳細形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（詳細形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def _parse_sbi_savefile_format(csv_content: str) -> list[dict[str, Any]]:
        """SaveFile形式のCSVを解析"""
        holdings: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")
            current_section = None

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # セクションの判定
                if ("特定預り" in line or "特定口座" in line) and "合計" not in line:
                    current_section = "特定口座"
                    continue
                elif (
                    ("NISA口座" in line or "NISA預り" in line)
                    and "旧" not in line
                    and "合計" not in line
                ):
                    current_section = "NISA口座"
                    continue
                elif (
                    "旧NISA口座" in line or "旧NISA預り" in line
                ) and "合計" not in line:
                    current_section = "旧NISA口座"
                    continue
                elif "投資信託" in line or "ファンド名" in line:
                    # 投資信託セクションはスキップ
                    break
                elif (
                    "株式" in line
                    and ("特定" in line or "NISA" in line)
                    and "合計" not in line
                ):
                    # 「株式（特定預り）」のようなパターンも処理
                    if "特定" in line:
                        current_section = "特定口座"
                    elif "旧NISA" in line:
                        current_section = "旧NISA口座"
                    elif "NISA" in line:
                        current_section = "NISA口座"
                    continue

                # CSVデータ行の解析
                if line.startswith('"') and current_section:
                    reader = csv.reader(io.StringIO(line))
                    row = next(reader, None)
                    if not row or len(row) < 8:
                        continue

                    code = SBICSVParser._normalize_code(row[0])
                    if not code:
                        continue

                    holding = {
                        "code": code,
                        "name": row[1].strip() if len(row) > 1 else "",
                        "quantity": (
                            SBICSVParser._parse_number(row[2]) if len(row) > 2 else None
                        ),
                        "average_price": (
                            SBICSVParser._parse_number(row[4]) if len(row) > 4 else None
                        ),
                        "current_price": (
                            SBICSVParser._parse_number(row[5]) if len(row) > 5 else None
                        ),
                        "market_value": (
                            SBICSVParser._parse_number(row[7]) if len(row) > 7 else None
                        ),
                        "profit_loss": (
                            SBICSVParser._parse_number(row[8]) if len(row) > 8 else None
                        ),
                        "profit_loss_ratio": None,  # SaveFile形式には評価損益率がない
                    }

                    # 必須フィールドのチェック
                    if holding["code"] and holding["quantity"] is not None:
                        holdings.append(holding)
                        logger.debug(
                            f"保有銘柄解析（SaveFile形式）: {holding['code']} - {holding['quantity']}株 ({current_section})"
                        )

            logger.info(f"保有銘柄CSV解析完了（SaveFile形式）: {len(holdings)}銘柄")
            return holdings

        except Exception as e:
            logger.error(f"保有銘柄CSV解析エラー（SaveFile形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def parse_transactions_csv(csv_content: str | bytes) -> list[dict[str, Any]]:
        """
        取引履歴CSVを解析

        複数のSBI証券取引履歴CSVフォーマットに対応:
        1. 標準形式: 約定日,銘柄コード,銘柄名,売買区分,数量,約定単価,手数料,税金,受渡金額,...
        2. SaveFile形式: 日時,銘柄名,銘柄コード,市場,信用・現物,売買,預り,状態,株数,単価,...
        3. 注文一覧形式: 銘柄（コード）,銘柄（名前）,銘柄（市場）,取引区分,期限,預り区分,約定日,...

        Args:
            csv_content: CSVファイルの内容（文字列またはバイト列）

        Returns:
            取引履歴情報のリスト
        """
        # バイト列の場合はエンコーディングを検出してデコード
        if isinstance(csv_content, bytes):
            csv_bytes = csv_content  # 型アサーションのために変数に代入
            encoding = SBICSVParser.detect_encoding(csv_bytes)
            logger.info(f"検出されたエンコーディング: {encoding}")
            try:
                csv_content = csv_bytes.decode(encoding)
            except UnicodeDecodeError:
                logger.warning(
                    f"エンコーディング {encoding} でのデコードに失敗、フォールバックを試行"
                )
                # フォールバック
                for enc in ["utf-8-sig", "shift_jis", "cp932", "utf-8"]:
                    try:
                        csv_content = csv_bytes.decode(enc)
                        logger.info(
                            f"フォールバックエンコーディング {enc} でデコード成功"
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    logger.error("全てのエンコーディング試行が失敗しました")
                    raise ValueError("CSVファイルのエンコーディングを判定できません")

        # BOMを除去
        if csv_content.startswith("\ufeff"):
            csv_content = csv_content[1:]

        # フォーマットを判定
        lines = csv_content.strip().split("\n")
        if not lines:
            return []

        # 最初の数行でフォーマットを判定
        header_line = lines[0] if lines else ""

        # SaveFile形式の判定（「約定履歴照会」または「約定日,銘柄,銘柄コード」のパターン）
        if "約定履歴照会" in csv_content or (
            "約定日" in csv_content
            and "銘柄" in csv_content
            and "銘柄コード" in csv_content
            and "取引" in csv_content
        ):
            return SBICSVParser._parse_transactions_savefile_format(csv_content)

        # 注文一覧形式の判定（「銘柄」が3列繰り返される）
        elif header_line.count("銘柄") >= 3:
            return SBICSVParser._parse_transactions_order_list_format(csv_content)

        # 標準形式
        else:
            return SBICSVParser._parse_transactions_standard_format(csv_content)

    @staticmethod
    def _parse_transactions_standard_format(csv_content: str) -> list[dict[str, Any]]:
        """標準形式の取引履歴CSVを解析"""
        transactions: list[dict[str, Any]] = []

        try:
            # CSVを読み込み
            csv_reader = csv.DictReader(io.StringIO(csv_content))

            for row in csv_reader:
                # 銘柄コードの取得と正規化
                code = row.get("銘柄コード") or row.get("コード") or ""
                code = SBICSVParser._normalize_code(code)

                if not code:
                    continue

                # 売買区分の判定
                trade_type = row.get("売買区分") or row.get("取引区分") or ""
                transaction_type = SBICSVParser._parse_transaction_type(trade_type)

                # 約定日の解析
                trade_date = SBICSVParser._parse_date(
                    row.get("約定日") or row.get("取引日")
                )

                if not trade_date:
                    continue

                transaction = {
                    "code": code,
                    "name": row.get("銘柄名", "").strip(),
                    "transaction_date": trade_date,
                    "transaction_type": transaction_type,
                    "quantity": SBICSVParser._parse_number(
                        row.get("数量") or row.get("株数")
                    ),
                    "price": SBICSVParser._parse_number(
                        row.get("約定単価") or row.get("単価")
                    ),
                    "commission": SBICSVParser._parse_number(
                        row.get("手数料") or row.get("委託手数料"), default=0
                    ),
                    "tax": SBICSVParser._parse_number(
                        row.get("税金") or row.get("消費税"), default=0
                    ),
                    "total_amount": SBICSVParser._parse_number(
                        row.get("受渡金額") or row.get("約定代金")
                    ),
                    "remarks": row.get("備考", "").strip(),
                }

                # 必須フィールドのチェック
                if (
                    transaction["code"]
                    and transaction["transaction_date"]
                    and transaction["quantity"] is not None
                    and transaction["price"] is not None
                ):
                    transactions.append(transaction)
                    logger.debug(
                        f"取引解析: {transaction['transaction_date']} "
                        f"{transaction['code']} {transaction['transaction_type']} "
                        f"{transaction['quantity']}株"
                    )

            # 日付順にソート（古い順）
            transactions.sort(key=lambda x: x["transaction_date"] or "")

            logger.info(f"取引履歴CSV解析完了（標準形式）: {len(transactions)}件")
            return transactions

        except Exception as e:
            logger.error(f"取引履歴CSV解析エラー（標準形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def _parse_transactions_savefile_format(csv_content: str) -> list[dict[str, Any]]:
        """SaveFile形式（約定履歴照会）の取引履歴CSVを解析"""
        transactions: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")

            # ヘッダー行を探す（「約定日,銘柄,銘柄コード」のパターン）
            header_index = -1
            for i, line in enumerate(lines):
                if "約定日" in line and "銘柄" in line and "銘柄コード" in line:
                    header_index = i
                    break

            if header_index == -1:
                raise ValueError("取引履歴のヘッダーが見つかりません")

            # データ行をパース
            for line in lines[header_index + 1 :]:
                if not line.strip() or line.startswith("(注)"):
                    continue

                # CSVとして解析
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if not row or len(row) < 10:
                    continue

                # 取引区分の判定（「信用新規売」「信用新規買」「信用返済売」「信用返済買」など）
                trade_type_str = row[4].strip() if len(row) > 4 else ""

                # 売買の判定
                if "新規買" in trade_type_str or "買" in trade_type_str:
                    transaction_type = "buy"
                elif (
                    "返済売" in trade_type_str
                    or "新規売" in trade_type_str
                    or "売" in trade_type_str
                ):
                    transaction_type = "sell"
                else:
                    # 不明な取引タイプはスキップ
                    continue

                # 信用取引か現物取引かを判定
                is_margin = "信用" in trade_type_str

                # 約定日の解析
                trade_date = SBICSVParser._parse_date(row[0]) if len(row) > 0 else None
                if not trade_date:
                    continue

                transaction = {
                    "code": (
                        SBICSVParser._normalize_code(row[2]) if len(row) > 2 else ""
                    ),
                    "name": row[1].strip() if len(row) > 1 else "",
                    "transaction_date": trade_date,
                    "transaction_type": transaction_type,
                    "quantity": (
                        SBICSVParser._parse_number(row[8]) if len(row) > 8 else None
                    ),
                    "price": (
                        SBICSVParser._parse_number(row[9]) if len(row) > 9 else None
                    ),
                    "commission": (
                        SBICSVParser._parse_number(row[10], default=0)
                        if len(row) > 10
                        else 0
                    ),
                    "tax": (
                        SBICSVParser._parse_number(row[11], default=0)
                        if len(row) > 11
                        else 0
                    ),
                    "total_amount": (
                        SBICSVParser._parse_number(row[13]) if len(row) > 13 else None
                    ),
                    "remarks": "信用" if is_margin else "",
                }

                # 必須フィールドのチェック
                if (
                    transaction["code"]
                    and transaction["transaction_date"]
                    and transaction["quantity"] is not None
                    and transaction["price"] is not None
                ):
                    transactions.append(transaction)
                    logger.debug(
                        f"取引解析（SaveFile形式）: {transaction['transaction_date']} "
                        f"{transaction['code']} {transaction['transaction_type']} "
                        f"{transaction['quantity']}株"
                    )

            # 日付順にソート（古い順）
            transactions.sort(key=lambda x: x["transaction_date"] or "")

            logger.info(f"取引履歴CSV解析完了（SaveFile形式）: {len(transactions)}件")
            return transactions

        except Exception as e:
            logger.error(f"取引履歴CSV解析エラー（SaveFile形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def _parse_transactions_order_list_format(csv_content: str) -> list[dict[str, Any]]:
        """注文一覧形式の取引履歴CSVを解析"""
        transactions: list[dict[str, Any]] = []

        try:
            lines = csv_content.strip().split("\n")
            if len(lines) < 2:
                return transactions

            # データ行をパース（ヘッダー行をスキップ）
            for line in lines[1:]:
                if not line.strip():
                    continue

                # CSVとして解析
                reader = csv.reader(io.StringIO(line))
                row = next(reader, None)
                if not row or len(row) < 9:
                    continue

                # 銘柄コード（最初の列）
                code = SBICSVParser._normalize_code(row[0])
                if not code:
                    continue

                # 取引区分から売買を判定
                trade_type_str = row[3].strip() if len(row) > 3 else ""

                # 信用取引か現物取引かを判定
                is_margin = "信用" in trade_type_str

                # 売買の判定
                if "現物売" in trade_type_str or "信用返済売" in trade_type_str:
                    transaction_type = "sell"
                elif "現物買" in trade_type_str or "信用新規買" in trade_type_str:
                    transaction_type = "buy"
                else:
                    # 不明な取引タイプはスキップ
                    logger.warning(f"不明な取引タイプ: {trade_type_str}")
                    continue

                # 約定日の解析
                trade_date = SBICSVParser._parse_date(row[6]) if len(row) > 6 else None
                if not trade_date:
                    continue

                transaction = {
                    "code": code,
                    "name": row[1].strip() if len(row) > 1 else "",
                    "transaction_date": trade_date,
                    "transaction_type": transaction_type,
                    "quantity": (
                        SBICSVParser._parse_number(row[8]) if len(row) > 8 else None
                    ),
                    "price": (
                        SBICSVParser._parse_number(row[9]) if len(row) > 9 else None
                    ),
                    "commission": (
                        SBICSVParser._parse_number(row[10], default=0)
                        if len(row) > 10
                        else 0
                    ),
                    "tax": (
                        SBICSVParser._parse_number(row[11], default=0)
                        if len(row) > 11
                        else 0
                    ),
                    "total_amount": (
                        SBICSVParser._parse_number(row[12]) if len(row) > 12 else None
                    ),
                    "remarks": "信用" if is_margin else "",
                }

                # 必須フィールドのチェック
                if (
                    transaction["code"]
                    and transaction["transaction_date"]
                    and transaction["quantity"] is not None
                    and transaction["price"] is not None
                ):
                    transactions.append(transaction)
                    logger.debug(
                        f"取引解析（注文一覧形式）: {transaction['transaction_date']} "
                        f"{transaction['code']} {transaction['transaction_type']} "
                        f"{transaction['quantity']}株"
                    )

            # 日付順にソート（古い順）
            transactions.sort(key=lambda x: x["transaction_date"] or "")

            logger.info(f"取引履歴CSV解析完了（注文一覧形式）: {len(transactions)}件")
            return transactions

        except Exception as e:
            logger.error(f"取引履歴CSV解析エラー（注文一覧形式）: {e}")
            raise ValueError(f"CSVファイルの解析に失敗しました: {str(e)}") from e

    @staticmethod
    def _normalize_code(code: str) -> str:
        """銘柄コードを4桁に正規化"""
        if not code:
            return ""

        # 数字のみを抽出
        code_digits = re.findall(r"\d+", str(code))
        if code_digits:
            # 4桁にパディング
            return str(code_digits[0]).zfill(4)
        return ""

    @staticmethod
    def _parse_number(value: Any, default: float | None = None) -> float | None:
        """数値を解析（カンマ、マイナス記号対応）"""
        if value is None or value == "":
            return default

        try:
            # 文字列に変換
            value_str = str(value).strip()

            # 「--」や「--%」は空値として扱う
            if value_str == "--" or value_str == "--%":
                return default

            # カンマを除去
            value_str = value_str.replace(",", "")

            # 括弧付きマイナス値の処理
            if value_str.startswith("(") and value_str.endswith(")"):
                value_str = "-" + value_str[1:-1]

            # △や▲をマイナスに変換
            value_str = value_str.replace("△", "-").replace("▲", "-")

            # 「+」記号を除去
            value_str = value_str.replace("+", "")

            # %記号を除去
            value_str = value_str.replace("%", "")

            return float(value_str)
        except (ValueError, AttributeError):
            logger.warning(f"数値解析エラー: {value}")
            return default

    @staticmethod
    def _parse_date(date_str: Any) -> str | None:
        """日付文字列を解析してYYYY-MM-DD形式に変換"""
        if not date_str:
            return None

        date_str = str(date_str).strip()

        # 一般的な日付フォーマットを試行
        date_formats = [
            "%Y/%m/%d",
            "%Y-%m-%d",
            "%Y年%m月%d日",
            "%y/%m/%d",
            "%y-%m-%d",
        ]

        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue

        logger.warning(f"日付解析エラー: {date_str}")
        return None

    @staticmethod
    def _parse_transaction_type(trade_type: str) -> str:
        """売買区分を解析してbuy/sellに変換"""
        if not trade_type:
            return "buy"

        trade_type = trade_type.strip().lower()

        # 売却を示すキーワード
        sell_keywords = ["売", "売却", "売付", "sell"]
        for keyword in sell_keywords:
            if keyword in trade_type:
                return "sell"

        # デフォルトは買付
        return "buy"
