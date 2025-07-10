"""エンコーディング検出機能"""

import chardet

from src.utils.logging_config import get_logger

logger = get_logger("portfolio.csv_parser.encodings")


def detect_encoding(content: bytes) -> str:
    """
    バイト列のエンコーディングを検出

    Args:
        content: 検出対象のバイト列

    Returns:
        検出されたエンコーディング名
    """
    # BOMをチェック
    if content.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    # chardetで検出
    result = chardet.detect(content)
    encoding = result["encoding"]

    # 日本語のエンコーディングを優先
    if encoding and encoding.lower() in [
        "shift_jis",
        "cp932",
        "euc-jp",
        "iso-2022-jp",
    ]:
        return str(encoding)
    elif encoding and "utf" in encoding.lower():
        return str(encoding)
    else:
        # デフォルトでShift-JISとUTF-8を試す
        return "shift_jis"


def decode_content(content: bytes) -> str:
    """
    バイト列をデコード

    Args:
        content: デコード対象のバイト列

    Returns:
        デコードされた文字列

    Raises:
        ValueError: デコードに失敗した場合
    """
    if isinstance(content, str):
        return content

    encoding = detect_encoding(content)
    logger.info(f"検出されたエンコーディング: {encoding}")

    try:
        decoded = content.decode(encoding)
    except UnicodeDecodeError:
        # フォールバック
        for enc in ["utf-8-sig", "shift_jis", "cp932", "utf-8"]:
            try:
                decoded = content.decode(enc)
                logger.info(f"フォールバックエンコーディング {enc} でデコード成功")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("CSVファイルのエンコーディングを判定できません")

    # BOMを除去
    if decoded.startswith("\ufeff"):
        decoded = decoded[1:]

    return decoded
