"""カスタム例外クラス定義"""


class SwingTradeError(Exception):
    """基底例外クラス"""
    pass


class APIError(SwingTradeError):
    """API関連のエラー"""
    pass


class DataError(SwingTradeError):
    """データ処理関連のエラー"""
    pass


class ConfigError(SwingTradeError):
    """設定関連のエラー"""
    pass


class DatabaseError(SwingTradeError):
    """データベース関連のエラー"""
    pass


class ValidationError(SwingTradeError):
    """バリデーションエラー"""
    pass