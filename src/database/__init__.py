"""データベースアダプター層の実装

SQLiteとPostgreSQLの両方をサポートするための抽象化層を提供します。
"""

from .factory import get_database_adapter

__all__ = ["get_database_adapter"]
