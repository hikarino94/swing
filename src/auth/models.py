"""認証関連のデータモデル"""

import sqlite3
from typing import Optional

from src.config import DB_PATH
from src.utils.logging_config import get_logger

logger = get_logger("auth.models")


class User:
    """ユーザーモデル"""

    def __init__(
        self,
        id: int | None = None,
        username: str = "",
        email: str = "",
        password_hash: str = "",  # nosec: B107
    ):
        self.id = id
        self.username = username
        self.email = email
        self.password_hash = password_hash
        self.created_at = None
        self.updated_at = None

    @classmethod
    def find_by_username(cls, username: str) -> Optional["User"]:
        """ユーザー名でユーザーを検索"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, username, email, password_hash, created_at, updated_at
                FROM users WHERE username = ?
            """,
                (username,),
            )
            row = cursor.fetchone()
            if row:
                user = cls(
                    id=row[0], username=row[1], email=row[2], password_hash=row[3]
                )
                user.created_at = row[4]
                user.updated_at = row[5]
                return user
            return None
        finally:
            conn.close()

    @classmethod
    def find_by_email(cls, email: str) -> Optional["User"]:
        """メールアドレスでユーザーを検索"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, username, email, password_hash, created_at, updated_at
                FROM users WHERE email = ?
            """,
                (email,),
            )
            row = cursor.fetchone()
            if row:
                user = cls(
                    id=row[0], username=row[1], email=row[2], password_hash=row[3]
                )
                user.created_at = row[4]
                user.updated_at = row[5]
                return user
            return None
        finally:
            conn.close()

    @classmethod
    def find_by_id(cls, user_id: int) -> Optional["User"]:
        """IDでユーザーを検索"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, username, email, password_hash, created_at, updated_at
                FROM users WHERE id = ?
            """,
                (user_id,),
            )
            row = cursor.fetchone()
            if row:
                user = cls(
                    id=row[0], username=row[1], email=row[2], password_hash=row[3]
                )
                user.created_at = row[4]
                user.updated_at = row[5]
                return user
            return None
        finally:
            conn.close()

    def save(self) -> bool:
        """ユーザー情報を保存（新規作成または更新）"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            if self.id is None:
                # 新規作成
                cursor.execute(
                    """
                    INSERT INTO users (username, email, password_hash)
                    VALUES (?, ?, ?)
                """,
                    (self.username, self.email, self.password_hash),
                )
                self.id = cursor.lastrowid
            else:
                # 更新
                cursor.execute(
                    """
                    UPDATE users SET username = ?, email = ?, password_hash = ?,
                    updated_at = datetime('now')
                    WHERE id = ?
                """,
                    (self.username, self.email, self.password_hash, self.id),
                )

            conn.commit()
            logger.info(f"ユーザー保存成功: {self.username}")
            return True
        except sqlite3.IntegrityError as e:
            logger.error(f"ユーザー保存エラー: {e}")
            return False
        finally:
            conn.close()


class Session:
    """セッションモデル"""

    def __init__(self, session_id: str, user_id: int, expires_at: str):
        self.id = session_id
        self.user_id = user_id
        self.expires_at = expires_at
        self.created_at = None
        self.remember_me = False  # Remember Meフラグ

    @classmethod
    def find_by_id(cls, session_id: str) -> Optional["Session"]:
        """セッションIDでセッションを検索"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            from datetime import datetime

            current_time = datetime.now().isoformat()
            cursor.execute(
                """
                SELECT id, user_id, expires_at, created_at, remember_me
                FROM sessions
                WHERE id = ? AND expires_at > ?
            """,
                (session_id, current_time),
            )
            row = cursor.fetchone()
            if row:
                session = cls(session_id=row[0], user_id=row[1], expires_at=row[2])
                session.created_at = row[3]
                session.remember_me = bool(row[4]) if len(row) > 4 else False
                return session
            return None
        finally:
            conn.close()

    def save(self) -> bool:
        """セッション情報を保存"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            # テーブルのremember_meカラムが存在するか確認
            cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'"
            )
            table_sql = cursor.fetchone()[0]

            if "remember_me" in table_sql:
                cursor.execute(
                    """
                    INSERT INTO sessions (id, user_id, expires_at, remember_me)
                    VALUES (?, ?, ?, ?)
                """,
                    (self.id, self.user_id, self.expires_at, int(self.remember_me)),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO sessions (id, user_id, expires_at)
                    VALUES (?, ?, ?)
                """,
                    (self.id, self.user_id, self.expires_at),
                )
            conn.commit()
            logger.info(f"セッション保存成功: {self.id}")
            return True
        except sqlite3.Error as e:
            logger.error(f"セッション保存エラー: {e}")
            return False
        finally:
            conn.close()

    def delete(self) -> bool:
        """セッションを削除"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM sessions WHERE id = ?", (self.id,))
            conn.commit()
            logger.info(f"セッション削除成功: {self.id}")
            return True
        except sqlite3.Error as e:
            logger.error(f"セッション削除エラー: {e}")
            return False
        finally:
            conn.close()

    @classmethod
    def cleanup_expired(cls) -> int:
        """期限切れセッションをクリーンアップ"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            from datetime import datetime

            current_time = datetime.now().isoformat()
            cursor.execute(
                "DELETE FROM sessions WHERE expires_at <= ?", (current_time,)
            )
            deleted_count = cursor.rowcount
            conn.commit()
            if deleted_count > 0:
                logger.info(f"期限切れセッション削除: {deleted_count}件")
            return deleted_count
        except sqlite3.Error as e:
            logger.error(f"セッションクリーンアップエラー: {e}")
            return 0
        finally:
            conn.close()
