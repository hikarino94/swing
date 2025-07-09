"""シンプルなメモリキャッシュユーティリティ"""

import hashlib
import json
import time
from functools import wraps
from typing import Any


class MemoryCache:
    """インメモリキャッシュ実装"""

    def __init__(self):
        self._cache: dict[str, tuple[Any, float]] = {}
        self._default_ttl = 300  # デフォルト5分

    def get(self, key: str) -> Any | None:
        """キャッシュから値を取得"""
        if key in self._cache:
            value, expire_time = self._cache[key]
            if time.time() < expire_time:
                return value
            else:
                # 期限切れの場合は削除
                del self._cache[key]
        return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """キャッシュに値を設定"""
        if ttl is None:
            ttl = self._default_ttl
        expire_time = time.time() + ttl
        self._cache[key] = (value, expire_time)

    def delete(self, key: str) -> None:
        """キャッシュから値を削除"""
        if key in self._cache:
            del self._cache[key]

    def clear(self) -> None:
        """キャッシュをクリア"""
        self._cache.clear()

    def cleanup(self) -> None:
        """期限切れのエントリをクリーンアップ"""
        current_time = time.time()
        expired_keys = [
            key
            for key, (_, expire_time) in self._cache.items()
            if current_time >= expire_time
        ]
        for key in expired_keys:
            del self._cache[key]


# グローバルキャッシュインスタンス
_cache = MemoryCache()


def get_cache_key(prefix: str, **kwargs) -> str:
    """キャッシュキーを生成"""
    # kwargs を決定的な順序でシリアライズ
    sorted_kwargs = sorted(kwargs.items())
    key_data = json.dumps(sorted_kwargs, sort_keys=True, ensure_ascii=True)
    key_hash = hashlib.md5(key_data.encode(), usedforsecurity=False).hexdigest()
    return f"{prefix}:{key_hash}"


def cache_result(prefix: str, ttl: int = 300):
    """関数の結果をキャッシュするデコレータ

    Args:
        prefix: キャッシュキーのプレフィックス
        ttl: キャッシュの有効期限（秒）
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 引数からキャッシュキーを生成
            cache_key = get_cache_key(prefix, args=args, kwargs=kwargs)

            # キャッシュから取得
            cached_value = _cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # 関数を実行
            result = func(*args, **kwargs)

            # 結果をキャッシュ
            _cache.set(cache_key, result, ttl)

            return result

        # キャッシュクリア用のメソッドを追加
        wrapper.clear_cache = lambda: clear_cache_by_prefix(prefix)

        return wrapper

    return decorator


def clear_cache_by_prefix(prefix: str) -> None:
    """特定のプレフィックスを持つキャッシュをクリア"""
    keys_to_delete = [
        key for key in _cache._cache.keys() if key.startswith(f"{prefix}:")
    ]
    for key in keys_to_delete:
        _cache.delete(key)


def get_cache() -> MemoryCache:
    """グローバルキャッシュインスタンスを取得"""
    return _cache
