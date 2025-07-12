"""Tests for src/utils/cache.py"""

from unittest.mock import patch

from src.utils.cache import (
    MemoryCache,
    cache_result,
    clear_cache_by_prefix,
    get_cache,
    get_cache_key,
)


class TestMemoryCache:
    """MemoryCacheクラスのテスト"""

    def test_initialization(self):
        """初期化のテスト"""
        cache = MemoryCache()

        assert isinstance(cache._cache, dict)
        assert len(cache._cache) == 0
        assert cache._default_ttl == 300

    def test_set_and_get(self):
        """値の設定と取得のテスト"""
        cache = MemoryCache()

        # 値を設定
        cache.set("key1", "value1")
        cache.set("key2", {"data": 123}, ttl=60)

        # 値を取得
        assert cache.get("key1") == "value1"
        assert cache.get("key2") == {"data": 123}
        assert cache.get("nonexistent") is None

    @patch("src.utils.cache.time.time")
    def test_expiration(self, mock_time):
        """有効期限のテスト"""
        cache = MemoryCache()

        # 現在時刻を100に設定
        mock_time.return_value = 100

        # TTL=10秒で値を設定
        cache.set("key1", "value1", ttl=10)

        # 5秒後（まだ有効）
        mock_time.return_value = 105
        assert cache.get("key1") == "value1"

        # 15秒後（期限切れ）
        mock_time.return_value = 115
        assert cache.get("key1") is None

        # 期限切れのキーは削除されている
        assert "key1" not in cache._cache

    def test_delete(self):
        """値の削除のテスト"""
        cache = MemoryCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        # key1を削除
        cache.delete("key1")

        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"

        # 存在しないキーの削除はエラーにならない
        cache.delete("nonexistent")

    def test_clear(self):
        """キャッシュのクリアのテスト"""
        cache = MemoryCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # 全てクリア
        cache.clear()

        assert len(cache._cache) == 0
        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.get("key3") is None

    @patch("src.utils.cache.time.time")
    def test_cleanup(self, mock_time):
        """期限切れエントリのクリーンアップのテスト"""
        cache = MemoryCache()

        # 現在時刻を100に設定
        mock_time.return_value = 100

        # 異なるTTLで値を設定
        cache.set("key1", "value1", ttl=10)
        cache.set("key2", "value2", ttl=20)
        cache.set("key3", "value3", ttl=30)

        # 15秒後
        mock_time.return_value = 115

        # クリーンアップ実行
        cache.cleanup()

        # key1は期限切れで削除される
        assert "key1" not in cache._cache
        assert "key2" in cache._cache
        assert "key3" in cache._cache


class TestGetCacheKey:
    """get_cache_key関数のテスト"""

    def test_basic_key_generation(self):
        """基本的なキー生成のテスト"""
        key1 = get_cache_key("test", arg1="value1", arg2=123)
        key2 = get_cache_key("test", arg1="value1", arg2=123)
        key3 = get_cache_key("test", arg1="value1", arg2=456)

        # 同じ引数なら同じキー
        assert key1 == key2
        # 異なる引数なら異なるキー
        assert key1 != key3
        # プレフィックスが含まれる
        assert key1.startswith("test:")

    def test_key_order_independence(self):
        """引数の順序に依存しないことのテスト"""
        key1 = get_cache_key("test", a=1, b=2, c=3)
        key2 = get_cache_key("test", c=3, a=1, b=2)

        assert key1 == key2

    def test_different_prefixes(self):
        """異なるプレフィックスのテスト"""
        key1 = get_cache_key("prefix1", value=123)
        key2 = get_cache_key("prefix2", value=123)

        assert key1 != key2
        assert key1.startswith("prefix1:")
        assert key2.startswith("prefix2:")


class TestCacheResultDecorator:
    """cache_resultデコレータのテスト"""

    def test_basic_caching(self):
        """基本的なキャッシングのテスト"""
        call_count = 0

        @cache_result("test_func", ttl=60)
        def expensive_function(x, y):
            nonlocal call_count
            call_count += 1
            return x + y

        # 最初の呼び出し
        result1 = expensive_function(1, 2)
        assert result1 == 3
        assert call_count == 1

        # 2回目の呼び出し（キャッシュから）
        result2 = expensive_function(1, 2)
        assert result2 == 3
        assert call_count == 1  # 関数は再実行されない

        # 異なる引数での呼び出し
        result3 = expensive_function(2, 3)
        assert result3 == 5
        assert call_count == 2

    def test_clear_cache_method(self):
        """キャッシュクリアメソッドのテスト"""
        call_count = 0

        @cache_result("test_func2")
        def cached_function():
            nonlocal call_count
            call_count += 1
            return call_count

        # 最初の呼び出し
        assert cached_function() == 1

        # キャッシュから
        assert cached_function() == 1

        # キャッシュをクリア
        cached_function.clear_cache()

        # 再度実行される
        assert cached_function() == 2

    @patch("src.utils.cache.time.time")
    def test_ttl_expiration(self, mock_time):
        """TTL期限切れのテスト"""
        call_count = 0

        @cache_result("test_ttl", ttl=10)
        def timed_function():
            nonlocal call_count
            call_count += 1
            return call_count

        # 現在時刻を100に設定
        mock_time.return_value = 100

        # 最初の呼び出し
        assert timed_function() == 1

        # 5秒後（まだ有効）
        mock_time.return_value = 105
        assert timed_function() == 1

        # 15秒後（期限切れ）
        mock_time.return_value = 115
        assert timed_function() == 2


class TestClearCacheByPrefix:
    """clear_cache_by_prefix関数のテスト"""

    def test_clear_specific_prefix(self):
        """特定のプレフィックスのキャッシュクリアのテスト"""
        cache = get_cache()
        cache.clear()  # 既存のキャッシュをクリア

        # 異なるプレフィックスでキャッシュを設定
        cache.set("prefix1:key1", "value1")
        cache.set("prefix1:key2", "value2")
        cache.set("prefix2:key1", "value3")
        cache.set("prefix2:key2", "value4")

        # prefix1のみクリア
        clear_cache_by_prefix("prefix1")

        # prefix1のキーは削除される
        assert cache.get("prefix1:key1") is None
        assert cache.get("prefix1:key2") is None

        # prefix2のキーは残る
        assert cache.get("prefix2:key1") == "value3"
        assert cache.get("prefix2:key2") == "value4"


class TestGetCache:
    """get_cache関数のテスト"""

    def test_returns_global_instance(self):
        """グローバルインスタンスを返すことのテスト"""
        cache1 = get_cache()
        cache2 = get_cache()

        # 同じインスタンス
        assert cache1 is cache2

        # MemoryCacheのインスタンス
        assert isinstance(cache1, MemoryCache)

    def test_global_instance_sharing(self):
        """グローバルインスタンスが共有されることのテスト"""
        cache = get_cache()
        cache.clear()

        # 一つの場所で設定
        cache.set("shared_key", "shared_value")

        # 別の取得でも同じ値が取れる
        another_cache = get_cache()
        assert another_cache.get("shared_key") == "shared_value"
