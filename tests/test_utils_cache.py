"""Test suite for src/utils/cache.py"""

import sys
import time
from pathlib import Path

import pytest

# モジュールをインポート
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.cache import (
    MemoryCache,
    _cache,
    cache_result,
    clear_cache_by_prefix,
    get_cache,
    get_cache_key,
)


class TestMemoryCache:
    """Test MemoryCache class"""

    def test_basic_set_and_get(self):
        """基本的なset/get操作"""
        cache = MemoryCache()

        # 値を設定
        cache.set("key1", "value1")

        # 値を取得
        assert cache.get("key1") == "value1"

        # 存在しないキー
        assert cache.get("nonexistent") is None

    def test_ttl_expiration(self):
        """TTL期限切れのテスト"""
        cache = MemoryCache()

        # 短いTTLで設定
        cache.set("key1", "value1", ttl=0.1)  # 0.1秒

        # すぐに取得（期限内）
        assert cache.get("key1") == "value1"

        # 期限切れまで待機
        time.sleep(0.15)
        assert cache.get("key1") is None

        # 期限切れ後はキーも削除されている
        assert "key1" not in cache._cache

    def test_default_ttl(self):
        """デフォルトTTLのテスト"""
        cache = MemoryCache()
        assert cache._default_ttl == 300  # 5分

        cache.set("key1", "value1")  # TTL指定なし

        # 値が設定されていることを確認
        assert "key1" in cache._cache
        value, expire_time = cache._cache["key1"]

        # 期限が約5分後に設定されていることを確認
        current_time = time.time()
        assert abs(expire_time - current_time - 300) < 1

    def test_delete(self):
        """削除操作のテスト"""
        cache = MemoryCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        # key1を削除
        cache.delete("key1")

        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"

        # 存在しないキーの削除（エラーにならない）
        cache.delete("nonexistent")

    def test_clear(self):
        """全削除のテスト"""
        cache = MemoryCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # 全削除
        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.get("key3") is None
        assert len(cache._cache) == 0

    def test_cleanup(self):
        """期限切れエントリのクリーンアップ"""
        cache = MemoryCache()

        # 異なるTTLで複数の値を設定
        cache.set("key1", "value1", ttl=0.1)
        cache.set("key2", "value2", ttl=10)
        cache.set("key3", "value3", ttl=0.1)

        # 一部を期限切れにする
        time.sleep(0.15)

        # クリーンアップ実行
        cache.cleanup()

        # 期限切れのものは削除される
        assert cache.get("key1") is None
        assert cache.get("key3") is None
        # 期限内のものは残る
        assert cache.get("key2") == "value2"

    def test_various_value_types(self):
        """様々な値の型のテスト"""
        cache = MemoryCache()

        # 文字列
        cache.set("str", "hello")
        assert cache.get("str") == "hello"

        # 数値
        cache.set("int", 42)
        assert cache.get("int") == 42

        # リスト
        cache.set("list", [1, 2, 3])
        assert cache.get("list") == [1, 2, 3]

        # 辞書
        cache.set("dict", {"a": 1, "b": 2})
        assert cache.get("dict") == {"a": 1, "b": 2}

        # None
        cache.set("none", None)
        assert cache.get("none") is None


class TestGetCacheKey:
    """Test get_cache_key function"""

    def test_basic_key_generation(self):
        """基本的なキー生成"""
        key = get_cache_key("test", param1="value1", param2="value2")
        assert key.startswith("test:")
        assert len(key) > len("test:")  # ハッシュが追加されている

    def test_consistent_key_generation(self):
        """同じ引数で同じキーが生成される"""
        key1 = get_cache_key("test", a=1, b=2)
        key2 = get_cache_key("test", a=1, b=2)
        assert key1 == key2

    def test_order_independence(self):
        """引数の順序に依存しない"""
        key1 = get_cache_key("test", a=1, b=2, c=3)
        key2 = get_cache_key("test", c=3, a=1, b=2)
        assert key1 == key2

    def test_different_values_different_keys(self):
        """異なる値は異なるキー"""
        key1 = get_cache_key("test", value="a")
        key2 = get_cache_key("test", value="b")
        assert key1 != key2

    def test_complex_arguments(self):
        """複雑な引数のテスト"""
        key = get_cache_key(
            "complex",
            list_param=[1, 2, 3],
            dict_param={"nested": {"value": 42}},
            tuple_param=(1, 2),
        )
        assert key.startswith("complex:")

    def test_empty_kwargs(self):
        """空のkwargsのテスト"""
        key = get_cache_key("empty")
        assert key.startswith("empty:")


class TestCacheResultDecorator:
    """Test cache_result decorator"""

    def setup_method(self):
        """各テストメソッドの前にキャッシュをクリア"""
        _cache.clear()

    def test_basic_caching(self):
        """基本的なキャッシング動作"""
        call_count = 0

        @cache_result("test_func", ttl=1)
        def expensive_function(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # 最初の呼び出し
        result1 = expensive_function(5)
        assert result1 == 10
        assert call_count == 1

        # 2回目の呼び出し（キャッシュから）
        result2 = expensive_function(5)
        assert result2 == 10
        assert call_count == 1  # 関数は実行されない

        # 異なる引数
        result3 = expensive_function(3)
        assert result3 == 6
        assert call_count == 2

    def test_cache_expiration(self):
        """キャッシュ期限切れのテスト"""
        call_count = 0

        @cache_result("test_func", ttl=0.1)
        def counter():
            nonlocal call_count
            call_count += 1
            return call_count

        # 最初の呼び出し
        assert counter() == 1

        # キャッシュから
        assert counter() == 1

        # 期限切れ後
        time.sleep(0.15)
        assert counter() == 2

    def test_with_args_and_kwargs(self):
        """引数とキーワード引数のテスト"""

        @cache_result("calc", ttl=10)
        def calculate(a, b, operation="add"):
            if operation == "add":
                return a + b
            elif operation == "multiply":
                return a * b

        # 異なる引数の組み合わせ
        assert calculate(2, 3) == 5
        assert calculate(2, 3, operation="add") == 5  # 同じ結果
        assert calculate(2, 3, operation="multiply") == 6
        assert calculate(3, 2, operation="add") == 5  # 引数の順序が違う

    def test_clear_cache_method(self):
        """キャッシュクリアメソッドのテスト"""
        call_count = 0

        @cache_result("clearable", ttl=10)
        def func(x):
            nonlocal call_count
            call_count += 1
            return x

        # キャッシュに保存
        assert func(1) == 1
        assert call_count == 1

        # キャッシュから取得
        assert func(1) == 1
        assert call_count == 1

        # キャッシュをクリア
        func.clear_cache()

        # 再度実行される
        assert func(1) == 1
        assert call_count == 2

    def test_none_return_value(self):
        """戻り値がNoneの場合"""
        # グローバルキャッシュをクリア
        _cache.clear()

        call_count = 0

        @cache_result("none_func_unique_test", ttl=10)
        def return_none():
            nonlocal call_count
            call_count += 1
            return None

        # Noneもキャッシュされる
        result1 = return_none()
        assert result1 is None
        assert call_count == 1

        result2 = return_none()
        assert result2 is None
        assert call_count == 1  # キャッシュから


class TestClearCacheByPrefix:
    """Test clear_cache_by_prefix function"""

    def test_clear_by_prefix(self):
        """プレフィックスによるクリア"""
        # グローバルキャッシュに直接値を設定
        _cache.set("api:key1", "value1")
        _cache.set("api:key2", "value2")
        _cache.set("db:key1", "value3")
        _cache.set("other", "value4")

        # api: プレフィックスのみクリア
        clear_cache_by_prefix("api")

        assert _cache.get("api:key1") is None
        assert _cache.get("api:key2") is None
        assert _cache.get("db:key1") == "value3"
        assert _cache.get("other") == "value4"

    def test_clear_nonexistent_prefix(self):
        """存在しないプレフィックスのクリア"""
        _cache.set("test:key", "value")

        # 存在しないプレフィックスでもエラーにならない
        clear_cache_by_prefix("nonexistent")

        assert _cache.get("test:key") == "value"


class TestGetCache:
    """Test get_cache function"""

    def test_returns_global_cache(self):
        """グローバルキャッシュインスタンスを返す"""
        cache = get_cache()
        assert cache is _cache
        assert isinstance(cache, MemoryCache)


class TestIntegration:
    """Integration tests"""

    def test_multiple_decorated_functions(self):
        """複数のデコレートされた関数"""

        @cache_result("func1", ttl=10)
        def func1(x):
            return x + 1

        @cache_result("func2", ttl=10)
        def func2(x):
            return x * 2

        # それぞれ独立してキャッシュされる
        assert func1(5) == 6
        assert func2(5) == 10

        # func1のキャッシュをクリア
        func1.clear_cache()

        # func2のキャッシュは影響を受けない
        cache = get_cache()
        assert cache.get(get_cache_key("func2", args=(5,), kwargs={})) == 10

    def test_cache_with_exceptions(self):
        """例外が発生する関数のキャッシング"""
        call_count = 0

        @cache_result("error_func", ttl=10)
        def may_fail(should_fail):
            nonlocal call_count
            call_count += 1
            if should_fail:
                raise ValueError("Failed")
            return "success"

        # 成功ケース
        assert may_fail(False) == "success"
        assert call_count == 1

        # キャッシュから
        assert may_fail(False) == "success"
        assert call_count == 1

        # 失敗ケース（例外はキャッシュされない）
        with pytest.raises(ValueError):
            may_fail(True)
        assert call_count == 2

        # 再度失敗（キャッシュされていないので再実行）
        with pytest.raises(ValueError):
            may_fail(True)
        assert call_count == 3

    def test_concurrent_cache_operations(self):
        """並行キャッシュ操作のシミュレーション"""
        cache = MemoryCache()

        # 複数のキーを短いTTLで設定
        for i in range(10):
            cache.set(f"key{i}", f"value{i}", ttl=0.1 * (i + 1))

        # 一部が期限切れ
        time.sleep(0.5)

        # クリーンアップ
        cache.cleanup()

        # 期限が短いものは削除されている
        for i in range(5):
            assert cache.get(f"key{i}") is None

        # 期限が長いものは残っている可能性がある
        for i in range(5, 10):
            # TTLによっては残っているかもしれない
            value = cache.get(f"key{i}")
            if value is not None:
                assert value == f"value{i}"
