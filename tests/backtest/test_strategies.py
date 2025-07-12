"""backtest.strategies モジュールのテスト"""

import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backtest.strategies.base_strategy import BaseStrategy
from backtest.strategies.technical_long import TechnicalLongStrategy
from backtest.strategies.technical_short import TechnicalShortStrategy


class TestBaseStrategy:
    """BaseStrategy クラスのテスト"""

    def test_abstract_base_class(self):
        """抽象基底クラスとして正しく定義されているか"""
        with pytest.raises(TypeError):
            BaseStrategy()

    def test_concrete_implementation(self):
        """具象実装のテスト用クラス"""

        class ConcreteStrategy(BaseStrategy):
            def get_entry_signals(self, conn, as_of):
                return pd.DataFrame({"code": ["1234"]})

            def calculate_profit(self, entry_price, exit_price):
                return 0.1, 100000

            def get_stop_price(self, entry_price):
                return entry_price * 0.95

            def get_side_label(self):
                return "test"

        strategy = ConcreteStrategy(
            capital=1_000_000, hold_days=30, stop_loss_pct=0.05, min_price=300
        )
        assert strategy.capital == 1_000_000
        assert strategy.hold_days == 30
        assert strategy.stop_loss_pct == 0.05
        assert strategy.min_price == 300

    def test_calculate_position_size(self):
        """ポジションサイズ計算のテスト"""

        class ConcreteStrategy(BaseStrategy):
            def get_entry_signals(self, conn, as_of):
                pass

            def calculate_profit(self, entry_price, exit_price):
                pass

            def get_stop_price(self, entry_price):
                pass

            def get_side_label(self):
                pass

        strategy = ConcreteStrategy(capital=1_000_000)

        # 通常のケース
        assert strategy.calculate_position_size(1000.0) == 1000

        # 端数の処理
        assert strategy.calculate_position_size(1500.0) == 666

        # 高額株のケース
        assert strategy.calculate_position_size(10000.0) == 100

    def test_is_stopped_out_long(self):
        """ロングポジションの損切り判定テスト"""

        class ConcreteStrategy(BaseStrategy):
            def get_entry_signals(self, conn, as_of):
                pass

            def calculate_profit(self, entry_price, exit_price):
                pass

            def get_stop_price(self, entry_price):
                pass

            def get_side_label(self):
                pass

        strategy = ConcreteStrategy()

        # ロングポジション: 価格が損切り価格以下なら損切り
        assert strategy.is_stopped_out(950.0, 1000.0, "long") is True
        assert strategy.is_stopped_out(1000.0, 1000.0, "long") is True
        assert strategy.is_stopped_out(1050.0, 1000.0, "long") is False

    def test_is_stopped_out_short(self):
        """ショートポジションの損切り判定テスト"""

        class ConcreteStrategy(BaseStrategy):
            def get_entry_signals(self, conn, as_of):
                pass

            def calculate_profit(self, entry_price, exit_price):
                pass

            def get_stop_price(self, entry_price):
                pass

            def get_side_label(self):
                pass

        strategy = ConcreteStrategy()

        # ショートポジション: 価格が損切り価格以上なら損切り
        assert strategy.is_stopped_out(1050.0, 1000.0, "short") is True
        assert strategy.is_stopped_out(1000.0, 1000.0, "short") is True
        assert strategy.is_stopped_out(950.0, 1000.0, "short") is False


class TestTechnicalLongStrategy:
    """TechnicalLongStrategy クラスのテスト"""

    def test_initialization(self):
        """初期化のテスト"""
        strategy = TechnicalLongStrategy(
            capital=2_000_000, hold_days=45, stop_loss_pct=0.03, min_price=500
        )
        assert strategy.capital == 2_000_000
        assert strategy.hold_days == 45
        assert strategy.stop_loss_pct == 0.03
        assert strategy.min_price == 500

    @patch("pandas.read_sql")
    def test_get_entry_signals(self, mock_read_sql):
        """エントリーシグナル取得のテスト"""
        expected_df = pd.DataFrame({"code": ["1234", "5678"]})
        mock_read_sql.return_value = expected_df

        strategy = TechnicalLongStrategy()
        conn = MagicMock(spec=sqlite3.Connection)
        result = strategy.get_entry_signals(conn, "2024-01-01")

        assert result.equals(expected_df)
        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        assert "technical_indicators" in call_args[0][0]
        assert "signals_count>=?" in call_args[0][0]
        assert "signals_first=1" in call_args[0][0]
        assert call_args[1]["params"][0] == "2024-01-01"

    def test_calculate_profit(self):
        """利益計算のテスト"""
        strategy = TechnicalLongStrategy(capital=1_000_000)

        # 利益が出るケース
        profit_pct, profit_jpy = strategy.calculate_profit(1000.0, 1100.0)
        assert profit_pct == pytest.approx(0.1)
        assert profit_jpy == pytest.approx(100_000)

        # 損失が出るケース
        profit_pct, profit_jpy = strategy.calculate_profit(1000.0, 900.0)
        assert profit_pct == pytest.approx(-0.1)
        assert profit_jpy == pytest.approx(-100_000)

        # 変化なしのケース
        profit_pct, profit_jpy = strategy.calculate_profit(1000.0, 1000.0)
        assert profit_pct == pytest.approx(0.0)
        assert profit_jpy == pytest.approx(0.0)

    def test_get_stop_price(self):
        """損切り価格計算のテスト"""
        strategy = TechnicalLongStrategy(stop_loss_pct=0.05)

        # ロングポジションは下方向に損切り
        assert strategy.get_stop_price(1000.0) == pytest.approx(950.0)
        assert strategy.get_stop_price(500.0) == pytest.approx(475.0)

    def test_get_side_label(self):
        """戦略ラベルのテスト"""
        strategy = TechnicalLongStrategy()
        assert strategy.get_side_label() == "long"


class TestTechnicalShortStrategy:
    """TechnicalShortStrategy クラスのテスト"""

    def test_initialization(self):
        """初期化のテスト"""
        strategy = TechnicalShortStrategy(
            capital=3_000_000, hold_days=20, stop_loss_pct=0.02, min_price=100
        )
        assert strategy.capital == 3_000_000
        assert strategy.hold_days == 20
        assert strategy.stop_loss_pct == 0.02
        assert strategy.min_price == 100

    @patch("pandas.read_sql")
    def test_get_entry_signals(self, mock_read_sql):
        """エントリーシグナル取得のテスト"""
        expected_df = pd.DataFrame({"code": ["2222", "3333"]})
        mock_read_sql.return_value = expected_df

        strategy = TechnicalShortStrategy()
        conn = MagicMock(spec=sqlite3.Connection)
        result = strategy.get_entry_signals(conn, "2024-01-01")

        assert result.equals(expected_df)
        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        assert "technical_indicators" in call_args[0][0]
        assert "signals_short_count>=?" in call_args[0][0]
        assert "signals_short_first=1" in call_args[0][0]
        assert call_args[1]["params"][0] == "2024-01-01"

    def test_calculate_profit(self):
        """利益計算のテスト（ショート）"""
        strategy = TechnicalShortStrategy(capital=1_000_000)

        # ショートで利益が出るケース（価格が下がる）
        profit_pct, profit_jpy = strategy.calculate_profit(1000.0, 900.0)
        assert profit_pct == pytest.approx(0.1)
        assert profit_jpy == pytest.approx(100_000)

        # ショートで損失が出るケース（価格が上がる）
        profit_pct, profit_jpy = strategy.calculate_profit(1000.0, 1100.0)
        assert profit_pct == pytest.approx(-0.1)
        assert profit_jpy == pytest.approx(-100_000)

        # 変化なしのケース
        profit_pct, profit_jpy = strategy.calculate_profit(1000.0, 1000.0)
        assert profit_pct == pytest.approx(0.0)
        assert profit_jpy == pytest.approx(0.0)

    def test_get_stop_price(self):
        """損切り価格計算のテスト（ショート）"""
        strategy = TechnicalShortStrategy(stop_loss_pct=0.05)

        # ショートポジションは上方向に損切り
        assert strategy.get_stop_price(1000.0) == pytest.approx(1050.0)
        assert strategy.get_stop_price(500.0) == pytest.approx(525.0)

    def test_get_side_label(self):
        """戦略ラベルのテスト"""
        strategy = TechnicalShortStrategy()
        assert strategy.get_side_label() == "short"


class TestStrategyIntegration:
    """戦略クラス間の統合テスト"""

    def test_strategy_polymorphism(self):
        """ポリモーフィズムのテスト"""
        strategies = [
            TechnicalLongStrategy(capital=1_000_000),
            TechnicalShortStrategy(capital=1_000_000),
        ]

        for strategy in strategies:
            # 全ての戦略が BaseStrategy のインターフェースを実装
            assert isinstance(strategy, BaseStrategy)
            assert hasattr(strategy, "get_entry_signals")
            assert hasattr(strategy, "calculate_profit")
            assert hasattr(strategy, "get_stop_price")
            assert hasattr(strategy, "get_side_label")
            assert hasattr(strategy, "calculate_position_size")
            assert hasattr(strategy, "is_stopped_out")

    def test_long_short_profit_symmetry(self):
        """ロングとショートの利益計算の対称性テスト"""
        long_strategy = TechnicalLongStrategy(capital=1_000_000)
        short_strategy = TechnicalShortStrategy(capital=1_000_000)

        # 同じ価格変動に対して、ロングとショートの利益は符号が逆
        long_profit_pct, _ = long_strategy.calculate_profit(1000.0, 1100.0)
        short_profit_pct, _ = short_strategy.calculate_profit(1000.0, 1100.0)
        assert long_profit_pct == pytest.approx(-short_profit_pct)

        long_profit_pct, _ = long_strategy.calculate_profit(1000.0, 900.0)
        short_profit_pct, _ = short_strategy.calculate_profit(1000.0, 900.0)
        assert long_profit_pct == pytest.approx(-short_profit_pct)

    def test_stop_loss_directions(self):
        """ロングとショートの損切り方向テスト"""
        long_strategy = TechnicalLongStrategy(stop_loss_pct=0.05)
        short_strategy = TechnicalShortStrategy(stop_loss_pct=0.05)

        entry_price = 1000.0
        long_stop = long_strategy.get_stop_price(entry_price)
        short_stop = short_strategy.get_stop_price(entry_price)

        # ロングの損切りは下、ショートの損切りは上
        assert long_stop < entry_price
        assert short_stop > entry_price

        # 損切り幅は同じ
        assert abs(entry_price - long_stop) == pytest.approx(
            abs(entry_price - short_stop)
        )
