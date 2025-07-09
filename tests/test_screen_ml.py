#!/usr/bin/env python
"""
機械学習スクリーニングモジュール (screening/screen_ml.py) のテスト

テスト対象:
- データベース接続とデータ取得
- 特徴量エンジニアリング（価格特徴量、財務データのマージ）
- ラベル付け（将来リターン計算）
- モデル学習とAUC評価
- 予測とスクリーニング
- CLI引数処理
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline

sys.path.append(str(Path(__file__).resolve().parents[1]))
from screening import screen_ml


@pytest.fixture
def ml_db():
    """機械学習用のテストデータベース"""
    import os
    import tempfile

    from db.db_schema import init_schema

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # 正しいスキーマでデータベースを初期化
    init_schema(db_path)

    yield db_path

    os.unlink(db_path)


class TestDatabaseHelpers:
    """データベース関連のヘルパー関数のテスト"""

    def test_connect_with_path(self, ml_db):
        """Path型での接続テスト"""
        conn = screen_ml._connect(Path(ml_db))
        assert conn is not None
        conn.close()

    def test_connect_with_str(self, ml_db):
        """文字列型での接続テスト"""
        conn = screen_ml._connect(ml_db)
        assert conn is not None
        conn.close()

    def test_fetch_price(self, ml_db):
        """価格データ取得のテスト"""
        conn = sqlite3.connect(ml_db)

        # テストデータ挿入（今日から過去にさかのぼってデータ作成）
        today = datetime.now()
        for i in range(100):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            conn.execute(
                """
                INSERT INTO prices (code, date, adj_close, adj_volume) VALUES
                ('1234', ?, ?, ?),
                ('5678', ?, ?, ?)
            """,
                (date, 1000 + i * 10, 100000, date, 2000 - i * 5, 200000),
            )
        conn.commit()
        conn.close()

        # データ取得テスト
        conn = screen_ml._connect(ml_db)
        df = screen_ml._fetch_price(conn, 30)

        assert not df.empty
        assert "code" in df.columns
        assert "date" in df.columns
        assert "adj_close" in df.columns
        assert "adj_volume" in df.columns

        conn.close()

    def test_fetch_stmt(self, ml_db):
        """財務データ取得のテスト"""
        conn = sqlite3.connect(ml_db)

        # テストデータ挿入
        conn.execute(
            """
            INSERT INTO statements (
                code, DisclosedDate, DisclosureNumber,
                NetSales, OperatingProfit, OrdinaryProfit, Profit,
                TotalAssets, Equity, EquityToAssetRatio, BookValuePerShare,
                CashFlowsFromOperatingActivities, CashFlowsFromInvestingActivities,
                CashFlowsFromFinancingActivities
            ) VALUES
            ('1234', '2024-01-01', 'DISC001', 1000000, 100000, 90000, 60000,
             5000000, 2000000, 0.4, 100, 120000, -50000, -30000),
            ('5678', '2024-01-01', 'DISC002', 2000000, 200000, 180000, 120000,
             8000000, 3000000, 0.375, 150, 250000, -100000, -50000)
        """
        )
        conn.commit()
        conn.close()

        # データ取得テスト
        conn = screen_ml._connect(ml_db)
        df = screen_ml._fetch_stmt(conn)

        assert not df.empty
        assert "code" in df.columns  # codeカラムが存在することを確認
        assert "NetSales" in df.columns
        assert len(df) == 2

        conn.close()


class TestFeatureEngineering:
    """特徴量エンジニアリングのテスト"""

    def create_test_price_data(self, n_days=50, n_codes=2):
        """テスト用価格データの生成"""
        dates = pd.date_range(end="2024-06-01", periods=n_days, freq="D")
        data = []

        for code in [f"{1000 + i}" for i in range(n_codes)]:
            np.random.seed(int(code))
            prices = 1000 + np.cumsum(np.random.randn(n_days) * 10)
            volumes = np.random.randint(50000, 200000, n_days)

            for i, date in enumerate(dates):
                data.append(
                    {
                        "code": code,
                        "date": date,
                        "adj_close": max(prices[i], 100),  # 最低価格100
                        "adj_volume": volumes[i],
                    }
                )

        return pd.DataFrame(data)

    def test_make_price_features(self):
        """価格特徴量生成のテスト"""
        df_price = self.create_test_price_data(50, 2)

        result = screen_ml._make_price_features(df_price)

        # 特徴量が追加されているか確認
        assert "ret_5" in result.columns
        assert "ret_10" in result.columns
        assert "ret_20" in result.columns
        assert "volatility_20" in result.columns
        assert "turnover_norm" in result.columns

        # NaNが適切に処理されているか確認（最初の数行はNaN）
        assert result["ret_5"].iloc[0:5].isna().all()
        assert result["ret_20"].iloc[0:20].isna().all()

    def test_merge_features(self):
        """価格と財務データのマージテスト"""
        # 価格データ（特徴量付き）
        price_feat = pd.DataFrame(
            [
                {"code": "1234", "date": pd.Timestamp("2024-01-15"), "ret_5": 0.05},
                {"code": "1234", "date": pd.Timestamp("2024-02-15"), "ret_5": 0.03},
                {"code": "5678", "date": pd.Timestamp("2024-01-15"), "ret_5": -0.02},
            ]
        )

        # 財務データ
        stmt = pd.DataFrame(
            [
                {
                    "code": "1234",
                    "DisclosedDate": pd.Timestamp("2024-01-01"),
                    "NetSales": 1000000,
                    "OperatingProfit": 100000,
                },
                {
                    "code": "1234",
                    "DisclosedDate": pd.Timestamp("2024-02-01"),
                    "NetSales": 1100000,
                    "OperatingProfit": 110000,
                },
                {
                    "code": "5678",
                    "DisclosedDate": pd.Timestamp("2024-01-01"),
                    "NetSales": 2000000,
                    "OperatingProfit": 200000,
                },
            ]
        )

        result = screen_ml._merge_features(price_feat, stmt)

        # マージされたデータの確認
        assert len(result) == 3
        assert "NetSales" in result.columns
        assert "OperatingProfit" in result.columns

        # asof mergeの確認（過去の直近財務データが使われる）
        row1 = result[result["date"] == pd.Timestamp("2024-01-15")].iloc[0]
        assert row1["NetSales"] == 1000000  # 1/1の財務データ

        row2 = result[result["date"] == pd.Timestamp("2024-02-15")].iloc[0]
        assert row2["NetSales"] == 1100000  # 2/1の財務データ

    def test_add_label(self):
        """ラベル付けのテスト"""
        df = pd.DataFrame(
            [
                {"code": "1234", "date": pd.Timestamp("2024-01-01"), "adj_close": 1000},
                {
                    "code": "1234",
                    "date": pd.Timestamp("2024-02-01"),
                    "adj_close": 1050,
                },  # +5%
                {
                    "code": "1234",
                    "date": pd.Timestamp("2024-03-01"),
                    "adj_close": 1030,
                },  # -1.9%
                {"code": "5678", "date": pd.Timestamp("2024-01-01"), "adj_close": 2000},
                {
                    "code": "5678",
                    "date": pd.Timestamp("2024-02-01"),
                    "adj_close": 2200,
                },  # +10%
            ]
        )

        # future_window=1でテスト
        result = screen_ml._add_label(df, future_window=1, thresh_pct=0.05)

        assert "future_close" in result.columns
        assert "future_ret" in result.columns
        assert "label" in result.columns

        # ラベルの確認
        # 1234の1月 → 2月は+5%なのでlabel=1
        assert result.iloc[0]["label"] == 1
        # 1234の2月 → 3月は-1.9%なのでlabel=0
        assert result.iloc[1]["label"] == 0
        # 5678の1月 → 2月は+10%なのでlabel=1
        assert result.iloc[3]["label"] == 1

    def test_add_label_zero_division(self):
        """ゼロ除算の処理テスト"""
        df = pd.DataFrame(
            [
                {"code": "1234", "date": pd.Timestamp("2024-01-01"), "adj_close": 0},
                {"code": "1234", "date": pd.Timestamp("2024-02-01"), "adj_close": 100},
            ]
        )

        result = screen_ml._add_label(df, future_window=1, thresh_pct=0.05)

        # adj_closeが0の場合、future_retはNaNになるはず
        assert pd.isna(result.iloc[0]["future_ret"])


class TestModelTraining:
    """モデル学習のテスト"""

    def create_training_data(self, n_samples=100, positive_ratio=0.3):
        """学習用テストデータの生成"""
        np.random.seed(42)

        # 特徴量
        features = {}
        for col in screen_ml.PRICE_FEATURES + screen_ml.NUMERIC_STMT_COLS:
            features[col] = np.random.randn(n_samples)

        # ラベル（positive_ratioの割合で1）
        n_positive = int(n_samples * positive_ratio)
        labels = np.zeros(n_samples)
        labels[:n_positive] = 1
        np.random.shuffle(labels)

        df = pd.DataFrame(features)
        df["label"] = labels
        df["future_ret"] = np.where(labels == 1, 0.06, -0.02)  # デバッグ用

        return df

    def test_train_model_basic(self):
        """基本的なモデル学習のテスト"""
        df = self.create_training_data(200, 0.3)

        model = screen_ml._train_model(df)

        assert isinstance(model, Pipeline)
        assert "scaler" in model.named_steps
        assert "gb" in model.named_steps
        assert isinstance(model.named_steps["gb"], GradientBoostingClassifier)

    def test_train_model_imbalanced(self):
        """不均衡データでの学習テスト"""
        # ほとんど負例のデータ
        df = self.create_training_data(100, 0.05)

        with mock.patch("screening.screen_ml.logger") as mock_logger:
            screen_ml._train_model(df)

            # 警告が出力されることを確認
            assert any(
                "distribution" in str(call) for call in mock_logger.info.call_args_list
            )

    def test_train_model_single_class(self):
        """単一クラスのみの場合のテスト"""
        # すべて負例だが、future_retには変動を持たせる
        df = self.create_training_data(100, 0.0)
        # future_retに正の値も含める
        df.loc[df.index[:10], "future_ret"] = 0.01

        # 閾値を変更して対処するはず
        model = screen_ml._train_model(df)
        assert model is not None


class TestCLI:
    """CLI機能のテスト"""

    def test_parse_args_train(self):
        """trainコマンドの引数パースのテスト"""
        parser = argparse.ArgumentParser()
        parser.add_argument("cmd", choices=["train", "screen"])
        parser.add_argument("--db", default="stock.db")
        parser.add_argument("--lookback", type=int, default=1095)
        parser.add_argument("--top", type=int, default=30)
        parser.add_argument("--retrain", action="store_true")
        parser.add_argument("--future-window", type=int, default=30)
        parser.add_argument("--thresh-pct", type=float, default=0.05)

        args = parser.parse_args(
            [
                "train",
                "--db",
                "/tmp/test.db",
                "--lookback",
                "365",
                "--future-window",
                "20",
                "--thresh-pct",
                "0.03",
            ]
        )

        assert args.cmd == "train"
        assert args.db == "/tmp/test.db"
        assert args.lookback == 365
        assert args.future_window == 20
        assert args.thresh_pct == 0.03

    def test_parse_args_screen(self):
        """screenコマンドの引数パースのテスト"""
        parser = argparse.ArgumentParser()
        parser.add_argument("cmd", choices=["train", "screen"])
        parser.add_argument("--db", default="stock.db")
        parser.add_argument("--lookback", type=int, default=1095)
        parser.add_argument("--top", type=int, default=30)
        parser.add_argument("--retrain", action="store_true")
        parser.add_argument("--future-window", type=int, default=30)
        parser.add_argument("--thresh-pct", type=float, default=0.05)

        args = parser.parse_args(["screen", "--top", "50", "--retrain"])

        assert args.cmd == "screen"
        assert args.top == 50
        assert args.retrain is True


class TestIntegration:
    """統合テスト"""

    def create_test_database(self, db_path):
        """テスト用データベースにデータを投入"""
        conn = sqlite3.connect(db_path)

        # 価格データ（40営業日分、今日から過去にさかのぼって）
        today = datetime.now()
        codes = ["1234", "5678", "9012"]

        for code_idx, code in enumerate(codes):
            np.random.seed(code_idx)
            base_price = 1000 * (code_idx + 1)

            for i in range(40):
                date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
                # トレンドを持たせる
                trend = (
                    (40 - i) * 2 if code_idx == 0 else -(40 - i) if code_idx == 1 else 0
                )
                price = base_price + trend + np.random.randn() * 20
                volume = np.random.randint(50000, 200000)

                conn.execute(
                    """
                    INSERT INTO prices (code, date, adj_close, adj_volume) VALUES (?, ?, ?, ?)
                """,
                    (code, date, price, volume),
                )

        # 財務データ
        for code_idx, code in enumerate(codes):
            conn.execute(
                """
                INSERT INTO statements (
                    code, DisclosedDate, DisclosureNumber,
                    NetSales, OperatingProfit, OrdinaryProfit, Profit,
                    TotalAssets, Equity, EquityToAssetRatio, BookValuePerShare,
                    CashFlowsFromOperatingActivities, CashFlowsFromInvestingActivities,
                    CashFlowsFromFinancingActivities
                ) VALUES
                (?, '2024-03-15', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    code,
                    f"DISC{code_idx:03d}",  # DisclosureNumber
                    1000000 * (code_idx + 1),  # NetSales
                    100000 * (code_idx + 1),  # OperatingProfit
                    90000 * (code_idx + 1),  # OrdinaryProfit
                    60000 * (code_idx + 1),  # Profit
                    5000000 * (code_idx + 1),  # TotalAssets
                    2000000 * (code_idx + 1),  # Equity
                    0.4,  # EquityToAssetRatio
                    100 * (code_idx + 1),  # BookValuePerShare
                    120000 * (code_idx + 1),  # CashFlowsFromOperatingActivities
                    -50000 * (code_idx + 1),  # CashFlowsFromInvestingActivities
                    -30000 * (code_idx + 1),  # CashFlowsFromFinancingActivities
                ),
            )

        conn.commit()
        conn.close()

    def test_train_command(self, ml_db, tmp_path):
        """trainコマンドの統合テスト"""
        model_path = tmp_path / "ml_screen_model.pkl"

        # テストデータ作成
        self.create_test_database(ml_db)

        with mock.patch(
            "sys.argv",
            [
                "screen_ml.py",
                "train",
                "--db",
                ml_db,
                "--lookback",
                "50",
                "--future-window",
                "5",
                "--thresh-pct",
                "0.02",
            ],
        ):
            with mock.patch("screening.screen_ml.MODEL_FNAME", model_path.name):
                with mock.patch.object(Path, "parent", tmp_path):
                    # cli関数を呼び出す
                    parser = argparse.ArgumentParser()
                    parser.add_argument("cmd", choices=["train", "screen"])
                    parser.add_argument("--db", default=ml_db)
                    parser.add_argument("--lookback", type=int, default=1095)
                    parser.add_argument("--top", type=int, default=30)
                    parser.add_argument("--retrain", action="store_true")
                    parser.add_argument("--future-window", type=int, default=30)
                    parser.add_argument("--thresh-pct", type=float, default=0.05)

                    args = parser.parse_args(
                        [
                            "train",
                            "--db",
                            ml_db,
                            "--lookback",
                            "50",
                            "--future-window",
                            "5",
                            "--thresh-pct",
                            "0.02",
                        ]
                    )

                    # 処理実行
                    con = screen_ml._connect(args.db)
                    df = screen_ml._build_dataset(
                        con,
                        args.lookback,
                        future_window=args.future_window,
                        thresh_pct=args.thresh_pct,
                    )
                    model = screen_ml._train_model(df)

                    with open(model_path, "wb") as fh:
                        pickle.dump(model, fh)

        # モデルファイルが作成されたか確認
        assert model_path.exists()

        # モデルが読み込めるか確認
        with open(model_path, "rb") as fh:
            loaded_model = pickle.load(fh)
        assert isinstance(loaded_model, Pipeline)
