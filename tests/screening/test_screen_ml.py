"""Tests for screening/screen_ml.py"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from screening.screen_ml import (
    _add_label,
    _build_dataset,
    _connect,
    _fetch_price,
    _fetch_stmt,
    _make_price_features,
    _merge_features,
    _train_model,
    cli,
)


class TestConnect:
    """データベース接続のテスト"""

    @patch("screening.screen_ml.sqlite3.connect")
    def test_connect_success(self, mock_connect):
        """正常な接続"""
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        conn = _connect("test.db")

        assert conn == mock_conn
        mock_connect.assert_called_once_with("test.db")

    @patch("screening.screen_ml.sqlite3.connect")
    def test_connect_with_path(self, mock_connect):
        """Pathオブジェクトでの接続"""
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        conn = _connect(Path("test.db"))

        assert conn == mock_conn
        mock_connect.assert_called_once_with("test.db")


class TestFetchPrice:
    """価格データ取得のテスト"""

    @patch("screening.screen_ml.pd.read_sql")
    def test_fetch_price_basic(self, mock_read_sql):
        """基本的な価格データ取得"""
        mock_conn = MagicMock(spec=sqlite3.Connection)

        mock_df = pd.DataFrame(
            {
                "code": ["1234", "1234", "5678", "5678"],
                "date": ["2024-01-15", "2024-01-16", "2024-01-15", "2024-01-16"],
                "adj_close": [1000, 1010, 2000, 2020],
                "adj_volume": [100000, 110000, 200000, 210000],
            }
        )
        mock_read_sql.return_value = mock_df

        result = _fetch_price(mock_conn, lookback=30, as_of="2024-01-16")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        # SQLクエリに日付が含まれることを確認
        call_args = mock_read_sql.call_args[0][0]
        assert "2024-01-16" in call_args
        assert "-30 day" in call_args

    def test_fetch_price_no_as_of(self):
        """as_of指定なしの場合"""
        mock_conn = MagicMock(spec=sqlite3.Connection)

        with patch("screening.screen_ml.pd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame()
            _fetch_price(mock_conn, lookback=30)

            call_args = mock_read_sql.call_args[0][0]
            assert "date('now'" in call_args


class TestFetchStmt:
    """財務データ取得のテスト"""

    @patch("screening.screen_ml.pd.read_sql")
    def test_fetch_stmt_success(self, mock_read_sql):
        """正常な財務データ取得"""
        mock_conn = MagicMock(spec=sqlite3.Connection)

        mock_df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "DisclosedDate": ["2024-01-15", "2024-01-16"],
                "NetSales": [1000000, 2000000],
                "OperatingProfit": [100000, 200000],
                "OrdinaryProfit": [110000, 210000],
                "Profit": [80000, 160000],
            }
        )
        mock_read_sql.return_value = mock_df

        result = _fetch_stmt(mock_conn)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "code" in result.columns

    def test_fetch_stmt_empty(self):
        """空のデータの場合"""
        mock_conn = MagicMock(spec=sqlite3.Connection)

        with patch("screening.screen_ml.pd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame()
            result = _fetch_stmt(mock_conn)

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0


class TestMakePriceFeatures:
    """価格特徴量生成のテスト"""

    def test_make_price_features_basic(self):
        """基本的な特徴量生成"""
        # 各銘柄30日分のデータ
        dates = pd.date_range("2024-01-01", periods=30, freq="D")
        df_price = pd.DataFrame(
            {
                "code": ["1234"] * 30 + ["5678"] * 30,
                "date": list(dates) + list(dates),
                "adj_close": list(range(1000, 1030)) + list(range(2000, 2030)),
                "adj_volume": [100000 + i * 1000 for i in range(30)] * 2,
            }
        )

        result = _make_price_features(df_price)

        assert isinstance(result, pd.DataFrame)
        assert "code" in result.columns
        assert "ret_5" in result.columns
        assert "ret_10" in result.columns
        assert "ret_20" in result.columns
        assert "volatility_20" in result.columns
        assert "turnover_norm" in result.columns

        # 各銘柄の行数は元と同じ
        assert len(result[result["code"] == "1234"]) == 30

    def test_make_price_features_insufficient_data(self):
        """データ不足の場合"""
        # 5日分しかないデータ
        df_price = pd.DataFrame(
            {
                "code": ["1234"] * 5,
                "date": pd.date_range("2024-01-01", periods=5),
                "adj_close": [1000, 1001, 1002, 1003, 1004],
                "adj_volume": [100000] * 5,
            }
        )

        result = _make_price_features(df_price)

        # 20日リターンが計算できないため、NaNになる
        assert pd.isna(result["ret_20"].iloc[-1])


class TestMergeFeatures:
    """特徴量マージのテスト"""

    def test_merge_features_basic(self):
        """基本的なマージ"""
        price_feat = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "adj_close": [1000, 2000],
                "ret_5": [0.05, 0.03],
            }
        )

        stmt = pd.DataFrame(
            {
                "code": ["1234", "1234", "5678"],
                "DisclosedDate": pd.to_datetime(
                    ["2024-01-10", "2024-01-14", "2024-01-10"]
                ),
                "NetSales": [1000000, 1100000, 2000000],
                "OperatingProfit": [100000, 110000, 200000],
            }
        )

        result = _merge_features(price_feat, stmt)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "adj_close" in result.columns
        assert "NetSales" in result.columns
        # 直近の財務データが使われる
        assert result[result["code"] == "1234"]["NetSales"].iloc[0] == 1100000

    def test_merge_features_no_stmt(self):
        """財務データがない銘柄の場合"""
        price_feat = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "adj_close": [1000, 2000],
            }
        )

        stmt = pd.DataFrame(
            {
                "code": ["1234"],
                "DisclosedDate": pd.to_datetime(["2024-01-10"]),
                "NetSales": [1000000],
            }
        )

        result = _merge_features(price_feat, stmt)

        # 財務データがない銘柄も含まれるが、値は0で埋められる
        assert len(result) == 2
        assert result[result["code"] == "5678"]["NetSales"].iloc[0] == 0


class TestAddLabel:
    """ラベル付与のテスト"""

    def test_add_label_basic(self):
        """基本的なラベル付与"""
        df = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "date": pd.to_datetime(["2024-01-15", "2024-01-16"]),
                "adj_close": [1000, 2000],
            }
        )

        # 将来の価格データを追加
        future_dates = pd.date_range("2024-01-15", periods=40, freq="D")
        df_all = pd.DataFrame(
            {
                "code": ["1234"] * 40 + ["5678"] * 40,
                "date": list(future_dates)
                + list(pd.date_range("2024-01-16", periods=40, freq="D")),
                "adj_close": [1000 * (1 + 0.002 * i) for i in range(40)]  # 上昇
                + [2000 * (1 - 0.002 * i) for i in range(40)],  # 下落
            }
        )

        # dfとdf_allを結合
        df_combined = (
            pd.concat([df, df_all])
            .drop_duplicates(["code", "date"])
            .sort_values(["code", "date"])
        )

        result = _add_label(df_combined, future_window=30, thresh_pct=0.05)

        assert isinstance(result, pd.DataFrame)
        assert "label" in result.columns
        assert "future_ret" in result.columns

        # 最初の日付のラベルを確認
        first_1234 = result[
            (result["code"] == "1234")
            & (result["date"] == pd.to_datetime("2024-01-15"))
        ].iloc[0]
        first_5678 = result[
            (result["code"] == "5678")
            & (result["date"] == pd.to_datetime("2024-01-16"))
        ].iloc[0]

        # 1234は6%上昇、5678は6%下落
        assert first_1234["label"] == 1
        assert first_5678["label"] == 0

    def test_add_label_insufficient_future(self):
        """将来データが不足している場合"""
        df = pd.DataFrame(
            {
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-15", periods=10),
                "adj_close": [1000] * 10,
            }
        )

        result = _add_label(df, future_window=30)

        # 最後の30日分はラベルが付与できない（0になる）
        assert result["label"].iloc[-1] == 0


class TestBuildDataset:
    """データセット構築のテスト"""

    @patch("screening.screen_ml._add_label")
    @patch("screening.screen_ml._merge_features")
    @patch("screening.screen_ml._make_price_features")
    @patch("screening.screen_ml._fetch_stmt")
    @patch("screening.screen_ml._fetch_price")
    def test_build_dataset_success(
        self,
        mock_fetch_price,
        mock_fetch_stmt,
        mock_make_features,
        mock_merge,
        mock_add_label,
    ):
        """正常なデータセット構築"""
        mock_conn = MagicMock(spec=sqlite3.Connection)

        # モックデータ
        price_data = pd.DataFrame(
            {
                "code": ["1234"] * 10,
                "date": pd.date_range("2024-01-01", periods=10),
                "adj_close": range(1000, 1010),
                "adj_volume": [100000] * 10,
            }
        )
        mock_fetch_price.return_value = price_data

        stmt_data = pd.DataFrame(
            {
                "code": ["1234"],
                "DisclosedDate": pd.to_datetime(["2024-01-01"]),
                "NetSales": [1000000],
            }
        )
        mock_fetch_stmt.return_value = stmt_data

        features = pd.DataFrame(
            {
                "code": ["1234"],
                "date": pd.to_datetime(["2024-01-10"]),
                "adj_close": [1005],
                "ret_5": [0.01],
                "ret_10": [0.02],
                "ret_20": [0.03],
                "volatility_20": [0.015],
                "turnover_norm": [1.1],
            }
        )
        mock_make_features.return_value = features

        # 全ての必要なカラムを含むデータ
        all_cols = {
            "code": ["1234"],
            "date": pd.to_datetime(["2024-01-10"]),
            "adj_close": [1005],
            "ret_5": [0.01],
            "ret_10": [0.02],
            "ret_20": [0.03],
            "volatility_20": [0.015],
            "turnover_norm": [1.1],
            "NetSales": [1000000],
            "OperatingProfit": [100000],
            "OrdinaryProfit": [90000],
            "Profit": [80000],
            "TotalAssets": [5000000],
            "Equity": [2000000],
            "EquityToAssetRatio": [0.4],
            "BookValuePerShare": [1000],
            "CashFlowsFromOperatingActivities": [150000],
            "CashFlowsFromInvestingActivities": [-50000],
            "CashFlowsFromFinancingActivities": [-30000],
        }

        merged = pd.DataFrame(all_cols)
        mock_merge.return_value = merged

        labeled = pd.DataFrame({**all_cols, "label": [1], "future_ret": [0.06]})
        mock_add_label.return_value = labeled

        result = _build_dataset(mock_conn, lookback=30, as_of="2024-01-10")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "label" in result.columns


class TestTrainModel:
    """モデル学習のテスト"""

    def test_train_model_success(self):
        """正常なモデル学習"""
        # ダミーデータ生成
        np.random.seed(42)
        n_samples = 100

        # 必要なカラムを全て含むデータ
        data = {
            # 価格特徴量
            "ret_5": np.random.randn(n_samples) * 0.01,
            "ret_10": np.random.randn(n_samples) * 0.02,
            "ret_20": np.random.randn(n_samples) * 0.03,
            "volatility_20": np.abs(np.random.randn(n_samples) * 0.01),
            "turnover_norm": np.abs(np.random.randn(n_samples)),
            # 財務データ
            "NetSales": np.abs(np.random.randn(n_samples)) * 1000000,
            "OperatingProfit": np.random.randn(n_samples) * 100000,
            "OrdinaryProfit": np.random.randn(n_samples) * 90000,
            "Profit": np.random.randn(n_samples) * 80000,
            "TotalAssets": np.abs(np.random.randn(n_samples)) * 5000000,
            "Equity": np.abs(np.random.randn(n_samples)) * 2000000,
            "EquityToAssetRatio": np.abs(np.random.randn(n_samples)) * 0.5,
            "BookValuePerShare": np.abs(np.random.randn(n_samples)) * 1000,
            "CashFlowsFromOperatingActivities": np.random.randn(n_samples) * 150000,
            "CashFlowsFromInvestingActivities": np.random.randn(n_samples) * 50000,
            "CashFlowsFromFinancingActivities": np.random.randn(n_samples) * 30000,
        }

        df = pd.DataFrame(data)
        # ラベルを両方のクラスが含まれるように設定
        df["label"] = np.array([0, 1] * 50)
        df["future_ret"] = np.random.randn(n_samples) * 0.1

        pipeline = _train_model(df)

        assert isinstance(pipeline, Pipeline)
        # pipelineの構成要素を確認
        assert len(pipeline.steps) == 2
        assert pipeline.steps[0][0] == "scaler"
        assert pipeline.steps[1][0] == "gb"

    def test_train_model_insufficient_data(self):
        """データ不足の場合"""
        # 最小限のデータ（2行のみ）
        data = {
            # 価格特徴量
            "ret_5": [0.01, 0.02],
            "ret_10": [0.02, 0.03],
            "ret_20": [0.03, 0.04],
            "volatility_20": [0.01, 0.015],
            "turnover_norm": [1.0, 1.1],
            # 財務データ
            "NetSales": [1000000, 2000000],
            "OperatingProfit": [100000, 200000],
            "OrdinaryProfit": [90000, 180000],
            "Profit": [80000, 160000],
            "TotalAssets": [5000000, 10000000],
            "Equity": [2000000, 4000000],
            "EquityToAssetRatio": [0.4, 0.4],
            "BookValuePerShare": [1000, 2000],
            "CashFlowsFromOperatingActivities": [150000, 300000],
            "CashFlowsFromInvestingActivities": [-50000, -100000],
            "CashFlowsFromFinancingActivities": [-30000, -60000],
            "label": [0, 1],
            "future_ret": [0.01, 0.02],
        }
        df = pd.DataFrame(data)

        # データが少なすぎても学習は実行される（警告は出る）
        pipeline = _train_model(df)
        assert isinstance(pipeline, Pipeline)


class TestCLI:
    """CLIのテスト"""

    @patch("screening.screen_ml.pickle.dump")
    @patch("screening.screen_ml.open", new_callable=mock_open)
    @patch("screening.screen_ml._train_model")
    @patch("screening.screen_ml._build_dataset")
    @patch("screening.screen_ml._connect")
    @patch("sys.argv", ["screen_ml.py", "train", "--lookback", "365"])
    def test_cli_train(
        self, mock_connect, mock_build, mock_train, mock_file, mock_pickle
    ):
        """学習モードのテスト"""
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        # データセットのモック
        dataset = pd.DataFrame(
            {
                "feature_1": range(100),
                "feature_2": range(100),
                "label": [0, 1] * 50,
                "future_ret": np.random.randn(100) * 0.1,
            }
        )
        mock_build.return_value = dataset

        # モデルのモック
        pipeline = Pipeline(
            [("scaler", StandardScaler()), ("gb", GradientBoostingClassifier())]
        )
        mock_train.return_value = pipeline

        # CLI実行
        cli()

        # モデルが保存されたことを確認
        mock_pickle.assert_called_once()
        # 注: 現在の実装ではcon.close()は呼ばれない

    @patch("screening.screen_ml._merge_features")
    @patch("screening.screen_ml._make_price_features")
    @patch("screening.screen_ml._fetch_stmt")
    @patch("screening.screen_ml._fetch_price")
    @patch("screening.screen_ml.Path")
    @patch("screening.screen_ml.pickle.load")
    @patch("screening.screen_ml.open", new_callable=mock_open)
    @patch("screening.screen_ml._build_dataset")
    @patch("screening.screen_ml._connect")
    @patch("sys.argv", ["screen_ml.py", "screen", "--top", "10"])
    def test_cli_screen(
        self,
        mock_connect,
        mock_build,
        mock_file,
        mock_pickle,
        mock_path_class,
        mock_fetch_price,
        mock_fetch_stmt,
        mock_make_features,
        mock_merge,
    ):
        """スクリーニングモードのテスト"""
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        # パスのモック（モデルファイルが存在する）
        mock_db_path = MagicMock()
        mock_db_path.parent = MagicMock()
        mock_model_path = MagicMock()
        mock_model_path.exists.return_value = True  # モデルが存在
        mock_model_path.parent = MagicMock()
        mock_model_path.parent.mkdir = MagicMock()
        mock_db_path.parent.__truediv__.return_value = mock_model_path
        mock_path_class.return_value = mock_db_path

        # モデルのモック
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array(
            [
                [0.3, 0.7],
                [0.8, 0.2],
                [0.4, 0.6],
            ]
        )
        mock_pickle.return_value = mock_model

        # 価格データのモック
        price_data = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999"],
                "date": pd.to_datetime(["2024-01-10", "2024-01-10", "2024-01-10"]),
                "adj_close": [1000, 2000, 3000],
                "adj_volume": [100000, 200000, 300000],
            }
        )
        mock_fetch_price.return_value = price_data

        # 価格特徴量のモック
        price_features = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999"],
                "date": pd.to_datetime(["2024-01-10", "2024-01-10", "2024-01-10"]),
                "adj_close": [1000, 2000, 3000],
                "ret_5": [0.01, 0.02, 0.03],
                "ret_10": [0.02, 0.03, 0.04],
                "ret_20": [0.03, 0.04, 0.05],
                "volatility_20": [0.01, 0.015, 0.02],
                "turnover_norm": [1.0, 1.1, 1.2],
            }
        )
        mock_make_features.return_value = price_features

        # 財務データのモック
        stmt_data = pd.DataFrame(
            {
                "code": ["1234", "5678", "9999"],
                "DisclosedDate": pd.to_datetime(
                    ["2024-01-01", "2024-01-01", "2024-01-01"]
                ),
                "NetSales": [1000000, 2000000, 3000000],
                "OperatingProfit": [100000, 200000, 300000],
                "OrdinaryProfit": [90000, 180000, 270000],
                "Profit": [80000, 160000, 240000],
                "TotalAssets": [5000000, 10000000, 15000000],
                "Equity": [2000000, 4000000, 6000000],
                "EquityToAssetRatio": [0.4, 0.4, 0.4],
                "BookValuePerShare": [1000, 2000, 3000],
                "CashFlowsFromOperatingActivities": [150000, 300000, 450000],
                "CashFlowsFromInvestingActivities": [-50000, -100000, -150000],
                "CashFlowsFromFinancingActivities": [-30000, -60000, -90000],
            }
        )
        mock_fetch_stmt.return_value = stmt_data

        # マージされたデータのモック
        merged_data = price_features.copy()
        for col in [
            "NetSales",
            "OperatingProfit",
            "OrdinaryProfit",
            "Profit",
            "TotalAssets",
            "Equity",
            "EquityToAssetRatio",
            "BookValuePerShare",
            "CashFlowsFromOperatingActivities",
            "CashFlowsFromInvestingActivities",
            "CashFlowsFromFinancingActivities",
        ]:
            merged_data[col] = stmt_data[col].values
        mock_merge.return_value = merged_data

        # CLI実行（エラーが出ないことを確認）
        try:
            cli()
        except SystemExit:
            pass  # 正常終了

        mock_model.predict_proba.assert_called_once()
        # 注: 現在の実装ではcon.close()は呼ばれない

    @patch("screening.screen_ml._train_model")
    @patch("screening.screen_ml._merge_features")
    @patch("screening.screen_ml._make_price_features")
    @patch("screening.screen_ml._fetch_stmt")
    @patch("screening.screen_ml._fetch_price")
    @patch("screening.screen_ml.pickle.dump")
    @patch("screening.screen_ml.open", new_callable=mock_open)
    @patch("screening.screen_ml._build_dataset")
    @patch("screening.screen_ml._connect")
    @patch("sys.argv", ["screen_ml.py", "screen", "--top", "10"])
    def test_cli_screen_no_model(
        self,
        mock_connect,
        mock_build,
        mock_file,
        mock_pickle_dump,
        mock_fetch_price,
        mock_fetch_stmt,
        mock_make_features,
        mock_merge,
        mock_train,
    ):
        """モデルファイルがない場合"""
        mock_conn = MagicMock(spec=sqlite3.Connection)
        mock_connect.return_value = mock_conn

        # データセットのモック（両方のクラスを含む）
        dataset = pd.DataFrame(
            {
                "code": ["1234", "5678"],
                "ret_5": [0.01, 0.02],
                "ret_10": [0.02, 0.03],
                "ret_20": [0.03, 0.04],
                "volatility_20": [0.01, 0.015],
                "turnover_norm": [1.0, 1.1],
                "NetSales": [1000000, 2000000],
                "OperatingProfit": [100000, 200000],
                "OrdinaryProfit": [90000, 180000],
                "Profit": [80000, 160000],
                "TotalAssets": [5000000, 10000000],
                "Equity": [2000000, 4000000],
                "EquityToAssetRatio": [0.4, 0.4],
                "BookValuePerShare": [1000, 2000],
                "CashFlowsFromOperatingActivities": [150000, 300000],
                "CashFlowsFromInvestingActivities": [-50000, -100000],
                "CashFlowsFromFinancingActivities": [-30000, -60000],
                "label": [0, 1],  # 両方のクラスを含む
                "future_ret": [0.01, 0.06],
            }
        )
        mock_build.return_value = dataset

        # モデルのモック（fitされた状態）
        pipeline = MagicMock(spec=Pipeline)
        pipeline.predict_proba.return_value = np.array([[0.7, 0.3], [0.2, 0.8]])
        mock_train.return_value = pipeline

        # 他の必要なモック（dateカラムを追加）
        price_data = dataset.copy()
        price_data["date"] = pd.to_datetime(["2024-01-10", "2024-01-10"])
        mock_fetch_price.return_value = price_data[
            [
                "code",
                "date",
                "ret_5",
                "ret_10",
                "ret_20",
                "volatility_20",
                "turnover_norm",
            ]
        ]
        mock_make_features.return_value = price_data[
            [
                "code",
                "date",
                "ret_5",
                "ret_10",
                "ret_20",
                "volatility_20",
                "turnover_norm",
            ]
        ]
        mock_fetch_stmt.return_value = dataset.drop(
            [
                "ret_5",
                "ret_10",
                "ret_20",
                "volatility_20",
                "turnover_norm",
                "label",
                "future_ret",
            ],
            axis=1,
        )
        merged_data = dataset.drop(["label", "future_ret"], axis=1)
        merged_data["date"] = pd.to_datetime(["2024-01-10", "2024-01-10"])
        mock_merge.return_value = merged_data

        with patch("screening.screen_ml.Path") as mock_path_class:
            # db_pathのモック
            mock_db_path = MagicMock()
            mock_db_path.parent = MagicMock()

            # model_pathのモック
            mock_model_path = MagicMock()
            mock_model_path.exists.return_value = False
            mock_model_path.parent = MagicMock()
            mock_model_path.parent.mkdir = MagicMock()

            # db_path.parent / MODEL_FNAME の動作をモック
            mock_db_path.parent.__truediv__.return_value = mock_model_path

            mock_path_class.return_value = mock_db_path

            # モデルファイルがない場合は再学習されるため、正常終了する
            try:
                cli()
            except SystemExit:
                pass  # 正常終了
