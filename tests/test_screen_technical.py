import pandas as pd
from screening.screen_technical import compute_indicators


def test_compute_indicators_basic():
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    df = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "adj_open": range(60),
        "adj_high": [x + 1 for x in range(60)],
        "adj_low": [x - 1 for x in range(60)],
        "adj_close": range(60),
    })
    flags = compute_indicators(df)
    expected_cols = {
        "signal_date",
        "signal_ma",
        "signal_rsi",
        "signal_adx",
        "signal_bb",
        "signal_macd",
        "signals_overheating",
        "signals_count",
    }
    assert expected_cols.issubset(flags.columns)
    assert len(flags) == 60
