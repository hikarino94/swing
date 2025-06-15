import datetime as dt
import pandas as pd
from fetch.daily_quotes import _daterange, _norm


def test_daterange_weekdays():
    start = dt.date(2024, 1, 1)
    end = dt.date(2024, 1, 10)
    days = _daterange(start, end)
    assert len(days) == 8
    assert all(d.weekday() < 5 for d in days)


def test_norm_columns():
    df = pd.DataFrame({
        "Code": ["1234"],
        "Date": ["2024-01-01"],
        "Open": [100],
        "Close": [110],
    })
    out = _norm(df)
    assert list(out.columns) == ["code", "date", "open", "close"]
    assert out.loc[0, "open"] == 100
