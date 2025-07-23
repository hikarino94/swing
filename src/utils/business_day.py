"""
営業日判定ユーティリティ
"""

from datetime import datetime, timedelta

import jpholiday


def is_business_day(date: datetime) -> bool:
    """指定日が営業日かどうかを判定"""
    # 土日判定（0=月曜日, 5=土曜日, 6=日曜日）
    if date.weekday() >= 5:
        return False

    # 祝日判定
    if jpholiday.is_holiday(date):
        return False

    # 年末年始の判定（12/31〜1/3は休業）
    if date.month == 12 and date.day >= 31:
        return False
    if date.month == 1 and date.day <= 3:
        return False

    return True


def get_next_business_day(date: datetime) -> datetime:
    """次の営業日を取得"""
    next_day = date + timedelta(days=1)
    while not is_business_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def adjust_trade_date(trade_datetime: datetime) -> datetime:
    """取引日時を営業日に調整

    Args:
        trade_datetime: 実際の取引日時

    Returns:
        調整後の取引日（営業日）

    Rules:
        - 16時以降の取引は翌営業日扱い
        - 土日祝日の取引は翌営業日扱い
    """
    # 16時以降の場合は翌日扱い
    if trade_datetime.hour >= 16:
        adjusted_date = trade_datetime + timedelta(days=1)
    else:
        adjusted_date = trade_datetime

    # 営業日でない場合は次の営業日に調整
    if not is_business_day(adjusted_date):
        adjusted_date = get_next_business_day(adjusted_date)

    return adjusted_date.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_trade_datetime(datetime_str: str) -> tuple[datetime, datetime]:
    """取引日時文字列から実際の取引日時と調整後の取引日を返す

    Args:
        datetime_str: 取引日時文字列（例: "2024-07-22 18:30:00"）

    Returns:
        (実際の取引日時, 調整後の取引日)
    """
    # 日時文字列をパース
    if " " in datetime_str:
        actual_datetime = datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
    else:
        # 時刻がない場合は日付のみでパース（時刻は00:00:00）
        actual_datetime = datetime.strptime(datetime_str, "%Y/%m/%d")

    # 営業日に調整
    adjusted_date = adjust_trade_date(actual_datetime)

    return actual_datetime, adjusted_date


def get_previous_business_day(date: datetime) -> datetime:
    """前の営業日を取得"""
    prev_day = date - timedelta(days=1)
    while not is_business_day(prev_day):
        prev_day -= timedelta(days=1)
    return prev_day
