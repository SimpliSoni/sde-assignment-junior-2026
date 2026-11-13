"""Unit tests for window math, regulatory constraints, and next eligible time calculations."""
from datetime import datetime
import pytest

from solution.config import (
    IST,
    DAYS,
    in_window,
    is_in_calling_window,
    next_open,
    next_eligible_window_open,
)


def make_dt(year=2026, month=11, day=12, hour=10, minute=0, second=0):
    # 2026-11-12 is a Thursday (DAYS[3] == 'thu')
    return datetime(year, month, day, hour, minute, second, tzinfo=IST)


def test_in_window_basic_hours():
    # 10:00 to 19:00
    start = "10:00"
    end = "19:00"
    days = ["thu"]

    # Before window
    assert not in_window(make_dt(hour=9, minute=59, second=59), start, end, days)

    # Exact start
    assert in_window(make_dt(hour=10, minute=0, second=0), start, end, days)

    # Mid window
    assert in_window(make_dt(hour=14, minute=30, second=0), start, end, days)

    # Just before end
    assert in_window(make_dt(hour=18, minute=59, second=59), start, end, days)

    # Exact end (half-open [start, end) -> False)
    assert not in_window(make_dt(hour=19, minute=0, second=0), start, end, days)

    # After end
    assert not in_window(make_dt(hour=19, minute=30, second=0), start, end, days)


def test_in_window_days_filter():
    start = "10:00"
    end = "19:00"
    allowed_days = ["mon", "tue", "wed"]

    # 2026-11-12 is Thursday, not in allowed days
    thu_dt = make_dt(year=2026, month=11, day=12, hour=12, minute=0)
    assert not in_window(thu_dt, start, end, allowed_days)

    # 2026-11-11 is Wednesday, which is allowed
    wed_dt = make_dt(year=2026, month=11, day=11, hour=12, minute=0)
    assert in_window(wed_dt, start, end, allowed_days)


def test_is_in_calling_window_regulatory_intersection():
    # Campaign window allows 08:00 to 22:00, but regulatory window is 09:00 to 21:00
    camp_start = "08:00"
    camp_end = "22:00"
    camp_days = ["thu"]

    # At 08:30 (inside campaign, outside regulatory 09:00-21:00) -> False
    assert not is_in_calling_window(make_dt(hour=8, minute=30), camp_start, camp_end, camp_days)

    # At 09:00 (inside both) -> True
    assert is_in_calling_window(make_dt(hour=9, minute=0), camp_start, camp_end, camp_days)

    # At 20:30 (inside both) -> True
    assert is_in_calling_window(make_dt(hour=20, minute=30), camp_start, camp_end, camp_days)

    # At 21:15 (inside campaign, outside regulatory) -> False
    assert not is_in_calling_window(make_dt(hour=21, minute=15), camp_start, camp_end, camp_days)


def test_next_open_same_day():
    start = "10:00"
    end = "19:00"
    days = ["thu", "fri"]

    # 08:15 Thursday -> next open is 10:00 Thursday
    t = make_dt(day=12, hour=8, minute=15)
    nxt = next_open(t, start, end, days)
    assert nxt.hour == 10 and nxt.minute == 0 and nxt.day == 12

    # 12:00 Thursday (already inside window) -> returns t
    t_inside = make_dt(day=12, hour=12, minute=0)
    assert next_open(t_inside, start, end, days) == t_inside


def test_next_open_next_day_and_skipping_weekend():
    start = "10:00"
    end = "19:00"
    # Campaign only runs mon-fri
    days = ["mon", "tue", "wed", "thu", "fri"]

    # 20:00 on Friday 2026-11-13 -> window closed
    # Saturday 14th and Sunday 15th are not allowed
    # Next open must be Monday 2026-11-16 at 10:00 AM!
    fri_night = make_dt(day=13, hour=20, minute=0)
    nxt = next_open(fri_night, start, end, days)
    assert nxt.day == 16  # Monday
    assert nxt.hour == 10
    assert nxt.minute == 0

