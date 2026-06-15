#!/usr/bin/env python3

import sys
import pandas as pd

try:
    import holidays
except ImportError:
    holidays = None


def detect_timeframe(series: pd.Series) -> pd.Timedelta:
    deltas = series.diff().dropna()
    return deltas.mode()[0]


def is_trading_break(ts, holiday_calendar=None):
    # weekend
    if ts.weekday() >= 5:
        return True

    # święta (opcjonalnie)
    if holiday_calendar is not None:
        if ts.date() in holiday_calendar:
            return True

    return False


def find_gaps(df, expected_delta, holiday_calendar=None):
    gaps = []

    for i in range(1, len(df)):
        prev_ts = df.iloc[i - 1]["timestamp"]
        curr_ts = df.iloc[i]["timestamp"]

        delta = curr_ts - prev_ts

        if delta <= expected_delta:
            continue

        gap_start = prev_ts + expected_delta
        gap_end = curr_ts - expected_delta

        # usuń weekend/święta z początku i końca (opcjonalnie “trim”)
        if is_trading_break(gap_start, holiday_calendar):
            continue
        if is_trading_break(gap_end, holiday_calendar):
            continue

        if gap_start == gap_end:
            gaps.append({"timestamp": gap_start})
        else:
            gaps.append({"from": gap_start, "to": gap_end})

    return gaps


def main(file_path):
    df = pd.read_csv(file_path, sep=";")

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%Y.%m.%d %H:%M:%S"
    )

    df = df.sort_values("timestamp").reset_index(drop=True)

    expected_delta = detect_timeframe(df["timestamp"])

    holiday_calendar = None
    if holidays is not None:
        # możesz zmienić kraj np. "US", "GB", "DE"
        holiday_calendar = holidays.Poland()

    gaps = find_gaps(df, expected_delta, holiday_calendar)

    for g in gaps:
        print(g)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_gaps.py path/to/file.csv")
        sys.exit(1)

    main(sys.argv[1])
