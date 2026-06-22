# streaks_chrono.py
# !/usr/bin/env python3
"""
streaks_chrono.py

Usage:
    python3 streaks_chrono.py <file.csv> <streak_threshold>

Example:
    python3 streaks_chrono.py EURUSD.csv 5

Input CSV format:
    timestamp;open;high;low;close

Output directory:
    counted_streaks_<input_filename>/

Generated files:
    up_streaks.csv
    down_streaks.csv
    up_streak_summary.csv
    down_streak_summary.csv
    up_streak_hourly_summary.csv
    down_streak_hourly_summary.csv

Definitions:

    Up streak:
        low[i] < low[i+1] < low[i+2] < ...

    Down streak:
        high[i] > high[i+1] > high[i+2] > ...

Only maximal streaks are counted.

Threshold:
    Minimum number of consecutive bars required for a streak
    to be included in the output.
"""

import csv
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


TIMESTAMP_FORMAT = "%Y.%m.%d %H:%M:%S.%f"


def load_bars(csv_path):
    """
    Load bars from CSV.

    Returns:
        list of dicts:
        {
            "timestamp": str,
            "dt": datetime,
            "high": float,
            "low": float
        }
    """
    bars = []

    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        if reader.fieldnames is None:
            raise ValueError("CSV file is empty.")

        required = {"timestamp", "high", "low"}

        missing = required - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"Missing required columns: {', '.join(sorted(missing))}"
            )

        for line_number, row in enumerate(reader, start=2):
            try:
                timestamp_str = row["timestamp"].strip()

                bars.append(
                    {
                        "timestamp": timestamp_str,
                        "dt": datetime.strptime(
                            timestamp_str,
                            TIMESTAMP_FORMAT,
                        ),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                    }
                )

            except Exception as exc:
                raise ValueError(
                    f"Invalid data at line {line_number}: {exc}"
                ) from exc

    return bars


def find_up_streaks(bars, threshold):
    """
    Find maximal streaks where:

        low[i] < low[i+1] < ...

    Returns:
        list of tuples:
            (
                from_timestamp,
                to_timestamp,
                bars_count,
                from_datetime
            )
    """
    streaks = []

    n = len(bars)
    i = 0

    while i < n - 1:
        start = i

        while (
            i < n - 1
            and bars[i]["low"] < bars[i + 1]["low"]
        ):
            i += 1

        end = i

        length = end - start + 1

        if length >= threshold:
            streaks.append(
                (
                    bars[start]["timestamp"],
                    bars[end]["timestamp"],
                    length,
                    bars[start]["dt"],
                )
            )

        if start == i:
            i += 1

    return streaks


def find_down_streaks(bars, threshold):
    """
    Find maximal streaks where:

        high[i] > high[i+1] > ...

    Returns:
        list of tuples:
            (
                from_timestamp,
                to_timestamp,
                bars_count,
                from_datetime
            )
    """
    streaks = []

    n = len(bars)
    i = 0

    while i < n - 1:
        start = i

        while (
            i < n - 1
            and bars[i]["high"] > bars[i + 1]["high"]
        ):
            i += 1

        end = i

        length = end - start + 1

        if length >= threshold:
            streaks.append(
                (
                    bars[start]["timestamp"],
                    bars[end]["timestamp"],
                    length,
                    bars[start]["dt"],
                )
            )

        if start == i:
            i += 1

    return streaks


def save_streaks(output_path, streaks):
    """
    Save chronological streak list.

    Format:
        from,to,bars
    """
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow(["from", "to", "bars"])

        for from_ts, to_ts, bars_count, _ in streaks:
            writer.writerow(
                [
                    from_ts,
                    to_ts,
                    bars_count,
                ]
            )


def save_length_summary(output_path, streaks):
    """
    Summary by streak length.

    Format:
        bars,count
    """
    counter = Counter(
        bars_count
        for _, _, bars_count, _ in streaks
    )

    summary = sorted(
        counter.items(),
        key=lambda x: -x[0]
    )

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow(["bars", "count"])

        for bars_count, occurrences in summary:
            writer.writerow(
                [
                    bars_count,
                    occurrences,
                ]
            )


def save_hourly_summary(output_path, streaks):
    """
    Summary by starting hour.

    Uses streak START TIME.

    Example:
        hour_bucket,count
        00:00-01:00,15
        01:00-02:00,12
        ...
    """
    counter = Counter()

    for _, _, _, start_dt in streaks:
        hour = start_dt.hour

        bucket = (
            f"{hour:02d}:00-"
            f"{(hour + 1) % 24:02d}:00"
        )

        counter[bucket] += 1

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow(
            [
                "hour_bucket",
                "count",
            ]
        )

        for hour in range(24):
            bucket = (
                f"{hour:02d}:00-"
                f"{(hour + 1) % 24:02d}:00"
            )

            writer.writerow(
                [
                    bucket,
                    counter.get(bucket, 0),
                ]
            )


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python3 streaks_chrono.py "
            "<file.csv> <streak_threshold>"
        )
        sys.exit(1)

    input_path = Path(sys.argv[1])

    if not input_path.exists():
        print(f"File not found: {input_path}")
        sys.exit(1)

    if not input_path.is_file():
        print(f"Not a file: {input_path}")
        sys.exit(1)

    try:
        threshold = int(sys.argv[2])

    except ValueError:
        print("Threshold must be an integer.")
        sys.exit(1)

    if threshold < 2:
        print("Threshold must be >= 2.")
        sys.exit(1)

    bars = load_bars(input_path)

    up_streaks = find_up_streaks(
        bars,
        threshold,
    )

    down_streaks = find_down_streaks(
        bars,
        threshold,
    )

    #
    # Chronological ordering
    #
    up_streaks.sort(
        key=lambda x: x[3]
    )

    down_streaks.sort(
        key=lambda x: x[3]
    )

    output_dir = (
        input_path.parent
        / f"counted_streaks_{input_path.stem}"
    )

    output_dir.mkdir(exist_ok=True)

    #
    # Detailed streak lists
    #
    save_streaks(
        output_dir / "up_streaks.csv",
        up_streaks,
    )

    save_streaks(
        output_dir / "down_streaks.csv",
        down_streaks,
    )

    #
    # Summary by streak length
    #
    save_length_summary(
        output_dir / "up_streak_summary.csv",
        up_streaks,
    )

    save_length_summary(
        output_dir / "down_streak_summary.csv",
        down_streaks,
    )

    #
    # Hourly summaries
    #
    save_hourly_summary(
        output_dir / "up_streak_hourly_summary.csv",
        up_streaks,
    )

    save_hourly_summary(
        output_dir / "down_streak_hourly_summary.csv",
        down_streaks,
    )

    print(f"Processed file: {input_path}")
    print(f"Threshold: {threshold}")
    print(f"Output directory: {output_dir}")

    print()

    print(
        f"Up streaks found: {len(up_streaks)}"
    )

    print(
        f"Down streaks found: {len(down_streaks)}"
    )

    print()

    print("Generated files:")

    generated = [
        "up_streaks.csv",
        "down_streaks.csv",
        "up_streak_summary.csv",
        "down_streak_summary.csv",
        "up_streak_hourly_summary.csv",
        "down_streak_hourly_summary.csv",
    ]

    for filename in generated:
        print(f"  - {output_dir / filename}")


if __name__ == "__main__":
    main()
