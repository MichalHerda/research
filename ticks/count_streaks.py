# count_streaks.py
# !/usr/bin/env python3
"""
count_streaks.py

Usage:
    python3 count_streaks.py <file.csv>

Input CSV format:
    timestamp;open;high;low;close

Example:
    timestamp;open;high;low;close
    2026.06.15 00:05:00.030000;1.1575250000;1.1575250000;1.1572750000;1.1572850000

Output:
    counted_streaks_<input_filename>/
        up_streaks.csv
        down_streaks.csv
        up_streak_summary.csv
        down_streak_summary.csv

Definitions:
    Up streak:
        low[i] < low[i+1] < low[i+2] < ...

    Down streak:
        high[i] > high[i+1] > high[i+2] > ...

Only maximal streaks are counted.
Minimum streak length: 3 bars.
"""

import csv
import sys
from collections import Counter
from pathlib import Path


MIN_STREAK_LENGTH = 3


def load_bars(csv_path):
    """
    Load bars from CSV.

    Returns:
        list of dicts:
            {
                "timestamp": str,
                "high": float,
                "low": float
            }
    """
    bars = []

    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        required = {"timestamp", "high", "low"}

        if reader.fieldnames is None:
            raise ValueError("CSV file is empty.")

        missing = required - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"Missing required columns: {', '.join(sorted(missing))}"
            )

        for line_number, row in enumerate(reader, start=2):
            try:
                bars.append(
                    {
                        "timestamp": row["timestamp"].strip(),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                    }
                )
            except Exception as exc:
                raise ValueError(
                    f"Invalid data at line {line_number}: {exc}"
                ) from exc

    return bars


def find_up_streaks(bars, min_length=MIN_STREAK_LENGTH):
    """
    Find maximal streaks where:

        low[i] < low[i+1] < ...

    Returns:
        list of tuples:
            (
                from_timestamp,
                to_timestamp,
                bar_count
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

        if length >= min_length:
            streaks.append(
                (
                    bars[start]["timestamp"],
                    bars[end]["timestamp"],
                    length,
                )
            )

        if start == i:
            i += 1

    return streaks


def find_down_streaks(bars, min_length=MIN_STREAK_LENGTH):
    """
    Find maximal streaks where:

        high[i] > high[i+1] > ...

    Returns:
        list of tuples:
            (
                from_timestamp,
                to_timestamp,
                bar_count
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

        if length >= min_length:
            streaks.append(
                (
                    bars[start]["timestamp"],
                    bars[end]["timestamp"],
                    length,
                )
            )

        if start == i:
            i += 1

    return streaks


def save_streaks(output_path, streaks):
    """
    Save detailed streak list.

    Format:
        from,to,bars
    """
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow(["from", "to", "bars"])

        for from_ts, to_ts, bars_count in streaks:
            writer.writerow(
                [
                    from_ts,
                    to_ts,
                    bars_count,
                ]
            )


def save_summary(output_path, streaks):
    """
    Save streak length distribution.

    Example:
        bars,count
        13,5
        12,7
        11,3

    Meaning:
        13-bar streaks occurred 5 times.
        12-bar streaks occurred 7 times.
    """
    counter = Counter(
        bars_count
        for _, _, bars_count in streaks
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


def main():
    if len(sys.argv) != 2:
        print(
            "Usage: python3 count_streaks.py <file.csv>"
        )
        sys.exit(1)

    input_path = Path(sys.argv[1])

    if not input_path.exists():
        print(f"File not found: {input_path}")
        sys.exit(1)

    if not input_path.is_file():
        print(f"Not a file: {input_path}")
        sys.exit(1)

    bars = load_bars(input_path)

    up_streaks = find_up_streaks(bars)
    down_streaks = find_down_streaks(bars)

    # longest streaks first
    up_streaks.sort(
        key=lambda x: (-x[2], x[0])
    )

    down_streaks.sort(
        key=lambda x: (-x[2], x[0])
    )

    output_dir = (
        input_path.parent
        / f"counted_streaks_{input_path.stem}"
    )

    output_dir.mkdir(exist_ok=True)

    save_streaks(
        output_dir / "up_streaks.csv",
        up_streaks,
    )

    save_streaks(
        output_dir / "down_streaks.csv",
        down_streaks,
    )

    save_summary(
        output_dir / "up_streak_summary.csv",
        up_streaks,
    )

    save_summary(
        output_dir / "down_streak_summary.csv",
        down_streaks,
    )

    print(f"Processed file: {input_path}")
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

    print(
        f"  - {output_dir / 'up_streaks.csv'}"
    )

    print(
        f"  - {output_dir / 'down_streaks.csv'}"
    )

    print(
        f"  - {output_dir / 'up_streak_summary.csv'}"
    )

    print(
        f"  - {output_dir / 'down_streak_summary.csv'}"
    )


if __name__ == "__main__":
    main()
