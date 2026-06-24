#!/usr/bin/env python3

"""
slice_period.py

Generic intraday slicer for time-series CSV data.

This tool works with both:
- raw MT4 tick data (datetime;bid;ask)
- OHLC tick bars (timestamp;open;high;low;close)
- any other structured CSV dataset containing a time column

The script filters rows by intraday time window per day:

    start_time <= time < end_time

Supported time column names:
    - datetime
    - timestamp

No other assumptions are made about the dataset.

Input files are never modified.
Output is written into a new directory preserving file structure.
"""

from __future__ import annotations

import csv
import re
import sys
from datetime import datetime, time
from pathlib import Path
from typing import List, Tuple

TIME_FORMATS = [
    "%Y.%m.%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y.%m.%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
]

PERIOD_PATTERN = re.compile(
    r"^\s*(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})\s*$"
)

TIME_COLUMNS = ("datetime", "timestamp")


def parse_arguments() -> Tuple[Path, str]:
    """
    CLI usage:
        python3 slice_period.py <file_or_directory> "<HH:MM - HH:MM>"
    """
    if len(sys.argv) != 3:
        print(
            "Usage:\n"
            '    python3 slice_period.py <file_or_directory> "<HH:MM - HH:MM>"',
            file=sys.stderr,
        )
        sys.exit(1)

    input_path = Path(sys.argv[1])
    period = sys.argv[2]

    if not input_path.exists():
        print(
            f"Error: Input path does not exist: {input_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    return input_path, period


def parse_period(period: str) -> Tuple[time, time]:
    """
    Parse intraday window.

    Format:
        HH:MM - HH:MM

    Semantics:
        start_time <= t < end_time
    """
    match = PERIOD_PATTERN.match(period)

    if not match:
        raise ValueError(
            'Invalid period format. Expected "HH:MM - HH:MM"'
        )

    start_str, end_str = match.groups()

    start_time = datetime.strptime(start_str, "%H:%M").time()
    end_time = datetime.strptime(end_str, "%H:%M").time()

    if start_time >= end_time:
        raise ValueError("start_time must be earlier than end_time")

    return start_time, end_time


def discover_input_files(path: Path) -> List[Path]:
    """
    Accept single CSV file or directory of CSV files.
    """
    if path.is_file():
        return [path]

    files = sorted(
        f for f in path.iterdir()
        if f.is_file() and f.suffix.lower() == ".csv"
    )

    if not files:
        raise FileNotFoundError(f"No CSV files in {path}")

    return files


def detect_time_column(fieldnames: List[str]) -> str:
    """
    Detect supported time column.

    Allowed:
        - datetime
        - timestamp
    """
    for col in TIME_COLUMNS:
        if col in fieldnames:
            return col

    raise ValueError(
        "No valid time column found. "
        "Expected one of: datetime, timestamp"
    )


def parse_datetime(value: str) -> datetime:
    """
    Parse timestamp using multiple common MT/ET formats.
    """
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unrecognized datetime format: {value}")


def build_output_dir(input_path: Path, period: str) -> Path:
    """
    Create deterministic output directory name.
    """
    safe = period.replace(" ", "").replace(":", "-").replace("-", "_", 1)

    base = input_path.stem if input_path.is_file() else input_path.name

    return input_path.parent / f"{base}_{safe}"


def slice_file(
    input_file: Path,
    output_file: Path,
    start_time: time,
    end_time: time,
) -> int:
    """
    Filter rows by intraday window.

    Returns number of written rows.
    """
    written = 0

    with input_file.open("r", newline="", encoding="utf-8") as src, \
         output_file.open("w", newline="", encoding="utf-8") as dst:

        reader = csv.DictReader(src, delimiter=";")

        if not reader.fieldnames:
            raise ValueError(f"{input_file}: missing header")

        time_col = detect_time_column(reader.fieldnames)

        writer = csv.DictWriter(
            dst,
            fieldnames=reader.fieldnames,
            delimiter=";",
        )
        writer.writeheader()

        for i, row in enumerate(reader, start=2):
            try:
                dt = parse_datetime(row[time_col])
            except Exception as exc:
                raise ValueError(
                    f"{input_file}: bad datetime at line {i}"
                ) from exc

            t = dt.time()

            if start_time <= t < end_time:
                writer.writerow(row)
                written += 1

    return written


def main() -> None:
    input_path, period = parse_arguments()

    try:
        start, end = parse_period(period)
        files = discover_input_files(input_path)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    output_dir = build_output_dir(input_path, period)
    output_dir.mkdir(parents=True, exist_ok=True)

    for file in files:
        out_file = output_dir / file.name

        try:
            n = slice_file(file, out_file, start, end)
            print(f"[OK] {file.name} -> {n} rows")
        except Exception as exc:
            print(f"[ERROR] {file.name}: {exc}", file=sys.stderr)

    print(f"Done. Output: {output_dir}")


if __name__ == "__main__":
    main()
