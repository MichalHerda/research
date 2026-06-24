#!/usr/bin/env python3

"""
agg_quant.py

Aggregate MT4 tick data into fixed-size tick OHLC bars.

Input format (semicolon-separated):

datetime;bid;ask
2026.06.23 01:00:08.085;7483.95;7484.65
2026.06.23 01:00:08.324;7484.07;7484.77
...

Only BID prices are used.

Output format:

timestamp;open;high;low;close

The timestamp corresponds to the first tick of the aggregated bar.
Incomplete trailing bars are discarded.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Iterable, Iterator, List, Tuple

INPUT_HEADER = "datetime"
OUTPUT_HEADER = ["timestamp", "open", "high", "low", "close"]


Tick = Tuple[str, float]
Bar = Tuple[str, float, float, float, float]


def parse_arguments() -> Tuple[Path, int, Path]:
    """
    Parse and validate CLI arguments.

    Usage:
        python3 agg_quant.py <file_or_directory> <ticks_per_bar> <output_dir>
    """
    if len(sys.argv) != 4:
        print(
            "Usage:\n"
            "    python3 agg_quant.py "
            "<file_or_directory> <ticks_per_bar> <output_dir>",
            file=sys.stderr,
        )
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[3])

    try:
        ticks_per_bar = int(sys.argv[2])

        if ticks_per_bar <= 0:
            raise ValueError()

    except ValueError:
        print(
            "Error: <ticks_per_bar> must be a positive integer.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not input_path.exists():
        print(
            f"Error: Input path does not exist: {input_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    return input_path, ticks_per_bar, output_dir


def discover_input_files(input_path: Path) -> List[Path]:
    """
    Resolve the list of CSV files to process.

    If the input path is a file, return that file.
    If it is a directory, return all CSV files sorted by filename.
    """
    if input_path.is_file():
        return [input_path]

    files = sorted(
        f for f in input_path.iterdir()
        if f.is_file() and f.suffix.lower() == ".csv"
    )

    if not files:
        raise FileNotFoundError(
            f"No CSV files found in directory: {input_path}"
        )

    return files


def read_ticks(file_path: Path) -> Iterator[Tick]:
    """
    Read MT4 tick data from a CSV file.

    Only the timestamp and BID price are returned.

    Expected input format:
        datetime;bid;ask
    """
    with file_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")

        required_columns = {"datetime", "bid"}

        if reader.fieldnames is None:
            raise ValueError(f"{file_path}: missing CSV header.")

        missing = required_columns - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"{file_path}: missing required columns: "
                f"{', '.join(sorted(missing))}"
            )

        for line_number, row in enumerate(reader, start=2):
            try:
                timestamp = row["datetime"]
                bid = float(row["bid"])

            except (KeyError, ValueError) as exc:
                raise ValueError(
                    f"{file_path}: invalid data at line "
                    f"{line_number}"
                ) from exc

            yield timestamp, bid


def aggregate_ticks(
    ticks: Iterable[Tick],
    ticks_per_bar: int,
) -> Iterator[Bar]:
    """
    Aggregate ticks into fixed-size tick bars.

    The output timestamp corresponds to the first tick
    of each aggregated bar.

    Incomplete trailing bars are discarded.
    """
    buffer: List[Tick] = []

    for tick in ticks:
        buffer.append(tick)

        if len(buffer) < ticks_per_bar:
            continue

        timestamp = buffer[0][0]

        prices = [price for _, price in buffer]

        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]

        yield (
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
        )

        buffer.clear()


def build_output_path(
    input_file: Path,
    output_dir: Path,
    ticks_per_bar: int,
) -> Path:
    """
    Construct the output filename.

    Example:
        [SP500]_2026-06-23.csv

    becomes:

        [SP500]_2026-06-23_agg_5.csv
    """
    filename = (
        f"{input_file.stem}_agg_{ticks_per_bar}.csv"
    )

    return output_dir / filename


def write_bars(
    bars: Iterable[Bar],
    output_file: Path,
) -> int:
    """
    Write aggregated OHLC bars to disk.

    Returns:
        Number of bars written.
    """
    count = 0

    with output_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.writer(handle, delimiter=";")

        writer.writerow(OUTPUT_HEADER)

        for bar in bars:
            writer.writerow(
                [
                    bar[0],
                    f"{bar[1]:.10f}",
                    f"{bar[2]:.10f}",
                    f"{bar[3]:.10f}",
                    f"{bar[4]:.10f}",
                ]
            )

            count += 1

    return count


def process_file(
    input_file: Path,
    output_dir: Path,
    ticks_per_bar: int,
) -> None:
    """
    Process a single CSV file.
    """
    output_file = build_output_path(
        input_file,
        output_dir,
        ticks_per_bar,
    )

    ticks = read_ticks(input_file)
    bars = aggregate_ticks(
        ticks,
        ticks_per_bar,
    )

    count = write_bars(
        bars,
        output_file,
    )

    print(
        f"[OK] {input_file.name} "
        f"-> {output_file.name} "
        f"({count} bars)"
    )


def main() -> None:
    """
    Program entry point.
    """
    input_path, ticks_per_bar, output_dir = parse_arguments()

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        files = discover_input_files(input_path)

    except Exception as exc:
        print(
            f"Error: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    for file_path in files:
        try:
            process_file(
                file_path,
                output_dir,
                ticks_per_bar,
            )

        except Exception as exc:
            print(
                f"[ERROR] {file_path.name}: {exc}",
                file=sys.stderr,
            )

    print("Done.")


if __name__ == "__main__":
    main()
