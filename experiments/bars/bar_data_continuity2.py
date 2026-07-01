#!/usr/bin/env python3
"""
bar_data_continuity2.py — Batch OHLCV bar data continuity checker (v2).

Scans a single CSV file or a directory tree of CSV files (semicolon-delimited,
timestamp;open;high;low;close;volume) and produces ONE combined gap report
covering every file found.

CHANGES vs. bar_data_continuity.py (v1)
----------------------------------------
1. New optional 2nd positional argument: `skip_range`.
   Format: "HH:MM-HH:MM" (spaces around the dash are fine too, e.g.
   "00:00 - 01:00" — just quote it so the shell passes it as one argument).
   Any bar-to-bar pair where *either* timestamp falls inside this daily
   window is fully excluded from gap detection — every day, not just once.
   Handles windows that cross midnight (e.g. "23:00-01:00") automatically.
   Optional — default is "don't skip anything", same as v1's behaviour.
   Positioned right after `input` since you said it'll be the most
   frequently used option.

2. New flag: `--detach-weekends` (default: False).
   - False (default, same spirit as v1): Friday->Sunday/Monday transitions
     (and anything touching Saturday) are auto-detected and classified as
     "weekend" in the report — NOT counted as data problems. This is the
     "we don't take weekends into account [as issues]" behaviour you asked
     for as the default.
   - True: disables that special-casing entirely — weekend transitions are
     treated exactly like any other gap and can land in "unexplained" if
     they exceed the threshold. Use this if you ever want to double-check
     that your weekend-closure assumption actually holds for a given
     instrument/broker.

Everything else — timeframe detection from filename, median-based
fallback, --holidays file, --tolerance, combined single-CSV report,
one-symbol-column-per-row layout — is unchanged from v1.

Usage
-----
    python3 bar_data_continuity2.py <input> [skip_range] [-o report.csv]
                                     [-t tolerance] [--holidays holidays.txt]
                                     [--detach-weekends]

Examples
--------
    python3 bar_data_continuity2.py ~/Desktop/sample_data_2026.06.21/csv
    python3 bar_data_continuity2.py ~/Desktop/sample_data_2026.06.21/csv "00:00-01:00"
    python3 bar_data_continuity2.py ~/Desktop/sample_data_2026.06.21/csv "23:00 - 01:00" -t 2 --detach-weekends
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close"]

# Filename suffix -> bar interval in minutes.
TIMEFRAME_MINUTES: dict[str, int] = {
    "M1": 1, "M5": 5, "M15": 15, "M30": 30,
    "H1": 60, "H4": 240,
    "D1": 1440, "W1": 10080,
}
# Months have non-constant length -> excluded from fixed-interval gap checks.
VARIABLE_TIMEFRAMES = {"MN1"}

TIMEFRAME_RE = re.compile(r"_(M1|M5|M15|M30|H1|H4|D1|W1|MN1)$", re.IGNORECASE)

SKIP_RANGE_RE = re.compile(r"(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})")


@dataclass
class Gap:
    symbol: str
    timeframe: str
    file: Path
    gap_from: datetime
    gap_to: datetime
    gap_duration_minutes: float
    bars_missing: int
    classification: str  # "weekend" | "holiday" | "unexplained"


# ---------------------------------------------------------------------------
# Input discovery
# ---------------------------------------------------------------------------

def get_input_files(path: Path) -> list[Path]:
    """
    Returns list of CSV files. Input can be a single file or a directory
    (scanned recursively, so the whole csv/<SYMBOL>/ tree is picked up in
    one call).
    """
    if not path.exists():
        sys.exit(f"[ERROR] Path does not exist: {path}")

    if path.is_file():
        return [path]

    if path.is_dir():
        files = sorted(path.rglob("*.csv"))
        if not files:
            sys.exit(f"[ERROR] No CSV files found in {path}")
        return files

    sys.exit(f"[ERROR] Unsupported path type: {path}")


def parse_timeframe(file_path: Path) -> tuple[str, int | None]:
    """
    Extract timeframe code from filename (e.g. AUDCAD_M15.csv -> 'M15').
    Returns (timeframe_code, interval_minutes_or_None). interval is None
    when the suffix is unrecognised (-> infer from data) or variable-length
    (monthly bars).
    """
    m = TIMEFRAME_RE.search(file_path.stem.upper())
    if not m:
        return "UNKNOWN", None
    tf = m.group(1).upper()
    if tf in VARIABLE_TIMEFRAMES:
        return tf, None
    return tf, TIMEFRAME_MINUTES.get(tf)


def parse_symbol(file_path: Path) -> str:
    """AUDCAD_M15.csv -> 'AUDCAD'. Falls back to the parent folder name
    (covers the '[SP500]' style folder/file naming seen in the tree)."""
    m = TIMEFRAME_RE.search(file_path.stem.upper())
    if m:
        return file_path.stem[: m.start()]
    return file_path.parent.name


def infer_interval_minutes(timestamps: list[datetime]) -> float:
    """Median consecutive diff (minutes) — robust fallback when the
    filename doesn't carry a recognised timeframe suffix."""
    diffs = sorted(
        (t1 - t0).total_seconds() / 60.0
        for t0, t1 in zip(timestamps, timestamps[1:])
    )
    if not diffs:
        return 0.0
    mid = len(diffs) // 2
    if len(diffs) % 2:
        return diffs[mid]
    return (diffs[mid - 1] + diffs[mid]) / 2


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def load_timestamps(file_path: Path) -> list[datetime]:
    """Read just the timestamp column — that's all continuity checking needs."""
    timestamps: list[datetime] = []
    try:
        with file_path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter=";")
            missing = set(REQUIRED_COLUMNS) - set(reader.fieldnames or [])
            if missing:
                print(f"  [WARN] {file_path}: missing columns {missing}, skipping.", file=sys.stderr)
                return []
            for row in reader:
                try:
                    timestamps.append(datetime.strptime(row["timestamp"].strip(), DATE_FORMAT))
                except (KeyError, ValueError):
                    continue
    except Exception as exc:
        print(f"  [WARN] Cannot read {file_path}: {exc}", file=sys.stderr)
        return []
    timestamps.sort()
    return timestamps


# ---------------------------------------------------------------------------
# Skip-range (hour-of-day exclusion) — new in v2
# ---------------------------------------------------------------------------

def parse_skip_range(raw: str | None) -> tuple[time, time] | None:
    """
    Parse "HH:MM-HH:MM" (spaces around the dash tolerated) into a
    (start_time, end_time) pair. Returns None if raw is empty/None.
    Raises ValueError on bad format.
    """
    if not raw:
        return None
    m = SKIP_RANGE_RE.fullmatch(raw.strip())
    if not m:
        raise ValueError(
            f"Invalid skip-range format '{raw}'. Expected HH:MM-HH:MM, e.g. '00:00-01:00'."
        )
    sh, sm, eh, em = (int(x) for x in m.groups())
    return time(sh, sm), time(eh, em)


def in_skip_window(ts: datetime, skip: tuple[time, time] | None) -> bool:
    """Return True if *ts* falls inside the skip window (crossing midnight OK)."""
    if skip is None:
        return False
    start, end = skip
    t = ts.time()
    if start <= end:
        return start <= t < end
    # crosses midnight
    return t >= start or t < end


# ---------------------------------------------------------------------------
# Calendar-aware classification
# ---------------------------------------------------------------------------

def is_weekend_gap(t0: datetime, t1: datetime) -> bool:
    """
    Heuristic: FX/CFD markets are closed roughly Friday evening -> Sunday
    evening. A gap is treated as an expected weekend closure (not missing
    data) if it starts on Friday and ends on Sunday/Monday, or touches
    Saturday at all. No fixed session hours are assumed — exact open/close
    times differ per instrument/broker and aren't reliably knowable here,
    so this only checks calendar days, not hours.
    """
    if t0.weekday() == 4 and t1.weekday() in (6, 0):  # Fri -> Sun/Mon
        return True
    if t0.weekday() == 5 or t1.weekday() == 5:          # touches Saturday
        return True
    return False


def classify_gap(t0: datetime, t1: datetime, holidays: set[date], detach_weekends: bool) -> str:
    if holidays and any(t0.date() <= d <= t1.date() for d in holidays):
        return "holiday"
    if not detach_weekends and is_weekend_gap(t0, t1):
        return "weekend"
    return "unexplained"


def load_holidays(path: Path | None) -> set[date]:
    """
    Optional file with one ISO date (YYYY-MM-DD) per line — e.g. exchange
    holidays you maintain by hand. Gaps overlapping a listed date get
    classified as 'holiday' instead of 'unexplained'. Without this file,
    holiday gaps simply land in 'unexplained' for manual review, since
    there's no reliable embedded market-holiday calendar to fall back on.
    """
    if path is None:
        return set()
    if not path.exists():
        sys.exit(f"[ERROR] Holidays file does not exist: {path}")
    out: set[date] = set()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                out.add(datetime.strptime(line, "%Y-%m-%d").date())
            except ValueError:
                print(f"  [WARN] Skipping unparsable holiday line: '{line}'", file=sys.stderr)
    return out


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyse_file(
    file_path: Path,
    holidays: set[date],
    tolerance: float,
    skip: tuple[time, time] | None,
    detach_weekends: bool,
) -> list[Gap]:
    """
    Detect gaps in a single bar file. A gap is flagged when the actual
    spacing between two consecutive bars exceeds expected_interval * tolerance.
    Pairs where either endpoint falls inside `skip` are ignored entirely —
    they're not written to the report at all, matching v1's tick-checker
    skip-range behaviour.
    """
    timestamps = load_timestamps(file_path)
    if len(timestamps) < 2:
        if timestamps:
            print(f"  [WARN] {file_path.name}: fewer than 2 rows, skipping.", file=sys.stderr)
        return []

    symbol = parse_symbol(file_path)
    timeframe, interval = parse_timeframe(file_path)

    if timeframe in VARIABLE_TIMEFRAMES:
        print(f"  [INFO] {file_path.name}: '{timeframe}' bars have variable length, gap check skipped.")
        return []

    if interval is None:
        interval = infer_interval_minutes(timestamps)
        if interval <= 0:
            print(f"  [WARN] {file_path.name}: could not determine interval, skipping.", file=sys.stderr)
            return []
        print(f"  [INFO] {file_path.name}: timeframe not in filename, inferred interval = {interval:.1f} min")

    threshold_minutes = interval * tolerance
    gaps: list[Gap] = []

    for t0, t1 in zip(timestamps, timestamps[1:]):
        if in_skip_window(t0, skip) or in_skip_window(t1, skip):
            continue

        delta_minutes = (t1 - t0).total_seconds() / 60.0
        if delta_minutes <= threshold_minutes:
            continue

        gaps.append(Gap(
            symbol=symbol,
            timeframe=timeframe,
            file=file_path,
            gap_from=t0,
            gap_to=t1,
            gap_duration_minutes=delta_minutes,
            bars_missing=max(round(delta_minutes / interval) - 1, 0),
            classification=classify_gap(t0, t1, holidays, detach_weekends),
        ))

    return gaps


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_combined_report(gaps: list[Gap], output: Path) -> None:
    """One file, every gap, across every instrument/timeframe scanned."""
    gaps_sorted = sorted(gaps, key=lambda g: (g.symbol, g.timeframe, g.gap_from))
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow([
            "symbol", "timeframe", "file", "gap_from", "gap_to",
            "gap_duration_minutes", "bars_missing", "classification",
        ])
        for g in gaps_sorted:
            writer.writerow([
                g.symbol,
                g.timeframe,
                g.file,
                g.gap_from.strftime(DATE_FORMAT),
                g.gap_to.strftime(DATE_FORMAT),
                f"{g.gap_duration_minutes:.1f}",
                g.bars_missing,
                g.classification,
            ])


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bar_data_continuity2.py",
        description="Batch OHLCV bar continuity checker (v2) — one combined gap report for a whole folder.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("input", type=Path, help="Single CSV file or a directory (scanned recursively).")
    parser.add_argument(
        "skip_range", nargs="?", default=None,
        help='Optional. Daily hour window to exclude from gap detection, '
             'e.g. "00:00-01:00" (quote it if it contains spaces). Default: none.',
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=None,
        help="Combined report path. Default: <input>/continuity_report.csv",
    )
    parser.add_argument(
        "-t", "--tolerance", type=float, default=1.5,
        help="Gap flagged when actual interval > expected_interval * tolerance. Default: 1.5",
    )
    parser.add_argument(
        "--holidays", type=Path, default=None,
        help="Optional file with one YYYY-MM-DD exchange holiday per line.",
    )
    parser.add_argument(
        "--detach-weekends", action="store_true", default=False,
        help="Default False: weekend transitions (Fri->Sun/Mon, or touching Sat) are "
             "auto-detected and classified as 'weekend', not counted as data problems. "
             "Pass this flag to disable that and treat weekends like any other gap.",
    )
    args = parser.parse_args()

    files = get_input_files(args.input)
    holidays = load_holidays(args.holidays)

    try:
        skip = parse_skip_range(args.skip_range)
    except ValueError as exc:
        sys.exit(f"[ERROR] {exc}")

    output = args.output
    if output is None:
        base = args.input if args.input.is_dir() else args.input.parent
        output = base / "continuity_report.csv"

    print("\n  bar_data_continuity2.py")
    print(f"  {'-' * 46}")
    print(f"  Input           : {args.input}")
    print(f"  Files found     : {len(files)}")
    print(f"  Tolerance       : x{args.tolerance}")
    print(f"  Skip range      : {args.skip_range if skip else '(none)'}")
    print(f"  Detach weekends : {args.detach_weekends}")
    if holidays:
        print(f"  Holidays        : {len(holidays)} dates loaded")
    print(f"  Report          : {output}")
    print()

    all_gaps: list[Gap] = []
    for idx, file_path in enumerate(files, 1):
        label = file_path.relative_to(args.input) if args.input.is_dir() else file_path.name
        print(f"  [{idx}/{len(files)}] {label}")
        all_gaps.extend(analyse_file(file_path, holidays, args.tolerance, skip, args.detach_weekends))

    save_combined_report(all_gaps, output)

    unexplained = [g for g in all_gaps if g.classification == "unexplained"]
    weekend = [g for g in all_gaps if g.classification == "weekend"]
    holiday_gaps = [g for g in all_gaps if g.classification == "holiday"]

    print()
    print("  Results")
    print(f"  {'-' * 46}")
    print(f"  Total gaps found   : {len(all_gaps)}")
    print(f"  - unexplained      : {len(unexplained)}  <- review these first")
    print(f"  - weekend (normal) : {len(weekend)}")
    print(f"  - holiday          : {len(holiday_gaps)}")
    print(f"  Combined report    : {output}")
    print()


if __name__ == "__main__":
    main()
