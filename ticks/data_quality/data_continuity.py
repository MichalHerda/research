#!/usr/bin/env python3
"""
data_continuity.py — Tick data continuity checker.

Scans one CSV file or a directory tree of CSV files (semicolon-delimited,
datetime;bid;ask format) and produces:
  • Per-file gap reports (.csv + standalone HTML dashboard)
  • A single combined gap report (.csv) across all files

Usage
-----
    python3 data_continuity.py <input> [threshold_seconds] [skip_range]

Arguments
---------
input               Path to a single .csv file or a directory (scanned
                    recursively for *.csv files).

threshold_seconds   Gaps longer than this value (in seconds, float) are
                    reported as "missing data".  Default: 10.0

skip_range          Optional time window to exclude from gap detection —
                    useful for exchange-closed hours.
                    Format: "HH:MM-HH:MM"  (24-hour clock, no spaces)
                    Examples:
                        "23:00-01:15"   — skip 23:00 tonight to 01:15 next day
                        "16:30-09:00"   — skip from 16:30 to 09:00 next day
                    If the end time is *before* the start time (crosses midnight)
                    the window is treated as spanning midnight automatically.

Output layout
-------------
For a directory input  /path/to/SP500
    /path/to/SP500_csv/         ← individual + combined gap CSVs
    /path/to/SP500_html/        ← individual HTML dashboards

For a single-file input  /path/to/data/[SP500]_2026-06-02.csv
    /path/to/data/[SP500]_2026-06-02_csv/
    /path/to/data/[SP500]_2026-06-02_html/
"""

from __future__ import annotations

import argparse
import csv
import html
import json
# import math
# import os
import re
import sys
from collections import defaultdict
from datetime import datetime, time         # , timedelta
from pathlib import Path
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class Gap(NamedTuple):
    gap_from: datetime
    gap_to: datetime
    gap_duration: float          # seconds


class FileStats(NamedTuple):
    path: Path
    ticks: int
    first_ts: datetime
    last_ts: datetime
    gaps: list[Gap]              # only gaps that exceeded threshold
    near_misses: list[Gap]       # gaps that fit within threshold (top 50)
    bucket_counts: dict[str, int]   # "HH:MM" → tick count per 30-min bucket


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_DT_FORMAT = "%Y.%m.%d %H:%M:%S.%f"


def parse_datetime(raw: str) -> datetime:
    """Parse the custom datetime format used in the source files."""
    return datetime.strptime(raw.strip(), _DT_FORMAT)


def parse_skip_range(raw: str) -> tuple[time, time] | None:
    """
    Parse "HH:MM-HH:MM" into a (start_time, end_time) pair.
    Returns None if raw is empty/None.
    Raises ValueError on bad format.
    """
    if not raw:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})", raw.strip())
    if not m:
        raise ValueError(
            f"Invalid skip-range format '{raw}'.  Expected HH:MM-HH:MM, e.g. '23:00-01:15'."
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
# Core analysis
# ---------------------------------------------------------------------------

def analyse_file(
    path: Path,
    threshold: float,
    skip: tuple[time, time] | None,
) -> FileStats | None:
    """
    Parse a single CSV file and compute gaps + statistics.
    Returns None if the file cannot be processed.
    """
    timestamps: list[datetime] = []

    try:
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter=";")
            for row in reader:
                try:
                    timestamps.append(parse_datetime(row["datetime"]))
                except (KeyError, ValueError):
                    continue
    except Exception as exc:
        print(f"  [WARN] Cannot read {path}: {exc}", file=sys.stderr)
        return None

    if len(timestamps) < 2:
        print(f"  [WARN] {path.name}: fewer than 2 parseable rows, skipping.", file=sys.stderr)
        return None

    timestamps.sort()

    # ----- gap detection ---------------------------------------------------
    gaps: list[Gap] = []
    near_misses: list[Gap] = []

    for i in range(1, len(timestamps)):
        t0, t1 = timestamps[i - 1], timestamps[i]

        # skip pairs that span the excluded window
        if in_skip_window(t0, skip) or in_skip_window(t1, skip):
            continue

        delta = (t1 - t0).total_seconds()

        if delta > threshold:
            gaps.append(Gap(t0, t1, delta))
        elif delta > 0:
            near_misses.append(Gap(t0, t1, delta))

    gaps.sort(key=lambda g: g.gap_from)          # chronological for combined CSV

    near_misses.sort(key=lambda g: g.gap_duration, reverse=True)
    near_misses = near_misses[:50]

    # ----- 30-minute buckets -----------------------------------------------
    bucket_counts: dict[str, int] = defaultdict(int)
    for ts in timestamps:
        bucket_minute = (ts.minute // 30) * 30
        label = f"{ts.hour:02d}:{bucket_minute:02d}"
        bucket_counts[label] += 1

    return FileStats(
        path=path,
        ticks=len(timestamps),
        first_ts=timestamps[0],
        last_ts=timestamps[-1],
        gaps=gaps,
        near_misses=near_misses,
        bucket_counts=dict(sorted(bucket_counts.items())),
    )


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

def write_gap_csv(gaps: list[Gap], out_path: Path) -> None:
    """Write a list of gaps to a CSV file (gap_from, gap_to, gap_duration)."""
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["gap_from", "gap_to", "gap_duration_seconds"])
        for g in gaps:
            writer.writerow([
                g.gap_from.strftime(_DT_FORMAT),
                g.gap_to.strftime(_DT_FORMAT),
                f"{g.gap_duration:.3f}",
            ])


def write_per_file_csv(stats: FileStats, csv_dir: Path) -> None:
    """
    Write per-file gap CSV sorted largest-gap-first
    (spec: "od największej luki do najmniejszej").
    """
    stem = stats.path.stem
    out = csv_dir / f"{stem}_gaps.csv"
    sorted_gaps = sorted(stats.gaps, key=lambda g: g.gap_duration, reverse=True)
    write_gap_csv(sorted_gaps, out)


def write_combined_csv(all_stats: list[FileStats], csv_dir: Path) -> None:
    """Write all gaps across all files in chronological order."""
    all_gaps: list[Gap] = []
    for s in all_stats:
        all_gaps.extend(s.gaps)
    all_gaps.sort(key=lambda g: g.gap_from)
    write_gap_csv(all_gaps, csv_dir / "_combined_gaps.csv")


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _fmt_dur(sec: float) -> str:
    if sec < 60:
        return f"{sec:.3f} s"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{int(m)}m {s:.1f}s"
    h, m = divmod(m, 60)
    return f"{int(h)}h {int(m)}m {s:.0f}s"


def _total_span_seconds(stats: FileStats) -> float:
    return max((stats.last_ts - stats.first_ts).total_seconds(), 1)


def write_html_dashboard(stats: FileStats, html_dir: Path, threshold: float) -> None:
    """Render a self-contained HTML dashboard for a single file."""
    stem = stats.path.stem
    out = html_dir / f"{stem}_dashboard.html"

    span_seconds = _total_span_seconds(stats)
    span_minutes = span_seconds / 60
    avg_tpm = stats.ticks / max(span_minutes, 1)

    # bucket data for chart
    bucket_labels = list(stats.bucket_counts.keys())
    bucket_values = list(stats.bucket_counts.values())

    # per-bucket avg tpm (each bucket is 30 min)
    # bucket_tpm = [round(v / 30, 2) for v in bucket_values]

    chrono_gaps = sorted(stats.gaps, key=lambda g: g.gap_from)
    top_near = stats.near_misses  # already top-50, sorted by duration desc

    def gap_rows(gaps: list[Gap]) -> str:
        rows = []
        for g in gaps:
            rows.append(
                f"<tr>"
                f"<td>{html.escape(_fmt_dt(g.gap_from))}</td>"
                f"<td>{html.escape(_fmt_dt(g.gap_to))}</td>"
                f"<td class='dur'>{html.escape(_fmt_dur(g.gap_duration))}</td>"
                f"</tr>"
            )
        return "\n".join(rows) if rows else "<tr><td colspan='3' class='empty'>No gaps found</td></tr>"

    severity_class = "ok" if not stats.gaps else ("warn" if len(stats.gaps) < 5 else "crit")
    severity_label = "CLEAN" if not stats.gaps else f"{len(stats.gaps)} GAP{'S' if len(stats.gaps) != 1 else ''} DETECTED"

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Continuity Report — {html.escape(stats.path.name)}</title>
<style>
  :root {{
    --bg:       #0d1117;
    --surface:  #161b22;
    --border:   #21262d;
    --accent:   #58a6ff;
    --accent2:  #3fb950;
    --warn:     #d29922;
    --crit:     #f85149;
    --text:     #e6edf3;
    --muted:    #8b949e;
    --mono:     'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
    --sans:     'Inter', 'Segoe UI', system-ui, sans-serif;
    --radius:   6px;
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    font-size: 14px;
    line-height: 1.6;
    padding: 32px 24px;
    max-width: 1100px;
    margin: 0 auto;
  }}

  /* ── header ── */
  .file-header {{
    border-bottom: 1px solid var(--border);
    padding-bottom: 18px;
    margin-bottom: 24px;
  }}
  .file-header h1 {{
    font-family: var(--mono);
    font-size: 18px;
    font-weight: 600;
    color: var(--accent);
    word-break: break-all;
  }}
  .file-header p {{
    color: var(--muted);
    font-size: 12px;
    margin-top: 4px;
  }}

  /* ── status badge ── */
  .badge {{
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .05em;
    margin-top: 10px;
  }}
  .ok   {{ background: rgba(63,185,80,.15);  color: var(--accent2); border: 1px solid rgba(63,185,80,.4); }}
  .warn {{ background: rgba(210,153,34,.15); color: var(--warn);    border: 1px solid rgba(210,153,34,.4); }}
  .crit {{ background: rgba(248,81,73,.15);  color: var(--crit);    border: 1px solid rgba(248,81,73,.4); }}

  /* ── stat cards ── */
  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    gap: 12px;
    margin-bottom: 28px;
  }}
  .card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
  }}
  .card .label {{
    font-size: 11px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .06em;
  }}
  .card .value {{
    font-family: var(--mono);
    font-size: 22px;
    font-weight: 700;
    color: var(--text);
    margin-top: 4px;
  }}
  .card .sub {{
    font-size: 11px;
    color: var(--muted);
    margin-top: 2px;
  }}

  /* ── section headings ── */
  h2 {{
    font-size: 13px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .07em;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding-bottom: 6px;
    margin: 28px 0 14px;
  }}

  /* ── chart container ── */
  .chart-wrap {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px;
    margin-bottom: 6px;
  }}
  canvas {{ width: 100% !important; height: 220px !important; }}

  /* ── tables ── */
  .tbl-wrap {{
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-family: var(--mono);
    font-size: 12px;
  }}
  th {{
    background: var(--surface);
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .05em;
    font-size: 11px;
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}
  td {{
    padding: 7px 12px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: rgba(88,166,255,.05); }}
  td.dur {{ color: var(--warn); font-weight: 600; }}
  td.empty {{ color: var(--muted); text-align: center; padding: 20px; font-family: var(--sans); }}

  /* ── bucket table helper ── */
  .bucket-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 8px;
    margin-bottom: 6px;
  }}
  .bucket-cell {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 8px 12px;
    font-family: var(--mono);
    font-size: 12px;
  }}
  .bucket-cell .btime {{ color: var(--accent); font-weight: 600; }}
  .bucket-cell .bticks {{ color: var(--text); }}
  .bucket-cell .btpm   {{ color: var(--muted); font-size: 11px; }}

  footer {{
    margin-top: 48px;
    color: var(--muted);
    font-size: 11px;
    border-top: 1px solid var(--border);
    padding-top: 14px;
  }}
</style>
</head>
<body>

<div class="file-header">
  <h1>{html.escape(stats.path.name)}</h1>
  <p>Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &nbsp;·&nbsp;
     Threshold: {threshold:.1f} s &nbsp;·&nbsp;
     Range: {html.escape(_fmt_dt(stats.first_ts))} → {html.escape(_fmt_dt(stats.last_ts))}
  </p>
  <span class="badge {severity_class}">{html.escape(severity_label)}</span>
</div>

<!-- ── SUMMARY CARDS ── -->
<h2>Summary</h2>
<div class="cards">
  <div class="card">
    <div class="label">Total ticks</div>
    <div class="value">{stats.ticks:,}</div>
  </div>
  <div class="card">
    <div class="label">Avg ticks / min</div>
    <div class="value">{avg_tpm:.1f}</div>
    <div class="sub">over entire session</div>
  </div>
  <div class="card">
    <div class="label">Gaps detected</div>
    <div class="value" style="color:{'var(--crit)' if stats.gaps else 'var(--accent2)'}">
      {len(stats.gaps)}
    </div>
    <div class="sub">exceeding {threshold:.1f} s</div>
  </div>
  <div class="card">
    <div class="label">Largest gap</div>
    <div class="value" style="color:var(--warn)">
      {_fmt_dur(max((g.gap_duration for g in stats.gaps), default=0))}
    </div>
  </div>
  <div class="card">
    <div class="label">Session span</div>
    <div class="value" style="font-size:16px">{_fmt_dur(span_seconds)}</div>
  </div>
</div>

<!-- ── 30-MIN BUCKET CHART ── -->
<h2>Tick density — 30-minute buckets</h2>
<div class="chart-wrap">
  <canvas id="bucketChart"></canvas>
</div>

<!-- ── BUCKET GRID ── -->
<div class="bucket-grid">
{"".join(
    f'<div class="bucket-cell">'
    f'<span class="btime">{html.escape(lbl)}–{html.escape(_next_bucket(lbl))}</span><br>'
    f'<span class="bticks">{cnt:,} ticks</span><br>'
    f'<span class="btpm">{cnt/30:.1f} t/min avg</span>'
    f'</div>'
    for lbl, cnt in stats.bucket_counts.items()
)}
</div>

<!-- ── GAP LIST ── -->
<h2>Missing data — gaps &gt; {threshold:.1f} s (chronological)</h2>
<div class="tbl-wrap">
<table>
<thead><tr><th>Gap from</th><th>Gap to</th><th>Duration</th></tr></thead>
<tbody>
{gap_rows(chrono_gaps)}
</tbody>
</table>
</div>

<!-- ── NEAR-MISSES ── -->
<h2>Largest sub-threshold gaps (top {len(top_near)} of {len(stats.near_misses) if hasattr(stats, '_raw_near') else "≤50"})</h2>
<div class="tbl-wrap">
<table>
<thead><tr><th>Gap from</th><th>Gap to</th><th>Duration</th></tr></thead>
<tbody>
{gap_rows(top_near)}
</tbody>
</table>
</div>

<footer>
  data_continuity.py &nbsp;·&nbsp;
  Input: {html.escape(str(stats.path))} &nbsp;·&nbsp;
  {stats.ticks:,} ticks analysed
</footer>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"
        integrity="sha512-CQBWl4fJHWbryGE+Pc3UJWW1h3GLBQH5h8g9ALCGfFPaVGgOSOE4gjBCkZ7Ax2p5gOB3J7jEDMHEWrHNAzaA=="
        crossorigin="anonymous" referrerpolicy="no-referrer"></script>
<script>
const ctx = document.getElementById('bucketChart').getContext('2d');
new Chart(ctx, {{
  type: 'bar',
  data: {{
    labels: {json.dumps(bucket_labels)},
    datasets: [{{
      label: 'Ticks',
      data: {json.dumps(bucket_values)},
      backgroundColor: 'rgba(88,166,255,0.55)',
      borderColor:     'rgba(88,166,255,0.9)',
      borderWidth: 1,
      borderRadius: 3,
    }}]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: ctx => `${{ctx.parsed.y.toLocaleString()}} ticks  (${{(ctx.parsed.y/30).toFixed(1)}} t/min)`
        }}
      }}
    }},
    scales: {{
      x: {{
        ticks: {{ color: '#8b949e', font: {{ family: 'monospace', size: 11 }} }},
        grid: {{ color: 'rgba(255,255,255,.04)' }},
      }},
      y: {{
        ticks: {{ color: '#8b949e', font: {{ family: 'monospace', size: 11 }} }},
        grid: {{ color: 'rgba(255,255,255,.06)' }},
      }}
    }}
  }}
}});
</script>
</body>
</html>
"""
    out.write_text(doc, encoding="utf-8")


def _next_bucket(label: str) -> str:
    """'09:30' → '10:00', '23:30' → '00:00'"""
    h, m = (int(x) for x in label.split(":"))
    m += 30
    if m >= 60:
        m = 0
        h = (h + 1) % 24
    return f"{h:02d}:{m:02d}"


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def collect_csv_files(root: Path) -> list[Path]:
    """Recursively collect all *.csv files under root."""
    return sorted(root.rglob("*.csv"))


def output_dirs(input_path: Path) -> tuple[Path, Path]:
    """
    Derive csv_dir and html_dir from the input path.
    For a directory:  parent / name_csv  and  parent / name_html
    For a file:       parent / stem_csv  and  parent / stem_html
    """
    if input_path.is_dir():
        base = input_path.parent / input_path.name
    else:
        base = input_path.parent / input_path.stem
    csv_dir = base.parent / (base.name + "_csv")
    html_dir = base.parent / (base.name + "_html")
    csv_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir, html_dir


# ---------------------------------------------------------------------------
# Progress / logging
# ---------------------------------------------------------------------------

def _bar(done: int, total: int, width: int = 30) -> str:
    filled = int(width * done / max(total, 1))
    return f"[{'█' * filled}{'░' * (width - filled)}] {done}/{total}"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="data_continuity.py",
        description="Tick data continuity checker — detects gaps in OHLCV/tick CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to a single .csv file or a directory (scanned recursively).",
    )
    parser.add_argument(
        "threshold",
        nargs="?",
        type=float,
        default=10.0,
        help="Gap threshold in seconds (default: 10.0).",
    )
    parser.add_argument(
        "skip_range",
        nargs="?",
        default=None,
        help='Time window to skip, e.g. "23:00-01:15". Format: HH:MM-HH:MM (no spaces).',
    )
    args = parser.parse_args()

    # ── validate input ──────────────────────────────────────────────────────
    if not args.input.exists():
        sys.exit(f"ERROR: '{args.input}' does not exist.")

    try:
        skip = parse_skip_range(args.skip_range)
    except ValueError as exc:
        sys.exit(f"ERROR: {exc}")

    # ── collect files ───────────────────────────────────────────────────────
    if args.input.is_file():
        csv_files = [args.input]
    else:
        csv_files = collect_csv_files(args.input)

    if not csv_files:
        sys.exit("ERROR: No .csv files found.")

    csv_dir, html_dir = output_dirs(args.input)

    print("\n  data_continuity.py")
    print(f"  {'─' * 46}")
    print(f"  Input       : {args.input}")
    print(f"  Files found : {len(csv_files)}")
    print(f"  Threshold   : {args.threshold:.1f} s")
    if skip:
        print(f"  Skip window : {skip[0].strftime('%H:%M')} – {skip[1].strftime('%H:%M')}")
    print(f"  CSV output  : {csv_dir}")
    print(f"  HTML output : {html_dir}")
    print()

    # ── process files ───────────────────────────────────────────────────────
    all_stats: list[FileStats] = []

    for idx, path in enumerate(csv_files, 1):
        print(f"  {_bar(idx, len(csv_files))}  {path.name}", end="\r", flush=True)
        stats = analyse_file(path, args.threshold, skip)
        if stats is None:
            continue
        all_stats.append(stats)
        write_per_file_csv(stats, csv_dir)
        write_html_dashboard(stats, html_dir, args.threshold)

    print(f"  {_bar(len(all_stats), len(csv_files))}  done{' ' * 40}")

    # ── combined CSV ────────────────────────────────────────────────────────
    write_combined_csv(all_stats, csv_dir)

    # ── final summary ───────────────────────────────────────────────────────
    total_ticks = sum(s.ticks for s in all_stats)
    total_gaps = sum(len(s.gaps) for s in all_stats)
    files_clean = sum(1 for s in all_stats if not s.gaps)

    print()
    print("  ┌─ Results ──────────────────────────────────┐")
    print(f"  │  Files processed : {len(all_stats):<26}│")
    print(f"  │  Files clean     : {files_clean:<26}│")
    print(f"  │  Total ticks     : {total_ticks:<26,}│")
    print(f"  │  Total gaps      : {total_gaps:<26}│")
    print(f"  │  Combined CSV    : {str(csv_dir / '_combined_gaps.csv')[-26:]:<26}│")
    print("  └────────────────────────────────────────────┘")
    print()


if __name__ == "__main__":
    main()
