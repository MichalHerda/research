# `data_continuity.py` — Tick Data Continuity Checker

A command-line tool for detecting and reporting temporal gaps in tick/quote CSV data.
Works on individual files or entire directory trees and produces both machine-readable
CSV reports and self-contained HTML dashboards.

---

## Features

- **Single-file or recursive directory** processing — point it at one `.csv` or at a
  folder hierarchy and every `.csv` found inside is analysed.
- **Configurable gap threshold** — only gaps longer than `N` seconds are flagged.
- **Skip-window support** — exclude exchange-closed hours (e.g. overnight) so
  time-zone or lunch-break silences don't pollute the report.
- **Per-file CSV reports** — sorted largest-gap-first, ready for further analysis.
- **Combined chronological CSV** — all gaps from all files merged and time-ordered.
- **Self-contained HTML dashboards** — no server required, open in any browser:
  - Summary cards (tick count, average ticks/min, gap count, largest gap)
  - Interactive bar chart of tick density per 30-minute bucket
  - Chronological gap list
  - Top-50 largest *sub-threshold* gaps (near-miss density view)

---

## Input format

Semicolon-delimited CSV with a header row:

```
datetime;bid;ask
2026.06.02 01:00:08.405;7602.11;7602.81
2026.06.02 01:00:09.265;7599.98;7600.68
```

| Column     | Format                          |
|------------|---------------------------------|
| `datetime` | `YYYY.MM.DD HH:MM:SS.mmm`       |
| `bid`      | float                           |
| `ask`      | float                           |

---

## Usage

```bash
python3 data_continuity.py <input> [threshold_seconds] [skip_range]
```

| Argument            | Required | Default | Description |
|---------------------|----------|---------|-------------|
| `input`             | ✓        | —       | Path to a `.csv` file **or** a directory |
| `threshold_seconds` |          | `10.0`  | Gaps ≥ this many seconds are reported |
| `skip_range`        |          | none    | Time window to ignore — see below |

### Examples

```bash
# Single file, default 10-second threshold
python3 data_continuity.py data/[SP500]_2026-06-02.csv

# Whole directory, 5-second threshold
python3 data_continuity.py data/SP500/ 5.0

# Directory + skip overnight hours (23:00 to 01:15 next day)
python3 data_continuity.py data/SP500/ 10.0 "23:00-01:15"

# Directory + skip US equity off-hours (16:30 to 09:30 next day)
python3 data_continuity.py data/SP500/ 10.0 "16:30-09:30"
```

### `skip_range` format

```
HH:MM-HH:MM
```

- 24-hour clock, **no spaces**, hyphen as separator.
- If the end time is earlier than the start time, the window is assumed to
  **cross midnight** automatically — no special flag needed.

| Example          | Meaning                                             |
|------------------|-----------------------------------------------------|
| `23:00-01:15`    | Skip 23:00 → 01:15 the following day (crosses midnight) |
| `16:30-09:30`    | Skip 16:30 → 09:30 the following day                |
| `12:00-13:00`    | Skip noon break (same day)                          |

---

## Output layout

All output is written to sibling folders **next to the input**, never inside it,
so source files are never touched.

### Directory input: `data/SP500/`

```
data/
├── SP500/                          ← your source files (untouched)
│   ├── [SP500]_2026-06-02.csv
│   └── ...
├── SP500_csv/                      ← gap CSV reports
│   ├── [SP500]_2026-06-02_gaps.csv
│   ├── [SP500]_2026-06-03_gaps.csv
│   └── _combined_gaps.csv          ← all gaps, all files, chronological
└── SP500_html/                     ← HTML dashboards
    ├── [SP500]_2026-06-02_dashboard.html
    └── ...
```

### Single-file input: `data/[SP500]_2026-06-02.csv`

```
data/
├── [SP500]_2026-06-02.csv           ← your source file (untouched)
├── [SP500]_2026-06-02_csv/
│   ├── [SP500]_2026-06-02_gaps.csv
│   └── _combined_gaps.csv
└── [SP500]_2026-06-02_html/
    └── [SP500]_2026-06-02_dashboard.html
```

### Gap CSV columns

| Column                  | Description                              |
|-------------------------|------------------------------------------|
| `gap_from`              | Timestamp of the last tick before the gap |
| `gap_to`                | Timestamp of the first tick after the gap |
| `gap_duration_seconds`  | Gap length in seconds (3 decimal places)  |

- **Per-file CSVs** are sorted *largest gap first*.
- **`_combined_gaps.csv`** is sorted *chronologically* across all files.

---

## HTML dashboard sections

Each dashboard is a single self-contained `.html` file (no server, no install).

| Section | Content |
|---------|---------|
| **Header** | File name, generation time, threshold, session range, and a colour-coded status badge (`CLEAN` / `N GAPS DETECTED`) |
| **Summary cards** | Total ticks · Average ticks/min · Gap count · Largest gap · Session span |
| **30-min bucket chart** | Interactive bar chart (Chart.js CDN) — tick count per half-hour window |
| **Bucket detail grid** | Each 30-min window: tick count + average ticks/min |
| **Gap list** | All threshold-exceeding gaps in chronological order |
| **Near-miss list** | Top 50 *largest sub-threshold* gaps — useful for spotting thin liquidity periods |

---

## Requirements

- Python 3.8+
- Standard library only (`csv`, `argparse`, `datetime`, `pathlib`, `json`, `html`, …)
- No external packages required

---

## Notes

- Rows with unparseable `datetime` values are silently skipped with a warning to stderr.
- Files with fewer than 2 parseable rows are skipped.
- The tool is safe to re-run — output folders are created if missing and files are overwritten.
- Timestamps within a file are **sorted before analysis**, so out-of-order ticks in a
  source file do not create false gaps.
