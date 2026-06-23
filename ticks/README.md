# ticks

This directory contains experiments and utilities built directly on top of raw tick data.

Most of the repository operates on time bars, but the purpose of this folder is to go one level deeper. The idea is to work with the original market stream and investigate what happens below the standard one-minute abstraction.

The main motivation is twofold:

* testing strategies directly on tick data,
* experimenting with custom tick bars built from a fixed number of ticks.

Tick bars are particularly interesting because they remove the assumption that time is the natural unit of market activity. Instead of producing a new bar every minute, they produce a new bar every *N ticks*. During active periods bars form quickly, while during quiet periods they take longer to complete. This often reveals structures that are less visible on traditional time-based charts.

Only the scripts that implement the actual research ideas are documented below. Generated outputs and local datasets are intentionally excluded from this documentation and are ignored by Git.

---

## Philosophy

The goal of this folder is not to provide a production-grade framework.

Instead, it serves as a research sandbox for answering questions such as:

* How do fixed-size tick bars behave compared to M1 bars?
* Do certain directional structures emerge more clearly on tick-based charts?
* How often do monotonic price sequences occur?
* Are those sequences distributed uniformly throughout the trading day?
* Can simple signal definitions built on tick bars survive a realistic backtest?

Most scripts are intentionally small and focused on one task. They are designed to be easy to modify and reuse in future experiments.

---

## `iter_ticks_template.py`

A minimal template for iterating through raw tick files.

This script acts as the starting point for new experiments. It implements the repetitive infrastructure that almost every tick-based study requires:

* CSV loading,
* input validation,
* timestamp parsing,
* numeric conversion,
* iteration over ticks,
* result collection,
* exporting outputs.

The example implementation simply calculates spread values:

```python
spread = row.ask - row.bid
```

but the actual logic inside `run_backtest()` is deliberately trivial.

The intention is that whenever a new tick-based idea appears, this template becomes the starting point instead of rewriting the entire data pipeline from scratch.

Typical workflow:

1. Copy the template.
2. Replace `run_backtest()` with the new logic.
3. Save the generated results.

---

## `aggregate_quant.py`

A quantity-based tick aggregator.

This script converts raw ticks into fixed-size tick bars.

Instead of asking:

> "What happened during the next minute?"

it asks:

> "What happened during the next *N trades/ticks*?"

Each generated bar contains exactly the same number of ticks.

Input:

```text
timestamp;bid;ask
```

Output:

```text
timestamp;open;high;low;close
```

where:

* `timestamp` is the first tick in the bar,
* `open` is the first price,
* `high` is the maximum price,
* `low` is the minimum price,
* `close` is the final price.

The price used for aggregation is the midpoint:

```text
mid = (bid + ask) / 2
```

Features:

* accepts either a single CSV file or a directory of CSV files,
* processes multiple files as one continuous stream,
* automatically detects output precision,
* validates input structure,
* emits the final partial bar if the dataset ends before completion.

Example:

```bash
python aggregate_quant.py EURUSD_ticks.csv 20
```

produces:

```text
EURUSD_T20.csv
```

representing 20-tick bars.

This script is effectively the foundation for most experiments in this directory.

---

## Statistical analysis of price movement

The folder also contains scripts focused on describing and measuring directional structures observed in generated bars.

Rather than testing complete trading systems, these scripts attempt to answer questions such as:

* How long do directional sequences persist?
* How frequently do they occur?
* Are they concentrated during particular hours?
* What does the distribution of micro-movements look like?

---

## `count_streaks.py`

Counts monotonic streaks in OHLC data.

Definitions:

### Up streak

A sequence satisfying:

```text
low[i] < low[i+1] < low[i+2] < ...
```

meaning that every subsequent bar forms a higher low.

### Down streak

A sequence satisfying:

```text
high[i] > high[i+1] > high[i+2] > ...
```

meaning that every subsequent bar forms a lower high.

Only **maximal streaks** are counted.

For example:

```text
5 bars in sequence
```

is reported as one 5-bar streak, rather than three overlapping 3-bar streaks.

The default minimum streak length is:

```text
3 bars
```

Outputs include:

* detailed lists of detected streaks,
* summaries of streak-length distributions.

Example questions this script helps answer:

* Are 6-bar directional sequences common?
* How rare are 10-bar runs?
* Is persistence stronger than expected by chance?

---

## `streaks_chrono.py`

An extension of the previous streak analysis.

It uses the same streak definitions but introduces two additional ideas:

* configurable minimum streak thresholds,
* chronological and hourly analysis.

Besides standard streak statistics, it records when those streaks begin.

Additional outputs include:

* hourly distributions of up streaks,
* hourly distributions of down streaks.

This allows investigating whether certain structures cluster during specific trading sessions.

Examples:

* Are long up streaks more common during London?
* Do down streaks tend to start around New York open?
* Is directional persistence evenly distributed across the day?

---

## `momentum1.py`

A simple tick-to-tick momentum classifier.

The script examines every consecutive bid update and classifies it as either positive or negative momentum.

For each tick:

```text
diff = current_bid - previous_bid
```

Positive differences are grouped separately from negative differences.

The output provides:

* detailed lists of positive moves,
* detailed lists of negative moves,
* frequency distributions grouped by movement size.

Movement magnitude is expressed in pips.

This script is useful for quickly exploring questions such as:

* How large are most individual bid changes?
* Is the distribution symmetric?
* Do certain move sizes dominate?

It is intentionally descriptive rather than predictive.

---

## `test_agg.py`

A reference backtester built around tick bars.

This is not intended to be an optimized trading engine.

Its role is to answer a much simpler question:

> "Does this basic tick-bar idea survive contact with actual tick data?"

The strategy works as follows.

### Entry logic

Signal bars are constructed from fixed-size tick bars.

For BUY signals:

```text
low(current) > low(previous)
```

must occur for a specified number of consecutive bars.

For SELL signals:

```text
high(current) < high(previous)
```

must occur for a specified number of consecutive bars.

Only when the required streak length is reached does the strategy attempt to enter.

Spread filtering is applied before opening a position.

---

### Position management

Once a trade is opened, management switches back to M1 bars.

The first incomplete M1 bar after entry is ignored.

Each subsequent M1 bar determines whether the position remains open.

BUY positions remain active while:

```text
close >= open
```

SELL positions remain active while:

```text
close <= open
```

When the condition fails, the trade is closed at the open of the next M1 bar.

This design intentionally separates:

* tick-based entries,
* time-based exits.

---

### Why this approach?

The objective was never to build a complete strategy.

Instead, the idea was to isolate one hypothesis:

> directional structures observed on tick bars may provide useful entries, while simple time-based management may be sufficient for exits.

Whether that hypothesis holds is ultimately an empirical question.

This script exists to test exactly that.

---

## Closing notes

Most ideas in this folder started as small observations made while looking at charts or examining generated datasets.

Some of them will turn out to be dead ends.

Others may evolve into more sophisticated models.

The common theme is simple: before trusting higher-level abstractions, it is worth occasionally returning to the raw stream of market events and asking what the data itself is actually doing.



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