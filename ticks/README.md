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
