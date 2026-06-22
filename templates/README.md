# Templates

This directory contains reusable boilerplates, utility scripts, and reference implementations extracted from recurring research workflows.

Unlike the strategy directories, the goal of these files is not to provide complete trading systems, but to reduce the amount of repetitive setup required when prototyping new ideas.

## Purpose

Most quantitative experiments follow similar stages:

1. Loading and validating historical market data.
2. Computing derived features and indicators.
3. Iterating through time-series observations.
4. Recording results and exporting them for further analysis.

The templates collected here provide minimal implementations of these stages so that new hypotheses can be tested quickly without rewriting infrastructure code from scratch.

---

## Directory Contents

### `backtest_tpl.py`

A reusable boilerplate for implementing **path-dependent backtests**.

This template is specifically designed for strategies whose behaviour depends on historical state transitions and cannot be safely vectorized using pure Pandas or NumPy operations.

Examples include:

* trailing stop management,
* breakeven logic,
* staged entries and exits,
* position state machines,
* time-dependent execution constraints.

Key characteristics:

* Explicit `Trade` and `MarketState` data models implemented with dataclasses.
* Chronological iteration using `DataFrame.itertuples()`.
* Placeholder hooks for custom strategy logic:

  * `is_signal()`
  * `calculate_stop_loss()`
  * `calculate_take_profit()`
  * `is_stop_loss_reached()`
  * `is_take_profit_reached()`
  * `is_breakeven_condition()`
* Built-in trade lifecycle management.
* Conservative execution assumptions:
  if both Stop Loss and Take Profit are touched within the same candle, Stop Loss is processed first to avoid optimistic bias.
* CSV export of completed trade logs.

The template intentionally separates infrastructure from strategy-specific decision logic, making it suitable as a starting point for new backtesting experiments.

---

### `indicators.py`

A small collection of reusable market indicators and helper computations frequently used throughout the repository.

Currently included:

#### Trend & Volatility

* `compute_ema()` – Exponential Moving Average.
* `compute_atr()` – Average True Range.

#### Normalized Price Behaviour

* `compute_ema_deviation()` – ATR-normalized distance between price extremes and a reference EMA.
* `compute_slope()` – ATR-adjusted EMA slope calculation.

#### Market Structure

* `compute_fractals()` – Standard Bill Williams-style fractal detection, including forward-filled last confirmed fractal levels.
* `compute_proto_fractals()` – Experimental variation of fractal identification used during exploratory studies.

The implementations prioritize readability and reusability over micro-optimizations.

---

### `read_ohlcv_tpl.py`

Minimal OHLCV ingestion template.

Responsibilities:

* loading semicolon-separated CSV files,
* validating required columns,
* timestamp parsing,
* numeric conversion,
* basic data cleaning.

Useful whenever a new experiment requires a standardized market data loading pipeline.

Required columns:

```
timestamp
open
high
low
close
```

---

### `read_test_write_tpl.py`

A lightweight end-to-end pipeline example demonstrating the complete research workflow:

```
Read → Validate → Iterate → Collect → Export
```

The script illustrates how to:

* load market data,
* traverse observations chronologically,
* collect results during iteration,
* save outputs back to CSV.

It is intended primarily as a teaching aid and as the smallest possible starting point for new prototypes.

---

## Philosophy

These templates exist to optimize researcher time rather than execution speed.

The emphasis is on:

* clarity,
* explicit state handling,
* reproducibility,
* rapid experimentation.

Once an idea proves useful, it can later be migrated into more specialized and optimized implementations elsewhere in the repository.
