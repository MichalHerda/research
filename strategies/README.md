# Strategies

This directory contains the repository's bar-based trading research environment.

It includes both strategy implementations and a collection of supporting utilities used throughout the research workflow. The focus of this part of the repository is the development, validation, and iterative refinement of strategies operating on aggregated OHLC price bars.

The directory combines two complementary aspects of the research process:

* preparing and maintaining historical datasets,
* implementing and evaluating trading ideas built on top of bar-based representations of the market.

Further sections describe the supporting utilities first, followed by the strategy implementations themselves.

---

## Directory Structure

```text
strategies/
├── helpers/
├── fractals/
├── fractals_notp/
├── proto_fr/
├── deprecated_versions/
└── requirements.txt
```

---

## Helpers

The `helpers/` directory contains standalone utility scripts supporting the day-to-day workflow of quantitative research.

These tools are not part of the trading logic itself. Instead, they assist with data preparation, validation, maintenance, and dataset management tasks frequently required during experimentation.

### `check_gaps.py`

Detects missing periods in chronological market datasets.

The script automatically infers the expected timeframe from the data and identifies interruptions exceeding the normal interval between observations.

Features include:

* automatic timeframe detection,
* identification of unexpected gaps,
* optional exclusion of weekends,
* optional exclusion of holidays using country calendars,
* reporting of isolated missing observations as well as longer missing intervals.

This utility is primarily used to assess dataset integrity before running research experiments.

---

### `cut_data.py`

Extracts multiple independent time intervals from a dataset.

A separate configuration file defines the periods of interest, allowing researchers to isolate numerous market events in a single execution.

Typical use cases include:

* building event-specific datasets,
* extracting periods associated with particular market conditions,
* preparing focused samples for hypothesis testing,
* constructing reusable benchmark subsets.

The original dataset remains unchanged.

---

### `delete_forbidden.py`

Directory-cleaning utility used to remove predefined groups of instruments from larger collections of market data.

The script recursively deletes first-level subdirectories whose names match a configurable blacklist of symbols.

Typical applications include:

* narrowing the research universe,
* excluding unsupported instruments,
* removing exotic assets,
* filtering out cryptocurrencies, indices, or commodities when focusing exclusively on a target asset class.

Because matching directories are permanently removed, this script should be used with caution.

---

### `merge_new.py`

Incrementally merges newer datasets into existing historical archives.

The utility compares timestamps between an older baseline dataset and a newer update source, appending only observations that extend beyond the current history.

Key characteristics:

* preserves original source directories,
* creates a separate merged output,
* appends only chronologically newer observations,
* copies entirely new files when no previous version exists,
* prevents accidental overwriting of existing merged outputs.

This script simplifies maintaining continuously growing historical datasets.

---

### `slice_tf.py`

Extracts a single continuous time window from a dataset.

Unlike `cut_data.py`, which supports multiple independent intervals, this utility focuses on isolating one specific period defined directly through command-line arguments.

Common applications include:

* generating train/test splits,
* analysing individual market regimes,
* reproducing experiments over identical date ranges,
* creating smaller datasets for rapid iteration and debugging.

The operation is non-destructive and preserves the original source file.

---

## Strategy Implementations

The remaining directories contain the actual trading research built on top of bar-based market representations.

Each strategy directory documents a particular stage in the evolution of the underlying ideas, ranging from exploratory prototypes to more refined implementations.

Further sections describe the individual strategy families, their assumptions, and the bar-based backtesting frameworks used to evaluate them.

*Additional documentation covering the architecture, execution model, and philosophy of the bar-based backtesters will be provided in subsequent sections.*

---

## Proto Fractals (`proto_fr/`)

The `proto_fr/` directory documents one of the earliest attempts to formalize the repository's core fractal-based ideas into executable trading systems.

Rather than representing a finalized strategy family, this directory captures an important transitional phase of the research process: moving from concept validation toward the development of dedicated backtesting infrastructure.

The underlying hypothesis explored throughout this stage is intentionally simple:

* local extrema are used as entry triggers,
* recent market structure defines the initial risk,
* exits are driven primarily by changes in short-term price behaviour rather than large predefined profit targets.

Although the trading rules themselves remained relatively stable, the implementation approach evolved significantly.

---

### `backtest_nonfui.py`

The earliest implementation relied on the external `backtesting.py` framework.

Its primary objective was to answer a straightforward question:

> Can the proposed fractal-based entry and exit logic demonstrate any statistical edge before investing time into building custom infrastructure?

The strategy operates independently for long and short positions.

#### Entry Logic

Entries are generated using a three-bar extremum pattern:

* BUY positions are opened when the middle observation forms a local low,
* SELL positions are opened when the middle observation forms a local high.

This mechanism serves as a simplified approximation of short-term market reversals.

#### Risk Management

The implementation introduced several concepts that later became recurring themes throughout the repository:

* spread-aware execution,
* minimum stop-loss constraints,
* position protection through breakeven adjustments,
* risk normalization using predefined capital exposure assumptions.

Notably, take-profit levels are intentionally set at extremely distant values.

As a consequence, most trades are not closed by reaching their targets. Instead, positions are typically exited either through protective stops or by detecting deterioration in the original market premise.

#### Exit Logic

Positions are closed when short-term momentum weakens.

This is approximated by monitoring changes in candle midpoints and interpreting them as evidence that the initial directional thesis is losing validity.

---

### `new_way.py`

As experimentation progressed, the limitations of generic backtesting frameworks became increasingly apparent.

This implementation represents the first transition toward a fully custom bar-by-bar execution engine.

Instead of adapting ideas to fit the abstractions imposed by external libraries, the objective shifted toward making every assumption explicit and fully controllable.

#### Architectural Principles

Several design choices introduced here later influenced subsequent backtesters throughout the repository:

* explicit state representation,
* chronological processing using sequential iteration,
* separation between infrastructure and strategy rules,
* dataclass-based modelling of trades and market conditions,
* detailed telemetry generation for debugging and validation.

The resulting implementation behaves less like a strategy script and more like a lightweight simulation engine.

#### Strategy Logic

Despite architectural changes, the underlying trading hypothesis remains largely unchanged.

The strategy still relies on:

* local extrema as entry signals,
* structurally derived stop-loss placement,
* signal-based discretionary exits,
* optional breakeven mechanisms.

This continuity allows implementation choices to be evaluated independently from the trading idea itself.

#### Research Motivation

The primary goal of this rewrite was not performance optimisation.

Instead, it aimed to improve:

* transparency,
* reproducibility,
* debuggability,
* confidence in execution assumptions.

The addition of step-by-step telemetry proved particularly valuable, enabling every decision taken by the engine to be reconstructed and audited retrospectively.

---

### Why This Directory Matters

Although these implementations are no longer considered the repository's most mature solutions, they represent an important milestone in the evolution of the research framework.

They capture the transition from:

```text id="l0n2jv"
idea validation
        ↓
framework-assisted prototyping
        ↓
custom execution engines
        ↓
more sophisticated strategy research
```

Understanding this stage provides valuable context for the more refined strategy families described in the following sections.

