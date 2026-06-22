# Algorithmic Trading Framework & Data Engineering Tools

A collection of pragmatic, fast-prototyped scripts focused on historical data manipulation, feature generation, and hypothesis validation. 
Developed as a personal research environment for backtesting systematic trading strategies.

### Key Features:
* **OHLCV Data Manipulation:** Tools for processing, cleaning, and formatting raw candle data from various sources.
* **Feature Engineering:** Scripts dedicated to generating derivative data structures and injecting technical indicators/custom metrics into Pandas DataFrames.
* **Backtesting Engine:** Light-weight scripts for validating historical performance, analyzing time-series data, and testing statistical market hypotheses.

*Note: This repository serves as a functional scratchpad for quantitative research and data pipeline prototyping.*

## Repository Structure

This repository is organized into several directories reflecting different stages of the research workflow. While the root README provides a high-level overview, each subdirectory contains its own documentation describing implementation details and intended usage.

```text
research/
├── strategies/          # Current strategy implementations and ongoing research
├── legacy_strategies/   # Archived prototypes and early experiments
├── templates/           # Reusable boilerplates and utility building blocks
├── scopes/              # Exploratory scripts and hypothesis-specific studies
├── ticks/               # Tick-based strategies and tick aggregation utilities
├── README.md
└── requirements.txt
```

### Where to Look

* **`strategies/`** – Contains actively maintained strategy implementations and more refined research code.
* **`legacy_strategies/`** – Historical archive documenting the evolution of earlier ideas, including experimental and "vibe-coded" prototypes primarily used for learning and concept validation.
* **`templates/`** – Reusable templates for common quantitative research tasks such as OHLCV ingestion, indicator computation, and path-dependent backtesting loops.
* **`scopes/`** – Focused exploratory studies, one-off investigations, and scripts built around validating specific market hypotheses.
* **`ticks/`** – Dedicated workspace for tick-based research. This directory contains strategies operating directly on tick-level data, as well as helper utilities for transforming raw ticks into alternative bar representations. Examples include fixed tick bars and other aggregation schemes used to study how different sampling methods influence market behaviour and strategy performance.

Each directory is intentionally documented separately to provide context, assumptions, and implementation notes relevant to its contents.
