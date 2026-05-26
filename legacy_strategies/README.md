# Legacy Strategies & Grid Testing (Vibe-Coded)

This directory serves as a historical archive of my early quantitative research, strategy validation, and automation learning curve. The scripts stored here are **vibe-coded prototypes** generated primarily via iterative prompting with ChatGPT. 

---

## 📌 Purpose of This Directory

The code in this section was created to serve specific educational and analytical milestones:
* **Concept Validation:** Quickly test basic technical indicators (e.g., RSI crossovers, pivot point support/resistance) across multi-timeframe setups.
* **Batch & Grid Testing Architecture:** Explore how to implement multi-parameter grid searches (`itertools.product`) to scan across varying Risk-Reward (RR) ratios and indicator thresholds simultaneously.
* **Reporting & Data Visualization:** Prototype automated HTML report generation featuring embedded performance metrics (Win Rate, Expectancy) and reactive JavaScript charts (`Chart.js`).

---

## ⚠️ Performance & Implementation Disclaimers

> [!IMPORTANT]
> These scripts represent an early phase of development and **contain significant performance bottlenecks** that make them unsuited for production use or heavy high-frequency historical data.

### Known Limitations
* **The `df.iloc` Bottleneck:** The backtesting core utilizes standard row-by-row DataFrame indexing (`df.iloc[i]`). In Pandas, this approach introduces extreme memory allocation overhead by instantiating `Series` objects at every single iteration step.
* **Lack of Event-Driven Abstraction:** Unlike modern event-driven backtesters that process clean streaming states (e.g., streaming ticks or bars), these scripts are tightly coupled to static Pandas matrices.

---

## 📈 Optimization Evolution

This directory exists to highlight the contrast between rapid prototyping and optimized software design. 

For high-performance execution, production-ready architectures, and zero-lookahead-bias pipelines, please refer to the modern implementations in the root directory. Those versions replace these structural limitations with streamlined **Vectorized/NumPy** processing and robust **Event-Driven Execution Engines**.d
