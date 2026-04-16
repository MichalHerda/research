#!/usr/bin/env python3

import os
import sys
import pandas as pd
import itertools

# IMPORT: Korzystamy z poprawionego silnika btzsc2.py
from btzsc2 import load_data, run_backtest, get_instrument, SPREADS

MIN_TRADES = 30


def frange(start, stop, step):
    vals = []
    curr = start
    while (step > 0 and curr <= stop) or (step < 0 and curr >= stop):
        vals.append(round(curr, 4))
        curr += step
    return vals


def generate_grid(direction):
    if direction == "buy":
        dev_vals = frange(0, -4.0, -0.5)
        ema_vals = frange(0, 4.0, 0.5)
    else:
        dev_vals = frange(0, 4.0, 0.5)
        ema_vals = frange(0, -4.0, -0.5)
    rr_vals = frange(1.5, 4.0, 0.5)
    return list(itertools.product(dev_vals, ema_vals, rr_vals))


def process_instrument(filepath, output_dir):
    instrument = get_instrument(filepath)
    spread = SPREADS.get(instrument, 0)

    print(f"\n>>> Processing: {instrument} (Spread: {spread})")

    df_raw = load_data(filepath)
    optimized_data = df_raw.to_dict('records')

    results = []

    for direction in ["buy", "sell"]:
        grid = generate_grid(direction)
        for dev, ema, rr in grid:
            _, tp, sl, wr = run_backtest(
                optimized_data, direction, dev, ema, rr, spread
            )

            total = tp + sl
            if total >= MIN_TRADES:
                win_prob = wr / 100.0
                expectancy = (win_prob * rr) - (1 - win_prob)

                results.append({
                    "symbol": instrument,
                    "dir": direction,
                    "dev": dev,
                    "ema": ema,
                    "rr": rr,
                    "trades": total,
                    "wr": round(wr, 2),
                    "exp": round(expectancy, 3)
                })
        print(f"  Done {direction} tests.")

    if results:
        df_res = pd.DataFrame(results)
        df_res.to_csv(os.path.join(output_dir, f"{instrument}_res.csv"),
                      index=False)
        return df_res
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 batch_btzsc2.py <data_dir>")
        return

    data_dir = sys.argv[1]
    out_dir = "results"
    os.makedirs(out_dir, exist_ok=True)

    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    all_dfs = []

    for f in files:
        res = process_instrument(os.path.join(data_dir, f), out_dir)
        if res is not None:
            all_dfs.append(res)

    if all_dfs:
        final_df = pd.concat(all_dfs).sort_values(by="exp", ascending=False)
        final_df.to_csv("global_ranking.csv", index=False)
        print("\n=== TOP 10 STRATEGIES ===")
        print(final_df.head(10))


if __name__ == "__main__":
    main()
