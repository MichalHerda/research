#!/usr/bin/env python3

import sys
import os
import pandas as pd
import numpy as np


def load_month_file(filepath):
    df = pd.read_csv(filepath)

    # sanity check
    required_cols = ['extreme_dev_abs']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column {col} in {filepath}")

    return df


def trimmed_mean(values):
    """
    Usuwamy:
    - 10% największych
    - 10% najmniejszych
    Następnie liczymy średnią

    Zaokrąglenie w dół (int)
    """
    if len(values) == 0:
        return np.nan

    values_sorted = np.sort(values)

    n = len(values_sorted)
    trim = int(n * 0.1)

    # jeśli za mało danych → fallback do zwykłej średniej
    if n - 2 * trim <= 0:
        return float(np.mean(values_sorted))

    trimmed = values_sorted[trim:n - trim]

    return float(np.mean(trimmed))


def compute_month_stats(df):
    values = df['extreme_dev_abs'].values

    max_dev = float(np.max(values))
    min_dev = float(np.min(values))
    avg_dev = float(np.mean(values))
    median_dev = trimmed_mean(values)

    return max_dev, min_dev, avg_dev, median_dev


def extract_year_month_from_filename(filename):
    """
    US100_2025_07.csv -> (2025, 7)
    """
    base = filename.replace(".csv", "")
    parts = base.split("_")

    year = int(parts[-2])
    month = int(parts[-1])

    return year, month


def process_instrument(instrument_path, output_dir, instrument_name):
    files = [f for f in os.listdir(instrument_path) if f.endswith(".csv")]

    results = []

    for f in files:
        filepath = os.path.join(instrument_path, f)

        try:
            df = load_month_file(filepath)

            if df.empty:
                continue

            year, month = extract_year_month_from_filename(f)

            max_dev, min_dev, avg_dev, median_dev = compute_month_stats(df)

            results.append({
                "year": year,
                "month": month,
                "max_dev": max_dev,
                "min_dev": min_dev,
                "avg_dev": avg_dev,
                "median_dev": median_dev
            })

        except Exception as e:
            print(f"[ERROR] {instrument_name}/{f}: {e}")

    if not results:
        return

    result_df = pd.DataFrame(results)

    # sortowanie chronologiczne
    result_df = result_df.sort_values(
        by=["year", "month"]
    ).reset_index(drop=True)

    output_path = os.path.join(output_dir, f"{instrument_name}.csv")
    result_df.to_csv(output_path, index=False)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 resume_months.py <directory>")
        sys.exit(1)

    base_dir = sys.argv[1]

    if not os.path.exists(base_dir):
        print(f"[ERROR] Missing directory: {base_dir}")
        sys.exit(1)

    output_dir = os.path.join(base_dir, "monthly_summary")
    os.makedirs(output_dir, exist_ok=True)

    instruments = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]

    for instrument in instruments:
        instrument_path = os.path.join(base_dir, instrument)

        try:
            process_instrument(instrument_path, output_dir, instrument)
            print(f"[OK] {instrument}")
        except Exception as e:
            print(f"[ERROR] {instrument}: {e}")


if __name__ == "__main__":
    main()
