#!/usr/bin/env python3

import sys
import os
import pandas as pd


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    return df


def extract_instrument_name(filename):
    """
    classified_plus_EURUSD_M5.csv -> EURUSD
    classified_minus_GOLD_M5.csv -> GOLD
    """
    parts = filename.replace(".csv", "").split("_")

    # usuń prefix classified_plus / classified_minus
    if parts[0] == "classified":
        parts = parts[2:]  # usuwa classified + plus/minus

    return parts[0]


def classify_monthly(df, top_n):
    """
    Grupowanie per miesiąc i wybór top N
    """

    df['year'] = df['period_begin'].dt.year
    df['month'] = df['period_begin'].dt.month

    grouped = df.groupby(['year', 'month'])

    results = {}

    for (year, month), group in grouped:
        # sortujemy po sile (ABS)
        group_sorted = group.sort_values(
            by='extreme_dev_abs',
            ascending=False
        )

        top = group_sorted.head(top_n).copy().reset_index(drop=True)

        results[(year, month)] = top

    return results


def process_file(filepath, output_base_dir, top_n):
    df = load_data(filepath)

    if df.empty:
        return

    filename = os.path.basename(filepath)
    instrument = extract_instrument_name(filename)

    instrument_dir = os.path.join(output_base_dir, instrument)
    os.makedirs(instrument_dir, exist_ok=True)

    monthly_data = classify_monthly(df, top_n)

    for (year, month), data in monthly_data.items():
        output_filename = f"{instrument}_{year}_{month:02d}.csv"
        output_path = os.path.join(instrument_dir, output_filename)

        data.to_csv(output_path, index=False)


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 class_monthly.py <directory> <top_n_per_month>")
        sys.exit(1)

    base_dir = sys.argv[1]
    top_n = int(sys.argv[2])

    if not os.path.exists(base_dir):
        print(f"[ERROR] Missing directory: {base_dir}")
        sys.exit(1)

    output_dir = os.path.join(base_dir, "monthly_classified")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(base_dir) if f.endswith(".csv")]

    for f in files:
        filepath = os.path.join(base_dir, f)

        try:
            process_file(filepath, output_dir, top_n)
            print(f"[OK] {f}")
        except Exception as e:
            print(f"[ERROR] {f}: {e}")


if __name__ == "__main__":
    main()
