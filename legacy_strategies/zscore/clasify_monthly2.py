#!/usr/bin/env python3

import sys
import os
import pandas as pd


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    return df


def extract_metadata(filename):
    """
    classified_plus_all.csv → (plus, all)
    classified_minus_uptrend_true.csv → (minus, uptrend_true)
    """

    name = filename.replace(".csv", "")
    parts = name.split("_")

    # classified_plus_all
    direction_type = parts[1]      # plus / minus
    subset_type = "_".join(parts[2:])  # all / uptrend_true / uptrend_false

    return direction_type, subset_type


def classify_monthly(df, top_n):
    """
    Grupowanie per miesiąc + TOP N po ABS(extreme_dev_atr)
    """

    df = df.copy()

    df['year'] = df['period_begin'].dt.year
    df['month'] = df['period_begin'].dt.month

    grouped = df.groupby(['year', 'month'])

    results = {}

    for (year, month), group in grouped:
        group_sorted = group.reindex(
            group['extreme_dev_atr'].abs().sort_values(ascending=False).index
        )

        top = group_sorted.head(top_n).reset_index(drop=True)

        results[(year, month)] = top

    return results


def process_instrument_dir(instrument_dir, output_base_dir, top_n):
    instrument_name = os.path.basename(instrument_dir)

    files = [f for f in os.listdir(instrument_dir) if f.endswith(".csv")]

    if not files:
        return

    for f in files:
        filepath = os.path.join(instrument_dir, f)

        df = load_data(filepath)

        if df.empty:
            continue

        direction_type, subset_type = extract_metadata(f)

        monthly_data = classify_monthly(df, top_n)

        for (year, month), data in monthly_data.items():

            output_dir = os.path.join(
                output_base_dir,
                direction_type,       # plus / minus
                instrument_name,
                subset_type           # all / uptrend_true / uptrend_false
            )

            os.makedirs(output_dir, exist_ok=True)

            output_filename = f"{instrument_name}_{year}_{month:02d}.csv"
            output_path = os.path.join(output_dir, output_filename)

            data.to_csv(output_path, index=False)


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 classify_monthly2.py <directory> <top_n_per_month>")
        sys.exit(1)

    base_dir = sys.argv[1]
    top_n = int(sys.argv[2])

    if not os.path.exists(base_dir):
        print(f"[ERROR] Missing directory: {base_dir}")
        sys.exit(1)

    output_dir = os.path.join(base_dir, "monthly_classified")
    os.makedirs(output_dir, exist_ok=True)

    instrument_dirs = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]

    for inst in instrument_dirs:
        inst_path = os.path.join(base_dir, inst)

        try:
            process_instrument_dir(inst_path, output_dir, top_n)
            print(f"[OK] {inst}")
        except Exception as e:
            print(f"[ERROR] {inst}: {e}")


if __name__ == "__main__":
    main()
