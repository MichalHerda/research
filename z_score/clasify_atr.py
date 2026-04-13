#!/usr/bin/env python3

import sys
import os
import pandas as pd


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    return df


def classify(df):
    """
    Sortowanie swingów:
    od największego deviation względem ATR
    """
    df_sorted = df.sort_values(
        by="max_dev_atr_pct",
        ascending=False
    ).reset_index(drop=True)

    # opcjonalnie: ranking
    df_sorted['rank'] = df_sorted.index + 1

    return df_sorted


def process_file(filepath, output_dir):
    df = load_data(filepath)

    if df.empty:
        return

    df_classified = classify(df)

    filename = os.path.basename(filepath)
    output_filename = filename.replace("swings_", "classified_")

    output_path = os.path.join(output_dir, output_filename)

    df_classified.to_csv(output_path, index=False)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 classify_atr.py <directory>")
        sys.exit(1)

    base_dir = sys.argv[1]

    # zakładamy, że input to folder ze swingami
    input_dir = base_dir

    if not os.path.exists(input_dir):
        print(f"[ERROR] Missing directory: {input_dir}")
        sys.exit(1)

    output_dir = os.path.join(base_dir, "classified_atr")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

    for f in files:
        filepath = os.path.join(input_dir, f)

        try:
            process_file(filepath, output_dir)
            print(f"[OK] {f}")
        except Exception as e:
            print(f"[ERROR] {f}: {e}")


if __name__ == "__main__":
    main()
