#!/usr/bin/env python3

import sys
import os
import pandas as pd


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    return df


def prepare_output(df):
    """
    Utrzymujemy tylko wymagane kolumny i kolejność
    """
    return df[[
        "period_begin",
        "period_end",
        "extreme_dev_atr",
        "extreme_dev",
        "duration",
        "EMA_diff",
        "uptrend"
    ]].copy()


def sort_by_abs_extreme(df):
    """
    Sortowanie po ABS(extreme_dev_atr) malejąco
    """
    return df.reindex(
        df['extreme_dev_atr'].abs().sort_values(ascending=False).index
    ).reset_index(drop=True)


def classify_sets(df):
    """
    Tworzy 3 zestawy:
    1. wszystkie UP
    2. UP + uptrend True
    3. UP + uptrend False
    """

    df_up = df[df['direction'] == 'UP'].copy()

    if df_up.empty:
        return None

    sets = {
        "all": df_up,
        "uptrend_true": df_up[df_up['uptrend'] == True],            # noqa
        "uptrend_false": df_up[df_up['uptrend'] == False]           # noqa
    }

    result = {}

    for key, subset in sets.items():
        if subset.empty:
            continue

        subset = sort_by_abs_extreme(subset)
        subset = prepare_output(subset)

        result[key] = subset

    return result


def process_file(filepath, output_base_dir):
    df = load_data(filepath)

    required_cols = {
        'direction',
        'extreme_dev_atr',
        'extreme_dev',
        'duration',
        'EMA_diff',
        'uptrend'
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    classified = classify_sets(df)

    if not classified:
        return

    filename = os.path.basename(filepath)

    # np. swings_US100_H1.csv → US100_H1
    instrument_name = filename.replace("swings_", "").replace(".csv", "")

    instrument_dir = os.path.join(output_base_dir, instrument_name)
    os.makedirs(instrument_dir, exist_ok=True)

    for key, data in classified.items():
        output_filename = f"classified_plus_{key}.csv"
        output_path = os.path.join(instrument_dir, output_filename)

        data.to_csv(output_path, index=False)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 classify_atr_plus.py <directory>")
        sys.exit(1)

    base_dir = sys.argv[1]

    if not os.path.exists(base_dir):
        print(f"[ERROR] Missing directory: {base_dir}")
        sys.exit(1)

    output_dir = os.path.join(base_dir, "classified_atr_plus")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(base_dir) if f.endswith(".csv")]

    for f in files:
        filepath = os.path.join(base_dir, f)

        try:
            process_file(filepath, output_dir)
            print(f"[OK] {f}")
        except Exception as e:
            print(f"[ERROR] {f}: {e}")


if __name__ == "__main__":
    main()
