#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
classify_regimes.py

Program:
- ładuje plik wygenerowany przez calc_zscore.py
- tworzy folder classify/
- tworzy osobne foldery dla każdego regime
- zapisuje osobne klasyfikacje CSV:
    - low ascending
    - high descending
    - duration descending

Usage:
python3 classify_regimes.py <file>

Przykład:
python3 classify_regimes.py EURUSD_M5_regimes.csv
"""

import os
import sys
import pandas as pd


# ============================================================
# Regime enum
# ============================================================

REGIMES = [
    'BULL_UP',
    'BULL_DOWN',
    'BULL_RANGE',

    'BEAR_UP',
    'BEAR_DOWN',
    'BEAR_RANGE',

    'UNDEFINED'
]


# ============================================================
# Load CSV
# ============================================================

def load_data(file_path):

    df = pd.read_csv(file_path, sep=';')

    required_cols = [
        'start',
        'end',
        'type',
        'duration',
        'average',
        'high',
        'low'
    ]

    for col in required_cols:

        if col not in df.columns:
            raise Exception(f"Missing required column: {col}")

    return df


# ============================================================
# Save classification
# ============================================================

def save_classification(df, output_file):

    df.to_csv(
        output_file,
        sep=';',
        index=False,
        float_format='%.8f'
    )

    print(f"[OK] Saved: {output_file}")


# ============================================================
# Main
# ============================================================

def main():

    if len(sys.argv) != 2:

        print(
            "Usage:\n"
            "python3 classify_regimes.py <file>"
        )

        sys.exit(1)

    file_path = sys.argv[1]

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    if not os.path.isfile(file_path):

        print(f"[ERROR] File does not exist: {file_path}")
        sys.exit(1)

    # --------------------------------------------------------
    # Load
    # --------------------------------------------------------

    print(f"[INFO] Loading: {file_path}")

    df = load_data(file_path)

    # --------------------------------------------------------
    # Output directory
    # --------------------------------------------------------

    base_dir = os.path.dirname(
        os.path.abspath(file_path)
    )

    classify_dir = os.path.join(
        base_dir,
        'classify'
    )

    os.makedirs(
        classify_dir,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Process all regimes
    # --------------------------------------------------------

    for regime in REGIMES:

        regime_df = df[
            df['type'] == regime
        ].copy()

        if regime_df.empty:

            print(f"[INFO] No rows for: {regime}")
            continue

        # ----------------------------------------------------
        # Create regime directory
        # ----------------------------------------------------

        regime_dir = os.path.join(
            classify_dir,
            regime
        )

        os.makedirs(
            regime_dir,
            exist_ok=True
        )

        # ----------------------------------------------------
        # LOW ASCENDING
        # ----------------------------------------------------

        low_sorted = regime_df.sort_values(
            by='low',
            ascending=True
        )

        save_classification(
            low_sorted,
            os.path.join(
                regime_dir,
                'low_ascending.csv'
            )
        )

        # ----------------------------------------------------
        # HIGH DESCENDING
        # ----------------------------------------------------

        high_sorted = regime_df.sort_values(
            by='high',
            ascending=False
        )

        save_classification(
            high_sorted,
            os.path.join(
                regime_dir,
                'high_descending.csv'
            )
        )

        # ----------------------------------------------------
        # DURATION DESCENDING
        # ----------------------------------------------------

        duration_sorted = regime_df.sort_values(
            by='duration',
            ascending=False
        )

        save_classification(
            duration_sorted,
            os.path.join(
                regime_dir,
                'duration_descending.csv'
            )
        )

    print("\n[DONE] Classification complete.")


# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    main()
