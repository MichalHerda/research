#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
slope_ctx_gen.py

Generuje rozszerzone datasety CSV dla backtestów:
- OHLCV
- ATR
- EMA
- EMA deviation normalized by ATR
- EMA slope normalized by ATR
- Market regime classification

Struktura wejściowa:
<DIR>/
    EURUSD/
        EURUSD_H1.csv
        EURUSD_M15.csv
        ...

Struktura wyjściowa:
<DIR>_processed/
    EURUSD/
        EURUSD_H1.csv
        EURUSD_M15.csv
        ...

Użycie:
python3 slope_ctx_gen.py <DIRECTORY> <EMA_PERIOD> <ATR_PERIOD> <SLOPE_PERIOD> <TREND_THRESHOLD>

Przykład:
python3 slope_ctx_gen.py ./sample_data 200 500 10 0.5
"""

import os
import sys
import shutil
import pandas as pd
import numpy as np


# ============================================================
# ATR (identycznie jak MT4: SMA z True Range)
# ============================================================

def compute_atr(df, period):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()

    return atr


# ============================================================
# EMA
# ============================================================

def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


# ============================================================
# EMA deviation normalized by ATR
# (close - ema) / atr
# ============================================================

def compute_ema_deviation(close, ema, atr):
    return (close - ema) / atr


# ============================================================
# EMA slope normalized by ATR
# (ema_current - ema_past) / atr
# ============================================================

def compute_slope(ema, atr, slope_period):
    ema_past = ema.shift(slope_period)
    slope = (ema - ema_past) / atr
    return slope


# ============================================================
# Regime classification
# ============================================================

def classify_regime(slope, threshold):
    conditions = [
        slope > threshold,
        slope < -threshold
    ]

    choices = ['up', 'down']

    return np.select(conditions, choices, default='range')


# ============================================================
# Processing single CSV
# ============================================================

def process_csv(
    input_csv,
    output_csv,
    ema_period,
    atr_period,
    slope_period,
    trend_threshold
):

    print(f"[INFO] Processing: {input_csv}")

    df = pd.read_csv(input_csv, sep=';')

    # --------------------------------------------------------
    # Ensure numeric
    # --------------------------------------------------------

    numeric_cols = ['open', 'high', 'low', 'close', 'volume']

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # --------------------------------------------------------
    # Indicators
    # --------------------------------------------------------

    df['atr'] = compute_atr(df, atr_period)

    df['ema'] = compute_ema(df['close'], ema_period)

    df['ema_dev_atr'] = compute_ema_deviation(
        df['close'],
        df['ema'],
        df['atr']
    )

    df['ema_slope_atr'] = compute_slope(
        df['ema'],
        df['atr'],
        slope_period
    )

    df['regime'] = classify_regime(
        df['ema_slope_atr'],
        trend_threshold
    )

    # --------------------------------------------------------
    # Remove rows with NaN
    # --------------------------------------------------------

    df = df.dropna().reset_index(drop=True)

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    df.to_csv(
        output_csv,
        sep=';',
        index=False,
        float_format='%.8f'
    )

    print(f"[OK] Saved: {output_csv}")


# ============================================================
# Main
# ============================================================

def main():

    if len(sys.argv) != 6:
        print(
            "Usage:\n"
            "python3 slope_ctx_gen.py "
            "<DIRECTORY> "
            "<EMA_PERIOD> "
            "<ATR_PERIOD> "
            "<SLOPE_PERIOD> "
            "<TREND_THRESHOLD>"
        )
        sys.exit(1)

    base_dir = sys.argv[1]
    ema_period = int(sys.argv[2])
    atr_period = int(sys.argv[3])
    slope_period = int(sys.argv[4])
    trend_threshold = float(sys.argv[5])

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    if not os.path.isdir(base_dir):
        print(f"[ERROR] Directory does not exist: {base_dir}")
        sys.exit(1)

    # --------------------------------------------------------
    # Output directory
    # --------------------------------------------------------

    base_dir = os.path.abspath(base_dir)

    output_dir = f"{base_dir}_processed"

    if os.path.exists(output_dir):
        print(f"[INFO] Removing existing output directory: {output_dir}")
        shutil.rmtree(output_dir)

    os.makedirs(output_dir)

    print(f"[INFO] Output directory: {output_dir}")

    # --------------------------------------------------------
    # Traverse instruments
    # --------------------------------------------------------

    for instrument in sorted(os.listdir(base_dir)):

        instrument_path = os.path.join(base_dir, instrument)

        if not os.path.isdir(instrument_path):
            continue

        output_instrument_path = os.path.join(output_dir, instrument)

        os.makedirs(output_instrument_path, exist_ok=True)

        # ----------------------------------------------------
        # Process all CSVs
        # ----------------------------------------------------

        for filename in sorted(os.listdir(instrument_path)):

            if not filename.endswith('.csv'):
                continue

            input_csv = os.path.join(instrument_path, filename)
            output_csv = os.path.join(output_instrument_path, filename)

            try:
                process_csv(
                    input_csv=input_csv,
                    output_csv=output_csv,
                    ema_period=ema_period,
                    atr_period=atr_period,
                    slope_period=slope_period,
                    trend_threshold=trend_threshold
                )

            except Exception as e:
                print(f"[ERROR] Failed processing {input_csv}")
                print(str(e))

    print("\n[DONE] All files processed.")


if __name__ == "__main__":
    main()