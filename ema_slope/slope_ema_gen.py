#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
slope_ema_gen.py

Generuje rozszerzone datasety CSV dla backtestów:
- OHLCV
- EMA deviation normalized by ATR (FAST EMA)
- Market context classification:
    BULL_UP
    BULL_DOWN
    BULL_RANGE
    BEAR_UP
    BEAR_DOWN
    BEAR_RANGE

Klasyfikacja:
- trend = slope FAST EMA normalized by ATR
- bias  = FAST EMA > SLOW EMA

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
python3 slope_ema_gen.py \
    <DIRECTORY> \
    <EMA_SLOW_PERIOD> \
    <EMA_FAST_PERIOD> \
    <ATR_PERIOD> \
    <SLOPE_PERIOD> \
    <TREND_THRESHOLD>

Przykład:
python3 slope_ema_gen.py ./sample_data 200 50 500 10 0.5
"""

import os
import sys
import shutil
import pandas as pd
import numpy as np


# ============================================================
# ATR (MT4 style: SMA z True Range)
# ============================================================

def compute_atr(df, period):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    ).max(axis=1)

    atr = tr.rolling(window=period).mean()

    return atr


# ============================================================
# EMA
# ============================================================

def compute_ema(series, period):
    return series.ewm(
        span=period,
        adjust=False
    ).mean()


# ============================================================
# EMA deviation normalized by ATR
# (close - fast_ema) / atr
# ============================================================

def compute_ema_deviation(close, fast_ema, atr):
    return (close - fast_ema) / atr


# ============================================================
# FAST EMA slope normalized by ATR
# ============================================================

def compute_slope(fast_ema, atr, slope_period):
    ema_past = fast_ema.shift(slope_period)
    slope = (fast_ema - ema_past) / atr
    return slope


# ============================================================
# Trend classification
# ============================================================

def classify_trend(slope, threshold):

    conditions = [
        slope > threshold,
        slope < -threshold
    ]

    choices = [
        'UP',
        'DOWN'
    ]

    return np.select(
        conditions,
        choices,
        default='RANGE'
    )


# ============================================================
# Bias classification
# ============================================================

def classify_bias(fast_ema, slow_ema):
    return np.where(
        fast_ema > slow_ema,
        'BULL',
        'BEAR'
    )


# ============================================================
# Final context classification
# ============================================================

def classify_context(bias, trend):

    context = []

    for b, t in zip(bias, trend):

        if b == 'BULL' and t == 'UP':
            context.append('BULL_UP')

        elif b == 'BULL' and t == 'DOWN':
            context.append('BULL_DOWN')

        elif b == 'BULL' and t == 'RANGE':
            context.append('BULL_RANGE')

        elif b == 'BEAR' and t == 'UP':
            context.append('BEAR_UP')

        elif b == 'BEAR' and t == 'DOWN':
            context.append('BEAR_DOWN')

        elif b == 'BEAR' and t == 'RANGE':
            context.append('BEAR_RANGE')

        else:
            context.append('UNDEFINED')

    return context


# ============================================================
# Processing single CSV
# ============================================================

def process_csv(
    input_csv,
    output_csv,
    ema_slow_period,
    ema_fast_period,
    atr_period,
    slope_period,
    trend_threshold
):

    print(f"[INFO] Processing: {input_csv}")

    df = pd.read_csv(input_csv, sep=';')

    # --------------------------------------------------------
    # Ensure numeric
    # --------------------------------------------------------

    numeric_cols = [
        'open',
        'high',
        'low',
        'close',
        'volume'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col],
            errors='coerce'
        )

    # --------------------------------------------------------
    # Indicators
    # --------------------------------------------------------

    df['atr'] = compute_atr(df, atr_period)

    df['ema_fast'] = compute_ema(
        df['close'],
        ema_fast_period
    )

    df['ema_slow'] = compute_ema(
        df['close'],
        ema_slow_period
    )

    df['ema_dev'] = compute_ema_deviation(
        df['close'],
        df['ema_fast'],
        df['atr']
    )

    df['ema_slope'] = compute_slope(
        df['ema_fast'],
        df['atr'],
        slope_period
    )

    # --------------------------------------------------------
    # Classification
    # --------------------------------------------------------

    df['trend'] = classify_trend(
        df['ema_slope'],
        trend_threshold
    )

    df['bias'] = classify_bias(
        df['ema_fast'],
        df['ema_slow']
    )

    df['regime'] = classify_context(
        df['bias'],
        df['trend']
    )

    # --------------------------------------------------------
    # Final columns
    # --------------------------------------------------------

    df = df[
        [
            'timestamp',
            'open',
            'high',
            'low',
            'close',
            'volume',
            'ema_dev',
            'regime'
        ]
    ]

    # --------------------------------------------------------
    # Remove NaN
    # --------------------------------------------------------

    df = df.dropna().reset_index(drop=True)

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    os.makedirs(
        os.path.dirname(output_csv),
        exist_ok=True
    )

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

    if len(sys.argv) != 7:
        print(
            "Usage:\n"
            "python3 slope_ema_gen.py "
            "<DIRECTORY> "
            "<EMA_SLOW_PERIOD> "
            "<EMA_FAST_PERIOD> "
            "<ATR_PERIOD> "
            "<SLOPE_PERIOD> "
            "<TREND_THRESHOLD>"
        )
        sys.exit(1)

    base_dir = sys.argv[1]

    ema_slow_period = int(sys.argv[2])
    ema_fast_period = int(sys.argv[3])
    atr_period = int(sys.argv[4])
    slope_period = int(sys.argv[5])
    trend_threshold = float(sys.argv[6])

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

        instrument_path = os.path.join(
            base_dir,
            instrument
        )

        if not os.path.isdir(instrument_path):
            continue

        output_instrument_path = os.path.join(
            output_dir,
            instrument
        )

        os.makedirs(
            output_instrument_path,
            exist_ok=True
        )

        # ----------------------------------------------------
        # Process CSV files
        # ----------------------------------------------------

        for filename in sorted(os.listdir(instrument_path)):

            if not filename.endswith('.csv'):
                continue

            input_csv = os.path.join(
                instrument_path,
                filename
            )

            output_csv = os.path.join(
                output_instrument_path,
                filename
            )

            try:

                process_csv(
                    input_csv=input_csv,
                    output_csv=output_csv,
                    ema_slow_period=ema_slow_period,
                    ema_fast_period=ema_fast_period,
                    atr_period=atr_period,
                    slope_period=slope_period,
                    trend_threshold=trend_threshold
                )

            except Exception as e:

                print(f"[ERROR] Failed processing {input_csv}")
                print(str(e))

    print("\n[DONE] All files processed.")


# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    main()
