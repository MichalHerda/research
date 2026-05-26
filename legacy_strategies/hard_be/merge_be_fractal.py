#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd
import numpy as np


# ============================================================
# CONFIG
# ============================================================

EMA_SLOW = 200
EMA_FAST = 50
ATR_PERIOD = 500
SLOPE_PERIOD = 10
TREND_THRESHOLD = 0.5


# ============================================================
# LOAD CSV
# ============================================================

def load_csv(path):

    df = pd.read_csv(path, sep=';')

    df['timestamp'] = pd.to_datetime(
        df['timestamp'],
        format='%Y.%m.%d %H:%M:%S'
    )

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

    df = df.sort_values(
        'timestamp'
    ).reset_index(drop=True)

    return df


# ============================================================
# ATR
# ============================================================

def compute_atr(df, period):

    high_low = df['high'] - df['low']

    high_close = (
        df['high']
        - df['close'].shift(1)
    ).abs()

    low_close = (
        df['low']
        - df['close'].shift(1)
    ).abs()

    tr = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    ).max(axis=1)

    return tr.rolling(period).mean()


# ============================================================
# EMA
# ============================================================

def compute_ema(series, period):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


# ============================================================
# EMA DEV
# ============================================================

def compute_ema_dev(
    high,
    low,
    ema_fast,
    atr
):

    z_high = (
        high - ema_fast
    ) / atr

    z_low = (
        low - ema_fast
    ) / atr

    return np.where(
        np.abs(z_high) >= np.abs(z_low),
        z_high,
        z_low
    )


# ============================================================
# EMA SLOPE
# ============================================================

def compute_slope(
    ema_fast,
    atr,
    slope_period
):

    ema_past = ema_fast.shift(
        slope_period
    )

    return (
        ema_fast - ema_past
    ) / atr


# ============================================================
# FRACTALS
# ============================================================

def compute_fractals(df):

    high = df['high']
    low = df['low']

    fractal_high_raw = (
        (high.shift(2) < high) &
        (high.shift(1) < high) &
        (high.shift(-1) < high) &
        (high.shift(-2) < high)
    )

    fractal_low_raw = (
        (low.shift(2) > low) &
        (low.shift(1) > low) &
        (low.shift(-1) > low) &
        (low.shift(-2) > low)
    )

    fractal_high = df['high'].where(
        fractal_high_raw
    )

    fractal_low = df['low'].where(
        fractal_low_raw
    )

    return fractal_low, fractal_high


# ============================================================
# CLASSIFICATION
# ============================================================

def classify_context(df):

    trend = np.where(
        df['ema_slope'] > TREND_THRESHOLD,
        'UP',
        np.where(
            df['ema_slope'] < -TREND_THRESHOLD,
            'DOWN',
            'RANGE'
        )
    )

    bias = np.where(
        df['ema_fast'] > df['ema_slow'],
        'BULL',
        'BEAR'
    )

    regime = []

    for b, t in zip(bias, trend):

        regime.append(
            f'{b}_{t}'
        )

    return regime


# ============================================================
# PREPARE HIGH TF
# ============================================================

def prepare_high_tf(df):

    # --------------------------------------------------------
    # Indicators
    # --------------------------------------------------------

    df['atr'] = compute_atr(
        df,
        ATR_PERIOD
    )

    df['ema_fast'] = compute_ema(
        df['close'],
        EMA_FAST
    )

    df['ema_slow'] = compute_ema(
        df['close'],
        EMA_SLOW
    )

    df['ema_dev'] = compute_ema_dev(
        df['high'],
        df['low'],
        df['ema_fast'],
        df['atr']
    )

    df['ema_slope'] = compute_slope(
        df['ema_fast'],
        df['atr'],
        SLOPE_PERIOD
    )

    # --------------------------------------------------------
    # Fractals
    # --------------------------------------------------------

    (
        df['fractal_low'],
        df['fractal_high']
    ) = compute_fractals(df)

    # ========================================================
    # LOOKAHEAD FIX
    # ========================================================

    # Fractal na świecy t
    # poznajemy dopiero po zamknięciu:
    #
    # t+2
    #
    # i handlować możemy dopiero od:
    #
    # t+3 open
    #
    # dlatego:
    #
    # shift(3)

    df['active_fractal_low'] = (
        df['fractal_low']
        .shift(3)
    )

    df['active_fractal_high'] = (
        df['fractal_high']
        .shift(3)
    )

    df['buy_fractal_active'] = (
        df['active_fractal_low']
        .notna()
    )

    df['sell_fractal_active'] = (
        df['active_fractal_high']
        .notna()
    )

    # --------------------------------------------------------
    # Previous close
    # --------------------------------------------------------

    df['prev_close'] = (
        df['close']
        .shift(1)
    )

    # --------------------------------------------------------
    # Regime
    # --------------------------------------------------------

    df['regime'] = classify_context(
        df
    )

    # --------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------

    df = df.dropna(
        subset=[
            'atr',
            'ema_dev',
            'ema_slope',
            'prev_close'
        ]
    ).reset_index(drop=True)

    # --------------------------------------------------------
    # Rename
    # --------------------------------------------------------

    rename = {}

    for col in df.columns:

        if col != 'timestamp':

            rename[col] = f'high_{col}'

    df = df.rename(
        columns=rename
    )

    return df


# ============================================================
# BUILD SIGNALS
# ============================================================

def build_signals(
    df,
    retrace_percent
):

    retrace = (
        retrace_percent / 100.0
    )

    df['buy_signal'] = False
    df['sell_signal'] = False

    # ========================================================
    # BUY
    # ========================================================

    buy_distance = (
        df['high_prev_close']
        - df['high_low']
    )

    buy_target = (
        df['high_prev_close']
        - (buy_distance * retrace)
    )

    buy_condition = (
        df['high_buy_fractal_active']
        &
        (df['low'] <= buy_target)
    )

    df.loc[
        buy_condition,
        'buy_signal'
    ] = True

    # ========================================================
    # SELL
    # ========================================================

    sell_distance = (
        df['high_high']
        - df['high_prev_close']
    )

    sell_target = (
        df['high_prev_close']
        + (sell_distance * retrace)
    )

    sell_condition = (
        df['high_sell_fractal_active']
        &
        (df['high'] >= sell_target)
    )

    df.loc[
        sell_condition,
        'sell_signal'
    ] = True

    return df


# ============================================================
# MAIN
# ============================================================

def main():

    if len(sys.argv) != 4:

        print(
            "\nUsage:\n"
            "python3 merge_bt_fractal.py "
            "<low_tf_csv> "
            "<high_tf_csv> "
            "<retrace_percent>\n"
        )

        sys.exit(1)

    low_file = sys.argv[1]
    high_file = sys.argv[2]

    retrace_percent = float(
        sys.argv[3]
    )

    # ========================================================
    # LOAD
    # ========================================================

    print("\n[INFO] Loading CSVs...")

    low_df = load_csv(low_file)
    high_df = load_csv(high_file)

    # ========================================================
    # PREPARE HIGH TF
    # ========================================================

    print("[INFO] Preparing HIGH TF...")

    high_df = prepare_high_tf(
        high_df
    )

    # ========================================================
    # ASOF MERGE
    # ========================================================

    print("[INFO] merge_asof...")

    merged = pd.merge_asof(
        low_df,
        high_df,
        on='timestamp',
        direction='backward'
    )

    # ========================================================
    # REMOVE INCOMPLETE ROWS
    # ========================================================

    # To jest dokładnie miejsce,
    # o które pytałeś.

    # Usuwamy okres,
    # gdzie HIGH TF jeszcze nie miał:
    #
    # ATR
    # EMA
    # fractali
    # regime
    #
    # itd.

    merged = merged.dropna(
        subset=[
            'high_regime',
            'high_prev_close'
        ]
    ).reset_index(drop=True)

    # ========================================================
    # BUILD SIGNALS
    # ========================================================

    print("[INFO] Building signals...")

    merged = build_signals(
        merged,
        retrace_percent
    )

    # ========================================================
    # OUTPUT
    # ========================================================

    low_name = os.path.splitext(
        os.path.basename(low_file)
    )[0]

    high_name = os.path.splitext(
        os.path.basename(high_file)
    )[0]

    output_file = (
        f'merged_{low_name}_{high_name}.csv'
    )

    merged.to_csv(
        output_file,
        sep=';',
        index=False,
        float_format='%.8f'
    )

    print("\n========================================")
    print("MERGE FINISHED")
    print("========================================")
    print(f"Output: {output_file}")
    print(f"Rows: {len(merged)}")
    print(
        f"BUY signals: "
        f"{merged['buy_signal'].sum()}"
    )
    print(
        f"SELL signals: "
        f"{merged['sell_signal'].sum()}"
    )
    print("========================================\n")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()
