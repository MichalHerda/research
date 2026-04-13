#!/usr/bin/env python3

import sys
import pandas as pd


# =========================
# UTILS
# =========================

def load_csv(path):
    df = pd.read_csv(path, sep=';')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')
    df = df.sort_values('timestamp').reset_index(drop=True)

    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def prefix_columns(df, prefix, exclude=['timestamp']):
    return df.rename(columns={
        col: f"{col}_{prefix}" for col in df.columns if col not in exclude
    })


# =========================
# INDICATORS
# =========================

def add_sma(df, period, col_name):
    df[col_name] = df['close'].rolling(window=period).mean()
    return df


def add_uptrend(df, sma_col, out_col):
    df[out_col] = df[sma_col] > df[sma_col].shift(1)
    return df


def add_rsi(df, period=14):
    delta = df['close'].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    df['RSI_M5'] = 100 - (100 / (1 + rs))

    return df


def add_pivots(df):
    cond = (
        (df['low'] < df['low'].shift(1)) &
        (df['low'] < df['low'].shift(2)) &
        (df['low'] < df['low'].shift(-1)) &
        (df['low'] < df['low'].shift(-2))
    )

    df['pivot_low'] = df['low'].where(cond)
    df['pivot_low'] = df['pivot_low'].shift(2)
    df['last_pivot'] = df['pivot_low'].ffill()

    return df


# =========================
# ALIGNMENT
# =========================

def align_timeframes(df_m5, df_h1, df_d1):

    df_h1 = df_h1.rename(columns={'timestamp': 'timestamp_H1'})
    df_d1 = df_d1.rename(columns={'timestamp': 'timestamp_D1'})

    # MERGE H1
    df = pd.merge_asof(
        df_m5.sort_values('timestamp'),
        df_h1.sort_values('timestamp_H1'),
        left_on='timestamp',
        right_on='timestamp_H1',
        direction='backward'
    )

    # MERGE D1
    df = pd.merge_asof(
        df.sort_values('timestamp'),
        df_d1.sort_values('timestamp_D1'),
        left_on='timestamp',
        right_on='timestamp_D1',
        direction='backward'
    )

    return df


# =========================
# TRIM
# =========================

def trim_to_valid_start(df):

    required_cols = [
        'timestamp_H1',
        'timestamp_D1'
    ]

    mask = df[required_cols].notna().all(axis=1)

    if not mask.any():
        raise ValueError("Brak wspólnego zakresu danych")

    first_valid_idx = mask.idxmax()

    return df.loc[first_valid_idx:].reset_index(drop=True)


# =========================
# MAIN
# =========================

def main():
    if len(sys.argv) != 6:
        print("Użycie:")
        print("python3 align_high.py <M5.csv> <H1.csv> <D1.csv> <period_H1> <period_D1>")
        sys.exit(1)

    file_m5 = sys.argv[1]
    file_h1 = sys.argv[2]
    file_d1 = sys.argv[3]
    period_h1 = int(sys.argv[4])
    period_d1 = int(sys.argv[5])

    # =========================
    # LOAD
    # =========================
    df_m5 = load_csv(file_m5)
    df_h1 = load_csv(file_h1)
    df_d1 = load_csv(file_d1)

    # =========================
    # H1
    # =========================
    df_h1 = add_sma(df_h1, period_h1, 'SMA_H1')
    df_h1 = add_uptrend(df_h1, 'SMA_H1', 'UP_H1')
    df_h1 = add_pivots(df_h1)

    # =========================
    # D1
    # =========================
    df_d1 = add_sma(df_d1, period_d1, 'SMA_D1')
    df_d1 = add_uptrend(df_d1, 'SMA_D1', 'UP_D1')

    # =========================
    # M5
    # =========================
    df_m5 = add_rsi(df_m5)

    # =========================
    # PREFIX (KLUCZOWE!)
    # =========================
    df_m5 = prefix_columns(df_m5, 'M5')
    df_h1 = prefix_columns(df_h1, 'H1')
    df_d1 = prefix_columns(df_d1, 'D1')

    # przywracamy timestampy (bez prefixu)
    df_m5 = df_m5.rename(columns={'timestamp_M5': 'timestamp'})
    df_h1 = df_h1.rename(columns={'timestamp_H1': 'timestamp'})
    df_d1 = df_d1.rename(columns={'timestamp_D1': 'timestamp'})

    # =========================
    # ALIGN
    # =========================
    df = align_timeframes(df_m5, df_h1, df_d1)

    # =========================
    # TRIM
    # =========================
    df = trim_to_valid_start(df)

    # =========================
    # OUTPUT
    # =========================
    output_file = "aligned_output.csv"
    df.to_csv(output_file, sep=';', index=False)

    print(f"✅ Zapisano: {output_file}")
    print(f"📊 Wiersze: {len(df)}")


if __name__ == "__main__":
    main()
