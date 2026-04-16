#!/usr/bin/env python3

import sys
import os
import glob
import pandas as pd


def load_csv(filepath):
    df = pd.read_csv(
        filepath,
        sep=';',
        dtype=str
    )

    df['volume'] = df['volume'].str.replace(r'[^0-9]', '', regex=True)

    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')

    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_atr(df, period):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()

    return atr


# ✅ FRACTALS (NO LOOKAHEAD BIAS)
def compute_fractals(df):
    high = df['high']
    low = df['low']

    # klasyczne fraktale (świeca środkowa i)
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

    # wartości fraktali (na świecy i)
    fractal_high_val = df['high'].where(fractal_high_raw)
    fractal_low_val = df['low'].where(fractal_low_raw)

    # ❗ KLUCZ: przesunięcie o +2 (brak lookahead bias)
    fractal_high_val = fractal_high_val.shift(2)
    fractal_low_val = fractal_low_val.shift(2)

    # forward fill → ostatni znany fraktal
    fractal_high_val = fractal_high_val.ffill()
    fractal_low_val = fractal_low_val.ffill()

    return fractal_high_val, fractal_low_val


def process_file(filepath, ema_period, ema_period_slow, atr_period):
    df = load_csv(filepath)

    df['EMA'] = compute_ema(df['close'], ema_period)
    df['EMA_SLOW'] = compute_ema(df['close'], ema_period_slow)

    df['deviation'] = df['close'] - df['EMA']

    df['ATR'] = compute_atr(df, atr_period)

    df['deviation_atr'] = df['deviation'] / df['ATR']

    # różnica EMA (najpierw liczona klasycznie)
    df['EMA_diff'] = df['EMA'] - df['EMA_SLOW']

    # >>> normalizacja do ATR <<<
    df['EMA_diff'] = df['EMA_diff'] / df['ATR']

    # trend liczony na podstawie znaku
    df['uptrend'] = df['EMA_diff'] > 0

    # ✅ FRACTALS
    df['fractal_high'], df['fractal_low'] = compute_fractals(df)

    df = df.dropna().reset_index(drop=True)

    return df


def main():
    if len(sys.argv) != 6:
        print("Usage: python3 z_score_gen3.py <directory> <timeframe> <ema_period> <ema_period_slow> <atr_period>")
        sys.exit(1)

    base_dir = sys.argv[1]
    timeframe = sys.argv[2]
    ema_period = int(sys.argv[3])
    ema_period_slow = int(sys.argv[4])
    atr_period = int(sys.argv[5])

    output_dir = os.path.join(base_dir, f"output_{timeframe}")
    os.makedirs(output_dir, exist_ok=True)

    instruments = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]

    for instrument in instruments:
        instrument_path = os.path.join(base_dir, instrument)

        # ✅ FIX dla [SP500]
        safe_path = glob.escape(instrument_path)

        pattern = os.path.join(safe_path, f"*_{timeframe}.csv")
        files = glob.glob(pattern)

        if not files:
            continue

        filepath = files[0]

        try:
            df = process_file(filepath, ema_period, ema_period_slow, atr_period)

            output_filename = f"{instrument}_{timeframe}.csv"
            output_path = os.path.join(output_dir, output_filename)

            df.to_csv(output_path, index=False)

            print(f"[OK] {instrument} -> {output_filename}")

        except Exception as e:
            print(f"[ERROR] {instrument}: {e}")


if __name__ == "__main__":
    main()
