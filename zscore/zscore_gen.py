#!/usr/bin/env python3

import sys
import os
import glob
import pandas as pd
# import numpy as np


def load_csv(filepath):
    df = pd.read_csv(
        filepath,
        sep=';',
        dtype=str
    )

    # Czyszczenie volume (np. "297s")
    df['volume'] = df['volume'].str.replace(r'[^0-9]', '', regex=True)

    # Konwersje typów
    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    # Timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')

    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_atr(df, period):
    high = df['high']
    low = df['low']
    close = df['close']

    # >>> KLUCZOWA POPRAWKA <<<
    prev_close = close.shift(-1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    atr = pd.Series(index=df.index, dtype=float)

    # pierwsza wartość ATR jak w MT4
    atr.iloc[period] = tr.iloc[1:period+1].mean()

    # Wilder smoothing
    for i in range(period + 1, len(df)):
        atr.iloc[i] = ((atr.iloc[i-1] * (period - 1)) + tr.iloc[i]) / period

    return atr


def process_file(filepath, ema_period, atr_period):
    df = load_csv(filepath)

    df['EMA'] = compute_ema(df['close'], ema_period)

    # deviation: close - EMA
    df['deviation'] = df['close'] - df['EMA']

    df['ATR'] = compute_atr(df, atr_period)

    # Przycięcie danych – usuwamy NaN
    df = df.dropna().reset_index(drop=True)

    return df


def main():
    if len(sys.argv) != 5:
        print("Usage: python3 z_score_gen.py <directory> <timeframe> <ema_period> <atr_period>")
        sys.exit(1)

    base_dir = sys.argv[1]
    timeframe = sys.argv[2]
    ema_period = int(sys.argv[3])
    atr_period = int(sys.argv[4])

    output_dir = os.path.join(base_dir, f"output_{timeframe}")
    os.makedirs(output_dir, exist_ok=True)

    instruments = [d for d in os.listdir(base_dir)
                   if os.path.isdir(os.path.join(base_dir, d))]

    for instrument in instruments:
        instrument_path = os.path.join(base_dir, instrument)

        pattern = os.path.join(instrument_path, f"*_{timeframe}.csv")
        files = glob.glob(pattern)

        if not files:
            continue

        filepath = files[0]

        try:
            df = process_file(filepath, ema_period, atr_period)

            output_filename = f"{instrument}_{timeframe}.csv"
            output_path = os.path.join(output_dir, output_filename)

            df.to_csv(output_path, index=False)

            print(f"[OK] {instrument} -> {output_filename}")

        except Exception as e:
            print(f"[ERROR] {instrument}: {e}")


if __name__ == "__main__":
    main()
