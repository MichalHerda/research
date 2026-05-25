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


# to jest sposób obliczania ATR zgodny z tym co jest na wykresach w MT4:

def compute_atr(df, period):
    # 1. Oblicz True Range (TR)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    # 2. MT4 używa SMA do ATR, a nie wygładzania Wildera
    # rolling(period).mean() to dokładnie to, co robi iATR w MT4
    atr = tr.rolling(window=period).mean()

    return atr


def process_file(filepath, ema_period, atr_period):
    df = load_csv(filepath)

    df['EMA'] = compute_ema(df['close'], ema_period)

    # deviation: close - EMA
    df['deviation'] = df['close'] - df['EMA']

    df['ATR'] = compute_atr(df, atr_period)

    # >>> DODANE: normalizacja dokładnie jak w MQL4 <<<
    df['deviation_atr'] = df['deviation'] / df['ATR']

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
