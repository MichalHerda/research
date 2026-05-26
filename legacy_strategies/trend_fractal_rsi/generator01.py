#!/usr/bin/env python3

import os               # noqa
import sys
import pandas as pd
from pathlib import Path


# =========================
# INDICATORS
# =========================

def add_rsi(df, period=14):
    delta = df['close'].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    df['RSI_M5'] = 100 - (100 / (1 + rs))

    return df


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

    fractal_high_val = df['high'].where(fractal_high_raw)
    fractal_low_val = df['low'].where(fractal_low_raw)

    # brak lookahead bias
    fractal_high_val = fractal_high_val.shift(2)
    fractal_low_val = fractal_low_val.shift(2)

    fractal_high_val = fractal_high_val.ffill()
    fractal_low_val = fractal_low_val.ffill()

    df['fractal_high'] = fractal_high_val
    df['fractal_low'] = fractal_low_val

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


# =========================
# IO
# =========================

def load_csv(path):
    df = pd.read_csv(path, sep=';')

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')
    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


# =========================
# CORE
# =========================

def process_instrument(instr_path, output_dir):
    instr_name = instr_path.name

    print(f"[INFO] Processing {instr_name}")

    m5_file = instr_path / f"{instr_name}_M5.csv"
    h1_file = instr_path / f"{instr_name}_H1.csv"

    if not m5_file.exists() or not h1_file.exists():
        print(f"[WARN] Missing M5 or H1 for {instr_name}, skipping")
        return

    df_m5 = load_csv(m5_file)
    df_h1 = load_csv(h1_file)

    # =========================
    # M5 FEATURES
    # =========================
    df_m5 = add_rsi(df_m5, 14)
    df_m5 = compute_fractals(df_m5)

    # =========================
    # H1 FEATURES
    # =========================
    df_h1['EMA50'] = compute_ema(df_h1['close'], 50)
    df_h1['EMA200'] = compute_ema(df_h1['close'], 200)

    df_h1['ATR'] = compute_atr(df_h1, 1000)

    df_h1['uptrend'] = df_h1['EMA50'] > df_h1['EMA200']

    df_h1['ema_diff'] = (df_h1['EMA50'] - df_h1['EMA200']) / df_h1['ATR']

    df_h1['zscore'] = (df_h1['close'] - df_h1['EMA50']) / df_h1['ATR']

    # =========================
    # MERGE (ASOF)
    # =========================
    df_m5 = df_m5.sort_values('timestamp')
    df_h1 = df_h1.sort_values('timestamp')

    df = pd.merge_asof(
        df_m5,
        df_h1,
        on='timestamp',
        direction='backward',
        suffixes=('', '_H1')
    )

    # =========================
    # CLEANUP
    # =========================
    df = df.dropna().reset_index(drop=True)

    # =========================
    # SAVE
    # =========================
    out_dir = output_dir                # / instr_name
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / f"{instr_name}_merged.csv"
    df.to_csv(out_file, index=False)

    print(f"[OK] Saved {out_file}")


# =========================
# MAIN
# =========================

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 generator01.py <directory>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])

    if not base_dir.exists():
        print("Directory does not exist")
        sys.exit(1)

    output_dir = base_dir / "merged_output"
    output_dir.mkdir(exist_ok=True)

    for item in base_dir.iterdir():
        if item.is_dir():
            process_instrument(item, output_dir)


if __name__ == "__main__":
    main()
