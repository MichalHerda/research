#!/usr/bin/env python3

import sys
import pandas as pd
from pathlib import Path


def load_csv(path):
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    return df


def find_signals(df, threshold):
    rsi = df['RSI_M5']

    cross_up = (rsi.shift(1) < threshold) & (rsi >= threshold)

    # sygnał na świecy poprzedniej
    signal_idx = cross_up.shift(1)

    signals = df[signal_idx & (df['uptrend'])].copy()

    return signals[['timestamp']]


def process_file(file_path, output_dir, threshold):
    instr_name = file_path.name.replace("_merged.csv", "")

    print(f"[INFO] Processing file: {file_path.name}")

    df = load_csv(file_path)

    signals = find_signals(df, threshold)

    if signals.empty:
        print(f"[INFO] No signals for {instr_name}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / f"{instr_name}_signals.csv"

    # DEBUG safeguard
    if out_file.exists():
        print(f"[WARN] Overwriting {out_file}")

    signals.to_csv(out_file, index=False)

    print(f"[OK] {instr_name}: {len(signals)} signals saved")


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 rsi_uptrend.py <dir> <rsi_threshold>")
        sys.exit(1)

    base_dir = Path(sys.argv[1])
    threshold = float(sys.argv[2])

    if not base_dir.exists():
        print("Directory does not exist")
        sys.exit(1)

    output_dir = base_dir / "signals_rsi_uptrend"

    files = list(base_dir.glob("*_merged.csv"))

    if not files:
        print("No merged files found!")
        sys.exit(1)

    print(f"[INFO] Found {len(files)} files")

    for file in files:
        process_file(file, output_dir, threshold)


if __name__ == "__main__":
    main()
