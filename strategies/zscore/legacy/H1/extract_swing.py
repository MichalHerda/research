#!/usr/bin/env python3

import sys
import os
import pandas as pd


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


def find_crossings(df):
    """
    Znajduje indeksy przecięć deviation przez zero
    """
    prev = df['deviation'].shift(1)

    cross_up = (prev < 0) & (df['deviation'] > 0)
    cross_down = (prev > 0) & (df['deviation'] < 0)

    crossings = df.index[cross_up | cross_down].tolist()

    return crossings


def extract_periods(df):
    crossings = find_crossings(df)
    periods = []

    if len(crossings) < 2:
        return periods

    for i in range(len(crossings) - 1):
        start_idx = crossings[i]
        end_idx = crossings[i + 1]

        segment = df.iloc[start_idx:end_idx + 1]

        # duration w świecach
        duration = len(segment)

        # określenie kierunku
        start_dev = df.loc[start_idx, 'deviation']

        if start_dev > 0:
            direction = "UP"
            extreme_idx = segment['deviation'].idxmax()
        else:
            direction = "DOWN"
            extreme_idx = segment['deviation'].idxmin()

        extreme_dev = df.loc[extreme_idx, 'deviation']
        extreme_dev_abs = abs(extreme_dev)

        # ATR dla tej samej świecy
        extreme_atr = df.loc[extreme_idx, 'ATR']

        # zabezpieczenie (teoretyczne)
        if extreme_atr == 0:
            extreme_dev_atr_pct = 0.0
        else:
            extreme_dev_atr_pct = (extreme_dev_abs / extreme_atr) * 100

        periods.append({
            "period_begin": df.loc[start_idx, 'timestamp'],
            "period_end": df.loc[end_idx, 'timestamp'],
            "duration": duration,
            "direction": direction,
            "extreme_dev": extreme_dev,
            "extreme_dev_abs": extreme_dev_abs,
            "extreme_dev_atr_pct": extreme_dev_atr_pct
        })

    return periods


def process_file(filepath, output_dir):
    df = load_data(filepath)

    periods = extract_periods(df)

    if not periods:
        return

    result_df = pd.DataFrame(periods)

    filename = os.path.basename(filepath)
    output_path = os.path.join(output_dir, f"swings_{filename}")

    result_df.to_csv(output_path, index=False)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 extract_swing.py <directory>")
        sys.exit(1)

    base_dir = sys.argv[1]

    output_dir = os.path.join(base_dir, "swings")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(base_dir) if f.endswith(".csv")]

    for f in files:
        filepath = os.path.join(base_dir, f)

        try:
            process_file(filepath, output_dir)
            print(f"[OK] {f}")
        except Exception as e:
            print(f"[ERROR] {f}: {e}")


if __name__ == "__main__":
    main()
