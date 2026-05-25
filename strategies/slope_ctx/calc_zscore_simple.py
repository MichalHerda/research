#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
calc_zscore.py

Analiza regime z wygenerowanych danych OHLCV.

Program:
- scala krótkie regime poniżej duration_threshold
- liczy statystyki dla pełnoprawnych regime
- zapisuje wynik do CSV

Wejście:
python3 calc_zscore.py <file> <duration_threshold>

Przykład:
python3 calc_zscore.py EURUSD_M5.csv 5

Wyjście:
EURUSD_M5_regimes.csv

Kolumny:
- start
- end
- type
- duration
- average
- high
- low
"""

import os
import sys
import pandas as pd


# ============================================================
# Load CSV
# ============================================================

def load_data(file_path):

    df = pd.read_csv(file_path, sep=';')

    required_cols = [
        'timestamp',
        'ema_dev_atr',
        'regime'
    ]

    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Missing required column: {col}")

    return df


# ============================================================
# Build raw regime blocks
# ============================================================

def build_regime_blocks(df):

    blocks = []

    current_regime = df.iloc[0]['regime']
    start_idx = 0

    for i in range(1, len(df)):

        regime = df.iloc[i]['regime']

        if regime != current_regime:

            end_idx = i - 1

            blocks.append({
                'type': current_regime,
                'start_idx': start_idx,
                'end_idx': end_idx,
                'duration': end_idx - start_idx + 1
            })

            current_regime = regime
            start_idx = i

    # last block

    blocks.append({
        'type': current_regime,
        'start_idx': start_idx,
        'end_idx': len(df) - 1,
        'duration': len(df) - start_idx
    })

    return blocks


# ============================================================
# Merge short regimes
# ============================================================

def merge_short_regimes(blocks, duration_threshold):

    if not blocks:
        return []

    merged = []

    i = 0

    while i < len(blocks):

        block = blocks[i]

        # ----------------------------------------------------
        # Long enough -> keep
        # ----------------------------------------------------

        if block['duration'] >= duration_threshold:
            merged.append(block)
            i += 1
            continue

        # ----------------------------------------------------
        # Short regime
        # Merge into previous if exists
        # Otherwise merge into next
        # ----------------------------------------------------

        if merged:

            prev_block = merged[-1]

            prev_block['end_idx'] = block['end_idx']
            prev_block['duration'] = (
                prev_block['end_idx']
                - prev_block['start_idx']
                + 1
            )

        else:

            # first block is short
            # merge into next block

            if i + 1 < len(blocks):

                next_block = blocks[i + 1]

                next_block['start_idx'] = block['start_idx']
                next_block['duration'] = (
                    next_block['end_idx']
                    - next_block['start_idx']
                    + 1
                )

            else:
                # single short block
                merged.append(block)

        i += 1

    return merged


# ============================================================
# Build output dataframe
# ============================================================

def build_output(df, blocks):

    rows = []

    for block in blocks:

        sub = df.iloc[
            block['start_idx']:block['end_idx'] + 1
        ]

        rows.append({
            'start': sub.iloc[0]['timestamp'],
            'end': sub.iloc[-1]['timestamp'],
            'type': block['type'],
            'duration': len(sub),
            'average': sub['ema_dev_atr'].mean(),
            'high': sub['ema_dev_atr'].max(),
            'low': sub['ema_dev_atr'].min()
        })

    return pd.DataFrame(rows)


# ============================================================
# Main
# ============================================================

def main():

    if len(sys.argv) != 3:

        print(
            "Usage:\n"
            "python3 calc_zscore.py "
            "<file> "
            "<duration_threshold>"
        )

        sys.exit(1)

    file_path = sys.argv[1]
    duration_threshold = int(sys.argv[2])

    # --------------------------------------------------------
    # Validate
    # --------------------------------------------------------

    if not os.path.isfile(file_path):

        print(f"[ERROR] File does not exist: {file_path}")
        sys.exit(1)

    # --------------------------------------------------------
    # Load
    # --------------------------------------------------------

    print(f"[INFO] Loading: {file_path}")

    df = load_data(file_path)

    # --------------------------------------------------------
    # Build regimes
    # --------------------------------------------------------

    raw_blocks = build_regime_blocks(df)

    print(f"[INFO] Raw regimes: {len(raw_blocks)}")

    # --------------------------------------------------------
    # Merge short regimes
    # --------------------------------------------------------

    merged_blocks = merge_short_regimes(
        raw_blocks,
        duration_threshold
    )

    print(f"[INFO] Final regimes: {len(merged_blocks)}")

    # --------------------------------------------------------
    # Build output
    # --------------------------------------------------------

    out_df = build_output(df, merged_blocks)

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    base, ext = os.path.splitext(file_path)

    output_file = f"{base}_regimes.csv"

    out_df.to_csv(
        output_file,
        sep=';',
        index=False,
        float_format='%.8f'
    )

    print(f"[OK] Saved: {output_file}")


if __name__ == "__main__":
    main()
