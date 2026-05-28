#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fractal_be_backtest.py

Backtest strategii CFD opartej o fractale MT4.

ZAŁOŻENIA:
------------------------------------------------------------
1. Gramy WYŁĄCZNIE jedną pozycją jednocześnie.

2. Fractal HIGH  -> SELL
   Fractal LOW   -> BUY

3. Fractal pojawia się dopiero po zamknięciu świecy +1
   (brak lookahead bias).

4. Wejście:
   - standardowo po OPEN świecy sygnałowej
   - opcjonalnie po retrace o X%
     względem zakresu poprzedniej świecy

5. SL:
   - BUY  -> ostatni fractal low - spread
   - SELL -> ostatni fractal high + spread

6. Po X barach:
   SL przesuwany na BREAKEVEN + offset_points

7. TP:
   ustawiany przez RRR

8. Wynik:
   - TP
   - SL
   - BE

9. Output:
   CSV ze wszystkimi trade'ami
   + summary na końcu pliku

------------------------------------------------------------

UŻYCIE:
------------------------------------------------------------

python3 fractal_be_backtest.py \
    <csv_file> \
    <instrument> \
    <rrr> \
    <be_after_bars> \
    <be_offset_points> \
    <entry_retrace_percent>

PRZYKŁAD:
------------------------------------------------------------

python3 fractal_be_backtest.py \
    EURUSD_H1.csv \
    EURUSD \
    1.5 \
    3 \
    2 \
    0

------------------------------------------------------------
"""

import sys
import os
import math
import pandas as pd
# import numpy as np


# ============================================================
# SPREADS
# ============================================================

SPREADS = {
    "EURUSD": 0.00009, "GBPUSD": 0.00012, "USDJPY": 0.012,
    "USDCHF": 0.00015, "USDCAD": 0.00018, "AUDUSD": 0.00010,
    "NZDUSD": 0.00018, "EURGBP": 0.00009, "EURJPY": 0.042,
    "EURCHF": 0.00018, "EURCAD": 0.00036, "EURAUD": 0.00030,
    "EURNZD": 0.00055, "GBPJPY": 0.030, "GBPCHF": 0.00058,
    "GBPCAD": 0.00055, "GBPAUD": 0.00052, "GBPNZD": 0.00090,
    "AUDJPY": 0.026, "AUDCHF": 0.00047, "AUDCAD": 0.00028,
    "AUDNZD": 0.00035, "NZDJPY": 0.033, "NZDCHF": 0.00056,
    "NZDCAD": 0.00038, "CADJPY": 0.032, "CADCHF": 0.00047,
    "CHFJPY": 0.065, "GOLD": 0.49, "US100": 1.90, "[SP500]": 0.60
}

# ============================================================
# DIGITS
# ============================================================

DIGITS = {
    "EURUSD": 5, "GBPUSD": 5, "USDJPY": 3, "USDCHF": 5,
    "USDCAD": 5, "AUDUSD": 5, "NZDUSD": 5, "EURGBP": 5,
    "EURJPY": 3, "EURCHF": 5, "EURCAD": 5, "EURAUD": 5,
    "EURNZD": 5, "GBPJPY": 3, "GBPCHF": 5, "GBPCAD": 5,
    "GBPAUD": 5, "GBPNZD": 5, "AUDJPY": 3, "AUDCHF": 5,
    "AUDCAD": 5, "AUDNZD": 5, "NZDJPY": 3, "NZDCHF": 5,
    "NZDCAD": 5, "CADJPY": 3, "CADCHF": 5, "CHFJPY": 3,
    "GOLD": 2, "US100": 2, "[SP500]": 2
}


# ============================================================
# HELPERS
# ============================================================

def round_price(price, digits):
    return round(price, digits)


def points_to_price(points, digits):
    return points * (10 ** (-digits))


# ============================================================
# LOAD CSV
# ============================================================

def load_csv(path):

    df = pd.read_csv(path, sep=';')

    numeric_cols = [
        'open',
        'high',
        'low',
        'close',
        'fractal_low',
        'fractal_high',
        'last_fractal_low',
        'last_fractal_high'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# ============================================================
# MAIN
# ============================================================

def run_backtest(
    csv_file,
    instrument,
    rrr,
    be_after_bars,
    be_offset_points,
    entry_retrace_percent
):

    spread = SPREADS[instrument]
    digits = DIGITS[instrument]

    be_offset_price = points_to_price(
        be_offset_points,
        digits
    )

    df = load_csv(csv_file)

    trades = []

    position = None

    # ========================================================
    # ITERATION
    # ========================================================

    for i in range(2, len(df)):

        row = df.iloc[i]

        # ====================================================
        # MANAGE OPEN POSITION
        # ====================================================

        if position is not None:

            bars_open = i - position['entry_index']

            # -----------------------------------------------
            # Move SL to BE
            # -----------------------------------------------

            if (
                not position['be_moved']
                and bars_open >= be_after_bars
            ):

                if position['direction'] == 'BUY':
                    position['sl'] = (
                        position['entry_price']
                        + be_offset_price
                    )

                else:
                    position['sl'] = (
                        position['entry_price']
                        - be_offset_price
                    )

                position['be_moved'] = True

            # -----------------------------------------------
            # Check exits
            # -----------------------------------------------

            high = row['high']
            low = row['low']

            exit_type = None
            exit_price = None

            # BUY
            if position['direction'] == 'BUY':

                # SL first
                if low <= position['sl']:

                    exit_price = position['sl']

                    if position['be_moved']:
                        exit_type = 'BE'
                    else:
                        exit_type = 'SL'

                # TP
                elif high >= position['tp']:

                    exit_price = position['tp']
                    exit_type = 'TP'

            # SELL
            else:

                # SL first
                if high >= position['sl']:

                    exit_price = position['sl']

                    if position['be_moved']:
                        exit_type = 'BE'
                    else:
                        exit_type = 'SL'

                # TP
                elif low <= position['tp']:

                    exit_price = position['tp']
                    exit_type = 'TP'

            # -----------------------------------------------
            # Close position
            # -----------------------------------------------

            if exit_type is not None:

                trades.append({
                    'entry_time': position['entry_time'],
                    'exit_time': row['timestamp'],
                    'direction': position['direction'],
                    'entry_price': position['entry_price'],
                    'sl_price': position['initial_sl'],
                    'tp_price': position['tp'],
                    'exit_price': exit_price,
                    'result': exit_type,
                    'bars_held': bars_open
                })

                position = None

            # jeśli zamknęliśmy trade -> nie otwieramy nowego
            if position is None:
                continue

        # ====================================================
        # ENTRY LOGIC
        # ====================================================

        if position is not None:
            continue

        signal = None

        # ----------------------------------------------------
        # FRACTAL LOW -> BUY
        # ----------------------------------------------------

        if not math.isnan(row['fractal_low']):

            signal = 'BUY'

        # ----------------------------------------------------
        # FRACTAL HIGH -> SELL
        # ----------------------------------------------------

        elif not math.isnan(row['fractal_high']):

            signal = 'SELL'

        if signal is None:
            continue

        # ====================================================
        # ENTRY PRICE
        # ====================================================

        open_price = row['open']

        prev_row = df.iloc[i - 1]

        prev_range = prev_row['high'] - prev_row['low']

        retrace = (
            prev_range
            * (entry_retrace_percent / 100.0)
        )

        # BUY
        if signal == 'BUY':

            desired_entry = (
                open_price - retrace
            )

            # sprawdzamy czy cena została osiągnięta
            if row['low'] > desired_entry:
                continue

            entry_price = desired_entry

            sl = (
                row['last_fractal_low']
                - spread
            )

            risk = entry_price - sl

            if risk <= 0:
                continue

            tp = entry_price + (risk * rrr)

        # SELL
        else:

            desired_entry = (
                open_price + retrace
            )

            if row['high'] < desired_entry:
                continue

            entry_price = desired_entry

            sl = (
                row['last_fractal_high']
                + spread
            )

            risk = sl - entry_price

            if risk <= 0:
                continue

            tp = entry_price - (risk * rrr)

        # ====================================================
        # ROUND
        # ====================================================

        entry_price = round_price(entry_price, digits)
        sl = round_price(sl, digits)
        tp = round_price(tp, digits)

        # ====================================================
        # OPEN POSITION
        # ====================================================

        position = {
            'direction': signal,
            'entry_time': row['timestamp'],
            'entry_index': i,
            'entry_price': entry_price,
            'initial_sl': sl,
            'sl': sl,
            'tp': tp,
            'be_moved': False
        }

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    trades_df = pd.DataFrame(trades)

    tp_count = (trades_df['result'] == 'TP').sum()
    sl_count = (trades_df['result'] == 'SL').sum()
    be_count = (trades_df['result'] == 'BE').sum()

    total = len(trades_df)

    summary = pd.DataFrame([
        {},
        {
            'entry_time': 'SUMMARY',
            'exit_time': '',
            'direction': '',
            'entry_price': '',
            'sl_price': '',
            'tp_price': '',
            'exit_price': '',
            'result': '',
            'bars_held': ''
        },
        {
            'entry_time': 'TOTAL_TRADES',
            'exit_time': total
        },
        {
            'entry_time': 'TP',
            'exit_time': tp_count
        },
        {
            'entry_time': 'SL',
            'exit_time': sl_count
        },
        {
            'entry_time': 'BE',
            'exit_time': be_count
        }
    ])

    output_df = pd.concat(
        [trades_df, summary],
        ignore_index=True
    )

    base_name = os.path.splitext(
        os.path.basename(csv_file)
    )[0]

    output_file = (
        f"{base_name}_backtest.csv"
    )

    output_df.to_csv(
        output_file,
        sep=';',
        index=False
    )

    print("\n================================================")
    print("BACKTEST FINISHED")
    print("================================================")
    print(f"Output: {output_file}")
    print(f"Trades: {total}")
    print(f"TP: {tp_count}")
    print(f"SL: {sl_count}")
    print(f"BE: {be_count}")
    print("================================================\n")


# ============================================================
# ENTRY
# ============================================================

def main():

    if len(sys.argv) != 7:

        print(
            "\nUsage:\n"
            "python3 fractal_be_backtest.py "
            "<csv_file> "
            "<instrument> "
            "<rrr> "
            "<be_after_bars> "
            "<be_offset_points> "
            "<entry_retrace_percent>\n"
        )

        sys.exit(1)

    csv_file = sys.argv[1]
    instrument = sys.argv[2]

    rrr = float(sys.argv[3])

    be_after_bars = int(sys.argv[4])

    be_offset_points = int(sys.argv[5])

    entry_retrace_percent = float(sys.argv[6])

    if instrument not in SPREADS:
        print(f"[ERROR] Unknown instrument: {instrument}")
        sys.exit(1)

    run_backtest(
        csv_file=csv_file,
        instrument=instrument,
        rrr=rrr,
        be_after_bars=be_after_bars,
        be_offset_points=be_offset_points,
        entry_retrace_percent=entry_retrace_percent
    )


if __name__ == "__main__":
    main()
