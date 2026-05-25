#!/usr/bin/env python3

import pandas as pd
import sys
import os
from zoneinfo import ZoneInfo


SPREADS = {
    "EURUSD": 0.00009,
    "GBPUSD": 0.00012,
    "USDJPY": 0.012,
    "USDCHF": 0.00015,
    "USDCAD": 0.00018,
    "AUDUSD": 0.00010,
    "NZDUSD": 0.00018,
    "EURGBP": 0.00009,
    "EURJPY": 0.042,
    "EURCHF": 0.00018,
    "EURCAD": 0.00036,
    "EURAUD": 0.00030,
    "EURNZD": 0.00055,
    "GBPJPY": 0.030,
    "GBPCHF": 0.00058,
    "GBPCAD": 0.00055,
    "GBPAUD": 0.00052,
    "GBPNZD": 0.00090,
    "AUDJPY": 0.026,
    "AUDCHF": 0.00047,
    "AUDCAD": 0.00028,
    "AUDNZD": 0.00035,
    "NZDJPY": 0.033,
    "NZDCHF": 0.00056,
    "NZDCAD": 0.00038,
    "CADJPY": 0.032,
    "CADCHF": 0.00047,
    "CHFJPY": 0.065,
    "GOLD": 0.49,
    "US100": 1.90,
    "[SP500]": 0.60
}

DIGITS = {
    "AUDCAD": 5, "AUDCHF": 5, "AUDJPY": 3, "AUDNZD": 5, "AUDUSD": 5,
    "CADCHF": 5, "CADJPY": 3, "CHFJPY": 3, "EURAUD": 5, "EURCAD": 5,
    "EURCHF": 5, "EURGBP": 5, "EURJPY": 3, "EURNZD": 5, "EURUSD": 5,
    "GBPAUD": 5, "GBPCAD": 5, "GBPCHF": 5, "GBPNZD": 5, "GBPUSD": 5,
    "NZDCAD": 5, "NZDCHF": 5, "NZDJPY": 3, "NZDUSD": 5,
    "US100": 2, "USDCAD": 5, "USDCHF": 5, "USDJPY": 3,
    "[SP500]": 2, "[NQ100]": 2,
    "GBPJPY": 3, "GOLD": 2
}

BEGIN_NIGHT_BREAK_NY = 15
END_NIGHT_BREAK_NY = 19

ATR_H1_SL_MIN = 0.5
ATR_D1_TP_MAX = 1.1
SPREAD_SL_MAX = 0.2


# =========================
# TIME FILTER
# =========================

def is_trading_allowed(timestamp):
    broker_tz = ZoneInfo("Europe/Bucharest")
    timestamp = timestamp.replace(tzinfo=broker_tz)

    ny_time = timestamp.astimezone(ZoneInfo("America/New_York"))
    hour = ny_time.hour

    return not (BEGIN_NIGHT_BREAK_NY <= hour < END_NIGHT_BREAK_NY)


# =========================
# BACKTEST CORE (BUY)
# =========================

def run_backtest(df, rsi_above, rr_ratio, spread, digits):

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    df['RSI_M5_M5'] = pd.to_numeric(df['RSI_M5_M5'], errors='coerce')
    df['last_pivot_H1'] = pd.to_numeric(df['last_pivot_H1'], errors='coerce')
    df['ATR_H1_H1'] = pd.to_numeric(df['ATR_H1_H1'], errors='coerce')
    df['ATR_D1_D1'] = pd.to_numeric(df['ATR_D1_D1'], errors='coerce')

    trades = []

    in_position = False
    entry_price = None
    sl = None
    tp = None
    open_time = None

    tp_count = 0
    sl_count = 0

    for i in range(2, len(df)):

        row_0 = df.iloc[i]
        row_1 = df.iloc[i - 1]
        row_2 = df.iloc[i - 2]

        # =========================
        # ENTRY FILTERS
        # =========================

        if not in_position and not is_trading_allowed(row_0['timestamp']):
            continue

        if not in_position:

            # --- PIVOT FILTER (BUY: pivot BELOW price) ---
            pivot = row_1['last_pivot_H1']
            if pd.isna(pivot):
                continue

            ask_price = row_0['open_M5']

            if pivot >= ask_price:
                continue

            # --- RSI CONDITION (lookback safe) ---
            if pd.isna(row_2['RSI_M5_M5']) or pd.isna(row_1['RSI_M5_M5']):
                continue

            if not (
                row_2['RSI_M5_M5'] < rsi_above and
                row_1['RSI_M5_M5'] > row_2['RSI_M5_M5']
            ):
                continue

            # =========================
            # ENTRY PRICE / SL
            # =========================

            entry_price = row_0['open_M5']
            sl = pivot - spread   # BUY: SL below pivot

            entry_price = round(entry_price, digits)
            sl = round(sl, digits)

            if sl >= entry_price:
                continue

            sl_ratio = entry_price - sl

            # --- ATR H1 FILTER ---
            atr_h1 = row_1['ATR_H1_H1']
            if pd.isna(atr_h1):
                continue

            if sl_ratio < (atr_h1 * ATR_H1_SL_MIN):
                continue

            # --- TP CALC ---
            tp = entry_price + (sl_ratio * rr_ratio)
            tp = round(tp, digits)

            # --- ATR D1 FILTER ---
            atr_d1 = row_1['ATR_D1_D1']
            if pd.isna(atr_d1):
                continue

            tp_distance = tp - entry_price

            if tp_distance > (atr_d1 * ATR_D1_TP_MAX):
                continue

            # --- SPREAD FILTER ---
            if spread > (sl_ratio * SPREAD_SL_MAX):
                continue

            # BUY mapping (ODWRÓCENIE logiki SELL)
            real_tp = tp - spread
            real_sl = sl + spread

            open_time = row_0['timestamp']
            in_position = True

        else:
            low = row_0['low_M5']
            high = row_0['high_M5']
            close_time = row_0['timestamp']

            # SL first (konserwatywnie)
            if high >= real_tp:
                trades.append({
                    'open_time': open_time,
                    'close_time': close_time,
                    'open_price': entry_price,
                    'close_price': real_tp,
                    'sl': real_sl,
                    'tp': real_tp,
                    'TP': True
                })
                tp_count += 1
                in_position = False

            elif low <= real_sl:
                trades.append({
                    'open_time': open_time,
                    'close_time': close_time,
                    'open_price': entry_price,
                    'close_price': real_sl,
                    'sl': real_sl,
                    'tp': real_tp,
                    'TP': ''
                })
                sl_count += 1
                in_position = False

    trades_df = pd.DataFrame(trades)

    total = tp_count + sl_count
    win_ratio = (tp_count / total * 100) if total > 0 else 0

    return trades_df, tp_count, sl_count, win_ratio


# =========================
# MAIN
# =========================

def main(input_dir, rsi_above, rr_ratio):

    output_dir = os.path.join(input_dir, "backtest_buy_results")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

    for file in files:

        file_path = os.path.join(input_dir, file)
        print(f"Processing: {file}")

        df = pd.read_csv(file_path, sep=';')

        instrument = file.replace('.csv', '').split('_')[0]
        spread = SPREADS.get(instrument, 0)
        digits = DIGITS.get(instrument, 0)

        if spread == 0:
            print(f"WARNING: No spread for {instrument}")
        if digits == 0:
            print(f"WARNING: No digits for {instrument}")

        print(instrument, ", spread:", spread, ", digits:", digits)

        trades_df, tp_count, sl_count, win_ratio = run_backtest(
            df, rsi_above, rr_ratio, spread, digits
        )

        output_file = os.path.join(output_dir, f"{instrument}_buy_backtest.csv")

        trades_df.to_csv(output_file, index=False)

        with open(output_file, 'a') as f:
            f.write("\n")
            f.write(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%\n")

        print(f"Saved: {output_file}")


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Użycie: python3 test_buy_pullback_batch.py <directory> <rsi_above> <risk_reward_ratio>")
        sys.exit(1)

    input_dir = sys.argv[1]
    rsi_above = float(sys.argv[2])
    rr_ratio = float(sys.argv[3])

    main(input_dir, rsi_above, rr_ratio)
