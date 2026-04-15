#!/usr/bin/env python3

import sys
import os
import pandas as pd


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


def get_instrument(filename):
    return os.path.basename(filename).split('_')[0]


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    numeric_cols = ['open', 'high', 'low', 'close', 'ATR', 'deviation_atr']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna().reset_index(drop=True)

    return df


def check_entry(prev2, prev1, direction, threshold):
    if direction == "buy":
        return prev2 > threshold and prev1 <= threshold
    else:
        return prev2 < threshold and prev1 >= threshold


def run_backtest(df, direction, threshold, sl_atr_mult, rr, spread):
    trades = []

    in_position = False

    entry_price = None
    sl = None
    tp = None
    open_time = None

    tp_count = 0
    sl_count = 0

    for i in range(2, len(df)):
        row = df.iloc[i]
        prev1 = df.iloc[i - 1]
        prev2 = df.iloc[i - 2]

        if not in_position:
            if not check_entry(prev2['deviation_atr'], prev1['deviation_atr'], direction, threshold):
                continue

            if direction == "buy":
                entry_price = row['open'] + spread
                sl = entry_price - (prev1['ATR'] * sl_atr_mult)
                tp = entry_price + ((entry_price - sl) * rr)
            else:
                entry_price = row['open']
                sl = entry_price + (prev1['ATR'] * sl_atr_mult)
                tp = entry_price - ((sl - entry_price) * rr)

            open_time = row['timestamp']
            in_position = True

        else:
            low = row['low']
            high = row['high']
            close_time = row['timestamp']

            if direction == "buy":
                if low <= sl:
                    trades.append({
                        'open_time': open_time,
                        'close_time': close_time,
                        'open_price': entry_price,
                        'close_price': sl,
                        'sl': sl,
                        'tp': tp,
                        'TP': ''
                    })
                    sl_count += 1
                    in_position = False

                elif high >= tp:
                    trades.append({
                        'open_time': open_time,
                        'close_time': close_time,
                        'open_price': entry_price,
                        'close_price': tp,
                        'sl': sl,
                        'tp': tp,
                        'TP': True
                    })
                    tp_count += 1
                    in_position = False

            else:
                if high >= sl:
                    trades.append({
                        'open_time': open_time,
                        'close_time': close_time,
                        'open_price': entry_price,
                        'close_price': sl,
                        'sl': sl,
                        'tp': tp,
                        'TP': ''
                    })
                    sl_count += 1
                    in_position = False

                elif low <= tp:
                    trades.append({
                        'open_time': open_time,
                        'close_time': close_time,
                        'open_price': entry_price,
                        'close_price': tp,
                        'sl': sl,
                        'tp': tp,
                        'TP': True
                    })
                    tp_count += 1
                    in_position = False

    trades_df = pd.DataFrame(trades)

    total = tp_count + sl_count
    win_ratio = (tp_count / total * 100) if total > 0 else 0

    return trades_df, tp_count, sl_count, win_ratio


def main():
    if len(sys.argv) != 6:
        print("Usage: python3 bt_atrdev.py <file> <buy/sell> <dev_atr_threshold> <sl_atr_mult> <rr>")
        sys.exit(1)

    filepath = sys.argv[1]
    direction = sys.argv[2].lower()
    threshold = float(sys.argv[3])
    sl_atr_mult = float(sys.argv[4])
    rr = float(sys.argv[5])

    instrument = get_instrument(filepath)
    spread = SPREADS.get(instrument, 0)

    print(f"Instrument: {instrument}, Spread: {spread}")

    df = load_data(filepath)

    trades_df, tp_count, sl_count, win_ratio = run_backtest(
        df, direction, threshold, sl_atr_mult, rr, spread
    )

    # ✅ NOWE: bezpieczna nazwa pliku
    thr_str = str(threshold).replace('.', '_')
    rr_str = str(rr).replace('.', '_')

    output_file = f"{instrument}_bt_thr{thr_str}_rr{rr_str}.csv"

    trades_df.to_csv(output_file, index=False)

    with open(output_file, 'a') as f:
        f.write("\n")
        f.write(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%\n")

    print(f"Saved: {output_file}")
    print(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%")


if __name__ == "__main__":
    main()
