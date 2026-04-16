#!/usr/bin/env python3

import sys
import os
import pandas as pd


# === GLOBAL CONFIG ===
MINIMUM_SL_ATR = 1
MAXIMUM_TP_ATR = 30
FOLLOW_TREND = True


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


def get_instrument(filepath):
    return os.path.basename(filepath).split('_')[0]


def load_data(filepath):
    df = pd.read_csv(filepath)

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    numeric_cols = [
        'open', 'high', 'low', 'close',
        'ATR', 'deviation_atr', 'EMA_diff',
        'fractal_high', 'fractal_low'
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['uptrend'] = df['uptrend'].astype(bool)

    df = df.dropna().reset_index(drop=True)

    return df


def run_backtest(df, direction, dev_threshold, ema_diff_threshold, rr, spread):
    trades = []

    in_position = False
    waiting_for_break = False  # po wybiciu dev_atr

    entry_price = sl = tp = None
    open_time = None

    tp_count = 0
    sl_count = 0

    for i in range(len(df)):
        row = df.iloc[i]

        # === TRYB OTWARTEJ POZYCJI ===
        if in_position:
            high = row['high']
            low = row['low']
            close_time = row['timestamp']

            if direction == "buy":
                # BUY zamyka się na BID
                if low <= sl:
                    trades.append(make_trade(open_time, close_time, entry_price, sl, sl, tp, False))
                    sl_count += 1
                    in_position = False

                elif high >= tp:
                    trades.append(make_trade(open_time, close_time, entry_price, tp, sl, tp, True))
                    tp_count += 1
                    in_position = False

            else:
                # SELL → high/low + spread
                high_adj = high + spread
                low_adj = low + spread

                if high_adj >= sl:
                    trades.append(make_trade(open_time, close_time, entry_price, sl, sl, tp, False))
                    sl_count += 1
                    in_position = False

                elif low_adj <= tp:
                    trades.append(make_trade(open_time, close_time, entry_price, tp, sl, tp, True))
                    tp_count += 1
                    in_position = False

            continue

        # === TRYB SZUKANIA SYGNAŁU ===

        # 1. Trigger deviation_atr
        if not waiting_for_break:
            if direction == "buy" and row['deviation_atr'] <= dev_threshold:
                waiting_for_break = True
            elif direction == "sell" and row['deviation_atr'] >= dev_threshold:
                waiting_for_break = True
            else:
                continue

        # 2. Filtr EMA_diff
        if direction == "buy" and row['EMA_diff'] < ema_diff_threshold:
            continue
        if direction == "sell" and row['EMA_diff'] > ema_diff_threshold:
            continue

        # 3. Trend filter
        if FOLLOW_TREND:
            if direction == "buy" and not row['uptrend']:
                continue
            if direction == "sell" and row['uptrend']:
                continue

        # 4. Break fractal
        if direction == "buy":
            if row['open'] <= row['fractal_high']:
                continue
        else:
            if row['open'] >= row['fractal_low']:
                continue

        # === OTWARCIE TRANSAKCJI ===
        atr = row['ATR']

        if direction == "buy":
            entry_price = row['open'] + spread
            sl = row['fractal_low']

            sl_distance = entry_price - sl

            # minimalny SL
            if sl_distance < atr * MINIMUM_SL_ATR:
                sl = entry_price - atr * MINIMUM_SL_ATR
                sl_distance = entry_price - sl

            tp = entry_price + (sl_distance * rr)

        else:
            entry_price = row['open']
            sl = row['fractal_high']

            sl_distance = sl - entry_price

            if sl_distance < atr * MINIMUM_SL_ATR:
                sl = entry_price + atr * MINIMUM_SL_ATR
                sl_distance = sl - entry_price

            tp = entry_price - (sl_distance * rr)

        # max TP constraint
        if (abs(tp - entry_price) / atr) > MAXIMUM_TP_ATR:
            waiting_for_break = False
            continue

        open_time = row['timestamp']
        in_position = True
        waiting_for_break = False

    trades_df = pd.DataFrame(trades)

    total = tp_count + sl_count
    win_ratio = (tp_count / total * 100) if total > 0 else 0

    return trades_df, tp_count, sl_count, win_ratio


def make_trade(open_time, close_time, open_price, close_price, sl, tp, is_tp):
    return {
        'open_time': open_time,
        'close_time': close_time,
        'open_price': open_price,
        'close_price': close_price,
        'sl': sl,
        'tp': tp,
        'TP': is_tp
    }


def main():
    if len(sys.argv) != 6:
        print("Usage: python3 btzsc1.py <file> <buy/sell> <dev_atr_threshold> <ema_diff_threshold> <rrr>")
        sys.exit(1)

    filepath = sys.argv[1]
    direction = sys.argv[2].lower()
    dev_threshold = float(sys.argv[3])
    ema_diff_threshold = float(sys.argv[4])
    rr = float(sys.argv[5])

    instrument = get_instrument(filepath)
    spread = SPREADS.get(instrument, 0)

    print(f"Instrument: {instrument}, Spread: {spread}")

    df = load_data(filepath)

    trades_df, tp_count, sl_count, win_ratio = run_backtest(
        df, direction, dev_threshold, ema_diff_threshold, rr, spread
    )

    dev_str = str(dev_threshold).replace('.', '_')
    ema_str = str(ema_diff_threshold).replace('.', '_')
    rr_str = str(rr).replace('.', '_')

    output_file = f"{instrument}_btzsc1_{direction}_dev{dev_str}_ema{ema_str}_rr{rr_str}.csv"

    trades_df.to_csv(output_file, index=False)

    with open(output_file, 'a') as f:
        f.write("\n")
        f.write(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%\n")

    print(f"Saved: {output_file}")
    print(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%")


if __name__ == "__main__":
    main()
