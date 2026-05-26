#!/usr/bin/env python3

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


def load_data(filepath):
    df = pd.read_csv(filepath)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df.dropna().reset_index(drop=True)


def get_instrument(filepath):
    return os.path.basename(filepath).split('_')[0]


def run_backtest(data, direction, dev_threshold, ema_diff_threshold, rr, spread):
    if isinstance(data, pd.DataFrame):
        data = data.to_dict('records')

    in_pos = False
    waiting_for_break = False
    pending_signal = False
    sl = tp = 0
    tp_count = sl_count = 0

    for row in data:
        if in_pos:
            if direction == "buy":
                if row['low'] <= sl:
                    sl_count += 1
                    in_pos = False
                elif row['high'] >= tp:
                    tp_count += 1
                    in_pos = False
            else:
                high_ask = row['high'] + spread
                low_ask = row['low'] + spread
                if high_ask >= sl:
                    sl_count += 1
                    in_pos = False
                elif low_ask <= tp:
                    tp_count += 1
                    in_pos = False
            continue

        if pending_signal:
            atr = row['ATR']
            if direction == "buy":
                entry = row['open'] + spread
                sl = row['fractal_low']
                dist = entry - sl
                if dist < atr:
                    sl = entry - atr
                    dist = atr
                tp = entry + (dist * rr)
            else:
                entry = row['open']
                sl = row['fractal_high']
                dist = sl - entry
                if dist < atr:
                    sl = entry + atr
                    dist = atr
                tp = entry - (dist * rr)
            in_pos = True
            pending_signal = False
            continue

        if not waiting_for_break:
            if direction == "buy" and row['deviation_atr'] <= dev_threshold:
                waiting_for_break = True
            elif direction == "sell" and row['deviation_atr'] >= dev_threshold:
                waiting_for_break = True
            continue

        if direction == "buy":
            if row['EMA_diff'] >= ema_diff_threshold and row['uptrend']:
                if row['close'] > row['fractal_high']:
                    pending_signal = True
                    waiting_for_break = False
        else:
            if row['EMA_diff'] <= ema_diff_threshold and not row['uptrend']:
                if row['close'] < row['fractal_low']:
                    pending_signal = True
                    waiting_for_break = False

    total = tp_count + sl_count
    wr = (tp_count / total * 100) if total > 0 else 0
    return None, tp_count, sl_count, wr
