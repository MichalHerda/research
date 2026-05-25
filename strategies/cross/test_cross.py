import pandas as pd
import os
import sys

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


# ====== CORE ======
def run_backtest(df, spread, digits):
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    cols = ['SMA_FAST_M15', 'SMA_SLOW_M15', 'LAST_MIN']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    trades = []

    in_position = False
    entry = sl = tp = None
    open_time = None

    pip_factor = 10 ** digits

    for i in range(2, len(df) - 1):
        r0 = df.iloc[i]
        r1 = df.iloc[i - 1]
        r2 = df.iloc[i - 2]
        r_next = df.iloc[i + 1]

        # ================= ENTRY =================
        if not in_position:
            if not (r0['FAST_ABOVE_D1'] == True):                           # noqa
                continue

            if pd.isna(r2['SMA_FAST_M15']) or pd.isna(r2['SMA_SLOW_M15']):
                continue
            if pd.isna(r1['SMA_FAST_M15']) or pd.isna(r1['SMA_SLOW_M15']):
                continue

            cross_up = (
                r2['SMA_FAST_M15'] <= r2['SMA_SLOW_M15'] and
                r1['SMA_FAST_M15'] > r1['SMA_SLOW_M15']
            )

            if not cross_up:
                continue

            if pd.isna(r1['LAST_MIN']):
                continue

            entry = r0['open_x'] + spread
            sl = r1['LAST_MIN'] - spread

            if sl >= entry:
                continue

            risk = entry - sl
            tp = entry + (risk * 3)

            entry = round(entry, digits)
            sl = round(sl, digits)
            tp = round(tp, digits)

            open_time = r0['timestamp']
            in_position = True

        # ================= EXIT =================
        else:
            low = r0['low_x']
            high = r0['high_x']

            # --- SL ---
            if low <= sl:
                result = (sl - entry) * pip_factor

                trades.append({
                    "open_time": open_time,
                    "close_time": r0['timestamp'],
                    "open_price": entry,
                    "close_price": sl,
                    "exit_type": "SL",
                    "result": result
                })

                in_position = False
                continue

            # --- TP ---
            if high >= tp:
                result = (tp - entry) * pip_factor

                trades.append({
                    "open_time": open_time,
                    "close_time": r0['timestamp'],
                    "open_price": entry,
                    "close_price": tp,
                    "exit_type": "TP",
                    "result": result
                })

                in_position = False
                continue

            # --- CROSS DOWN ---
            if (
                not pd.isna(r2['SMA_FAST_M15']) and
                not pd.isna(r2['SMA_SLOW_M15']) and
                not pd.isna(r1['SMA_FAST_M15']) and
                not pd.isna(r1['SMA_SLOW_M15'])
            ):
                cross_down = (
                    r2['SMA_FAST_M15'] >= r2['SMA_SLOW_M15'] and
                    r1['SMA_FAST_M15'] < r1['SMA_SLOW_M15']
                )

                if cross_down:
                    exit_price = r_next['open_x']
                    result = (exit_price - entry) * pip_factor

                    exit_type = "PLUS" if result > 0 else "MINUS"

                    trades.append({
                        "open_time": open_time,
                        "close_time": r_next['timestamp'],
                        "open_price": entry,
                        "close_price": exit_price,
                        "exit_type": exit_type,
                        "result": result
                    })

                    in_position = False

    trades_df = pd.DataFrame(trades)

    total_pips = trades_df['result'].sum() if len(trades_df) > 0 else 0
    trades_count = len(trades_df)

    return trades_df, total_pips, trades_count


# ====== MAIN ======
def main(input_dir):
    output_dir = os.path.join(input_dir, "cross_results")
    os.makedirs(output_dir, exist_ok=True)

    summary = []

    for file in os.listdir(input_dir):
        if not file.endswith(".csv"):
            continue

        path = os.path.join(input_dir, file)
        df = pd.read_csv(path, sep=';', low_memory=False)

        instrument = file.replace('_aligned.csv', '')
        spread = SPREADS.get(instrument, 0)
        digits = DIGITS.get(instrument, 5)

        print(f"Processing {instrument}")

        trades_df, total_pips, trades_count = run_backtest(df, spread, digits)

        out_file = os.path.join(output_dir, f"{instrument}_trades.csv")
        trades_df.to_csv(out_file, index=False)

        summary.append({
            "instrument": instrument,
            "trades": trades_count,
            "total_pips": total_pips
        })

    summary_df = pd.DataFrame(summary)
    summary_file = os.path.join(output_dir, "SUMMARY.csv")
    summary_df.to_csv(summary_file, index=False)

    print("DONE")


# ====== CLI ======
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Użycie: python3 test_cross.py <DIR>")
        sys.exit(1)

    main(sys.argv[1])
