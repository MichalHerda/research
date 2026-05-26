import pandas as pd
import sys
import os


def backtest(file_path, rsi_below, rr_ratio):
    df = pd.read_csv(file_path, sep=';')

    # konwersje
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['RSI_M5_M5'] = pd.to_numeric(df['RSI_M5_M5'], errors='coerce')
    df['last_pivot_H1'] = pd.to_numeric(df['last_pivot_H1'], errors='coerce')

    trades = []

    in_position = False
    entry_price = None
    sl = None
    tp = None
    open_time = None

    tp_count = 0
    sl_count = 0

    for i in range(2, len(df)):
        row_0 = df.iloc[i]     # current
        row_1 = df.iloc[i-1]
        row_2 = df.iloc[i-2]

        if not in_position:
            # --- WARUNKI TRENDU ---
            if not (row_0['UP_H1_H1'] == True and row_0['UP_D1_D1'] == True):   # noqa
                continue

            # --- RSI ---
            if pd.isna(row_2['RSI_M5_M5']) or pd.isna(row_1['RSI_M5_M5']):
                continue

            if not (row_2['RSI_M5_M5'] < rsi_below and row_1['RSI_M5_M5'] > row_2['RSI_M5_M5']):
                continue

            # --- SL ---
            if pd.isna(row_1['last_pivot_H1']):
                continue

            entry_price = row_0['open_M5']
            sl = row_1['last_pivot_H1']

            # zabezpieczenie (BUY: SL musi być niżej)
            if sl >= entry_price:
                continue

            risk = entry_price - sl
            tp = entry_price + (risk * rr_ratio)

            open_time = row_0['timestamp']
            in_position = True

        else:
            # --- SPRAWDZANIE WYJŚCIA ---
            low = row_0['low_M5']
            high = row_0['high_M5']

            close_time = row_0['timestamp']

            # SL first (konserwatywnie)
            if low <= sl:
                trades.append({
                    'open_time': open_time,
                    'close_time': close_time,
                    'open_price': entry_price,
                    'close_price': sl,
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
                    'TP': True
                })
                tp_count += 1
                in_position = False

    # --- OUTPUT ---
    trades_df = pd.DataFrame(trades)

    instrument = os.path.basename(file_path).replace('.csv', '')
    output_file = f"{instrument}_backtest.csv"

    trades_df.to_csv(output_file, index=False)

    total = tp_count + sl_count
    win_ratio = (tp_count / total * 100) if total > 0 else 0

    # dopisanie summary
    with open(output_file, 'a') as f:
        f.write("\n")
        f.write(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%\n")

    print(f"Zapisano: {output_file}")
    print(f"TP={tp_count}, SL={sl_count}, WinRatio={win_ratio:.2f}%")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Użycie: python3 test_pullback.py <file> <rsi_below> <risk_reward_ratio>")
        sys.exit(1)

    file_path = sys.argv[1]
    rsi_below = float(sys.argv[2])
    rr_ratio = float(sys.argv[3])

    backtest(file_path, rsi_below, rr_ratio)
