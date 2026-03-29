import pandas as pd
import os
import sys


def summarize_backtests(input_dir):
    files = [f for f in os.listdir(input_dir) if f.endswith("_backtest.csv")]

    total_tp = 0
    total_sl = 0
    all_trades = []

    for file in files:
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path)

        # filtrujemy linie podsumowania (jeśli istnieją)
        df = df[df['open_time'].notna()]

        tp_count = df['TP'].notna() & df['TP']
        sl_count = df['TP'].isna() | (df['TP'] == False)    # noqa

        total_tp += tp_count.sum()
        total_sl += sl_count.sum()
        all_trades.append(df)

    total_trades = total_tp + total_sl
    win_ratio = (total_tp / total_trades * 100) if total_trades > 0 else 0

    # zapis podsumowania
    summary_file = os.path.join(input_dir, "all_backtests_summary.csv")
    summary_df = pd.DataFrame({
        'TP': [total_tp],
        'SL': [total_sl],
        'WinRatio': [round(win_ratio, 2)]
    })
    summary_df.to_csv(summary_file, index=False)

    print(f"Zapisano podsumowanie: {summary_file}")
    print(f"TP={total_tp}, SL={total_sl}, WinRatio={win_ratio:.2f}%")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Użycie: python3 resume_backtest.py <directory>")
        sys.exit(1)

    input_dir = sys.argv[1]
    summarize_backtests(input_dir)
