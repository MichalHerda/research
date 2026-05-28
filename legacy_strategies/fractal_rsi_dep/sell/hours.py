import os
import sys
import pandas as pd

OUTPUT_DIR_NAME = "hours_output"


def process_file(filepath):
    rows = []
    header = None

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            if line.startswith("open_time"):
                header = line.split(",")
                continue

            if line.startswith("TP="):
                continue

            parts = line.split(",")

            if len(parts) < 6:
                continue

            while len(parts) < len(header):
                parts.append("")

            rows.append(parts)

    df = pd.DataFrame(rows, columns=header)

    # datetime
    df['open_time'] = pd.to_datetime(df['open_time'], errors='coerce')
    df = df[df['open_time'].notna()]

    # godzina (upewnij się że int)
    df['hour'] = df['open_time'].dt.hour.astype(int)

    # win detection
    # last_col = df.columns[-1]
    # df[last_col] = df[last_col].astype(str).str.strip().str.lower()

    # df['win'] = df[last_col].isin(['true', '1', 't'])

    # Win detection: sprawdzamy wszystkie kolumny od 7 włącznie
    df['win'] = df.iloc[:, 6:].apply(
        lambda row: row.astype(str).str.strip().str.lower().eq('true').any(),
        axis=1
    )
    # agregacja
    grouped = df.groupby('hour').agg(
        trades=('win', 'count'),
        wins=('win', 'sum')
    )

    grouped['win_ratio'] = grouped['wins'] / grouped['trades'] * 100

    grouped = grouped.reset_index()

    # FIX typu
    grouped['hour'] = grouped['hour'].astype(int)
    grouped['hour_range'] = grouped['hour'].apply(lambda h: f"{h}-{h+1}")

    return grouped[['hour_range', 'trades', 'wins', 'win_ratio']]


def main(directory):
    output_dir = os.path.join(directory, OUTPUT_DIR_NAME)
    os.makedirs(output_dir, exist_ok=True)

    all_results = []

    for filename in os.listdir(directory):
        if filename.endswith("_backtest.csv"):
            filepath = os.path.join(directory, filename)
            instrument = filename.replace("_backtest.csv", "")

            try:
                result = process_file(filepath)
                result['instrument'] = instrument

                output_path = os.path.join(output_dir, f"{instrument}_hours.csv")
                result.to_csv(output_path, index=False)

                all_results.append(result)

                print(f"[OK] {instrument}")

            except Exception as e:
                print(f"[ERROR] {filename}: {e}")

    if all_results:
        combined = pd.concat(all_results)

        summary = combined.groupby('hour_range').agg(
            total_trades=('trades', 'sum'),
            total_wins=('wins', 'sum')
        )

        summary['win_ratio'] = summary['total_wins'] / summary['total_trades'] * 100
        summary = summary.reset_index()

        summary_path = os.path.join(output_dir, "all_backtests_summary.csv")
        summary.to_csv(summary_path, index=False)

        print("\n[SUMMARY SAVED]")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 hours.py <DIR>")
        sys.exit(1)

    directory = sys.argv[1]
    main(directory)
