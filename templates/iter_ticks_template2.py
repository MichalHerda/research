# iter_ticks_template2.py
# it differs from iter_ticks_template in that, it recognizes and works both files and directories

import pandas as pd
import sys
from pathlib import Path

# DATE_FORMAT = "%Y.%m.%d %H:%M:%S"
DATE_FORMAT = "%Y.%m.%d %H:%M:%S.%f"

REQUIRED_COLUMNS = [
    'timestamp', 'bid', 'ask'
]


def get_input_files(path: Path) -> list[Path]:
    """
    Returns list of CSV files.
    Input can be:
    - single file
    - directory with CSV files
    """

    if not path.exists():
        sys.exit(f"[ERROR] Path does not exist: {path}")

    if path.is_file():
        return [path]

    if path.is_dir():
        files = sorted(path.rglob("*.csv"))

        if not files:
            sys.exit(f"[ERROR] No CSV files found in {path}")

        return files

    sys.exit(f"[ERROR] Unsupported path type: {path}")


def load_and_clean_ohlcv(file_path: Path) -> pd.DataFrame:
    """
    Read CSV, columns presence validation, data conversion
    Returns ready to backtests DataFrame
    """
    if not file_path.exists():                                                                              # is_file()
        sys.exit(f"[ERROR] File does not exist: {file_path}")

    df = pd.read_csv(file_path, sep=";")

    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        sys.exit(f"[ERROR] In {file_path} are missing columns: {missing_columns}")

    df['timestamp'] = pd.to_datetime(df['timestamp'], format=DATE_FORMAT, errors="coerce")
    df = df.dropna(subset=['timestamp'])

    float_columns = REQUIRED_COLUMNS[1:]
    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors="coerce")

    # na razie mam pewność że moje dane są okej, ale w przyszłości można to odkomentować na wszelki wypadek
    # df = df.dropna(subset=['timestamp', 'bid', 'ask'])
    # df = df.sort_values('timestamp')

    return df


def process_file(file_path: Path):
    print(f"\n[INFO] Processing: {file_path.name}")

    df = load_and_clean_ohlcv(file_path)

    print("[INFO] Data pipeline finished successfully.")
    print(f"[INFO] rows: {len(df)}")

    results = run_backtest(df)

    results_dir = file_path.parent / "results"
    results_dir.mkdir(exist_ok=True)

    output_file = results_dir / f"{file_path.stem}_results.csv"

    save_results_to_csv(results, output_file)

    print(f"[INFO] Saved: {output_file}")


def save_results_to_csv(results: list, output: Path) -> None:
    if not results:
        print("[WARNING] no results to save")
        return

    results_df = pd.DataFrame(results)
    # results_df.to_csv(output, sep=";", date_format=DATE_FORMAT, index=False)
    results_df.to_csv(
        output,
        sep=";",
        date_format=DATE_FORMAT,
        float_format="%.5f",
        index=False
    )


def run_backtest(df: pd.DataFrame) -> list:
    print("[INFO] start running backtest")
    trade_logs = []
    # in_position = False

    # spreads = df['ask'] - df['bid']
    for row in df.itertuples(index=False):
        spread = row.ask - row.bid
        trade_logs.append({
         'timestamp': row.timestamp,
         'spread': spread
        })

    return trade_logs


def main():
    if len(sys.argv) != 2:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file_or_directory>")

    input_path = Path(sys.argv[1])

    files = get_input_files(input_path)

    print(f"[INFO] Found {len(files)} file(s)")

    for file_path in files:
        process_file(file_path)


if __name__ == "__main__":
    main()
