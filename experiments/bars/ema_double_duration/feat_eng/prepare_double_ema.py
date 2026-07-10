# prepare_double_ema.py

import pandas as pd
import sys
from pathlib import Path

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = [
    'timestamp', 'open', 'high', 'low', 'close',
]


def compute_ema(series, period):
    return series.ewm(
        span=period,
        adjust=False
    ).mean()


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

    return df


def save_results_to_csv(results: pd.DataFrame, output: Path):

    if results.empty:
        print("[WARNING] no results to save")
        return

    results.to_csv(
        output,
        sep=";",
        date_format=DATE_FORMAT,
        index=False,
    )


def add_features(df: pd.DataFrame, ema1_duration: int, ema2_duration: int) -> pd.DataFrame:
    print("[INFO] start appending features")

    df[f"ema_{ema1_duration}"] = compute_ema(df["close"], ema1_duration)
    df[f"ema_{ema2_duration}"] = compute_ema(df["close"], ema2_duration)

    return df


def main():
    if len(sys.argv) != 4:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file or directory> <ema1_duration> <ema2_duration>")

    input_path = Path(sys.argv[1])
    ema1_duration = int(sys.argv[2])
    ema2_duration = int(sys.argv[3])
    files = get_input_files(input_path)

    print(f"[INFO] Found {len(files)} file(s)")

    for file_path in files:
        df = load_and_clean_ohlcv(file_path)
        print(f"[INFO] Processing {file_path.name}")
        features = add_features(df, ema1_duration, ema2_duration)
        output_dir = file_path.parent / "features"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{file_path.stem}_features.csv"
        save_results_to_csv(features, output_file)

    print("[INFO] All files processed successfully.")


if __name__ == "__main__":
    main()
