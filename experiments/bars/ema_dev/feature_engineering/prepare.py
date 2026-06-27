# feature_enginering_tpl.py
# NOTE: Minimal template for feature engineering on OHLCV data.
# In most cases features should be computed using vectorized pandas/numpy operations.
# Loop-based processing is only used for stateful or path-dependent logic.

import pandas as pd
import numpy as np
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


def compute_atr(df, period):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    ).max(axis=1)

    atr = tr.rolling(window=period).mean()

    return atr


def compute_ema_deviation(high, low, fast_ema, atr):

    z_high = (high - fast_ema) / atr
    z_low = (low - fast_ema) / atr

    return np.where(
        np.abs(z_high) >= np.abs(z_low),
        z_high,
        z_low
    )


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


def compute_proto_fractals(df):
    high = df['high']
    low = df['low']
    open = df['open']

    fractal_high_raw = (
        (high.shift(2) > open) &
        (high.shift(2) > high.shift(1)) &
        (high.shift(2) > high.shift(3))
        # (high.shift(2) > high.shift(4))
    )

    fractal_low_raw = (
        (low.shift(2) < open) &
        (low.shift(2) < low.shift(1)) &
        (low.shift(2) < low.shift(3))
        # (low.shift(2) < low.shift(4))
    )

    df['fractal_high'] = df['high'].where(fractal_high_raw.shift(-2))
    df['fractal_low'] = df['low'].where(fractal_low_raw.shift(-2))

    return df


def save_results_to_csv(results: list, output: Path) -> None:
    if not results:
        print("[WARNING] no results to save")
        return

    results_df = pd.DataFrame(results)
    results_df.to_csv(output, sep=";", date_format=DATE_FORMAT, index=False)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    print("[INFO] start appending features")

    print("[INFO] start feature engineering")

    df["ema_fast"] = compute_ema(df["close"], 20)
    df["atr"] = compute_atr(df, 14)
    df["ema_dev"] = compute_ema_deviation(df["high"], df["low"], df["ema_fast"], df["atr"])

    df = compute_proto_fractals(df)

    return df


def main():
    if len(sys.argv) != 2:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file>")

    file_path = Path(sys.argv[1])

    df = load_and_clean_ohlcv(file_path)
    print("[INFO] Data pipeline finished successfully.")
    print(f"[INFO] head: {df.head()}")
    print(f"[INFO] tail: {df.tail()}")

    features = add_features(df)
    output_dir = file_path.parent / "features"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{file_path.stem}_features.csv"
    save_results_to_csv(features, output_file)


if __name__ == "__main__":
    main()
