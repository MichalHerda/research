import sys
import pandas as pd
from pathlib import Path


def compute_fractals(df):
    high = df['high']
    low = df['low']

    fractal_high_raw = (
        (high.shift(2) < high) &
        (high.shift(1) < high) &
        (high.shift(-1) < high) &
        (high.shift(-2) < high)
    )

    fractal_low_raw = (
        (low.shift(2) > low) &
        (low.shift(1) > low) &
        (low.shift(-1) > low) &
        (low.shift(-2) > low)
    )

    df['fractal_low'] = df['low'].where(fractal_low_raw)
    df['fractal_high'] = df['high'].where(fractal_high_raw)

    return df


def main():
    print("\n=== START SKRYPTU ===")

    if len(sys.argv) != 2:
        print("[ERROR] Użycie: python3 add_fractals.py <nazwa_pliku>")
        sys.exit(1)

    file_path = Path(sys.argv[1]).resolve()
    print(f"[INFO] Search in path: {file_path}")

    if not file_path.exists():
        print(f"[ERROR] File {file_path.name} not exists!")
        sys.exit(2)

    ohlcv_types = {
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'int64'
    }

    print("[INFO] Read data...")
    df = pd.read_csv(
        file_path,
        sep=";",
        dtype=ohlcv_types,
        parse_dates=['timestamp'],
        date_format="%Y.%m.%d %H:%M:%S"
    )

    print(f"[INFO] Rows loaded: {len(df)}")

    df = compute_fractals(df)

    # Liczenie ile faktycznie fraktali wpadło (dla weryfikacji)
    print(f"[INFO] Found fractals high: {df['fractal_high'].notna().sum()}")
    print(f"[INFO] Found fractals low: {df['fractal_low'].notna().sum()}")

    output_path = file_path.parent / f"{file_path.stem}_fractals.csv"
    print(f"[INFO] Save new file to: {output_path}")

    df.to_csv(output_path, sep=";", index=False, date_format="%Y.%m.%d %H:%M:%S")
    print("=== EXIT ===\n")


if __name__ == "__main__":
    main()
