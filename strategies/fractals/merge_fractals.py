import sys
import pandas as pd
from pathlib import Path


def main():
    if len(sys.argv) != 3:
        print("[ERROR] usage: merge.py <file1> <file2>")
        sys.exit(1)

    file_path1 = Path(sys.argv[1])
    file_path2 = Path(sys.argv[2])

    if not file_path1.exists() or not file_path2.exists():
        print("[ERROR] either file1 or file2 not exists")
        sys.exit(1)

    low_tf = pd.read_csv(
        file_path1,
        sep=";"
    )

    high_tf = pd.read_csv(
        file_path2,
        sep=";"
    )

    float_columns_low = ['open', 'high', 'low', 'close']
    float_columns_high = ['open', 'high', 'low', 'close', 'fractal_low', 'fractal_high']

    low_tf['timestamp'] = pd.to_datetime(low_tf['timestamp'], format="%Y.%m.%d %H:%M:%S", errors='coerce')
    high_tf['timestamp'] = pd.to_datetime(high_tf['timestamp'], format="%Y.%m.%d %H:%M:%S", errors='coerce')

    low_tf = low_tf.dropna(subset=['timestamp'])
    high_tf = high_tf.dropna(subset=['timestamp'])

    for col in float_columns_low:
        low_tf[col] = pd.to_numeric(low_tf[col], errors='coerce')

    for col in float_columns_high:
        high_tf[col] = pd.to_numeric(high_tf[col], errors='coerce')

    if 'volume' in low_tf:
        low_tf['volume'] = pd.to_numeric(low_tf['volume'], errors='coerce')
    if 'volume' in high_tf:
        high_tf['volume'] = pd.to_numeric(high_tf['volume'], errors='coerce')

    low_tf = low_tf.sort_values('timestamp')
    high_tf = high_tf.sort_values('timestamp')

    high_tf = high_tf.rename(columns={
        'timestamp': 'high_timestamp',
        'open': 'high_open',
        'high': 'high_high',
        'low': 'high_low',
        'close': 'high_close',
        'volume': 'high_volume'
    })

    print(f"[INFO] merging: {file_path1} & {file_path2}")
    merged = pd.merge_asof(
        low_tf,
        high_tf,
        left_on='timestamp',
        right_on='high_timestamp',
        direction='backward'
    )

    output_file = file_path1.parent/f"{file_path1.stem}_merged.csv"

    print(f"[INFO] {file_path1} & {file_path2} merged succesfully")
    print(f"[INFO] {output_file} columns: {merged.shape[0]} rows: {merged.shape[1]}")

    print(f"[INFO] {output_file} delete volume columns")
    merged = merged.drop(['volume', 'high_volume'], axis=1)
    print(f"[INFO] {output_file} columns deleted, columns: {merged.shape[1]} rows: {merged.shape[0]}")
    merged.to_csv(output_file, sep=';', date_format="%Y.%m.%d %H:%M:%S", index=False)


if __name__ == "__main__":
    main()
