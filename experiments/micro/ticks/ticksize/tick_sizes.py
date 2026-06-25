# tick_sizes.py
# Robust tick size analysis (safe version)

import pandas as pd
import sys
from pathlib import Path
from math import floor

DATE_FORMAT = "%Y.%m.%d %H:%M:%S.%f"

REQUIRED_COLUMNS = ["timestamp", "bid", "ask"]


def get_input_files(path: Path) -> list[Path]:

    if not path.exists():
        sys.exit(f"[ERROR] Path does not exist: {path}")

    if path.is_file():
        return [path]

    if path.is_dir():
        files = sorted(path.rglob("*.csv"))
        if not files:
            sys.exit(f"[ERROR] No CSV files found in {path}")
        return files

    sys.exit(f"[ERROR] Unsupported path: {path}")


def load_and_clean(file_path: Path) -> pd.DataFrame:

    df = pd.read_csv(file_path, sep=";")

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        print(f"[WARNING] {file_path.name} missing columns {missing}")
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format=DATE_FORMAT,
        errors="coerce"
    )

    df = df.dropna(subset=["timestamp"])

    if df.empty:
        return df

    df[["bid", "ask"]] = df[["bid", "ask"]].apply(
        pd.to_numeric,
        errors="coerce"
    )

    return df


def build_diffs(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty or len(df) < 2:
        return pd.DataFrame(columns=["timestamp", "diff", "abs_diff"])

    records = []
    prev = None

    for row in df.itertuples(index=False):

        if prev is None:
            prev = row.bid
            continue

        diff = row.bid - prev

        records.append({
            "timestamp": row.timestamp,
            "diff": diff,
            "abs_diff": abs(diff)
        })

        prev = row.bid

    return pd.DataFrame(records)


def sort_table(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        return df

    return df.sort_values("abs_diff", ascending=False)


def build_buckets(df: pd.DataFrame, bucket_size: float) -> pd.DataFrame:

    if df.empty:
        return pd.DataFrame(columns=["bucket", "count"])

    buckets = {}

    for v in df["abs_diff"]:

        idx = floor(v / bucket_size)
        start = idx * bucket_size
        end = start + bucket_size

        label = f"{start:.5f} - {end:.5f}"
        buckets[label] = buckets.get(label, 0) + 1

    return pd.DataFrame(
        [{"bucket": k, "count": v} for k, v in sorted(buckets.items())]
    )


def process_file(file_path: Path, bucket_size: float):

    print(f"\n[INFO] Processing {file_path.name}")

    df = load_and_clean(file_path)

    if df.empty:
        print("[WARNING] empty dataframe -> skipping")
        return

    print(f"[INFO] rows: {len(df)}")

    diff = build_diffs(df)

    if diff.empty:
        print("[WARNING] no diffs -> skipping")
        return

    sorted_df = sort_table(diff)
    buckets_df = build_buckets(diff, bucket_size)

    out_dir = file_path.parent / "results" / file_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)

    sorted_df.to_csv(out_dir / "sorted.csv", sep=";", index=False)
    buckets_df.to_csv(out_dir / "buckets.csv", sep=";", index=False)

    print(f"[INFO] saved -> {out_dir}")


def main():

    if len(sys.argv) != 3:
        sys.exit("usage: python3 tick_sizes.py <file_or_dir> <bucket_size>")

    path = Path(sys.argv[1])
    bucket_size = float(sys.argv[2])

    files = get_input_files(path)

    print(f"[INFO] Found {len(files)} files")

    for f in files:
        try:
            process_file(f, bucket_size)
        except Exception as e:
            print(f"[ERROR] {f.name}: {e}")


if __name__ == "__main__":
    main()
