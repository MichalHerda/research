# momentum1.py

import pandas as pd
import sys
from pathlib import Path
from collections import defaultdict

REQUIRED_COLUMNS = ["timestamp", "bid", "ask"]

PIP_SIZE = 0.0001  # EURUSD default


def load_and_clean(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        sys.exit(f"[ERROR] File does not exist: {file_path}")

    df = pd.read_csv(file_path, sep=";")

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        sys.exit(f"[ERROR] Missing columns: {missing}")

    # robust timestamp parsing (FIXED)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    df = df.sort_values("timestamp")

    df[["bid", "ask"]] = df[["bid", "ask"]].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["bid", "ask"])

    return df


def save_df(df: pd.DataFrame, path: Path):
    df.to_csv(path, sep=";", index=False)


def save_buckets(bucket_dict, path: Path):
    if not bucket_dict:
        pd.DataFrame(columns=["size", "count"]).to_csv(path, sep=";", index=False)
        return

    df = pd.DataFrame(
        list(bucket_dict.items()),
        columns=["size", "count"]
    ).sort_values("size", ascending=False)

    df.to_csv(path, sep=";", index=False)


def run(df: pd.DataFrame, out_dir: Path):
    print("[INFO] Running momentum classification...")

    plus = []
    minus = []

    plus_buckets = defaultdict(int)
    minus_buckets = defaultdict(int)

    prev_bid = None

    for row in df.itertuples(index=False):
        if prev_bid is None:
            prev_bid = row.bid
            continue

        diff = row.bid - prev_bid
        size = round(abs(diff) / PIP_SIZE, 6)

        record = {
            "timestamp": row.timestamp,
            "bid": row.bid,
            "prev_bid": prev_bid,
            "diff": diff,
            "size_pips": size
        }

        if diff > 0:
            plus.append(record)
            plus_buckets[size] += 1
        elif diff < 0:
            minus.append(record)
            minus_buckets[size] += 1

        prev_bid = row.bid

    save_df(pd.DataFrame(plus), out_dir / "plus.csv")
    save_df(pd.DataFrame(minus), out_dir / "minus.csv")

    save_buckets(plus_buckets, out_dir / "plus_buckets.csv")
    save_buckets(minus_buckets, out_dir / "minus_buckets.csv")

    print("[INFO] Done.")


def main():
    if len(sys.argv) != 2:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file>")

    file_path = Path(sys.argv[1])

    df = load_and_clean(file_path)

    print("[INFO] Loaded:", len(df), "rows")

    out_dir = file_path.parent / f"momentum{file_path.stem}"
    out_dir.mkdir(exist_ok=True)

    run(df, out_dir)


if __name__ == "__main__":
    main()
