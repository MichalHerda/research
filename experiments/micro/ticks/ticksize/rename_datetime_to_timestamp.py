# rename_datetime_to_timestamp.py
#
# Utility script for normalizing tick/market data CSV files.
#
# It accepts either a single CSV file or a directory containing multiple CSV files.
# For each file it renames the column "datetime" -> "timestamp" if it exists,
# then saves the modified file (in-place overwrite or optional output folder).
#
# Purpose:
# Standardize dataset schema for downstream backtesting / analysis pipelines.

import pandas as pd
import sys
from pathlib import Path


REQUIRED_RENAME = {
    "datetime": "timestamp"
}


def get_input_files(path: Path) -> list[Path]:
    """
    Returns list of CSV files from input path.
    Input can be:
    - single file
    - directory (recursive search for .csv)
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


def rename_columns(file_path: Path) -> None:
    """
    Loads CSV, renames columns if needed, and overwrites file.
    """

    print(f"\n[INFO] Processing: {file_path.name}")

    df = pd.read_csv(file_path, sep=";")

    renamed = False

    for old_name, new_name in REQUIRED_RENAME.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
            renamed = True

    if not renamed:
        print("[INFO] No columns to rename")
    else:
        df.to_csv(
            file_path,
            sep=";",
            index=False
        )
        print("[INFO] Column renamed and file updated")


def main():

    if len(sys.argv) != 2:
        sys.exit(
            f"[ERROR] usage: python3 {Path(sys.argv[0]).name} "
            f"<file_or_directory>"
        )

    input_path = Path(sys.argv[1])

    files = get_input_files(input_path)

    print(f"[INFO] Found {len(files)} file(s)")

    for file_path in files:
        rename_columns(file_path)


if __name__ == "__main__":
    main()
