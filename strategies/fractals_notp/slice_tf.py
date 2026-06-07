"""
Data Slicing Script (Single Time Range)

Description:
    Extracts a specific continuous time range from a semicolon-separated (;) CSV file
    and saves the filtered data into a new CSV file without modifying the original.

Usage:
    python3 slice_tf.py <file_path> <time_from> <time_to>

Arguments:
    <file_path>  : Path to the source CSV file.
    <time_from>  : Start timestamp (inclusive), e.g., "2026-05-01 00:00:00".
    <time_to>    : End timestamp (inclusive), e.g., "2026-05-31 23:59:59".

Input Requirements:
    - The source CSV must use a semicolon (;) as a delimiter.
    - Must contain a column named 'timestamp'.

Output:
    Saves a new file in the same directory named:
    <original_filename>_<time_from>_<time_to>.csv
"""

import sys
import pandas as pd
from pathlib import Path


DATE_FORMAT = "%Y.%m.%d %H:%M:%S"


def main():
    if len(sys.argv) != 4:
        print("[ERROR] usage: python3 slice_tf.py <file> <time_from> <time_to>")
        sys.exit(1)

    file_path = Path(sys.argv[1])

    if not file_path.exists():
        print("[ERROR] file path not exists")
        sys.exit(1)

    time_from = sys.argv[2]
    time_to = sys.argv[3]

    df = pd.read_csv(file_path, sep=";", parse_dates=['timestamp'])
    df = df[(df['timestamp'] >= time_from) & (df['timestamp'] <= time_to)]

    output_file = file_path.parent/f"{file_path.stem}_{time_from}_{time_to}.csv"

    df.to_csv(output_file, sep=";", index=False, date_format=DATE_FORMAT)


if __name__ == "__main__":
    main()
