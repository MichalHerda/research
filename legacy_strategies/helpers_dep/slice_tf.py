import sys
import pandas as pd
from pathlib import Path


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
    df.to_csv(output_file, sep=";", index=False)


if __name__ == "__main__":
    main()
