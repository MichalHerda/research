import os
import sys
import csv
from collections import defaultdict


def get_range(file_path):
    first_ts = None
    last_ts = None

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')

        next(reader, None)  # pomijamy nagłówek

        for row in reader:
            if not row:
                continue

            ts = row[0]

            if first_ts is None:
                first_ts = ts
            last_ts = ts

    return first_ts, last_ts


def extract_tf(filename):
    # np. AUDCAD_M5.csv -> M5
    base = os.path.basename(filename)
    parts = base.split("_")
    if len(parts) < 2:
        return None

    tf_part = parts[-1]  # M5.csv
    return tf_part.replace(".csv", "")


def process_directory(main_dir):
    # słownik: TF -> lista wyników
    results_by_tf = defaultdict(list)

    for instrument in os.listdir(main_dir):
        instrument_path = os.path.join(main_dir, instrument)

        if not os.path.isdir(instrument_path):
            continue

        for file in os.listdir(instrument_path):
            if not file.endswith(".csv"):
                continue

            tf = extract_tf(file)
            if not tf:
                continue

            file_path = os.path.join(instrument_path, file)

            print(f"Przetwarzam: {file_path}")

            first_ts, last_ts = get_range(file_path)

            results_by_tf[tf].append([instrument, first_ts, last_ts])

    # zapis do osobnych plików
    for tf, rows in results_by_tf.items():
        output_file = f"scope_{tf}.csv"

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["instrument", "from", "to"])
            writer.writerows(rows)

        print(f"Zapisano: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Użycie: python scope_all.py <main_directory>")
        sys.exit(1)

    main_directory = sys.argv[1]
    process_directory(main_directory)
