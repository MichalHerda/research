import os
import sys
import csv


def get_m5_range(file_path):
    first_ts = None
    last_ts = None

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')

        header = next(reader, None)  # noqa

        for row in reader:
            if not row:
                continue

            ts = row[0]

            if first_ts is None:
                first_ts = ts
            last_ts = ts

    return first_ts, last_ts


def process_directory(main_dir, output_file="m5_scope.csv"):
    results = []

    for instrument in os.listdir(main_dir):
        instrument_path = os.path.join(main_dir, instrument)

        if not os.path.isdir(instrument_path):
            continue

        m5_file = None

        for file in os.listdir(instrument_path):
            if file.endswith("_M5.csv"):
                m5_file = os.path.join(instrument_path, file)
                break

        if m5_file:
            print(f"Przetwarzam: {m5_file}")
            first_ts, last_ts = get_m5_range(m5_file)

            results.append([instrument, first_ts, last_ts])
        else:
            print(f"Brak M5 dla: {instrument}")

    # zapis wyników
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["instrument", "from", "to"])
        writer.writerows(results)

    print(f"\nZapisano do: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Użycie: python scope_m5.py <main_directory>")
        sys.exit(1)

    main_directory = sys.argv[1]
    process_directory(main_directory)
