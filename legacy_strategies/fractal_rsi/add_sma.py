#!/usr/bin/env python3

import sys
import pandas as pd


def main():
    # Sprawdzenie liczby argumentów
    if len(sys.argv) != 3:
        print("Użycie: python3 add_sma.py <plik_csv> <period>")
        sys.exit(1)

    file_path = sys.argv[1]
    period = int(sys.argv[2])

    try:
        # Wczytanie danych
        df = pd.read_csv(file_path, sep=';')

        # Konwersja timestamp na datetime (opcjonalnie, ale dobra praktyka)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')

        # Sortowanie po czasie (na wszelki wypadek)
        df = df.sort_values(by='timestamp')

        # Obliczenie SMA na kolumnie 'close'
        df['SMA'] = df['close'].rolling(window=period).mean()

        # Zapis do nowego pliku
        output_file = file_path.replace('.csv', f'_sma_{period}.csv')
        df.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano plik: {output_file}")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
