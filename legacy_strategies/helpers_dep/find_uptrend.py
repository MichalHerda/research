#!/usr/bin/env python3

import sys
import pandas as pd


def main():
    # Sprawdzenie argumentów
    if len(sys.argv) != 2:
        print("Użycie: python3 find_uptrend.py <plik_csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    try:
        # Wczytanie pliku
        df = pd.read_csv(file_path, sep=';')

        # Konwersja SMA na float (ważne, bo mogą być puste wartości)
        df['SMA'] = pd.to_numeric(df['SMA'], errors='coerce')

        # Tworzymy kolumnę uptrend domyślnie False
        df['uptrend'] = False

        # Iterujemy po wierszach
        for i in range(1, len(df)):
            current_sma = df.loc[i, 'SMA']
            prev_sma = df.loc[i - 1, 'SMA']

            # Warunek: oba SMA muszą istnieć (nie NaN)
            if pd.notna(current_sma) and pd.notna(prev_sma):
                if current_sma > prev_sma:
                    df.loc[i, 'uptrend'] = True

        # Zapis do nowego pliku
        output_file = file_path.replace('.csv', '_uptrend.csv')
        df.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano plik: {output_file}")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
