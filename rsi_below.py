#!/usr/bin/env python3

import sys
import pandas as pd


def main():
    if len(sys.argv) != 3:
        print("Użycie: python3 rsi_below.py <plik_csv> <below_value>")
        sys.exit(1)

    file_path = sys.argv[1]
    try:
        threshold = float(sys.argv[2])
    except ValueError:
        print("Błąd: drugi argument musi być liczbą (np. 35)")
        sys.exit(1)

    try:
        df = pd.read_csv(file_path, sep=';')
        df['RSI'] = pd.to_numeric(df['RSI'], errors='coerce')

        # Kolumna poprzedniego RSI
        df['prev_RSI'] = df['RSI'].shift(1)

        # Wiersze, gdzie RSI spada poniżej progu
        crossings = df[(df['prev_RSI'] >= threshold) & (df['RSI'] < threshold)]

        output_file = file_path.replace('.csv', f'_rsi_cross_below_{int(threshold)}.csv')
        crossings.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano plik: {output_file} z {len(crossings)} wierszami (punkty przecięcia)")
        if len(crossings) == 0:
            print("Uwaga: nie znaleziono żadnych momentów, w których RSI spadło poniżej progu.")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
