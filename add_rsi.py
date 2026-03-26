#!/usr/bin/env python3

import sys
import pandas as pd


def calculate_rsi(df, period=14):
    # Różnice między kolejnymi cenami zamknięcia
    delta = df['close'].diff()

    # Zyski i straty
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # Średnie kroczące (EMA wg klasycznej definicji RSI Wildera)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    # RS i RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def main():
    if len(sys.argv) != 2:
        print("Użycie: python3 add_rsi.py <plik_csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    try:
        # Wczytanie danych
        df = pd.read_csv(file_path, sep=';')

        # Konwersja timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')

        # Sortowanie
        df = df.sort_values(by='timestamp')

        # Konwersja close na float (bezpiecznie)
        df['close'] = pd.to_numeric(df['close'], errors='coerce')

        # Obliczenie RSI14
        df['RSI'] = calculate_rsi(df, period=14)

        # Zapis
        output_file = file_path.replace('.csv', '_rsi.csv')
        df.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano plik: {output_file}")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
