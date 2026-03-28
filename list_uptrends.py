#!/usr/bin/env python3

import sys
import pandas as pd


def main():
    if len(sys.argv) != 2:
        print("Użycie: python3 list_uptrends.py <inputfile>")
        sys.exit(1)

    file_path = sys.argv[1]

    try:
        # Wczytanie
        df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
        df.columns = df.columns.str.strip().str.lower()

        # Sprawdzenie kolumn
        required_cols = ['timestamp_h1', 'uptrend_h1', 'uptrend_d1']
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Brak kolumny: {col}")

        # Typy
        df['timestamp_h1'] = pd.to_datetime(df['timestamp_h1'], errors='coerce')
        df['uptrend_h1'] = df['uptrend_h1'].astype(str).str.lower() == 'true'
        df['uptrend_d1'] = df['uptrend_d1'].astype(str).str.lower() == 'true'

        df = df.dropna(subset=['timestamp_h1'])
        df = df.sort_values('timestamp_h1')

        # Warunek wspólny
        df['both_uptrend'] = df['uptrend_h1'] & df['uptrend_d1']

        # Szukanie epizodów
        episodes = []
        in_episode = False
        start_time = None

        for i in range(len(df)):
            row = df.iloc[i]

            if row['both_uptrend'] and not in_episode:
                # start
                in_episode = True
                start_time = row['timestamp_h1']

            elif not row['both_uptrend'] and in_episode:
                # koniec
                end_time = df.iloc[i - 1]['timestamp_h1']
                episodes.append((start_time, end_time))
                in_episode = False

        # jeśli kończy się na True
        if in_episode:
            end_time = df.iloc[-1]['timestamp_h1']
            episodes.append((start_time, end_time))

        # DataFrame wynikowy
        result = pd.DataFrame(episodes, columns=['begin', 'end'])

        # Zapis
        output_file = file_path.replace('.csv', '_uptrend_episodes.csv')
        result.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano: {output_file} ({len(result)} epizodów)")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
