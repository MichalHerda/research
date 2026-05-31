"""
Batch Data Cutting Script (Multiple Time Ranges)

Description:
    Filters a main data CSV file by extracting multiple distinct time intervals
    defined in a separate periods configuration file. Useful for isolating specific
    events, incidents, or custom timeframes in bulk.

Usage:
    python3 cut_data.py <data_file> <periods_file>

Arguments:
    <data_file>    : Path to the main CSV file containing the source data.
    <periods_file> : Path to the CSV file defining the time windows to extract.

File Structure Requirements:
    1. <data_file>: Semicolon-separated (;) with a 'timestamp' column.
    2. <periods_file>: Semicolon-separated (;) with 'begin' and 'end' columns
       defining each desired interval.
    * Note: Column names are case-insensitive and stripped of whitespace automatically.

Output:
    Saves a new file in the same directory named: <original_filename>_cut.csv
"""

import sys
import pandas as pd


def main():
    if len(sys.argv) != 3:
        print("Użycie: python3 cut_data.py <data_file> <periods_file>")
        sys.exit(1)

    data_file = sys.argv[1]
    periods_file = sys.argv[2]

    try:
        # ===== Wczytanie danych =====
        df = pd.read_csv(data_file, sep=';', encoding='utf-8-sig')
        df.columns = df.columns.str.strip().str.lower()

        if 'timestamp' not in df.columns:
            raise ValueError("Brak kolumny 'timestamp' w data_file")

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        df = df.sort_values('timestamp')

        # ===== Wczytanie okresów =====
        periods = pd.read_csv(periods_file, sep=';', encoding='utf-8-sig')
        periods.columns = periods.columns.str.strip().str.lower()

        if 'begin' not in periods.columns or 'end' not in periods.columns:
            raise ValueError("Plik periods musi mieć kolumny: begin;end")

        periods['begin'] = pd.to_datetime(periods['begin'], errors='coerce')
        periods['end'] = pd.to_datetime(periods['end'], errors='coerce')
        periods = periods.dropna()

        # ===== Filtrowanie =====
        mask = pd.Series(False, index=df.index)

        for _, row in periods.iterrows():
            mask |= (df['timestamp'] >= row['begin']) & (df['timestamp'] <= row['end'])

        result = df[mask]

        # ===== Zapis =====
        output_file = data_file.replace('.csv', '_cut.csv')
        result.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano: {output_file} ({len(result)} wierszy)")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
