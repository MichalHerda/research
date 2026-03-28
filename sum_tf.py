#!/usr/bin/env python3

import sys
import pandas as pd


def load_csv(path):
    df = pd.read_csv(path, sep=';', encoding='utf-8-sig')
    df.columns = df.columns.str.strip().str.lower()

    if 'timestamp' not in df.columns:
        raise ValueError(f"Brak kolumny 'timestamp' w pliku: {path}")

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])

    return df


def detect_timeframe(df):
    # jeśli są godziny ≠ 00:00 → H1
    if (df['timestamp'].dt.hour != 0).any():
        return 'H1'
    return 'D1'


def prepare_d1(df_d1):
    df_d1['date'] = df_d1['timestamp'].dt.date

    rename_map = {
        'open': 'open_D1',
        'high': 'high_D1',
        'low': 'low_D1',
        'close': 'close_D1',
        'volume': 'volume_D1',
        'sma': 'SMA_D1',
        'uptrend': 'uptrend_D1',
        'timestamp': 'timestamp_D1'
    }

    return df_d1.rename(columns=rename_map)


def prepare_h1(df_h1):
    df_h1['date'] = df_h1['timestamp'].dt.date

    rename_map = {
        'open': 'open_H1',
        'high': 'high_H1',
        'low': 'low_H1',
        'close': 'close_H1',
        'volume': 'volume_H1',
        'sma': 'SMA_H1',
        'uptrend': 'uptrend_H1',
        'timestamp': 'timestamp_H1'
    }

    return df_h1.rename(columns=rename_map)


def main():
    if len(sys.argv) != 3:
        print("Użycie: python3 sum_tf.py <file1> <file2>")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    try:
        df1 = load_csv(file1)
        df2 = load_csv(file2)

        tf1 = detect_timeframe(df1)
        tf2 = detect_timeframe(df2)

        if tf1 == tf2:
            raise ValueError("Oba pliki mają ten sam timeframe — potrzebne H1 i D1")

        if tf1 == 'H1':
            df_h1, df_d1 = df1, df2
        else:
            df_h1, df_d1 = df2, df1

        df_h1 = prepare_h1(df_h1)
        df_d1 = prepare_d1(df_d1)

        # zakres wspólny (tylko gdzie oba TF istnieją)
        start_date = max(df_h1['date'].min(), df_d1['date'].min())

        df_h1 = df_h1[df_h1['date'] >= start_date]
        df_d1 = df_d1[df_d1['date'] >= start_date]

        # merge po dacie
        merged = pd.merge(df_h1, df_d1, on='date', how='inner')

        # sortowanie
        merged = merged.sort_values('timestamp_H1')

        output_file = "merged_H1_D1.csv"
        merged.to_csv(output_file, sep=';', index=False)

        print(f"Zapisano: {output_file} ({len(merged)} wierszy)")

    except Exception as e:
        print(f"Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
