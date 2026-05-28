#!/usr/bin/env python3

import os
import sys
import pandas as pd


def load_all_trades(input_dir):
    all_dfs = []

    for file in os.listdir(input_dir):
        if not file.endswith("_backtest.csv"):
            continue
        if file == "all_backtests_summary.csv":
            continue

        path = os.path.join(input_dir, file)

        df = pd.read_csv(path)
        # usuń śmieciowe linie (np. podsumowanie TP=...)
        df = df[df["open_time"].str.match(r"\d{4}-\d{2}-\d{2}", na=False)]

        # instrument z nazwy pliku
        instrument = file.replace("_backtest.csv", "")
        df["instrument"] = instrument

        all_dfs.append(df)

    if not all_dfs:
        raise ValueError("Brak plików backtestów w katalogu")

    combined = pd.concat(all_dfs, ignore_index=True)

    # konwersja czasu
    combined["close_time"] = pd.to_datetime(combined["close_time"])
    combined["open_time"] = pd.to_datetime(combined["open_time"])

    return combined


def create_chronological_table(df, output_path):
    df_sorted = df.sort_values(by="close_time")
    df_sorted.to_csv(output_path, index=False)
    return df_sorted


def detect_sl_series(df):
    df = df.sort_values(by="close_time").reset_index(drop=True)

    series = []
    current_count = 0
    current_start = None

    for _, row in df.iterrows():
        is_tp = str(row["TP"]).strip().lower() == "true"

        if not is_tp:
            # SL
            if current_count == 0:
                current_start = row["close_time"]
            current_count += 1
        else:
            # zakończenie serii
            if current_count > 5:
                series.append({
                    "start_time": current_start,
                    "count_sl": current_count
                })
            current_count = 0
            current_start = None

    # check końcówki
    if current_count > 5:
        series.append({
            "start_time": current_start,
            "count_sl": current_count
        })

    result = pd.DataFrame(series)

    if not result.empty:
        result = result.sort_values(by="count_sl", ascending=False)

    return result


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 chrono.py <dir>")
        sys.exit(1)

    input_dir = sys.argv[1]

    if not os.path.isdir(input_dir):
        print("Podany katalog nie istnieje")
        sys.exit(1)

    output_dir = os.path.join(input_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    print("Ładowanie danych...")
    df = load_all_trades(input_dir)

    print("Tworzenie tabeli chronologicznej...")
    chrono_path = os.path.join(output_dir, "all_trades_chronological.csv")
    df_sorted = create_chronological_table(df, chrono_path)

    print("Analiza serii SL...")
    sl_series = detect_sl_series(df_sorted)

    sl_path = os.path.join(output_dir, "sl_series.csv")
    sl_series.to_csv(sl_path, index=False)

    print("Gotowe!")
    print(f"Zapisano:\n- {chrono_path}\n- {sl_path}")


if __name__ == "__main__":
    main()
