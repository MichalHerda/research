#!/usr/bin/env python3

import sys
import os
import pandas as pd
from glob import glob


def load_files(directory):
    files = glob(os.path.join(directory, "*_res.csv"))
    if not files:
        raise ValueError("Brak plików *_res.csv w katalogu")

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df["source_file"] = os.path.basename(f)
            dfs.append(df)
        except Exception as e:
            print(f"[WARN] Nie udało się wczytać {f}: {e}")

    if not dfs:
        raise ValueError("Nie udało się wczytać żadnego pliku")

    return pd.concat(dfs, ignore_index=True)


def clean_data(df):
    # upewnij się, że kolumny istnieją
    required = ["symbol", "dir", "dev", "ema", "rr", "trades", "wr", "exp"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Brakuje kolumny: {col}")

    # konwersje typów
    df["exp"] = pd.to_numeric(df["exp"], errors="coerce")
    df["wr"] = pd.to_numeric(df["wr"], errors="coerce")
    df["trades"] = pd.to_numeric(df["trades"], errors="coerce")

    # usuń NaNy
    df = df.dropna(subset=["exp"])

    return df


def rank_and_save(df, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    # --- GLOBAL
    for direction in ["buy", "sell"]:
        d = df[df["dir"].str.lower() == direction].copy()
        d = d.sort_values(by="exp", ascending=False)

        out_path = os.path.join(out_dir, f"GLOBAL_{direction.upper()}.csv")
        d.to_csv(out_path, index=False)

        print(f"\n=== TOP 10 GLOBAL {direction.upper()} ===")
        print(d.head(10)[["symbol", "dev", "ema", "rr", "trades", "wr", "exp"]])

    # --- PER SYMBOL
    symbols = sorted(df["symbol"].unique())

    for sym in symbols:
        sym_df = df[df["symbol"] == sym]

        for direction in ["buy", "sell"]:
            d = sym_df[sym_df["dir"].str.lower() == direction].copy()

            if d.empty:
                continue

            d = d.sort_values(by="exp", ascending=False)

            filename = f"{sym}_{direction.upper()}.csv"
            out_path = os.path.join(out_dir, filename)
            d.to_csv(out_path, index=False)

            print(f"\n--- {sym} {direction.upper()} TOP 5 ---")
            print(d.head(5)[["dev", "ema", "rr", "trades", "wr", "exp"]])


def main():
    if len(sys.argv) != 2:
        print("Użycie: python3 expectancy.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]

    if not os.path.isdir(directory):
        print("Podany katalog nie istnieje")
        sys.exit(1)

    print(f"[INFO] Wczytywanie danych z: {directory}")
    df = load_files(directory)

    print(f"[INFO] Liczba rekordów: {len(df)}")
    df = clean_data(df)

    out_dir = os.path.join(directory, "rankings")
    rank_and_save(df, out_dir)

    print(f"\n[OK] Wyniki zapisane w: {out_dir}")


if __name__ == "__main__":
    main()
