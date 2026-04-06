#!/usr/bin/env python3

import sys
import os
import pandas as pd


# =========================
# LOAD
# =========================
def load_csv(path):
    df = pd.read_csv(path, sep=';')

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')
    df = df.sort_values('timestamp').reset_index(drop=True)

    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# =========================
# INDICATORS
# =========================
def add_sma(df, period, name):
    df[name] = df['close'].rolling(window=period).mean()
    return df


def add_uptrend_no_lookahead(df, sma_col, out_col):
    df[out_col] = df[sma_col].shift(1) > df[sma_col].shift(2)
    return df


def add_fast_above(df, fast, slow, out):
    df[out] = df[fast].shift(1) > df[slow].shift(1)
    return df


def add_signal_cross(df, fast, slow):
    cross = (
        (df[fast].shift(2) < df[slow].shift(2)) &
        (df[fast].shift(1) > df[slow].shift(1))
    )

    df['SIGNAL_M15'] = cross.shift(1)
    return df


def add_last_min(df, fast, slow):
    cond = df[fast] < df[slow]

    df['min_tmp'] = df['low'].where(cond)
    df['LAST_MIN'] = df['min_tmp'].ffill()

    df.drop(columns=['min_tmp'], inplace=True)

    return df


# =========================
# ALIGN
# =========================
def align(df_m15, df_d1):

    df_d1 = df_d1.rename(columns={'timestamp': 'timestamp_D1'})

    df = pd.merge_asof(
        df_m15.sort_values('timestamp'),
        df_d1.sort_values('timestamp_D1'),
        left_on='timestamp',
        right_on='timestamp_D1',
        direction='backward'
    )

    return df


# =========================
# MAIN PER SYMBOL
# =========================
def process_symbol(path, output_dir,
                   sma_m15_fast, sma_m15_slow,
                   sma_d1_fast, sma_d1_slow):

    symbol = os.path.basename(path)

    file_m15 = os.path.join(path, f"{symbol}_M15.csv")
    file_d1 = os.path.join(path, f"{symbol}_D1.csv")

    if not os.path.exists(file_m15) or not os.path.exists(file_d1):
        print(f"⛔ Brak plików dla {symbol}")
        return

    df_m15 = load_csv(file_m15)
    df_d1 = load_csv(file_d1)

    # =========================
    # M15
    # =========================
    df_m15 = add_sma(df_m15, sma_m15_fast, 'SMA_FAST_M15')
    df_m15 = add_sma(df_m15, sma_m15_slow, 'SMA_SLOW_M15')

    df_m15 = add_signal_cross(df_m15, 'SMA_FAST_M15', 'SMA_SLOW_M15')
    df_m15 = add_last_min(df_m15, 'SMA_FAST_M15', 'SMA_SLOW_M15')

    # =========================
    # D1
    # =========================
    df_d1 = add_sma(df_d1, sma_d1_fast, 'SMA_FAST_D1')
    df_d1 = add_sma(df_d1, sma_d1_slow, 'SMA_SLOW_D1')

    df_d1 = add_uptrend_no_lookahead(df_d1, 'SMA_FAST_D1', 'UP_FAST_D1')
    df_d1 = add_uptrend_no_lookahead(df_d1, 'SMA_SLOW_D1', 'UP_SLOW_D1')

    df_d1 = add_fast_above(df_d1, 'SMA_FAST_D1', 'SMA_SLOW_D1', 'FAST_ABOVE_D1')

    # =========================
    # ALIGN
    # =========================
    df = align(df_m15, df_d1)

    # =========================
    # OUTPUT (NOWY FOLDER!)
    # =========================
    out_file = os.path.join(output_dir, f"{symbol}_aligned.csv")
    df.to_csv(out_file, sep=';', index=False)

    print(f"✅ {symbol} -> {len(df)} rows")


# =========================
# MAIN GLOBAL
# =========================
def main():
    if len(sys.argv) != 6:
        print("Użycie:")
        print("python3 align_cross.py <DIR> <SMA_M15_FAST> <SMA_M15_SLOW> <SMA_D1_FAST> <SMA_D1_SLOW>")
        sys.exit(1)

    base_dir = sys.argv[1]
    sma_m15_fast = int(sys.argv[2])
    sma_m15_slow = int(sys.argv[3])
    sma_d1_fast = int(sys.argv[4])
    sma_d1_slow = int(sys.argv[5])

    # =========================
    # CREATE OUTPUT DIR
    # =========================
    output_dir = os.path.join(base_dir, "align_cross")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Utworzono folder: {output_dir}")
    else:
        print(f"📁 Folder istnieje: {output_dir}")

    # =========================
    # LOOP SYMBOLS
    # =========================
    for symbol in os.listdir(base_dir):
        path = os.path.join(base_dir, symbol)

        # pomijamy folder outputowy
        if not os.path.isdir(path) or symbol == "align_cross":
            continue

        process_symbol(
            path,
            output_dir,
            sma_m15_fast,
            sma_m15_slow,
            sma_d1_fast,
            sma_d1_slow
        )


if __name__ == "__main__":
    main()
