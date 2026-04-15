#!/usr/bin/env python3
#   tutaj jest cos nie tak z obliczeniami z-score!

import sys
import os
import glob
import pandas as pd


def load_csv(filepath):
    df = pd.read_csv(
        filepath,
        sep=';',
        dtype=str
    )

    df['volume'] = df['volume'].str.replace(r'[^0-9]', '', regex=True)

    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y.%m.%d %H:%M:%S')

    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def compute_atr(df, period):
    # 1. Oblicz True Range (TR)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    # 2. MT4 używa SMA do ATR, a nie wygładzania Wildera
    # rolling(period).mean() to dokładnie to, co robi iATR w MT4
    atr = tr.rolling(window=period).mean()

    return atr


def compute_adx(df, period=14):
    high = df['high']
    low = df['low']
    close = df['close']

    # 1. Przygotowanie serii
    size = len(df)
    tr = pd.Series(0.0, index=df.index)
    plus_dm = pd.Series(0.0, index=df.index)
    minus_dm = pd.Series(0.0, index=df.index)

    # Wyliczanie TR i surowych DM (zgodnie z MT4)
    for i in range(1, size):
        # True Range
        val1 = high.iloc[i] - low.iloc[i]
        val2 = abs(high.iloc[i] - close.iloc[i-1])
        val3 = abs(low.iloc[i] - close.iloc[i-1])
        tr.iloc[i] = max(val1, max(val2, val3))

        # Directional Movement
        up_move = high.iloc[i] - high.iloc[i-1]
        down_move = low.iloc[i-1] - low.iloc[i]

        if up_move > down_move and up_move > 0:
            plus_dm.iloc[i] = up_move
        else:
            plus_dm.iloc[i] = 0.0

        if down_move > up_move and down_move > 0:
            minus_dm.iloc[i] = down_move
        else:
            minus_dm.iloc[i] = 0.0

    # 2. Wygładzanie (Logic MT4 / Wilder's Smoothing)
    tr_smooth = pd.Series(0.0, index=df.index)
    plus_dm_s = pd.Series(0.0, index=df.index)
    minus_dm_s = pd.Series(0.0, index=df.index)

    # Pierwsza wartość to suma z okresu
    tr_smooth.iloc[period] = tr.iloc[1:period+1].sum()
    plus_dm_s.iloc[period] = plus_dm.iloc[1:period+1].sum()
    minus_dm_s.iloc[period] = minus_dm.iloc[1:period+1].sum()

    # Rekurencyjne wygładzanie identyczne z iADX w MT4
    for i in range(period + 1, size):
        tr_smooth.iloc[i] = tr_smooth.iloc[i-1] - (tr_smooth.iloc[i-1] / period) + tr.iloc[i]
        plus_dm_s.iloc[i] = plus_dm_s.iloc[i-1] - (plus_dm_s.iloc[i-1] / period) + plus_dm.iloc[i]
        minus_dm_s.iloc[i] = minus_dm_s.iloc[i-1] - (minus_dm_s.iloc[i-1] / period) + minus_dm.iloc[i]

    # 3. Obliczanie +DI, -DI i DX
    plus_di = 100.0 * (plus_dm_s / tr_smooth)
    minus_di = 100.0 * (minus_dm_s / tr_smooth)

    dx = 100.0 * (abs(plus_di - minus_di) / (plus_di + minus_di)).fillna(0)

    # 4. Finalne wygładzanie ADX
    adx = pd.Series(0.0, index=df.index)

    # MT4 potrzebuje dodatkowego okresu na zainicjowanie ADX
    adx_start = 2 * period - 1
    if size > adx_start:
        adx.iloc[adx_start] = dx.iloc[period:adx_start+1].mean()

        for i in range(adx_start + 1, size):
            # Formuła wygładzająca ADX w MT4
            adx.iloc[i] = (adx.iloc[i-1] * (period - 1) + dx.iloc[i]) / period

    return adx


def process_instrument(instrument_path, instrument, timeframe, ema_period, ema_period_slow, atr_period):

    # --- H1 ---
    h1_path = glob.glob(os.path.join(instrument_path, f"*_{timeframe}.csv"))
    if not h1_path:
        return None

    df_h1 = load_csv(h1_path[0])

    # --- D1 ---
    d1_path = glob.glob(os.path.join(instrument_path, "*_D1.csv"))
    if not d1_path:
        raise Exception(f"Missing D1 data for {instrument}")

    df_d1 = load_csv(d1_path[0])

    # --- INDICATORS H1 ---
    df_h1['EMA'] = compute_ema(df_h1['close'], ema_period)
    df_h1['EMA_SLOW'] = compute_ema(df_h1['close'], ema_period_slow)
    df_h1['deviation'] = df_h1['close'] - df_h1['EMA']
    df_h1['ATR'] = compute_atr(df_h1, atr_period)

    # --- ADX NA D1 ---
    df_d1['ADX'] = compute_adx(df_d1, 14)

    # tylko potrzebne kolumny
    df_d1 = df_d1[['timestamp', 'ADX']].dropna().reset_index(drop=True)

    # --- MERGE ASOF ---
    df_h1 = pd.merge_asof(
        df_h1.sort_values('timestamp'),
        df_d1.sort_values('timestamp'),
        on='timestamp',
        direction='backward'
    )

    # usuwamy NaN
    df_h1 = df_h1.dropna().reset_index(drop=True)

    return df_h1


def main():
    if len(sys.argv) != 6:
        print("Usage: python3 z_score_gen.py <directory> <timeframe> <ema_period> <ema_period_slow> <atr_period>")
        sys.exit(1)

    base_dir = sys.argv[1]
    timeframe = sys.argv[2]
    ema_period = int(sys.argv[3])
    ema_period_slow = int(sys.argv[4])
    atr_period = int(sys.argv[5])

    output_dir = os.path.join(base_dir, f"output_{timeframe}")
    os.makedirs(output_dir, exist_ok=True)

    instruments = [
        d for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]

    for instrument in instruments:
        instrument_path = os.path.join(base_dir, instrument)

        try:
            df = process_instrument(
                instrument_path,
                instrument,
                timeframe,
                ema_period,
                ema_period_slow,
                atr_period
            )

            if df is None:
                continue

            output_filename = f"{instrument}_{timeframe}.csv"
            output_path = os.path.join(output_dir, output_filename)

            df.to_csv(output_path, index=False)

            print(f"[OK] {instrument} -> {output_filename}")

        except Exception as e:
            print(f"[ERROR] {instrument}: {e}")


if __name__ == "__main__":
    main()
