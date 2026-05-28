# collection of market indicators for reusing purposes

import numpy as np
import pandas as pd


def compute_ema(series, period):
    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def compute_atr(df, period):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    ).max(axis=1)

    atr = tr.rolling(window=period).mean()

    return atr


def compute_ema_deviation(high, low, fast_ema, atr):

    z_high = (high - fast_ema) / atr
    z_low = (low - fast_ema) / atr

    return np.where(
        np.abs(z_high) >= np.abs(z_low),
        z_high,
        z_low
    )


def compute_slope(fast_ema, atr, slope_period):
    ema_past = fast_ema.shift(slope_period)
    slope = (fast_ema - ema_past) / atr
    return slope


def compute_fractals(df):

    high = df['high']
    low = df['low']

    # --------------------------------------------------------
    # Raw fractal detection
    # --------------------------------------------------------

    fractal_high_raw = (
        (high.shift(2) < high) &
        (high.shift(1) < high) &
        (high.shift(-1) < high) &
        (high.shift(-2) < high)
    )

    fractal_low_raw = (
        (low.shift(2) > low) &
        (low.shift(1) > low) &
        (low.shift(-1) > low) &
        (low.shift(-2) > low)
    )

    # --------------------------------------------------------
    # Fractal values
    # --------------------------------------------------------

    fractal_high = df['high'].where(fractal_high_raw)
    fractal_low = df['low'].where(fractal_low_raw)

    # --------------------------------------------------------
    # Last known fractals
    # --------------------------------------------------------

    last_fractal_high = fractal_high.shift(2).ffill()
    last_fractal_low = fractal_low.shift(2).ffill()

    return (
        fractal_low,
        fractal_high,
        last_fractal_low,
        last_fractal_high
    )


# version no last visible fractals:
def compute_fractals_(df):
    high = df['high']
    low = df['low']

    fractal_high_raw = (
        (high.shift(2) < high) &
        (high.shift(1) < high) &
        (high.shift(-1) < high) &
        (high.shift(-2) < high)
    )

    fractal_low_raw = (
        (low.shift(2) > low) &
        (low.shift(1) > low) &
        (low.shift(-1) > low) &
        (low.shift(-2) > low)
    )

    df['fractal_low'] = df['low'].where(fractal_low_raw)
    df['fractal_high'] = df['high'].where(fractal_high_raw)

    return df
