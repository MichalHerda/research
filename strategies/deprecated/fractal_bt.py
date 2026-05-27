# [DEPRECATED] Preserved strictly for historical and educational purposes.

"""
Custom Systematic Backtesting Engine for Fractal-Based Strategies

Description:
An independent, event-driven backtesting script that iterates through a
historical OHLCV DataFrame to evaluate market regimes and execution models.
The strategy uses a multi-timeframe approach, treating the emergence of
higher-timeframe fractal lows as BUY triggers and fractal highs as SELL triggers,
factoring in a specific visibility time-window.

Purpose:
Developed entirely from scratch as a practical hands-on exercise to master
native Python syntax, strict type hinting, and robust data pipeline handling
(using pandas and pathlib) without relying on high-level black-box backtesting
frameworks.

Architecture:
Employs a procedural, row-by-row simulation loop (via itertuples) to maintain
strict chronological state machine tracking (handling real-time entry visibility,
dynamic SL/TP calculations, and trade logging limits), crucial for time-dependent
market context recognition.
"""


import pandas as pd
import sys
from pathlib import Path
import math
import datetime


DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = [
    'timestamp', 'open', 'high', 'low', 'close',
    'high_open', 'high_high', 'high_low', 'high_close',
    'fractal_low', 'fractal_high'
]

ENABLE_BUY = True
ENABLE_SELL = True

STOP_LOSS_BUFFER = 1.0
RISK_REWARD_RATIO = 3.0

SPREAD = 0.9

BUY_LOGS = []
SELL_LOGS = []


def load_and_clean_ohlcv(file_path: Path) -> pd.DataFrame:
    """
    Read CSV, columns presence validation, data conversion
    Returns ready to backtests DataFrame
    """
    if not file_path.exists():                                                                              # is_file()
        sys.exit(f"[ERROR] File does not exist: {file_path}")

    df = pd.read_csv(file_path, sep=";")

    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        sys.exit(f"[ERROR] In {file_path} are missing columns: {missing_columns}")

    df['timestamp'] = pd.to_datetime(df['timestamp'], format=DATE_FORMAT, errors="coerce")
    df = df.dropna(subset=['timestamp'])

    float_columns = REQUIRED_COLUMNS[1:]
    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors="coerce")

    return df


def save_results_to_csv(results: list, output: Path) -> None:
    if not results:
        print("[WARNING] no results to save")
        return

    results_df = pd.DataFrame(results)
    results_df.to_csv(output, sep=";", date_format=DATE_FORMAT, index=False)


def is_last_fractal_visible(timestamp: datetime, lastfractaltime: datetime) -> bool:
    return (timestamp - lastfractaltime).total_seconds() >= 7200


def calculate_stop_loss(last_fractal: float, stop_loss_buffer: float, transaction_type: str) -> float:
    if transaction_type == 'BUY':
        return last_fractal - stop_loss_buffer
    if transaction_type == 'SELL':
        return last_fractal + stop_loss_buffer
    else:
        sys.exit("unsupported transaction type. Allowed: <BUY> <SELL>")


def calculate_take_profit(current_price: float, stop_loss: float, risk_reward_ratio: float, transaction_type: str) -> float:
    if transaction_type == 'BUY':
        stop_loss_points = current_price - stop_loss
        take_profit_points = stop_loss_points * risk_reward_ratio
        return current_price + take_profit_points
    if transaction_type == 'SELL':
        stop_loss_points = stop_loss - current_price
        take_profit_points = stop_loss_points * risk_reward_ratio
        return current_price - take_profit_points
    else:
        sys.exit("unsupported transaction type. Allowed: <BUY> <SELL>")


def run_backtest(df: pd.DataFrame) -> list:
    print("[INFO] start running backtest")

    trade_logs = []

    in_position_buy: bool = False
    last_fractal_low: float = None
    last_fractal_low_time: datetime = None
    check_last_fractal_high_visibility: bool = False
    current_sl_buy: float = None
    current_tp_buy: float = None
    check_sl_buy: float = None
    check_tp_buy: float = None
    reset_buy: bool = False
    open_time_buy: datetime = None
    close_time_buy: datetime = None
    result_buy: str = None

    in_position_sell: bool = False
    last_fractal_high: float = None
    last_fractal_high_time: datetime = None
    check_last_fractal_low_visibility: bool = False
    current_sl_sell: float = None
    current_tp_sell: float = None
    check_sl_sell: float = None
    check_tp_sell: float = None
    reset_sell: bool = False
    open_time_sell: datetime = None
    close_time_sell: datetime = None
    result_sell: str = None

    sl_buy = 0
    tp_buy = 0

    sl_sell = 0
    tp_sell = 0

    for row in df.itertuples(index=False):

        if ENABLE_BUY:
            if not in_position_buy:
                if not math.isnan(row.fractal_low):
                    last_fractal_low = row.fractal_low
                    last_fractal_low_time = row.timestamp
                    check_last_fractal_low_visibility = True
                if check_last_fractal_low_visibility and last_fractal_low_time is not None and is_last_fractal_visible(row.timestamp, last_fractal_low_time):                       # noqa
                    current_sl_buy = calculate_stop_loss(last_fractal_low, STOP_LOSS_BUFFER, 'BUY')
                    current_tp_buy = calculate_take_profit(row.open, current_sl_buy, RISK_REWARD_RATIO, 'BUY')
                    open_time_buy = row.timestamp
                    in_position_buy = True
                    check_last_fractal_low_visibility = False

            if in_position_buy:
                print(f"check is {current_sl_buy}")
                check_sl_buy = row.low
                check_tp_buy = row.high
                if check_sl_buy <= current_sl_buy:
                    print("[BUY] sl reached")
                    in_position_buy = False
                    result_buy = 'SL'
                    sl_buy += 1
                    reset_buy = True
                if check_tp_buy >= current_tp_buy:
                    print("[BUY] tp reached")
                    in_position_buy = False
                    result_buy = 'TP'
                    tp_buy += 1
                    reset_buy = True
                if reset_buy:
                    close_time_buy = row.timestamp
                    BUY_LOGS.append({
                        'open_time': open_time_buy,
                        'close_time': close_time_buy,
                        'stop_loss': current_sl_buy,
                        'take_profit': current_tp_buy,
                        'result': result_buy
                    })
                    current_sl_buy = None
                    current_tp_buy = None
                    reset_buy = False

            trade_logs.append({
                'type': 'BUY',
                'timestamp': row.timestamp,
                'in_position_buy': in_position_buy,
                'current_sl_buy': current_sl_buy,
                'current_tp_buy': current_tp_buy
            })

        if ENABLE_SELL:
            if not in_position_sell:
                if not math.isnan(row.fractal_high):
                    last_fractal_high = row.fractal_high
                    last_fractal_high_time = row.timestamp
                    check_last_fractal_high_visibility = True
                if check_last_fractal_high_visibility and last_fractal_high_time is not None and is_last_fractal_visible(row.timestamp, last_fractal_high_time):                   # noqa
                    current_sl_sell = calculate_stop_loss(last_fractal_high, STOP_LOSS_BUFFER, 'SELL')
                    current_tp_sell = calculate_take_profit(row.open, current_sl_sell, RISK_REWARD_RATIO, 'SELL')
                    open_time_sell = row.timestamp
                    in_position_sell = True
                    check_last_fractal_high_visibility = False

            if in_position_sell:
                print(f"check is {current_sl_sell}")
                check_sl_sell = row.high
                check_tp_sell = row.low
                if check_sl_sell >= current_sl_sell:
                    print("[SELL] sl reached")
                    in_position_sell = False
                    result_sell = 'SL'
                    sl_sell += 1
                    reset_sell = True
                if check_tp_sell <= current_tp_sell:
                    print("[SELL] tp reached")
                    in_position_sell = False
                    result_sell = 'TP'
                    tp_sell += 1
                    reset_sell = True
                if reset_sell:
                    close_time_sell = row.timestamp
                    SELL_LOGS.append({
                        'open_time': open_time_sell,
                        'close_time': close_time_sell,
                        'stop_loss': current_sl_sell,
                        'take_profit': current_tp_sell,
                        'result': result_sell
                    })
                    current_sl_sell = None
                    current_tp_sell = None
                    reset_sell = False

            trade_logs.append({
               'type': 'SELL',
               'timestamp': row.timestamp,
               'in_position_sell': in_position_sell,
               'check_tp_sell': row.high,
               'current_sl_sell': current_sl_sell,
               'current_tp_sell': current_tp_sell
            })

    print(f"SL BUY:  {sl_buy}, TP BUY:  {tp_buy}")
    print(f"SL SELL: {sl_sell}, TP SELL: {tp_sell}")
    return trade_logs


def main():
    if len(sys.argv) != 2:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file>")

    file_path = Path(sys.argv[1])

    df = load_and_clean_ohlcv(file_path)
    print("[INFO] Data pipeline finished successfully.")
    print(f"[INFO] head: {df.head()}")
    print(f"[INFO] tail: {df.tail()}")

    results = run_backtest(df)
    output_file = file_path.parent/f"{file_path.stem}_results.csv"
    save_results_to_csv(results, output_file)

    if ENABLE_BUY:
        output_file_results_buy = file_path.parent/f"{file_path.stem}_results_buy_list.csv"
        save_results_to_csv(BUY_LOGS, output_file_results_buy)

    if ENABLE_SELL:
        output_file_results_buy = file_path.parent/f"{file_path.stem}_results_sell_list.csv"
        save_results_to_csv(SELL_LOGS, output_file_results_buy)


if __name__ == "__main__":
    main()
