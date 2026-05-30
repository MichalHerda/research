# version based on new template: backtest_tpl.py

import pandas as pd
import sys
import math
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = [
    'timestamp', 'open', 'high', 'low', 'close',
    'high_timestamp',
    'high_open', 'high_high', 'high_low', 'high_close',
    'fractal_low', 'fractal_high'
]

LOT_SIZE_DEFAULT = 1
RISK_REWARD_RATIO = 1
TRANSACTION_TYPE = 'BUY'
BREAKEVEN_MODIFICATION = 0.1
STOP_LOSS_BUFFER = 0.9
BREAKEVEN = False


@dataclass
class BacktestResult:
    trades: list
    telemetry: list
    summary: dict


@dataclass
class Trade:
    transaction_type: str | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    lot_size: float = 0.0
    open_price: float = 0.0
    close_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    result: str | None = None

    def close(self, close_time: datetime, close_price: float, result: str):
        self.close_time = close_time
        self.close_price = close_price
        self.result = result


@dataclass
class MarketState:
    timestamp: datetime | None = None
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    last_low_fractal_appearance_time: datetime | None = None
    last_low_fractal_used_time: datetime | None = None
    last_low_fractal_value: float | None = None
    last_high_fractal_appearance_time: datetime | None = None
    last_high_fractal_used_time: datetime | None = None
    last_high_fractal_value: float | None = None


def detect_high_timeframe_seconds(df: pd.DataFrame) -> int:

    high_timestamps = (
        df['high_timestamp']
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    if len(high_timestamps) < 2:
        sys.exit("[ERROR] cannot detect high timeframe")

    deltas = high_timestamps.diff().dropna()

    timeframe_seconds = int(
        deltas.mode().iloc[0].total_seconds()
    )

    return timeframe_seconds


def is_last_fractal_visible(timestamp: datetime, last_fractal_time: datetime, high_tf_seconds: int) -> bool:
    if timestamp is None or last_fractal_time is None:
        return False
    return (timestamp - last_fractal_time).total_seconds() >= high_tf_seconds * 2     # <- visibility after 2 bars! no lookahead bias


def is_signal(state: MarketState, transaction_type: str, high_tf_seconds: int) -> bool:
    if transaction_type == 'BUY':
        last_fractal_time = state.last_low_fractal_appearance_time
        if state.last_low_fractal_appearance_time is None:
            return False
        if state.last_low_fractal_used_time == state.last_low_fractal_appearance_time:
            return False
    elif transaction_type == 'SELL':
        last_fractal_time = state.last_high_fractal_appearance_time
        if state.last_high_fractal_appearance_time is None:
            return False
        if state.last_high_fractal_used_time == state.last_high_fractal_appearance_time:
            return False
    else:
        print("unsupported transaction type")
        return False
    return is_last_fractal_visible(state.timestamp, last_fractal_time, high_tf_seconds)


def calculate_stop_loss(state: MarketState, transaction_type: str) -> float:
    if transaction_type == 'BUY':
        return state.last_low_fractal_value - STOP_LOSS_BUFFER
    elif transaction_type == 'SELL':
        return state.last_high_fractal_value + STOP_LOSS_BUFFER
    else:
        print("calculate_stop_loss: unsupported transaction type")
        return 0


def calculate_take_profit(state: MarketState, transaction_type: str, stop_loss: float) -> float:
    if transaction_type == 'BUY':
        stop_loss_point_value = state.open - stop_loss
        take_profit_point_value = stop_loss_point_value * RISK_REWARD_RATIO
        return state.open + take_profit_point_value
    elif transaction_type == 'SELL':
        stop_loss_point_value = stop_loss - state.open
        take_profit_point_value = stop_loss_point_value * RISK_REWARD_RATIO
        return state.open - take_profit_point_value
    else:
        print("calculate_take_profit: unsupported transaction type")
        return 0


def is_breakeven_condition(state: MarketState, current_trade: Trade) -> bool:
    # Prototype stub for strategy exit logic
    raise NotImplementedError("is_breakeven_condition not implemented")


def apply_breakeven(current_trade: Trade) -> None:
    if current_trade.transaction_type == "BUY":
        current_trade.stop_loss = current_trade.open_price + BREAKEVEN_MODIFICATION
        return
    if current_trade.transaction_type == "SELL":
        current_trade.stop_loss = current_trade.open_price - BREAKEVEN_MODIFICATION
        return
    print(f"[ERROR] unsupported transaction type: {current_trade.transaction_type}")


def open_position(state: MarketState, transaction_type: str, stop_loss: float, take_profit: float) -> Trade:
    return Trade(
        transaction_type=transaction_type,
        open_time=state.timestamp,
        lot_size=LOT_SIZE_DEFAULT,
        open_price=state.open,
        stop_loss=stop_loss,
        take_profit=take_profit
    )


def close_position(state: MarketState, current_trade: Trade, trade_result: str) -> None:
    close_price = 0
    if trade_result == 'TP':
        close_price = current_trade.take_profit
    if trade_result == 'SL':
        close_price = current_trade.stop_loss
    if trade_result != 'TP' and trade_result != 'SL':
        print(f"close position trade result not supported: {trade_result}")
    current_trade.close(state.timestamp, close_price, trade_result)


def is_stop_loss_reached(state: MarketState, current_trade: Trade) -> bool:
    if current_trade.transaction_type == 'BUY':
        return state.low <= current_trade.stop_loss
    if current_trade.transaction_type == 'SELL':
        return state.high >= current_trade.stop_loss
    return False


def is_take_profit_reached(state: MarketState, current_trade: Trade) -> bool:
    if current_trade.transaction_type == 'BUY':
        return state.high >= current_trade.take_profit
    if current_trade.transaction_type == 'SELL':
        return state.low <= current_trade.take_profit
    return False


def append_trade_logs(current_trade: Trade, trade_logs: list) -> None:
    trade_logs.append({
        'transaction_type': current_trade.transaction_type,
        'open_time': current_trade.open_time,
        'close_time': current_trade.close_time,
        'lot_size': current_trade.lot_size,
        'open_price': current_trade.open_price,
        'close_price': current_trade.close_price,
        'stop_loss': current_trade.stop_loss,
        'take_profit': current_trade.take_profit,
        'result': current_trade.result
    })


def append_backtest_logs(state: MarketState, in_position: bool, current_trade: Trade | None, trade_logs_list: list) -> None:
    log_entry = {
        'timestamp': state.timestamp,
        'in_position': in_position,
        'close_price': state.close,
        'current_sl': None,
        'current_tp': None,
        'trade_type': None
    }

    if in_position and current_trade is not None:
        log_entry['current_sl'] = current_trade.stop_loss
        log_entry['current_tp'] = current_trade.take_profit
        log_entry['trade_type'] = current_trade.transaction_type
    else:
        if TRANSACTION_TYPE == 'BUY':
            log_entry['last_low_fractal_value'] = state.last_low_fractal_value
            log_entry['last_low_fractal_appearance_time'] = state.last_low_fractal_appearance_time
        elif TRANSACTION_TYPE == 'SELL':
            log_entry['last_high_fractal_value'] = state.last_high_fractal_value
            log_entry['last_high_fractal_appearance_time'] = state.last_high_fractal_appearance_time

    trade_logs_list.append(log_entry)


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

    df['high_timestamp'] = pd.to_datetime(df['high_timestamp'], format=DATE_FORMAT, errors='coerce')
    df = df.dropna(subset=['high_timestamp'])

    float_columns = [
        'open', 'high', 'low', 'close',
        'high_open', 'high_high', 'high_low', 'high_close',
        'fractal_low', 'fractal_high'
    ]
    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors="coerce")

    return df


def save_results_to_csv(results: list, output: Path) -> None:
    if not results:
        print("[WARNING] no results to save")
        return

    results_df = pd.DataFrame(results)
    results_df.to_csv(output, sep=";", date_format=DATE_FORMAT, index=False)


def run_backtest(df: pd.DataFrame, high_tf_seconds: int, enable_logging=True) -> BacktestResult:
    print("[INFO] start running backtest")
    trade_logs = []
    telemetry_logs = []
    current_trade = None
    in_position = False
    state = MarketState()
    summary = {
        "TP": 0,
        "SL": 0
    }

    for row in df.itertuples(index=False):

        state.timestamp = row.timestamp
        state.open = row.open
        state.high = row.high
        state.low = row.low
        state.close = row.close

        if TRANSACTION_TYPE == 'BUY':
            if not math.isnan(row.fractal_low):
                if state.last_low_fractal_value is None or row.fractal_low != state.last_low_fractal_value:
                    state.last_low_fractal_appearance_time = row.timestamp
                    state.last_low_fractal_value = row.fractal_low

        if TRANSACTION_TYPE == 'SELL':
            if not math.isnan(row.fractal_high):
                if state.last_high_fractal_value is None or row.fractal_high != state.last_high_fractal_value:
                    state.last_high_fractal_appearance_time = row.timestamp
                    state.last_high_fractal_value = row.fractal_high

        if not in_position:
            if is_signal(state, TRANSACTION_TYPE, high_tf_seconds):
                stop_loss = calculate_stop_loss(state, TRANSACTION_TYPE)
                take_profit = calculate_take_profit(state, TRANSACTION_TYPE, stop_loss)
                current_trade = open_position(state, TRANSACTION_TYPE, stop_loss, take_profit)
                in_position = True

                if TRANSACTION_TYPE == 'BUY':
                    state.last_low_fractal_used_time = state.last_low_fractal_appearance_time
                if TRANSACTION_TYPE == 'SELL':
                    state.last_high_fractal_used_time = state.last_high_fractal_appearance_time

        elif in_position:

            # Worst-case scenario execution order (Conservative approach).
            # If a single bar triggers both SL and TP, we deliberately process the SL first
            # to keep the backtest realistic and avoid overestimating strategy performance.

            if is_stop_loss_reached(state, current_trade):
                close_position(state, current_trade, 'SL')
                summary["SL"] += 1
                append_trade_logs(current_trade, trade_logs)
                if enable_logging:
                    append_backtest_logs(state, in_position, current_trade, telemetry_logs)
                current_trade = None
                in_position = False
                continue

            if is_take_profit_reached(state, current_trade):
                close_position(state, current_trade, 'TP')
                summary["TP"] += 1
                append_trade_logs(current_trade, trade_logs)
                if enable_logging:
                    append_backtest_logs(state, in_position, current_trade, telemetry_logs)
                current_trade = None
                in_position = False
                continue

            if BREAKEVEN and is_breakeven_condition(state, current_trade):
                apply_breakeven(current_trade)

        if enable_logging:
            append_backtest_logs(state, in_position, current_trade, telemetry_logs)

    return BacktestResult(
        trades=trade_logs,
        telemetry=telemetry_logs,
        summary=summary
    )


def main():
    if len(sys.argv) != 2:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file>")

    file_path = Path(sys.argv[1])
    enable_backtest_logging = True

    df = load_and_clean_ohlcv(file_path)
    print("[INFO] Data pipeline finished successfully.")
    print(f"[INFO] head: {df.head()}")
    print(f"[INFO] tail: {df.tail()}")

    high_tf_seconds = detect_high_timeframe_seconds(df)

    print(
        f"[INFO] detected high timeframe: "
        f"{high_tf_seconds} seconds"
    )

    backtest_results = run_backtest(df, high_tf_seconds)
    output_file = file_path.parent/f"{file_path.stem}_results.csv"
    save_results_to_csv(backtest_results.trades, output_file)

    if enable_backtest_logging:
        telemetry_file = file_path.parent / f"{file_path.stem}_step_by_step.csv"
        save_results_to_csv(backtest_results.telemetry, telemetry_file)
        print(f"[INFO] Telemetry saved successfully to: {telemetry_file}")

    summary_file = file_path.parent / f"{file_path.stem}_summary.csv"
    save_results_to_csv([backtest_results.summary], summary_file)


if __name__ == "__main__":
    main()
