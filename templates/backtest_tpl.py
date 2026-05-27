# NOTE: This boilerplate is designed specifically for path-dependent trading strategies
# where state transitions (like trailing stops, multi-stage entries, or time-window visibility)
# depend strictly on the previous step's context. Since these strategies cannot be safely
# vectorized using pure Pandas/NumPy operations, this structured loop serves as a reusable
# template for chronological, time-series iteration.

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = [
    'timestamp', 'open', 'high', 'low', 'close',
]

LOT_SIZE_DEFAULT = 1
RISK_REWARD_RATIO = 1
TRANSACTION_TYPE = 'BUY'
BREAKEVEN_MODIFICATION = 0.1
BREAKEVEN = False


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
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


def is_signal(state: MarketState) -> bool:
    # Prototype stub for strategy entry logic
    pass


def calculate_stop_loss(state: MarketState, transaction_type: str) -> float:
    # Prototype stub for strategy exit logic
    pass


def calculate_take_profit(state: MarketState, transaction_type: str) -> float:
    # Prototype stub for strategy exit logic
    pass


def is_breakeven_condition(state: MarketState, current_trade: Trade) -> bool:
    # Prototype stub for strategy exit logic
    pass


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
        pass
        # return True
    if current_trade.transaction_type == 'SELL':
        pass
        # return True
    print(f"[ERROR] unsupported transaction type: {current_trade.transaction_type}")
    return False


def is_take_profit_reached(state: MarketState, current_trade: Trade) -> bool:
    if current_trade.transaction_type == 'BUY':
        pass
        # return True
    if current_trade.transaction_type == 'SELL':
        pass
        # return True
    print(f"[ERROR] unsupported transaction type: {current_trade.transaction_type}")
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


def run_backtest(df: pd.DataFrame) -> list:
    print("[INFO] start running backtest")
    trade_logs = []
    current_trade = None
    in_position = False

    for row in df.itertuples(index=False):

        state = MarketState(
            timestamp=row.timestamp,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close
        )

        if not in_position:
            if is_signal(state):
                stop_loss = calculate_stop_loss(state, TRANSACTION_TYPE)
                take_profit = calculate_take_profit(state, TRANSACTION_TYPE)
                current_trade = open_position(state, TRANSACTION_TYPE, stop_loss, take_profit)
                in_position = True

        elif in_position:

            # Worst-case scenario execution order (Conservative approach).
            # If a single bar triggers both SL and TP, we deliberately process the SL first
            # to keep the backtest realistic and avoid overestimating strategy performance.

            if is_stop_loss_reached(state, current_trade):
                close_position(state, current_trade, 'SL')
                append_trade_logs(current_trade, trade_logs)
                current_trade = None
                in_position = False
                continue

            if is_take_profit_reached(state, current_trade):
                close_position(state, current_trade, 'TP')
                append_trade_logs(current_trade, trade_logs)
                current_trade = None
                in_position = False
                continue

            if BREAKEVEN and is_breakeven_condition(state, current_trade):
                apply_breakeven(current_trade)

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


if __name__ == "__main__":
    main()
