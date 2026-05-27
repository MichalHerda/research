# NOTE: This boilerplate is designed specifically for path-dependent trading strategies
# where state transitions (like trailing stops, multi-stage entries, or time-window visibility)
# depend strictly on the previous step's context. Since these strategies cannot be safely
# vectorized using pure Pandas/NumPy operations, this structured loop serves as a reusable
# template for chronological, time-series iteration.

import pandas as pd
import sys
from pathlib import Path
import datetime
from dataclasses import dataclass

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = [
    'timestamp', 'open', 'high', 'low', 'close',
]


class Trade:
    transaction_type: str
    open_time: datetime
    close_time: datetime
    lot_size: float
    open_price: float
    close_price: float
    stop_loss: float
    take_profit: float
    result: str
    active: bool

    def __init__(self, transaction_type: str):
        self.transaction_type: str = transaction_type
        self.open_time: datetime = None
        self.close_time: datetime = None
        self.lot_size: float = 0.0
        self.open_price: float = 0.0
        self.close_price: float = 0.0
        self.stop_loss: float = 0.0
        self.take_profit: float = 0.0
        self.result: str | None = None
        self.active: bool = False

    def open(self, open_time: datetime, lot_size: float, open_price: float, stop_loss: float, take_profit: float):
        self.open_time = open_time
        self.lot_size = lot_size
        self.open_price = open_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.active = True

    def close(self, close_time: datetime, close_price: float, result: str):
        self.close_time = close_time
        self.close_price = close_price
        self.result = result
        self.active = False


@dataclass
class MarketState:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


def is_signal(state: MarketState) -> bool:
    pass


def open_position(state: MarketState) -> Trade:
    pass


def close_position(state: MarketState, current_trade: Trade, trade_result: str) -> None:
    pass


def is_stop_loss_reached(state: MarketState, current_trade: Trade) -> bool:
    pass


def is_take_profit_reached(state: MarketState, current_trade: Trade) -> bool:
    pass


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
                current_trade = open_position(state)
                in_position = True

        elif in_position:

            # Worst-case scenario execution order (Conservative approach).
            # If a single bar triggers both SL and TP, we deliberately process the SL first
            # to keep the backtest realistic and avoid overestimating strategy performance.

            if is_stop_loss_reached(state, current_trade):
                close_position(state, current_trade, 'SL')
                in_position = False
            if is_take_profit_reached(state, current_trade):
                close_position(state, current_trade, 'TP')
                in_position = False

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
