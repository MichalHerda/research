import pandas as pd
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"

REQUIRED_COLUMNS = [
    'timestamp', 'open', 'high', 'low', 'close',
]
SPREAD = 0.9
MAX_SPREAD_PERCENTAGE = 10
RISK_REWARD_RATIO = 1
LOT_SIZE_DEFAULT = 1

BREAKEVEN = True
BE_ACTIVATION = 0.5
BE_MODIFICATION = 1
POINT = 0.01


@dataclass
class BacktestResult:
    trades: list
    telemetry: list
    summary: dict
    # equity: list


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
    breakeven_applied: bool = False
    initial_sl_distance: float = 0.0

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
    idx1_extremum: float | None = None
    idx2_extremum: float | None = None
    idx3_extremum: float | None = None
    idx1_average: float | None = None
    idx2_average: float | None = None


def detect_timeframe_seconds(df: pd.DataFrame) -> int:
    timestamps = (
        df['timestamp']
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    if len(timestamps) < 2:
        sys.exit("[ERROR] cannot detect high timeframe")

    deltas = timestamps.diff().dropna()

    timeframe_seconds = int(
        deltas.mode().iloc[0].total_seconds()
    )

    return timeframe_seconds


def calculate_equity_curve(trade_logs: list) -> list:
    equity = 0.0
    result = []
    for trade in trade_logs:
        equity = equity + trade['r_multiple']
        result.append({
            'open_time': trade['open_time'],
            'close_time': trade['close_time'],
            'result': trade['result'],
            'r_multiple': trade['r_multiple'],
            'equity_r': round(equity, 4)
        })
    return result


def append_backtest_logs(state: MarketState, in_position: bool, current_trade: Trade | None, trade_logs_list: list, transaction_type: str) -> None:
    log_entry = {
        'timestamp': state.timestamp,
        'open': state.open,
        'high': state.high,
        'low': state.low,
        'close': state.close,
        'idx1_extremum': state.idx1_extremum,
        'idx2_extremum': state.idx2_extremum,
        'idx3_extremum': state.idx3_extremum,
        'idx1_average': state.idx1_average,
        'idx2_average': state.idx2_average,
        'in_position': in_position
    }

    if in_position and current_trade is not None:
        log_entry['current_sl'] = current_trade.stop_loss
        log_entry['current_tp'] = current_trade.take_profit
        log_entry['open_price'] = current_trade.open_price
        # log_entry['trade_type'] = current_trade.transaction_type
        # log_entry['breakeven_applied'] = current_trade.breakeven_applied
        pass
    else:
        # log_entry['breakeven_applied'] = False
        if transaction_type == 'BUY':
            pass
        elif transaction_type == 'SELL':
            pass

    trade_logs_list.append(log_entry)


def append_trade_log(current_trade: Trade, trade_logs: list) -> None:
    r_multiple = calculate_r_multiple(current_trade)
    trade_logs.append({
        'transaction_type': current_trade.transaction_type,
        'open_time': current_trade.open_time,
        'close_time': current_trade.close_time,
        'open_price': current_trade.open_price,
        'close_price': current_trade.close_price,
        'stop_loss': current_trade.stop_loss,
        'take_profit': current_trade.take_profit,
        'result': current_trade.result,
        'r_multiple': r_multiple
    })


def calculate_r_multiple(trade: Trade) -> float:
    # sl_distance = abs(trade.open_price - trade.stop_loss)
    sl_distance = trade.initial_sl_distance
    if sl_distance == 0:
        return 0.0
    if trade.transaction_type == 'BUY':
        raw_pnl = trade.close_price - trade.open_price
    elif trade.transaction_type == 'SELL':
        raw_pnl = trade.open_price - trade.close_price
    else:
        return 0.0
    return round(raw_pnl / sl_distance, 4)


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


def is_price_be_condition(state: MarketState, current_trade: Trade) -> bool:
    if current_trade.breakeven_applied:
        return False
    if current_trade.transaction_type == 'BUY':
        activation_level = current_trade.open_price + (
            (current_trade.take_profit - current_trade.open_price) * BE_ACTIVATION
        )
        return state.high >= activation_level
    if current_trade.transaction_type == 'SELL':
        activation_level = current_trade.open_price - (
            (current_trade.open_price - current_trade.take_profit) * BE_ACTIVATION
        )
        return state.low <= activation_level
    return False


def apply_breakeven(current_trade: Trade) -> None:
    if current_trade.transaction_type == 'BUY':
        current_trade.stop_loss = current_trade.open_price + BE_MODIFICATION * POINT
    elif current_trade.transaction_type == 'SELL':
        current_trade.stop_loss = current_trade.open_price - BE_MODIFICATION * POINT
    current_trade.breakeven_applied = True


def is_signal_in(state: MarketState, transaction_type: str):
    if transaction_type == 'BUY':
        if state.idx1_extremum > state.idx2_extremum and state.idx2_extremum < state.idx3_extremum:
            return True
    if transaction_type == 'SELL':
        if state.idx1_extremum < state.idx2_extremum and state.idx2_extremum > state.idx3_extremum:
            return True
    if transaction_type != 'BUY' and transaction_type != 'SELL':
        sys.exit("is_signal_in: unsupported transaction type")
    return False


def is_signal_out(state: MarketState, transaction_type: str):
    if transaction_type == 'BUY':
        if state.idx1_average < state.idx2_average:
            return True
    if transaction_type == 'SELL':
        if state.idx1_average > state.idx2_average:
            return True
    if transaction_type != 'BUY' and transaction_type != 'SELL':
        sys.exit("is_signal_out: unsupported transaction type")
    return False


def calculate_minimum_stop_loss() -> float:
    return (SPREAD / MAX_SPREAD_PERCENTAGE) * 100.0


def enforce_minimum_stop_loss(
    state: MarketState,
    transaction_type: str,
    stop_loss: float
) -> float:

    minimum_sl_distance = calculate_minimum_stop_loss()

    if transaction_type == "BUY":

        current_distance = state.open - stop_loss

        if current_distance < minimum_sl_distance:
            stop_loss = state.open - minimum_sl_distance

    elif transaction_type == "SELL":

        current_distance = stop_loss - state.open

        if current_distance < minimum_sl_distance:
            stop_loss = state.open + minimum_sl_distance

    return stop_loss


def calculate_stop_loss(state: MarketState, transaction_type: str) -> float:
    if transaction_type == 'BUY':
        return state.idx2_extremum - SPREAD
    elif transaction_type == 'SELL':
        return state.idx2_extremum + SPREAD
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


def open_position(state: MarketState, transaction_type: str, stop_loss: float, take_profit: float) -> Trade:
    if transaction_type == 'BUY':
        open_price = state.open + SPREAD
    else:
        open_price = state.open
    return Trade(
        transaction_type=transaction_type,
        open_time=state.timestamp,
        lot_size=LOT_SIZE_DEFAULT,
        open_price=open_price,
        stop_loss=stop_loss,
        take_profit=take_profit
    )


def close_position(state: MarketState, current_trade: Trade, trade_result: str) -> None:
    close_price = 0
    if trade_result == 'TP':
        close_price = current_trade.take_profit
    if trade_result == 'SL' or trade_result == "BE":
        close_price = current_trade.stop_loss
    if trade_result == 'DX':
        close_price = state.open
    if trade_result != 'TP' and trade_result != 'SL' and trade_result != "BE" and trade_result != "DX":
        print(f"close position trade result not supported: {trade_result}")
    current_trade.close(state.timestamp, close_price, trade_result)


def is_stop_loss_reached(state: MarketState, current_trade: Trade) -> bool:
    if current_trade.transaction_type == 'BUY':
        return state.low <= current_trade.stop_loss
    if current_trade.transaction_type == 'SELL':
        return state.high + SPREAD >= current_trade.stop_loss
    return False


def is_take_profit_reached(state: MarketState, current_trade: Trade) -> bool:
    if current_trade.transaction_type == 'BUY':
        return state.high >= current_trade.take_profit
    if current_trade.transaction_type == 'SELL':
        return state.low + SPREAD <= current_trade.take_profit
    return False


def save_results_to_csv(results: list, output: Path) -> None:
    if not results:
        print("[WARNING] no results to save")
        return

    results_df = pd.DataFrame(results)
    results_df.to_csv(output, sep=";", date_format=DATE_FORMAT, index=False)


def initialize(state: MarketState, row, transaction_type: str) -> bool:

    extremum_value = row.low if transaction_type == "BUY" else row.high

    if state.idx3_extremum is None:
        state.idx3_extremum = extremum_value
        return False
    if state.idx2_extremum is None:
        state.idx2_extremum = extremum_value
        state.idx2_average = (row.high + row.low) / 2
        return False
    if state.idx1_extremum is None:
        state.idx1_extremum = extremum_value
        state.idx1_average = (row.high + row.low) / 2
        return True

    return True


def shift(state: MarketState, transaction_type: str) -> None:
    """Przesuwa okno idx1/2/3 o jeden krok."""
    state.idx3_extremum = state.idx2_extremum
    state.idx2_extremum = state.idx1_extremum
    state.idx1_extremum = state.low if transaction_type == "BUY" else state.high

    state.idx2_average = state.idx1_average
    state.idx1_average = (state.high + state.low) / 2


def run_backtest(df: pd.DataFrame, high_tf_seconds: int, transaction_type: str) -> BacktestResult:
    print("[INFO] start running backtest")
    trade_logs = []
    telemetry_logs = []
    current_trade = None
    in_position = False
    state = MarketState()
    summary = {
        "TP": 0,
        "SL": 0,
        "BE": 0,
        "DX": 0,
        "total_r": 0.0
    }
    initial_data_completed = False
    # signal_out_detected = False

    for row in df.itertuples(index=False):
        state.timestamp = row.timestamp
        state.open = row.open
        state.high = row.high
        state.low = row.low
        state.close = row.close

        if not initial_data_completed:
            initial_data_completed = initialize(state, row, transaction_type)
            append_backtest_logs(state, in_position, current_trade, telemetry_logs, transaction_type)
            continue

        if not in_position:
            if is_signal_in(state, transaction_type):
                stop_loss = calculate_stop_loss(state, transaction_type)
                take_profit = calculate_take_profit(state, transaction_type, stop_loss)
                current_trade = open_position(state, transaction_type, stop_loss, take_profit)  # brakuje tego
                current_trade.initial_sl_distance = abs(current_trade.open_price - current_trade.stop_loss)
                in_position = True
        else:
            min_check_time = current_trade.open_time + timedelta(seconds=2 * high_tf_seconds)

            if is_stop_loss_reached(state, current_trade):
                if current_trade.breakeven_applied:        # ← rozróżnienie BE vs SL
                    close_position(state, current_trade, 'BE')
                    summary['BE'] += 1
                else:
                    close_position(state, current_trade, 'SL')
                    summary['SL'] += 1
                summary['total_r'] = round(summary['total_r'] + calculate_r_multiple(current_trade), 4)
                append_trade_log(current_trade, trade_logs)
                in_position = False
                # signal_out_detected = False

            # elif is_take_profit_reached(state, current_trade):
            #    close_position(state, current_trade, 'TP')
            #    summary['TP'] += 1
            #    summary['total_r'] = round(summary['total_r'] + calculate_r_multiple(current_trade), 4)
            #    append_trade_log(current_trade, trade_logs)
            #    in_position = False
            #    signal_out_detected = False

            elif state.timestamp >= min_check_time:
                # if not signal_out_detected:
                if is_signal_out(state, transaction_type):
                    #        signal_out_detected = True
                    # else:
                    close_position(state, current_trade, 'DX')
                    summary['DX'] += 1
                    summary['total_r'] = round(summary['total_r'] + calculate_r_multiple(current_trade), 4)
                    append_trade_log(current_trade, trade_logs)
                    in_position = False
                    # signal_out_detected = False

            if BREAKEVEN:
                if is_price_be_condition(state, current_trade) and in_position:
                    apply_breakeven(current_trade)

        append_backtest_logs(state, in_position, current_trade, telemetry_logs, transaction_type)
        shift(state, transaction_type)

    return BacktestResult(
        trades=trade_logs,
        telemetry=telemetry_logs,
        summary=summary
        # equity=equity_curve
    )


def main():
    if len(sys.argv) != 3:
        sys.exit(f"[ERROR] usage: python3 {Path(sys.argv[0]).name} <file> <transaction_type>")

    if sys.argv[2] != 'BUY' and sys.argv[2] != 'SELL':
        sys.exit(f"[ERROR] unsupported transaction type {sys.argv[2]}, exptected: <BUY> <SELL>")
    file_path = Path(sys.argv[1])
    transaction_type = sys.argv[2]

    df = load_and_clean_ohlcv(file_path)
    print("[INFO] Data pipeline finished successfully.")
    print(f"[INFO] head: {df.head()}")
    print(f"[INFO] tail: {df.tail()}")

    tf_seconds = detect_timeframe_seconds(df)

    print(
        f"[INFO] detected high timeframe: "
        f"{tf_seconds} seconds"
    )

    backtest_results = run_backtest(df, tf_seconds, transaction_type)
    output_file = file_path.parent/f"{file_path.stem}_{transaction_type}_results.csv"
    save_results_to_csv(backtest_results.trades, output_file)

    telemetry_file = file_path.parent / f"{file_path.stem}_{transaction_type}_step_by_step.csv"
    save_results_to_csv(backtest_results.telemetry, telemetry_file)
    print(f"[INFO] Telemetry saved successfully to: {telemetry_file}")

    summary_file = file_path.parent / f"{file_path.stem}_{transaction_type}_summary.csv"
    save_results_to_csv([backtest_results.summary], summary_file)

    # equity_file = file_path.parent / f"{file_path.stem}_{transaction_type}_equity.csv"
    # save_results_to_csv(backtest_results.equity, equity_file)


if __name__ == "__main__":
    main()
