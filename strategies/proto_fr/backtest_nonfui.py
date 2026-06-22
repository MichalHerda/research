# backtest_nonfui.py

"""
Backtest: NonFuiFuiNonSumNonCuro
Biblioteka: backtesting.py

Użycie:
    pip install backtesting pandas

    python backtest_nonfui.py <plik.csv> <BUY|SELL|BOTH> [timeframe]

Przykłady:
    python backtest_nonfui.py US100_M15.csv BUY
    python backtest_nonfui.py US100_M15.csv SELL
    python backtest_nonfui.py US100_M15.csv BOTH

Format CSV (separator średnik):
    timestamp;open;high;low;close
    2024.01.02 01:00:00;16800.5;16820.0;16795.0;16810.0
"""

import sys
import pandas as pd
# import numpy as np
from pathlib import Path
from backtesting import Backtest, Strategy
# from backtesting.lib import crossover

# ─────────────────────────────────────────────
# PARAMETRY — edytuj tutaj lub przez optymalizację
# ─────────────────────────────────────────────
SPREAD = 0.9
MAX_SPREAD_PERCENTAGE = 5.0
RISK_REWARD_RATIO = 500        # praktycznie nieosiągalny TP — pozycje zamykane przez SL/signal_out
INITIAL_BALANCE = 10_000
INITIAL_RISK = 0.5        # % kapitału na trade

BE_ABSOLUTE = True
BE_ABSOLUTE_ACTIVATION = 50.0     # punkty zysku aktywujące BE
BE_ACTIVATION = 0.5        # używane gdy BE_ABSOLUTE = False (% odległości SL)

DATE_FORMAT = "%Y.%m.%d %H:%M:%S"


# ─────────────────────────────────────────────
# WCZYTANIE DANYCH
# ─────────────────────────────────────────────
def load_data(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(file_path, sep=";")
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=DATE_FORMAT, errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df = df.set_index('timestamp')
    df.index.name = None
    df.columns = [c.capitalize() for c in df.columns]
    for col in ['Open', 'High', 'Low', 'Close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna()

    # backtesting.py wymaga kolumny Volume
    if 'Volume' not in df.columns:
        df['Volume'] = 1

    return df


# ─────────────────────────────────────────────
# POMOCNICZE
# ─────────────────────────────────────────────
def calc_minimum_sl_distance() -> float:
    spread = SPREAD
    return (spread / MAX_SPREAD_PERCENTAGE) * 100.0


def calc_lot_size(sl_distance: float) -> float:
    risk_amount = INITIAL_BALANCE * (INITIAL_RISK / 100.0)
    # uproszczenie: 1 punkt = 1 USD przy 1 locie (dostosuj do instrumentu)
    loss_per_lot = sl_distance
    if loss_per_lot <= 0:
        return 0.01
    lot = risk_amount / loss_per_lot
    lot = max(0.01, round(lot, 2))
    return lot


# ─────────────────────────────────────────────
# STRATEGIA BUY
# ─────────────────────────────────────────────
class NonFuiBuy(Strategy):
    # parametry dostępne przez optymalizację
    be_absolute = BE_ABSOLUTE
    be_absolute_activation = BE_ABSOLUTE_ACTIVATION
    be_activation = BE_ACTIVATION

    def init(self):
        # Liczymy rolling low i średnią świecy — musimy sam zaimplementować
        # bo backtesting.py nie ma iLow/iHigh per offset
        # low = self.data.Low
        # high = self.data.High

        # idx1/2/3 to przesunięcia: [1], [2], [3] od bieżącej świecy
        # w backtesting.py self.data[-1] to ostatnia zamknięta świeca
        # nie tworzymy osobnych indicatorów — liczymy inline w next()
        self._be_applied = False
        self._entry_price = 0.0
        self._initial_sl = 0.0

    def next(self):
        # potrzebujemy min. 4 świece historii
        if len(self.data) < 4:
            return

        low = self.data.Low
        high = self.data.High

        idx1_low = low[-2]   # świeca -1 od zamkniętej (bar index 1 w MQL4)
        idx2_low = low[-3]   # bar 2
        idx3_low = low[-4]   # bar 3

        idx1_avg = (high[-2] + low[-2]) / 2
        idx2_avg = (high[-3] + low[-3]) / 2

        ask = self.data.Close[-1] + SPREAD  # przybliżenie Ask

        if not self.position:
            self._be_applied = False

            # SIGNAL IN: lokalny dołek na idx2
            if idx1_low > idx2_low and idx2_low < idx3_low:
                sl_raw = idx2_low - SPREAD
                min_dist = calc_minimum_sl_distance()
                sl_dist = ask - sl_raw
                if sl_dist < min_dist:
                    sl_raw = ask - min_dist
                    sl_dist = min_dist

                tp = ask + sl_dist * RISK_REWARD_RATIO

                self._entry_price = ask
                self._initial_sl = sl_raw
                self._sl_distance = sl_dist

                self.buy(sl=sl_raw, tp=tp, size=1)
        else:
            current_price = self.data.Close[-1]

            # BREAKEVEN
            if not self._be_applied:
                if self.be_absolute:
                    trigger = self.be_absolute_activation
                else:
                    trigger = self._sl_distance * self.be_activation

                profit = current_price - self._entry_price
                if profit >= trigger:
                    # przesuń SL na open — backtesting.py pozwala modyfikować
                    self.position.sl = self._entry_price
                    self._be_applied = True

            # SIGNAL OUT: średnia świecy spada
            if idx1_avg < idx2_avg:
                self.position.close()


# ─────────────────────────────────────────────
# STRATEGIA SELL
# ─────────────────────────────────────────────
class NonFuiSell(Strategy):
    be_absolute = BE_ABSOLUTE
    be_absolute_activation = BE_ABSOLUTE_ACTIVATION
    be_activation = BE_ACTIVATION

    def init(self):
        self._be_applied = False
        self._entry_price = 0.0
        self._sl_distance = 0.0

    def next(self):
        if len(self.data) < 4:
            return

        low = self.data.Low
        high = self.data.High

        idx1_high = high[-2]
        idx2_high = high[-3]
        idx3_high = high[-4]

        idx1_avg = (high[-2] + low[-2]) / 2
        idx2_avg = (high[-3] + low[-3]) / 2

        bid = self.data.Close[-1]

        if not self.position:
            self._be_applied = False

            # SIGNAL IN: lokalny szczyt na idx2
            if idx1_high < idx2_high and idx2_high > idx3_high:
                sl_raw = idx2_high + SPREAD
                min_dist = calc_minimum_sl_distance()
                sl_dist = sl_raw - bid
                if sl_dist < min_dist:
                    sl_raw = bid + min_dist
                    sl_dist = min_dist

                tp = bid - sl_dist * RISK_REWARD_RATIO
                if tp <= 0:
                    tp = 0.01

                self._entry_price = bid
                self._sl_distance = sl_dist

                self.sell(sl=sl_raw, tp=tp, size=1)
        else:
            current_price = self.data.Close[-1]

            # BREAKEVEN
            if not self._be_applied:
                if self.be_absolute:
                    trigger = self.be_absolute_activation
                else:
                    trigger = self._sl_distance * self.be_activation

                profit = self._entry_price - current_price
                if profit >= trigger:
                    self.position.sl = self._entry_price
                    self._be_applied = True

            # SIGNAL OUT
            if idx1_avg > idx2_avg:
                self.position.close()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def run(file_path: Path, direction: str):
    print(f"[INFO] Wczytywanie: {file_path}")
    df = load_data(file_path)
    print(f"[INFO] Świece: {len(df)} | Od: {df.index[0]} | Do: {df.index[-1]}")

    results = {}

    if direction in ('BUY', 'BOTH'):
        print("\n[INFO] === BUY ===")
        bt = Backtest(
            df,
            NonFuiBuy,
            cash=INITIAL_BALANCE,
            commission=0,
            exclusive_orders=True,
            margin=0.05
        )
        stats = bt.run()
        print(stats)
        results['BUY'] = (bt, stats)
        bt.plot(filename=str(file_path.parent / f"{file_path.stem}_BUY.html"), open_browser=False)
        print(f"[INFO] Wykres zapisany: {file_path.stem}_BUY.html")

    if direction in ('SELL', 'BOTH'):
        print("\n[INFO] === SELL ===")
        bt = Backtest(
            df,
            NonFuiSell,
            cash=INITIAL_BALANCE,
            commission=0,
            exclusive_orders=True,
            margin=0.05
        )
        stats = bt.run()
        print(stats)
        results['SELL'] = (bt, stats)
        bt.plot(filename=str(file_path.parent / f"{file_path.stem}_SELL.html"), open_browser=False)
        print(f"[INFO] Wykres zapisany: {file_path.stem}_SELL.html")

    # zapis trade logu do CSV
    for direction_key, (bt, stats) in results.items():
        trades_df = stats['_trades']
        if not trades_df.empty:
            out = file_path.parent / f"{file_path.stem}_{direction_key}_trades.csv"
            trades_df.to_csv(out, sep=";", index=False)
            print(f"[INFO] Trade log: {out}")

    return results


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(
            f"Użycie: python {Path(sys.argv[0]).name} <plik.csv> <BUY|SELL|BOTH>\n"
            f"Przykład: python backtest_nonfui.py US100_M15.csv BUY"
        )

    fp = Path(sys.argv[1])
    direction = sys.argv[2].upper()

    if direction not in ('BUY', 'SELL', 'BOTH'):
        sys.exit("[ERROR] Drugi argument musi być: BUY, SELL lub BOTH")

    if not fp.exists():
        sys.exit(f"[ERROR] Plik nie istnieje: {fp}")

    run(fp, direction)
