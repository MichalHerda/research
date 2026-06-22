# test_agg.py
# !/usr/bin/env python3
"""
test_agg.py - poglądowy backtester strategii tick-bar (bez mechanizmu ladder).

Uzycie:
    python3 test_agg.py <file_input> <BUY/SELL> <consecutive_rows_signal> <max_spread> [ticks_quant]

Argumenty:
    file_input              - plik CSV z surowymi tickami: datetime;bid;ask
    BUY/SELL                - kierunek testowanej strategii
    consecutive_rows_signal - liczba kolejnych zgodnych tick-barow wymagana do sygnalu (int)
    max_spread               - maksymalny dopuszczalny spread (w jednostkach ceny, np. 0.00020)
    ticks_quant              - (opcjonalnie) liczba tickow na 1 tick-bar sygnalowy, domyslnie 5

Wyniki zapisywane sa do folderu TestAgg/:
    - trades.csv  : lista transakcji (open_time, close_time, open_price, close_price, result_points)
    - summary.txt : podsumowanie (profit_trades, lost_trades, profit_total_points,
                    lost_total_points, points_summary)
"""

import sys
import os
import csv
from datetime import datetime, timedelta

POINT = 0.00001  # najmniejsza jednostka ceny (5. miejsce po przecinku)


# ----------------------------------------------------------------------------
# Wczytywanie danych wejsciowych
# ----------------------------------------------------------------------------
def load_ticks(filepath):
    """
    Wczytuje plik CSV z surowymi tickami: datetime;bid;ask
    Zwraca liste krotek (datetime_obj, bid, ask).
    """
    ticks = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row or len(row) < 3:
                continue
            ts_str, bid_str, ask_str = row[0], row[1], row[2]
            try:
                ts = datetime.strptime(ts_str.strip(), "%Y.%m.%d %H:%M:%S.%f")
            except ValueError:
                # naglowek lub linia bledna - pomijamy
                continue
            bid = float(bid_str)
            ask = float(ask_str)
            ticks.append((ts, bid, ask))
    return ticks


# ----------------------------------------------------------------------------
# Agregator tick-barow (replika logiki TickAggregator.mqh, uproszczona do
# potrzeb backtestu: pracujemy na licznosci ticks_quant, budujemy na biezaco
# OHLC z ceny 'bid').
# ----------------------------------------------------------------------------
class TickBarAggregator:
    """
    Agreguje strumien cen (bid) w tick-bary o stalej liczbie tickow.
    Po kazdym wywolaniu push() zwraca zamknieta swiece (Candle) albo None,
    jesli swieca jeszcze sie nie zamknela.
    """

    def __init__(self, ticks_quant):
        self.ticks_quant = max(1, ticks_quant)
        self._count = 0
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._open_time = None

    def push(self, price, ts):
        if self._count == 0:
            # start nowego bara
            self._open = price
            self._high = price
            self._low = price
            self._close = price
            self._open_time = ts
            self._count = 1
            return None

        # aktualizacja bara
        if price > self._high:
            self._high = price
        if price < self._low:
            self._low = price
        self._close = price
        self._count += 1

        if self._count >= self.ticks_quant:
            bar = {
                "open_time": self._open_time,
                "open": self._open,
                "high": self._high,
                "low": self._low,
                "close": self._close,
            }
            self._count = 0
            self._open = None
            self._high = None
            self._low = None
            self._close = None
            self._open_time = None
            return bar

        return None


# ----------------------------------------------------------------------------
# Agregator barow M1 do zarzadzania pozycja (liczony od momentu otwarcia,
# pierwszy - niepelny - bar pomijany).
# ----------------------------------------------------------------------------
class M1Aggregator:
    """
    Buduje bary M1 na bazie ceny bid, startujac liczenie OD momentu podania
    open_anchor_time (czas otwarcia pozycji). Pierwszy bar (ten zawierajacy
    moment otwarcia) jest niepelny i jest pomijany - liczenie startuje od
    kolejnej pelnej minuty.
    """

    def __init__(self, anchor_time):
        # Pierwsza granica bara M1 to anchor_time + 60s (pomijamy biezacy,
        # niepelny bar otwarcia).
        self._bar_start = anchor_time + timedelta(seconds=60)
        self._bar_end = self._bar_start + timedelta(seconds=60)
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._has_data = False

    def push(self, price, ts):
        """
        Podaje kolejny tick (bid, timestamp) do agregatora.
        Zwraca zamknieta swiece M1 (dict) jesli ts przekroczyl granice biezacego
        bara, w przeciwnym razie None.
        Uwaga: tick, ktory nalezy juz do KOLEJNEGO bara, jest buforowany jako
        pierwszy tick nastepnego bara (nie jest tracony).
        """
        closed_bar = None

        # Ticki sprzed startu liczenia (w trakcie pomijanego bara otwarcia)
        # ignorujemy calkowicie.
        if ts < self._bar_start:
            return None

        if ts >= self._bar_end:
            # Zamykamy biezacy bar (jesli mial jakiekolwiek dane).
            if self._has_data:
                closed_bar = {
                    "open_time": self._bar_start,
                    "close_time": self._bar_end,
                    "open": self._open,
                    "high": self._high,
                    "low": self._low,
                    "close": self._close,
                }

            # Przesuwamy granice do bara, w ktorym faktycznie znajduje sie ts.
            while ts >= self._bar_end:
                self._bar_start = self._bar_end
                self._bar_end = self._bar_start + timedelta(seconds=60)

            self._open = price
            self._high = price
            self._low = price
            self._close = price
            self._has_data = True
            return closed_bar

        # Tick nalezy do biezacego bara.
        if not self._has_data:
            self._open = price
            self._high = price
            self._low = price
            self._close = price
            self._has_data = True
        else:
            if price > self._high:
                self._high = price
            if price < self._low:
                self._low = price
            self._close = price

        return None

    def peek_next_open(self):
        """Zwraca cene open biezaco formujacego sie bara (do ceny zamkniecia 'na open kolejnego bara')."""
        return self._open


# ----------------------------------------------------------------------------
# Logika sygnalu wejscia (replika IsSignalBuy / IsSignalSell)
# ----------------------------------------------------------------------------
class SignalCounter:
    """
    Sledzi consecutive zgodne tick-bary. Dziala na zasadzie: kazdy nowy
    zamkniety tick-bar jest porownywany z poprzednim - jesli low rosnie
    (BUY) / high maleje (SELL), licznik rosnie. W przeciwnym razie licznik
    resetuje sie do 1 (biezacy bar staje sie nowym punktem odniesienia).
    """

    def __init__(self, direction, required_count):
        self.direction = direction  # "BUY" lub "SELL"
        self.required_count = required_count
        self._prev_bar = None
        self._streak = 1  # liczba barow w biezacym, zgodnym ciagu

    def update(self, bar):
        """
        Podaje nowo zamkniety tick-bar. Zwraca True, jesli streak osiagnal
        wymagana dlugosc (sygnal wejscia), w przeciwnym razie False.
        """
        if self._prev_bar is None:
            self._prev_bar = bar
            self._streak = 1
            return False

        if self.direction == "BUY":
            aligned = bar["low"] > self._prev_bar["low"]
        else:
            aligned = bar["high"] < self._prev_bar["high"]

        if aligned:
            self._streak += 1
        else:
            self._streak = 1

        self._prev_bar = bar

        return self._streak >= self.required_count


# ----------------------------------------------------------------------------
# Glowna petla backtestu
# ----------------------------------------------------------------------------
def run_backtest(ticks, direction, consecutive_signal, max_spread, ticks_quant):
    tick_agg = TickBarAggregator(ticks_quant)
    signal_counter = SignalCounter(direction, consecutive_signal)

    trades = []
    position = None  # dict z otwarta pozycja, albo None
    m1_agg = None     # M1Aggregator aktywny tylko gdy mamy otwarta pozycje

    for ts, bid, ask in ticks:
        spread = ask - bid

        # --- Zarzadzanie otwarta pozycja (bary M1, decyzja trzymaj/zamknij) ---
        if position is not None:
            closed_m1_bar = m1_agg.push(bid, ts)

            if closed_m1_bar is not None:
                if direction == "BUY":
                    aligned = closed_m1_bar["close"] >= closed_m1_bar["open"]
                else:
                    aligned = closed_m1_bar["close"] <= closed_m1_bar["open"]

                if not aligned:
                    # Przeciwny bar M1 -> zamykamy na cenie OPEN nastepnego bara M1.
                    close_price_raw = m1_agg.peek_next_open()

                    if direction == "BUY":
                        close_price = close_price_raw  # zamkniecie BUY po BID
                    else:
                        # Dla SELL potrzebujemy ask z tego samego ticka co open
                        # kolejnego bara M1. M1Aggregator pracuje na bid, wiec
                        # przyblizamy ask jako bid + biezacy spread tego ticka.
                        close_price = close_price_raw + spread

                    points = _calc_points(position["open_price"], close_price, direction)

                    trades.append({
                        "open_time": position["open_time"],
                        "close_time": ts,
                        "open_price": position["open_price"],
                        "close_price": close_price,
                        "result_points": points,
                    })

                    position = None
                    m1_agg = None

        # --- Sygnal wejscia (tylko gdy brak otwartej pozycji) ---
        # Tick-bary sygnalowe agregujemy NIEZALEZNIE od stanu pozycji, zeby
        # licznik consecutive trwal dalej nawet gdy nie wchodzimy w transakcje.
        closed_signal_bar = tick_agg.push(bid, ts)

        if closed_signal_bar is not None:
            signal_fired = signal_counter.update(closed_signal_bar)

            if signal_fired and position is None:
                if spread <= max_spread:
                    if direction == "BUY":
                        open_price = ask
                    else:
                        open_price = bid

                    position = {
                        "open_time": ts,
                        "open_price": open_price,
                    }
                    m1_agg = M1Aggregator(ts)
                # jesli spread > max_spread: NIE wchodzimy, ale licznik
                # consecutive juz zostal zaktualizowany przez signal_counter.update()
                # powyzej i NIE jest resetowany - kolejny zgodny bar bedzie
                # nadal podnosil streak (signal_fired bedzie znow True przy
                # kazdym kolejnym zgodnym barze, az spread spadnie).

    # Pozycja otwarta na koniec danych - zamykamy na ostatniej dostepnej cenie
    # (na potrzeby przejrzystosci statystyk, oznaczona w logu).
    if position is not None and ticks:
        last_ts, last_bid, last_ask = ticks[-1]
        close_price = last_bid if direction == "BUY" else last_ask
        points = _calc_points(position["open_price"], close_price, direction)
        trades.append({
            "open_time": position["open_time"],
            "close_time": last_ts,
            "open_price": position["open_price"],
            "close_price": close_price,
            "result_points": points,
        })

    return trades


def _calc_points(open_price, close_price, direction):
    if direction == "BUY":
        diff = close_price - open_price
    else:
        diff = open_price - close_price
    return round(diff / POINT)


# ----------------------------------------------------------------------------
# Zapis wynikow
# ----------------------------------------------------------------------------
def save_results(trades, output_dir, direction, consecutive_signal, max_spread, ticks_quant, input_file):
    os.makedirs(output_dir, exist_ok=True)

    trades_path = os.path.join(output_dir, "trades.csv")
    summary_path = os.path.join(output_dir, "summary.txt")

    with open(trades_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["open_time", "close_time", "open_price", "close_price", "result_points"])
        for t in trades:
            writer.writerow([
                t["open_time"].strftime("%Y.%m.%d %H:%M:%S.%f")[:-3],
                t["close_time"].strftime("%Y.%m.%d %H:%M:%S.%f")[:-3],
                f"{t['open_price']:.5f}",
                f"{t['close_price']:.5f}",
                t["result_points"],
            ])

    profit_trades = [t for t in trades if t["result_points"] > 0]
    lost_trades = [t for t in trades if t["result_points"] <= 0]

    profit_total_points = sum(t["result_points"] for t in profit_trades)
    lost_total_points = sum(t["result_points"] for t in lost_trades)
    points_summary = profit_total_points + lost_total_points

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("=== TestAgg - podsumowanie backtestu ===\n")
        f.write(f"Plik wejsciowy:           {input_file}\n")
        f.write(f"Kierunek:                 {direction}\n")
        f.write(f"Consecutive rows signal:  {consecutive_signal}\n")
        f.write(f"Max spread:               {max_spread}\n")
        f.write(f"Ticks quant (sygnal):     {ticks_quant}\n")
        f.write("\n")
        f.write(f"Liczba transakcji ogolem: {len(trades)}\n")
        f.write(f"profit_trades_number:     {len(profit_trades)}\n")
        f.write(f"lost_trades_number:       {len(lost_trades)}\n")
        f.write(f"profit_total_points:      {profit_total_points}\n")
        f.write(f"lost_total_points:        {lost_total_points}\n")
        f.write(f"points_summary:           {points_summary}\n")

    return trades_path, summary_path


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------
def main():
    if len(sys.argv) < 5:
        print("Uzycie: python3 test_agg.py <file_input> <BUY/SELL> <consecutive_rows_signal> <max_spread> [ticks_quant]")
        sys.exit(1)

    file_input = sys.argv[1]
    direction = sys.argv[2].strip().upper()
    consecutive_signal = int(sys.argv[3])
    max_spread = float(sys.argv[4])
    ticks_quant = int(sys.argv[5]) if len(sys.argv) > 5 else 5

    if direction not in ("BUY", "SELL"):
        print("Blad: drugi argument musi byc BUY lub SELL.")
        sys.exit(1)

    if not os.path.isfile(file_input):
        print(f"Blad: plik wejsciowy nie istnieje: {file_input}")
        sys.exit(1)

    print(f"Wczytywanie ticks z: {file_input} ...")
    ticks = load_ticks(file_input)
    print(f"Wczytano {len(ticks)} tickow.")

    if len(ticks) < 2:
        print("Blad: za malo danych do backtestu.")
        sys.exit(1)

    print(f"Uruchamiam backtest: direction={direction}, consecutive_signal={consecutive_signal}, "
          f"max_spread={max_spread}, ticks_quant={ticks_quant}")

    trades = run_backtest(ticks, direction, consecutive_signal, max_spread, ticks_quant)

    output_dir = "TestAgg"
    trades_path, summary_path = save_results(
        trades, output_dir, direction, consecutive_signal, max_spread, ticks_quant, file_input
    )

    print(f"Zapisano {len(trades)} transakcji do: {trades_path}")
    print(f"Podsumowanie zapisano do: {summary_path}")


if __name__ == "__main__":
    main()
