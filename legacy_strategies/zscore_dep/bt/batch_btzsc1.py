# [DEPRECATED] Preserved strictly for historical and educational purposes.

import os
import sys
import pandas as pd
import itertools
import time

# Importujemy logikę z Twojego pliku btzsc1.py
from btzsc1 import load_data, run_backtest, get_instrument, SPREADS


# === CONFIG ===
VERBOSE = True
LOG_EVERY = 50   # progress info co X testów
MIN_TRADES = 40  # odrzucamy szum statystyczny w rankingu


# === GRID ===

def frange(start, stop, step):
    vals = []
    while (step > 0 and start <= stop) or (step < 0 and start >= stop):
        vals.append(round(start, 4))
        start += step
    return vals


def generate_grid(direction):
    if direction == "buy":
        dev_vals = frange(0, -5.0, -0.5)
        ema_vals = frange(0, 5.0, 0.5)
    else:
        dev_vals = frange(0, 5.0, 0.5)
        ema_vals = frange(0, -5.0, -0.5)

    rr_vals = frange(1.0, 5.0, 0.5)

    return list(itertools.product(dev_vals, ema_vals, rr_vals))


# === LOGGING ===

def log(msg):
    if VERBOSE:
        print(msg, flush=True)


def log_test(instrument, direction, dev, ema, rr, win_ratio, trades, expectancy, profitable):
    status = "PROFIT" if profitable else "     "
    print(
        f"[{instrument}] {direction.upper()} | dev={dev:5.2f} | ema={ema:5.2f} | rr={rr:4.2f} "
        f"| tr={trades:4} | win={win_ratio:5.2f}% | exp={expectancy:5.2f} | {status}"
    )


# === CORE ===

def process_instrument(filepath, output_dir):
    instrument = get_instrument(filepath)
    spread = SPREADS.get(instrument, 0)

    log(f"\n=== START {instrument} | spread={spread} ===")

    df_raw = load_data(filepath)

    # 🔥 KLUCZOWA OPTYMALIZACJA: Konwersja DataFrame na listę słowników RAZ na instrument
    # To sprawia, że run_backtest działa błyskawicznie
    df_dict_list = df_raw.to_dict('records')                        # noqa

    results = []
    total_tests = 0
    start_time = time.time()

    for direction in ["buy", "sell"]:
        grid = generate_grid(direction)
        log(f"[{instrument}] Direction: {direction} | tests: {len(grid)}")

        for idx, (dev_thr, ema_thr, rr) in enumerate(grid, 1):
            total_tests += 1

            # run_backtest musi przyjmować listę słowników lub musisz go zmodyfikować w btzsc1.py
            # Jeśli btzsc1.py nadal przyjmuje DataFrame, przekaż mu df_raw,
            # ale upewnij się, że w środku btzsc1.py używasz .to_dict('records')
            trades_df, tp, sl, win_ratio = run_backtest(
                df_raw, direction, dev_thr, ema_thr, rr, spread
            )

            total = tp + sl
            if total == 0:
                continue

            # Inżynierski Tip: Kalkulacja Expectancy
            # Expectancy = (Win_Prob * RRR) - (Loss_Prob * 1)
            win_prob = win_ratio / 100.0
            loss_prob = 1 - win_prob
            expectancy = (win_prob * rr) - (loss_prob * 1)

            breakeven = 100 / (1 + rr)
            profitable = win_ratio > breakeven

            # Logujemy tylko zyskowne, żeby nie spowalniać I/O konsoli
            if profitable:
                log_test(instrument, direction, dev_thr, ema_thr, rr, win_ratio, total, expectancy, profitable)

            results.append({
                "instrument": instrument,
                "direction": direction,
                "dev_atr_threshold": dev_thr,
                "ema_diff_threshold": ema_thr,
                "rrr": rr,
                "trades": total,
                "win_ratio": win_ratio,
                "expectancy": round(expectancy, 4),
                "profitable": profitable
            })

            if idx % LOG_EVERY == 0:
                elapsed = time.time() - start_time
                log(f"[{instrument}] progress: {idx}/{len(grid)} | elapsed: {elapsed:.1f}s")

    if not results:
        return None

    df_res = pd.DataFrame(results)
    full_path = os.path.join(output_dir, f"{instrument}_grid.csv")
    df_res.to_csv(full_path, index=False)

    elapsed_total = time.time() - start_time
    log(f"=== DONE {instrument} | tests={total_tests} | time={elapsed_total:.2f}s ===")

    return df_res


def build_ranking(all_results, output_dir):
    log("\n=== BUILDING GLOBAL RANKING (Min. 40 trades) ===")

    df = pd.concat(all_results, ignore_index=True)

    # Filtracja: tylko zyskowne i z odpowiednią liczbą transakcji
    df = df[(df['profitable']) & (df['trades'] >= MIN_TRADES)].copy()

    if df.empty:
        log("No profitable strategies found meeting criteria.")
        return

    # Sortujemy po Expectancy - to jest "święty graal"
    df = df.sort_values(
        by=['expectancy', 'win_ratio'],
        ascending=[False, False]
    ).reset_index(drop=True)

    df['rank'] = df.index + 1

    final = df[[
        'rank',
        'instrument',
        'direction',
        'dev_atr_threshold',
        'ema_diff_threshold',
        'rrr',
        'trades',
        'expectancy',
        'win_ratio'
    ]]

    output_path = os.path.join(output_dir, "ranking_profitable.csv")
    final.to_csv(output_path, index=False)

    log(f"[OK] ranking saved → {output_path}")
    log(f"\nTOP 10 STRATEGIES BY EXPECTANCY:\n{final.head(10).to_string(index=False)}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 batch_btzsc1.py <directory>")
        sys.exit(1)

    base_dir = sys.argv[1]
    if not os.path.exists(base_dir):
        print(f"[ERROR] Missing directory: {base_dir}")
        sys.exit(1)

    output_dir = os.path.join(base_dir, "batch_results")
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(base_dir) if f.endswith(".csv")]
    log(f"Found {len(files)} files to process.")

    all_results = []
    for f in files:
        filepath = os.path.join(base_dir, f)
        try:
            res = process_instrument(filepath, output_dir)
            if res is not None:
                all_results.append(res)
        except Exception as e:
            print(f"[ERROR] processing {f}: {e}")

    if all_results:
        build_ranking(all_results, output_dir)

    log("\n=== ALL DONE ===")


if __name__ == "__main__":
    main()
