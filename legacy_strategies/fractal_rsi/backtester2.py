import pandas as pd
# import numpy as np
import os
import sys
import itertools
from datetime import datetime


# ---------------------------------------------------------------------------
# Core backtest — identyczna logika jak oryginał, bez zmian w sygnałach
# ---------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame, rsi_below: float, rr_ratio: float) -> dict:
    trades = []
    in_position = False
    entry_price = sl = tp = open_time = None

    for i in range(2, len(df)):
        r0, r1, r2 = df.iloc[i], df.iloc[i - 1], df.iloc[i - 2]

        if not in_position:
            if not (r0['UP_H1_H1'] and r0['UP_D1_D1']):
                continue
            if pd.isna(r2['RSI_M5_M5']) or pd.isna(r1['RSI_M5_M5']):
                continue
            if not (r2['RSI_M5_M5'] < rsi_below and r1['RSI_M5_M5'] > r2['RSI_M5_M5']):
                continue
            if pd.isna(r1['last_pivot_H1']):
                continue

            entry_price = r0['open_M5']
            sl = r1['last_pivot_H1']
            if sl >= entry_price:
                continue

            risk = entry_price - sl
            tp = entry_price + risk * rr_ratio
            open_time = r0['timestamp']
            in_position = True

        else:
            hit_sl = r0['low_M5'] <= sl
            hit_tp = r0['high_M5'] >= tp

            if hit_sl or hit_tp:
                won = hit_tp and not hit_sl   # SL ma pierwszeństwo (konserwatywnie)
                trades.append({
                    'open_time':   open_time,
                    'close_time':  r0['timestamp'],
                    'open_price':  entry_price,
                    'close_price': tp if won else sl,
                    'sl': sl, 'tp': tp,
                    'result':      'TP' if won else 'SL',
                    'pnl_r':       rr_ratio if won else -1.0,
                })
                in_position = False

    if not trades:
        return {'trades': pd.DataFrame(), 'tp': 0, 'sl': 0, 'wr': 0.0, 'expectancy': 0.0, 'total': 0}

    df_t = pd.DataFrame(trades)
    tp_n = (df_t['result'] == 'TP').sum()
    sl_n = (df_t['result'] == 'SL').sum()
    total = tp_n + sl_n
    wr = tp_n / total if total else 0.0
    expectancy = df_t['pnl_r'].mean()

    return {
        'trades':      df_t,
        'tp':          int(tp_n),
        'sl':          int(sl_n),
        'total':       int(total),
        'wr':          round(wr * 100, 2),
        'expectancy':  round(expectancy, 4),
    }


# ---------------------------------------------------------------------------
# Grid search — przeszukuje wszystkie kombinacje parametrów
# ---------------------------------------------------------------------------

def grid_search(df: pd.DataFrame, rsi_values: list, rr_values: list) -> pd.DataFrame:
    rows = []
    for rsi, rr in itertools.product(rsi_values, rr_values):
        res = run_backtest(df, rsi, rr)
        rows.append({
            'rsi_below':   rsi,
            'rr_ratio':    rr,
            'total':       res['total'],
            'tp':          res['tp'],
            'sl':          res['sl'],
            'wr_%':        res['wr'],
            'expectancy_r': res['expectancy'],
        })
    return pd.DataFrame(rows).sort_values('expectancy_r', ascending=False)


# ---------------------------------------------------------------------------
# Filtrowanie złych warunków (korelacje, night trading itp.)
# Dodaj tu własne filtry — każda funkcja przyjmuje df i zwraca maskę bool
# ---------------------------------------------------------------------------

def filter_night_sessions(df: pd.DataFrame, night_start: int = 22, night_end: int = 6) -> pd.DataFrame:
    """Usuwa świece z sesji nocnej (np. 22:00–06:00 UTC)."""
    hour = pd.to_datetime(df['timestamp']).dt.hour
    mask = ~((hour >= night_start) | (hour < night_end))
    return df[mask].reset_index(drop=True)


def apply_filters(df: pd.DataFrame, filters: list) -> pd.DataFrame:
    """Aplikuje listę funkcji-filtrów po kolei."""
    for f in filters:
        df = f(df)
    return df


# ---------------------------------------------------------------------------
# Raport HTML — wykres equity + heatmapa grid search
# ---------------------------------------------------------------------------

def _equity_chart_data(trades_df: pd.DataFrame) -> str:
    if trades_df.empty:
        return '[], []'
    cumulative = trades_df['pnl_r'].cumsum().tolist()
    labels = [str(t)[:10] for t in trades_df['close_time'].tolist()]
    return str(labels), str(cumulative)


def build_html_report(
    symbol: str,
    best_result: dict,
    grid_df: pd.DataFrame,
    output_path: str,
) -> None:
    trades = best_result['trades']
    labels, equity = _equity_chart_data(trades)

    # Heatmapa: pivot WR po RSI × RR
    if not grid_df.empty:
        pivot_wr = grid_df.pivot_table(index='rsi_below', columns='rr_ratio', values='wr_%')
        pivot_exp = grid_df.pivot_table(index='rsi_below', columns='rr_ratio', values='expectancy_r')
        wr_html = pivot_wr.to_html(classes='htable', float_format='%.1f')
        exp_html = pivot_exp.to_html(classes='htable', float_format='%.4f')
    else:
        wr_html = exp_html = '<p>Brak danych</p>'

    best_rsi = grid_df.iloc[0]['rsi_below'] if not grid_df.empty else '—'
    best_rr = grid_df.iloc[0]['rr_ratio'] if not grid_df.empty else '—'

    html = f"""<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="utf-8">
<title>Backtest — {symbol}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; font-weight: 500; margin-bottom: 0.5rem; }}
  .meta {{ font-size: 0.85rem; color: #666; margin-bottom: 2rem; }}
  .cards {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 2rem; }}
  .card {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: 1rem 1.5rem; min-width: 130px; }}
  .card .val {{ font-size: 1.6rem; font-weight: 500; }}
  .card .lbl {{ font-size: 0.75rem; color: #888; margin-top: 2px; }}
  .card.green .val {{ color: #2d6a0f; }}
  .card.red .val   {{ color: #991f1f; }}
  .card.blue .val  {{ color: #1a4d8f; }}
  .section {{ font-size: 0.8rem; color: #888; font-weight: 500; margin: 2rem 0 0.5rem; text-transform: uppercase; letter-spacing: 0.05em; }}
  .chart-wrap {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: 1rem; margin-bottom: 1.5rem; }}
  canvas {{ max-height: 300px; }}
  .htable {{ border-collapse: collapse; font-size: 0.85rem; }}
  .htable th, .htable td {{ border: 1px solid #e5e5e5; padding: 6px 12px; text-align: right; }}
  .htable th {{ background: #f5f5f5; font-weight: 500; }}
  .grid-tables {{ display: flex; gap: 2rem; flex-wrap: wrap; }}
  .grid-tables > div {{ background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: 1rem; overflow-x: auto; }}
  .trades-wrap {{ overflow-x: auto; }}
  table.trades {{ border-collapse: collapse; font-size: 0.8rem; width: 100%; }}
  table.trades th, table.trades td {{ border: 1px solid #e5e5e5; padding: 5px 10px; white-space: nowrap; }}
  table.trades thead {{ background: #f5f5f5; }}
  .tp {{ color: #2d6a0f; }} .sl {{ color: #991f1f; }}
</style>
</head>
<body>
<h1>Backtest — {symbol}</h1>
<div class="meta">Wygenerowano: {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;|&nbsp;
Najlepsze parametry: RSI &lt; {best_rsi}, RR = {best_rr}</div>

<div class="cards">
  <div class="card blue"><div class="val">{best_result['total']}</div><div class="lbl">Transakcji łącznie</div></div>
  <div class="card green"><div class="val">{best_result['wr']}%</div><div class="lbl">Win Rate</div></div>
  <div class="card {'green' if best_result['expectancy'] > 0 else 'red'}">
    <div class="val">{best_result['expectancy']:+.4f}R</div><div class="lbl">Expectancy / tr.</div></div>
  <div class="card green"><div class="val">{best_result['tp']}</div><div class="lbl">TP</div></div>
  <div class="card red"><div class="val">{best_result['sl']}</div><div class="lbl">SL</div></div>
</div>

<div class="section">Equity curve (kumulatywne R)</div>
<div class="chart-wrap">
  <canvas id="equity"></canvas>
</div>

<div class="section">Grid search — Win Rate (%) [RSI × RR]</div>
<div class="grid-tables">
  <div><strong>Win Rate (%)</strong><br><br>{wr_html}</div>
  <div><strong>Expectancy (R)</strong><br><br>{exp_html}</div>
</div>

<div class="section">Lista transakcji (najlepsze parametry)</div>
<div class="trades-wrap">
{_trades_table(trades)}
</div>

<script>
const labels = {labels};
const equity = {equity};
new Chart(document.getElementById('equity'), {{
  type: 'line',
  data: {{
    labels: labels,
    datasets: [{{
      data: equity,
      borderColor: equity.length && equity[equity.length-1] >= 0 ? '#2d6a0f' : '#991f1f',
      backgroundColor: 'rgba(45,106,15,0.06)',
      borderWidth: 2, pointRadius: 0, fill: true, tension: 0.3
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ maxTicksLimit: 12, font: {{ size: 11 }} }}, grid: {{ display: false }} }},
      y: {{ title: {{ display: true, text: 'Skumulowane R', font: {{ size: 11 }} }},
            ticks: {{ font: {{ size: 11 }} }} }}
    }}
  }}
}});
</script>
</body>
</html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


def _trades_table(trades_df: pd.DataFrame) -> str:
    if trades_df.empty:
        return '<p>Brak transakcji.</p>'
    rows = []
    for _, t in trades_df.iterrows():
        cls = 'tp' if t['result'] == 'TP' else 'sl'
        rows.append(
            f"<tr class='{cls}'>"
            f"<td>{str(t['open_time'])[:16]}</td>"
            f"<td>{str(t['close_time'])[:16]}</td>"
            f"<td>{t['open_price']:.5f}</td>"
            f"<td>{t['sl']:.5f}</td>"
            f"<td>{t['tp']:.5f}</td>"
            f"<td>{t['close_price']:.5f}</td>"
            f"<td><strong>{t['result']}</strong></td>"
            f"<td>{t['pnl_r']:+.2f}R</td>"
            f"</tr>"
        )
    header = ("<table class='trades'><thead><tr>"
              "<th>Open</th><th>Close</th><th>Entry</th>"
              "<th>SL</th><th>TP</th><th>Close price</th>"
              "<th>Result</th><th>P&L (R)</th>"
              "</tr></thead><tbody>")
    return header + ''.join(rows) + "</tbody></table>"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(input_dir: str):
    # --- Parametry do przeszukania ---
    RSI_VALUES = [25, 30, 35, 40, 45]
    RR_VALUES = [0.8, 0.9, 1.0, 1.1, 1.2]

    # --- Filtry do zastosowania (dodaj/usuń wg potrzeb) ---
    FILTERS = [
        filter_night_sessions,
        # filter_high_correlation,   # dodaj własne
    ]

    output_dir = os.path.join(input_dir, 'backtest_results')
    os.makedirs(output_dir, exist_ok=True)

    summary_rows = []

    for fname in sorted(f for f in os.listdir(input_dir) if f.endswith('.csv')):
        symbol = fname.replace('.csv', '')
        print(f'\n=== {symbol} ===')

        df = pd.read_csv(os.path.join(input_dir, fname), sep=';')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['RSI_M5_M5'] = pd.to_numeric(df['RSI_M5_M5'], errors='coerce')
        df['last_pivot_H1'] = pd.to_numeric(df['last_pivot_H1'], errors='coerce')

        # Zastosuj filtry
        df_filtered = apply_filters(df, FILTERS)
        print(f'  Świece po filtrach: {len(df_filtered)} (było {len(df)})')

        # Grid search
        grid_df = grid_search(df_filtered, RSI_VALUES, RR_VALUES)
        grid_csv = os.path.join(output_dir, f'{symbol}_grid.csv')
        grid_df.to_csv(grid_csv, index=False)

        # Najlepsza konfiguracja
        if grid_df.empty or grid_df.iloc[0]['total'] < 10:
            print(f'  Za mało transakcji — pomijam.')
            continue

        best = grid_df.iloc[0]
        best_result = run_backtest(df_filtered, best['rsi_below'], best['rr_ratio'])

        print(f'  Najlepsze: RSI<{best["rsi_below"]}, RR={best["rr_ratio"]} '
              f'→ {best_result["total"]} tr., WR={best_result["wr"]}%, '
              f'E={best_result["expectancy"]:+.4f}R')

        # Raport HTML
        html_path = os.path.join(output_dir, f'{symbol}_report.html')
        build_html_report(symbol, best_result, grid_df, html_path)
        print(f'  Raport: {html_path}')

        summary_rows.append({
            'symbol':        symbol,
            'best_rsi':      best['rsi_below'],
            'best_rr':       best['rr_ratio'],
            'total':         best_result['total'],
            'wr_%':          best_result['wr'],
            'expectancy_r':  best_result['expectancy'],
        })

    # Zbiorczy summary
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows).sort_values('expectancy_r', ascending=False)
        summary_path = os.path.join(output_dir, '_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        print(f'\n=== SUMMARY ===')
        print(summary_df.to_string(index=False))
        print(f'\nZapisano: {summary_path}')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Użycie: python3 backtester_v2.py <directory>')
        sys.exit(1)
    main(sys.argv[1])
