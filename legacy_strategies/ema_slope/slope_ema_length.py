# slope_ema_length.py
# !/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
slope_ema_length.py

Analiza długości epizodów regime.

Wejście:
    python3 slope_ema_length.py <file> <gridsize>

Przykład:
    python3 slope_ema_length.py EURUSD_M5.csv 5

Program:
- analizuje kolejne epizody regime
- zapisuje klasyfikacje duration
- zapisuje klasyfikacje grid

Dla każdego regime powstają 2 pliki:
- bull_up_duration.csv
- bull_up_grid.csv
- itd.

Łącznie:
12 plików CSV.
"""

import os
import sys
import pandas as pd


# ============================================================
# CONSTANTS
# ============================================================

REGIMES = [
    'BULL_UP',
    'BULL_DOWN',
    'BULL_RANGE',
    'BEAR_UP',
    'BEAR_DOWN',
    'BEAR_RANGE'
]


# ============================================================
# LOAD CSV
# ============================================================


def load_csv(path):

    df = pd.read_csv(path, sep=';')

    df['timestamp'] = pd.to_datetime(
        df['timestamp'],
        format='%Y.%m.%d %H:%M:%S'
    )

    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


# ============================================================
# EXTRACT EPISODES
# ============================================================


def extract_episodes(df, regime_name):

    regime_df = df[df['regime'] == regime_name].copy()

    if regime_df.empty:
        return []

    episodes = []

    start_idx = None

    for i in range(len(df)):

        current_regime = df.iloc[i]['regime']

        # ----------------------------------------------------
        # START EPISODE
        # ----------------------------------------------------

        if current_regime == regime_name:

            if start_idx is None:
                start_idx = i

        # ----------------------------------------------------
        # END EPISODE
        # ----------------------------------------------------

        else:

            if start_idx is not None:

                end_idx = i - 1

                episode_df = df.iloc[start_idx:end_idx + 1]

                episodes.append({
                    'start': episode_df.iloc[0]['timestamp'],
                    'end': episode_df.iloc[-1]['timestamp'],
                    'duration': len(episode_df),
                    'maximum_ema_deviation': episode_df['ema_dev'].max(),
                    'minimum_ema_deviation': episode_df['ema_dev'].min()
                })

                start_idx = None

    # --------------------------------------------------------
    # HANDLE LAST EPISODE
    # --------------------------------------------------------

    if start_idx is not None:

        episode_df = df.iloc[start_idx:]

        episodes.append({
            'start': episode_df.iloc[0]['timestamp'],
            'end': episode_df.iloc[-1]['timestamp'],
            'duration': len(episode_df),
            'maximum_ema_deviation': episode_df['ema_dev'].max(),
            'minimum_ema_deviation': episode_df['ema_dev'].min()
        })

    return episodes


# ============================================================
# SAVE DURATION CLASSIFICATION
# ============================================================


def save_duration_csv(episodes, output_path):

    if not episodes:

        empty_df = pd.DataFrame(columns=[
            'start',
            'end',
            'duration',
            'maximum_ema_deviation',
            'minimum_ema_deviation'
        ])

        empty_df.to_csv(output_path, sep=';', index=False)
        return

    duration_df = pd.DataFrame(episodes)

    duration_df = duration_df.sort_values(
        'duration',
        ascending=False
    ).reset_index(drop=True)

    duration_df.to_csv(
        output_path,
        sep=';',
        index=False,
        float_format='%.8f'
    )


# ============================================================
# BUILD GRID CLASSIFICATION
# ============================================================


def build_grid_classification(episodes, grid_size):

    if not episodes:

        return pd.DataFrame(columns=[
            'range_start',
            'range_end',
            'episodes_count'
        ])

    durations = [ep['duration'] for ep in episodes]

    max_duration = max(durations)

    rows = []

    current_start = 1

    while current_start <= max_duration:

        current_end = current_start + grid_size - 1

        count = sum(
            current_start <= d <= current_end
            for d in durations
        )

        rows.append({
            'range_start': current_start,
            'range_end': current_end,
            'episodes_count': count
        })

        current_start += grid_size

    return pd.DataFrame(rows)


# ============================================================
# SAVE GRID CSV
# ============================================================


def save_grid_csv(grid_df, output_path):

    grid_df.to_csv(
        output_path,
        sep=';',
        index=False
    )


# ============================================================
# MAIN
# ============================================================


def main():

    if len(sys.argv) != 3:

        print(
            'Usage:\n'
            'python3 slope_ema_length.py <file> <gridsize>'
        )

        sys.exit(1)

    input_file = sys.argv[1]
    grid_size = int(sys.argv[2])

    if not os.path.isfile(input_file):

        print(f'[ERROR] File does not exist: {input_file}')
        sys.exit(1)

    if grid_size <= 0:

        print('[ERROR] gridsize must be > 0')
        sys.exit(1)

    # --------------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------------

    df = load_csv(input_file)

    # --------------------------------------------------------
    # OUTPUT DIRECTORY
    # --------------------------------------------------------

    base_name = os.path.splitext(
        os.path.basename(input_file)
    )[0]

    output_dir = f'{base_name}_length_analysis'

    os.makedirs(output_dir, exist_ok=True)

    print(f'[INFO] Output directory: {output_dir}')

    # --------------------------------------------------------
    # PROCESS EACH REGIME
    # --------------------------------------------------------

    for regime in REGIMES:

        print(f'[INFO] Processing {regime}')

        episodes = extract_episodes(df, regime)

        regime_lower = regime.lower()

        # ----------------------------------------------------
        # DURATION CSV
        # ----------------------------------------------------

        duration_output = os.path.join(
            output_dir,
            f'{regime_lower}_duration.csv'
        )

        save_duration_csv(
            episodes,
            duration_output
        )

        # ----------------------------------------------------
        # GRID CSV
        # ----------------------------------------------------

        grid_df = build_grid_classification(
            episodes,
            grid_size
        )

        grid_output = os.path.join(
            output_dir,
            f'{regime_lower}_grid.csv'
        )

        save_grid_csv(
            grid_df,
            grid_output
        )

    print('\n[DONE] Analysis complete.')


# ============================================================
# ENTRY
# ============================================================


if __name__ == '__main__':
    main()
