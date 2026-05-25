import sys
import pandas as pd


def detect_pivots(df):
    # Warunek swing low (fractal 5 świec)
    cond = (
        (df['low'] < df['low'].shift(1)) &
        (df['low'] < df['low'].shift(2)) &
        (df['low'] < df['low'].shift(-1)) &
        (df['low'] < df['low'].shift(-2))
    )

    # Pivot low (ale przesunięty o 2 świece w przyszłość!)
    df['pivot_low'] = df['low'].where(cond)
    df['pivot_low'] = df['pivot_low'].shift(2)

    # Forward fill → ostatni pivot
    df['last_pivot'] = df['pivot_low'].ffill()

    return df


def main():
    if len(sys.argv) < 2:
        print("Usage: python detect_pivots.py <file.csv>")
        sys.exit(1)

    file_path = sys.argv[1]

    # Wczytanie danych
    df = pd.read_csv(file_path, sep=';')

    # Konwersja timestamp (opcjonalna)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Sortowanie (na wszelki wypadek)
    df = df.sort_values('timestamp').reset_index(drop=True)

    # Detekcja pivotów
    df = detect_pivots(df)

    # Zapis
    output_file = file_path.replace('.csv', '_with_pivots.csv')
    df.to_csv(output_file, sep=';', index=False)

    print(f"Saved: {output_file}")


if __name__ == "__main__":
    main()
