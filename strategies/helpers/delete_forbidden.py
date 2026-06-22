# delete_forbidden.py

"""
Forbidden Financial Symbols Directory Cleaner

Description:
    Scans a specified root directory and automatically deletes any subdirectories
    whose names match a predefined blacklist of forbidden financial tickers/symbols
    (e.g., specific Forex exotics, crypto pairs, stock indices, or commodities).

Usage:
    python3 delete_forbidden.py <main_directory>

Arguments:
    <main_directory> : The parent directory containing the subfolders to be audited.

Important Logic & Behavior:
    1. It performs a shallow scan (1st level deep) within the provided <main_directory>.
    2. It strictly checks folder names against the 'forbiddenSymbolsAdmirals' set.
    3. Matching subdirectories are permanently deleted along with all their contents
       using 'shutil.rmtree'. Proceed with caution.
    4. Folders with names not present in the blacklist remain untouched.

Blacklist Coverage:
    - Exotic Forex pairs (e.g., USDZAR, EURPLN, USDHUF)
    - Cryptocurrencies (e.g., BTCUSD, ETHUSD)
    - Major Global Stock Indices (e.g., [DJI30], [FTSE100])
    - Commodities & Energies (e.g., BRENT, CRUDOIL, SILVER)
"""

import os
import sys
import shutil

forbiddenSymbolsAdmirals = {
    "USDZAR-Z", "USDZAR", "USDUAH-Z", "USDUAH", "USDTHB", "USDSGD", "USDSEK",
    "USDRUB", "EURRUB", "USDRON-Z", "USDPEN", "USDNOK", "USDMXN-Z", "USDMXN",
    "USDJOD-Z", "USDJOD", "USDHUF", "USDHRK-Z", "USDHRK", "USDHKD", "USDDKK-Z",
    "USDCNH-Z", "USDCLP-Z", "USDCLP", "USDBRL-Z", "USDBRL", "USDBGN-Z", "USDBGN",
    "USDAED-Z", "USDAED", "I.USDX", "I.EURX", "GLDUSD", "GBXUSD", "GBPHKD",
    "EURRON", "EURHKD", "BTCEUR", "BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD",
    "BCHUSD", "LTCEUR",

    # EGZOTICS
    "EURCZK", "EURHUF", "EURNOK", "EURPLN", "EURRUB", "EURSEK", "GBPPLN",
    "GBPSGD", "NZDSGD", "USDCNH", "USDCZK", "USDHKD", "USDHUF", "USDNOK",
    "USDPLN", "USDRON", "USDRUB", "USDSEK", "USDSGD",

    # INDICES
    "STXE50", "[CAC40]", "[IBEX35]", "[SMI20]", "[HSI50]", "[JP225]",
    "[AEX25]", "[OBX25]", "[FTSE100]", "[DJI30]",
    "GER.TEC30", "GER.MID50", "[ASX200]",

    # CMD
    "BRENT", "CRUDOIL", "NGAS", "PALLADIUM", "PLATINUM", "SILVER"
}


def delete_forbidden(main_dir):
    if not os.path.isdir(main_dir):
        print(f"Błąd: {main_dir} nie jest katalogiem")
        return

    for item in os.listdir(main_dir):
        item_path = os.path.join(main_dir, item)

        if os.path.isdir(item_path) and item in forbiddenSymbolsAdmirals:
            print(f"Usuwam: {item_path}")
            shutil.rmtree(item_path)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Użycie: python delete_forbidden.py <main_directory>")
        sys.exit(1)

    main_directory = sys.argv[1]
    delete_forbidden(main_directory)
