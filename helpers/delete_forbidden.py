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
    "[AEX25]", "[OBX25]", "[FTSE100]", "[DJI30]", "GERMANY40",
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
