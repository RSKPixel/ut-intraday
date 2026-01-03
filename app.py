from data import kiteconnect_backfill, fetch_portfolio, history, fetch_instruments
from universaltrader import ut
import pandas as pd
from datetime import datetime
import os


def main():
    print(
        "Universal Trader - KiteConnect Backfilling Data ",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    # kiteconnect_backfill(timeframe="D", exchange="NSE-FUT", no_of_candles=7)
    instruments = fetch_instruments()

    portfolio = fetch_portfolio("NSE-FUT")
    signals = pd.DataFrame()
    for symbol in portfolio["name"].tolist():
        print("Universal Trader Data: ", symbol, "                \r", end="")

        data = history(
            symbol=symbol,
            from_date="2025-11-01",
            to_date="2026-01-02",
        )
        data["symbol"] = symbol
        signal = ut.signals2(data, models=["KBDT"])

        if signal.empty:
            continue

        signals = pd.concat([signals, signal], ignore_index=True)

    for index, row in signals.iterrows():
        match = instruments.loc[instruments["name"] == row["symbol"], "tradingsymbol"]
        if not match.empty:
            signals.at[index, "tradingsymbol"] = match.iloc[0]
    signals = signals[
        ["datetime", "tradingsymbol", "symbol", "kbdt", "signal_price", "signal"]
    ]
    print(signals)


if __name__ == "__main__":
    # clear the console
    os.system("cls" if os.name == "nt" else "clear")
    main()
