from data import kiteconnect_backfill, fetch_portfolio, history, fetch_instruments
from dependencies import kite_connect
from universaltrader import ut
import pandas as pd
from datetime import datetime
import os
from tabulate import tabulate
from rich.console import Console
from rich.live import Live
from time import sleep

console = Console()


def main():
    start_time = datetime.now()
    print(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Universal Trader - Key Breakout Daily Dow Theory Scanner",
    )
    today = datetime.now().date()
    yesterday = today - pd.Timedelta(days=2)

    kite, status = kite_connect()
    if kite is None:
        print(status)
        return
    error = kiteconnect_backfill(timeframe="D", exchange="NFO", no_of_candles=3)
    if error:
        error_df = pd.DataFrame(error)
        print(tabulate(error_df, headers="keys", tablefmt="psql", showindex=False))

    instruments = fetch_instruments()

    portfolio = fetch_portfolio("NSE-FUT")
    signals = pd.DataFrame()
    for symbol in portfolio["name"].tolist():
        print(
            "Scanning for Key Breakout Daily Dow Theory : ",
            symbol,
            "                \r",
            end="",
        )

        data = history(
            symbol=symbol,
            from_date="2025-11-01",
        )
        data["symbol"] = symbol
        signal = ut.signals2(data, models=["KBDT"])

        if signal.empty:
            continue

        signals = pd.concat([signals, signal], ignore_index=True)

    entry = []
    for index, row in signals.iterrows():
        instrument = instruments[instruments["name"] == row["symbol"]].iloc[0]
        tradingsymbol = instrument["tradingsymbol"]
        instrument_token = str(instrument["instrument_token"])
        lot_size = instrument["lot_size"]

        signals.at[index, "tradingsymbol"] = tradingsymbol
        signals.at[index, "instrument_token"] = instrument_token

        idata = kite.historical_data(
            instrument_token=instrument_token,
            from_date=yesterday.strftime("%Y-%m-%d"),
            to_date=today.strftime("%Y-%m-%d"),
            interval="5minute",
        )

        idf = pd.DataFrame(idata)
        idf["date"] = pd.to_datetime(idf["date"])
        idf = idf.sort_values(by="date")
        filter = idf[idf["date"].dt.date == idf["date"].dt.date.max()]
        idx = max(filter.index[0] - 1, 0)
        idf = idf[idx:]
        idf.reset_index(drop=True, inplace=True)

        for i in range(len(idf) - 1):
            if i < 2:
                continue

            if row["kbdt"] == "buy":
                if (
                    idf.at[i - 1, "close"] >= row["signal_price"]
                    and idf.at[i - 2, "close"] < row["signal_price"]
                ):
                    entry.append(
                        {
                            "datetime": idf.at[i, "date"],
                            "symbol": row["symbol"],
                            "tradingsymbol": tradingsymbol,
                            "instrument_token": instrument_token,
                            "trading_model": "KBDT",
                            "signal": "buy",
                            "signal_price": row["signal_price"],
                            "lot_size": lot_size,
                            "entry_price": idf.at[i - 1, "close"],
                            "stop_loss": idf.at[i - 1, "low"],
                        }
                    )
                    break
            elif row["kbdt"] == "sell":
                if (
                    idf.at[i - 1, "close"] <= row["signal_price"]
                    and idf.at[i - 2, "close"] > row["signal_price"]
                ):
                    entry.append(
                        {
                            "datetime": idf.at[i, "date"],
                            "symbol": row["symbol"],
                            "tradingsymbol": tradingsymbol,
                            "instrument_token": instrument_token,
                            "trading_model": "KBDT",
                            "signal": "sell",
                            "signal_price": row["signal_price"],
                            "lot_size": lot_size,
                            "entry_price": idf.at[i - 1, "close"],
                            "stop_loss": idf.at[i - 1, "high"],
                        }
                    )
                    break
    entry = pd.DataFrame(entry)
    entry = entry.sort_values(by="datetime")
    entry.reset_index(drop=True, inplace=True)
    entry["entry_price"] = entry["entry_price"].round(2)
    # add coma and add decimal as string formatting
    entry["stop_loss"] = entry["stop_loss"].round(2)

    disp = entry.copy()

    disp["entry_price"] = disp["entry_price"].map("{:,.2f}".format)
    disp["stop_loss"] = disp["stop_loss"].map("{:,.2f}".format)
    disp["signal_price"] = disp["signal_price"].map("{:,.2f}".format)
    disp["datetime"] = disp["datetime"].dt.tz_localize(None)
    disp["datetime"] = disp["datetime"].dt.strftime("%Y-%m-%d %H:%M")

    print(
        tabulate(
            disp[
                [
                    "datetime",
                    "symbol",
                    "tradingsymbol",
                    "signal",
                    "lot_size",
                    "signal_price",
                    "entry_price",
                    "stop_loss",
                ]
            ],
            headers="keys",
            tablefmt="psql",
            colalign=(
                "left",
                "left",
                "left",
                "left",
                "right",
                "right",
                "right",
                "right",
            ),
            showindex=False,
        )
    )
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\nTime taken: {duration.seconds} seconds")
    entry.to_csv(
        datetime.now().strftime("signals/kbdt-signals-%Y%m%d.csv"), index=False
    )


def wait_until_next(waiting_minutes=1, seconds=1):
    now = datetime.now()

    next_minute = ((now.minute // waiting_minutes) + 1) * waiting_minutes

    if next_minute >= 60:
        next_run = now.replace(
            hour=(now.hour + 1) % 24,
            minute=0,
            second=seconds,
            microsecond=0,
        )
    else:
        next_run = now.replace(
            minute=next_minute,
            second=seconds,
            microsecond=0,
        )

    wait_seconds = int((next_run - now).total_seconds())

    if wait_seconds <= 0:
        return

    try:
        with Live(console=console, refresh_per_second=4) as live:
            for remaining in range(wait_seconds, 0, -1):
                mins, secs = divmod(remaining, 60)
                live.update(f"⏳ Sleeping... {mins:02d}m {secs:02d}s remaining")
                sleep(1)

    except KeyboardInterrupt:
        console.print("\n⛔️ Interrupted by user.")
        raise SystemExit(0)

    console.print("✅ Woke up for next run!\n")


if __name__ == "__main__":

    while True:
        os.system("cls" if os.name == "nt" else "clear")

        main()

        wait_until_next(waiting_minutes=5, seconds=0)
