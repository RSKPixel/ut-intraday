from dependencies import kite_connect
from dependencies import engine
from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import numpy as np
from sqlalchemy.sql import text
from universaltrader.ssc import SwingPoints2
from universaltrader.tools import ddt2, lv, atr, sma, asc, weekly_rdata


def fetch_portfolio(portfolio: str = "NFO") -> pd.DataFrame:
    sql = f"""
            SELECT * FROM tfw_portfolio WHERE portfolio = :portfolio ORDER BY name;
        """
    sql = text(sql)

    df = pd.read_sql_query(
        sql,
        engine,
        params={"portfolio": portfolio},
    )
    return df


def history(symbol: str, from_date: str, to_date: str = None) -> pd.DataFrame:

    symbol = symbol.upper()
    from_date = from_date if from_date else "2023-01-01"
    to_date = to_date if to_date else datetime.now().strftime("%Y-%m-%d")

    sql = f"""
            SELECT datetime AT TIME ZONE 'Asia/Kolkata' AS local_time, *
            FROM tfw_eod
            WHERE
                symbol = :symbol and
                DATE(datetime AT TIME ZONE 'Asia/Kolkata') >= :from_date and
                DATE(datetime AT TIME ZONE 'Asia/Kolkata') <= :to_date

            ORDER BY datetime ASC;
        """
    sql = text(sql)

    df = pd.read_sql_query(
        sql,
        engine,
        params={"symbol": symbol, "from_date": from_date, "to_date": to_date},
    )
    df.drop(columns=["datetime"], inplace=True)
    df.rename(columns={"local_time": "date"}, inplace=True)

    df.set_index("date", inplace=True)
    df = SwingPoints2(df)
    df = ddt2(df)

    df["mvf"] = (
        (asc(df["close"], lookback=20) - df["low"])
        / asc(df["close"], lookback=20)
        * 100
    )
    df["ldv"] = lv(df["high"], df["low"], df["close"], lookback=4)
    df["atr"] = atr(df["high"], df["low"], df["close"], lookback=20)
    df["sma200"] = sma(df["close"], lookback=200)
    df = weekly_rdata(df)
    df["time"] = df.index
    df["datetime"] = df.index

    df = df.where(pd.notnull(df), None)
    df["time"] = df["time"].dt.strftime("%Y-%m-%d")
    # df = df[["time", "open", "high", "low", "close"]]
    df = df[
        [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "bar_type",
            "swing_high",
            "swing_low",
            "swing_point",
            "direction",
            "intersection",
            "dow_cross",
            "dow_point",
            "mvf",
            "ldv",
            "lwv",
            "atr",
            "sma200",
        ]
    ]

    return df


def fetch_instruments() -> pd.DataFrame:
    sql = """
        WITH ordered AS (
            SELECT
                instrument_token,
                exchange_token,
                tradingsymbol,
                name,
                expiry,
                lot_size,
                tick_size,
                instrument_type,
                segment,
                exchange,
                LAG(tradingsymbol) OVER (
                    PARTITION BY name
                    ORDER BY expiry
                ) AS prev_symbol,
                LAG(expiry) OVER (
                    PARTITION BY name
                    ORDER BY expiry
                ) AS prev_expiry
            FROM tfw_instruments
        )
        SELECT
            name,
            instrument_token      AS instrument_token,
            exchange_token        AS exchange_token,
            tradingsymbol         AS tradingsymbol,
            expiry                AS expiry,
            lot_size              AS lot_size,
            tick_size             AS tick_size,
            instrument_type       AS instrument_type,
            segment               AS segment,
            exchange              AS exchange,
            prev_symbol           AS previous_tradingsymbol,
            prev_expiry           AS previous_expiry
        FROM ordered o
        WHERE expiry = (
            SELECT MIN(expiry)
            FROM tfw_instruments t
            WHERE t.name = o.name
            AND t.expiry >= CURRENT_DATE
        )
        ORDER BY name;
    """
    with engine.connect() as conn:
        instruments = pd.read_sql(sql, conn)

    instruments["previous_expiry"] = pd.to_datetime(
        np.where(
            instruments["previous_expiry"].isnull(),
            datetime(day=30, month=11, year=2025),
            instruments["previous_expiry"],
        )
    )
    return instruments


def kiteconnect_backfill(
    timeframe: str, exchange: str = "NFO", no_of_candles: int = 10
):
    kite, status = kite_connect()
    if kite is None:
        return status

    live_instruments = pd.DataFrame(fetch_instruments())
    live_instruments = live_instruments[(live_instruments["exchange"] == exchange)]

    if no_of_candles > 2000:
        no_of_candles = 2000

    from_date = (datetime.now() - timedelta(days=no_of_candles)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    conn = engine.raw_connection()
    error = []
    for index, row in live_instruments.iterrows():
        symbol = row["tradingsymbol"]
        print("Backfilling: ", symbol, "                \r", end="")
        instrument_token = row["instrument_token"]

        try:
            data = kite.historical_data(
                instrument_token,
                from_date=from_date,
                to_date=to_date,
                continuous=True,
                oi=True,
                interval="day",
            )
        except Exception as e:
            error.append(f"Error fetching data for {symbol}: {e}")
            print(f"Error fetching data for {symbol}: {e}")
            continue

        if not data:
            error.append(f"No data returned for {symbol}")
            print(f"No data returned for {symbol}")
            continue

        df = pd.DataFrame(data)
        df["datetime"] = pd.to_datetime(df["date"])
        df["symbol"] = row["name"]
        df.drop(columns=["date"], inplace=True)
        df = df[["symbol", "datetime", "open", "high", "low", "close", "volume", "oi"]]
        records = df.to_numpy().tolist()

        sql = """
            INSERT INTO tfw_eod (
                symbol,
                datetime,
                open,
                high,
                low,
                close,
                volume,
                oi
            )
            VALUES %s
            ON CONFLICT (datetime, symbol)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi;
        """

        with conn.cursor() as cursor:
            execute_values(cursor, sql, records)
            conn.commit()

    conn.close()

    return error
