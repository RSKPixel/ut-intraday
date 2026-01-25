import pandas as pd
from dependencies import engine
from psycopg2.extras import execute_values
from sqlalchemy.sql import text
import numpy as np


def main():

    sql = text("SELECT * FROM tfw_signals ORDER BY datetime ASC")

    df = pd.read_sql_query(sql, engine)
    df["entry_date"] = pd.to_datetime(df["datetime"]).dt.date
    df["close_0"] = np.nan
    df["close_1"] = np.nan
    df["close_2"] = np.nan
    df["close_3"] = np.nan
    df["close_4"] = np.nan

    for index, row in df.iterrows():
        sql = text(
            """
            SELECT * FROM tfw_eod WHERE symbol = :symbol AND DATE(datetime AT TIME ZONE 'Asia/Kolkata') >= :entry_date ORDER BY datetime ASC
            """
        )
        df_eod = pd.read_sql_query(
            sql,
            engine,
            params={"symbol": row["symbol"], "entry_date": row["entry_date"]},
        )
        df_eod = df_eod[:5]

        try:
            df.at[index, "close_0"] = df_eod["close"].iloc[0]
            df.at[index, "close_1"] = df_eod["close"].iloc[1]
            df.at[index, "close_2"] = df_eod["close"].iloc[2]
            df.at[index, "close_3"] = df_eod["close"].iloc[3]
            df.at[index, "close_4"] = df_eod["close"].iloc[4]
        except IndexError:
            pass

    df["pnl_0"] = np.where(
        df["signal"] == "buy",
        (df["close_0"] - df["signal_price"]) * df["lot_size"],
        (df["signal_price"] - df["close_0"]) * df["lot_size"],
    )

    df["pnl_1"] = np.where(
        df["signal"] == "buy",
        (df["close_1"] - df["signal_price"]) * df["lot_size"],
        (df["signal_price"] - df["close_1"]) * df["lot_size"],
    )

    df["pnl_2"] = np.where(
        df["signal"] == "buy",
        (df["close_2"] - df["signal_price"]) * df["lot_size"],
        (df["signal_price"] - df["close_2"]) * df["lot_size"],
    )

    df["pnl_3"] = np.where(
        df["signal"] == "buy",
        (df["close_3"] - df["signal_price"]) * df["lot_size"],
        (df["signal_price"] - df["close_3"]) * df["lot_size"],
    )

    df["pnl_4"] = np.where(
        df["signal"] == "buy",
        (df["close_4"] - df["signal_price"]) * df["lot_size"],
        (df["signal_price"] - df["close_4"]) * df["lot_size"],
    )

    # group by portfolio and calculate total pnl
    df_summary = df.groupby("entry_date").agg(
        total_pnl_0=pd.NamedAgg(column="pnl_0", aggfunc="sum"),
        total_pnl_1=pd.NamedAgg(column="pnl_1", aggfunc="sum"),
        total_pnl_2=pd.NamedAgg(column="pnl_2", aggfunc="sum"),
        total_pnl_3=pd.NamedAgg(column="pnl_3", aggfunc="sum"),
        total_pnl_4=pd.NamedAgg(column="pnl_4", aggfunc="sum"),
    )

    print(df_summary.to_clipboard())


if __name__ == "__main__":
    main()
