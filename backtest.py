import pandas as pd
from dependencies import engine
from psycopg2.extras import execute_values
from sqlalchemy.sql import text
import numpy as np


def main():

    sql = text("SELECT * FROM tfw_signals ORDER BY datetime ASC")

    df = pd.read_sql_query(sql, engine)
    df["entry_date"] = pd.to_datetime(df["datetime"]).dt.date
    df["close"] = np.nan

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

        if df_eod.empty:
            continue

        df.at[index, "close"] = df_eod["close"].iloc[0]

    df["pnl"] = np.where(
        df["signal"] == "buy",
        (df["close"] - df["signal_price"]) * df["lot_size"],
        (df["signal_price"] - df["close"]) * df["lot_size"],
    )

    df["sale_value"] = np.where(
        df["signal"] == "buy",
        df["close"] * df["lot_size"],
        df["signal_price"] * df["lot_size"],
    )

    df["pnl"] = df["pnl"] - (df["sale_value"] * 0.0003)  # brokerage cost
    df["pnlp"] = (df["pnl"] / df["sale_value"]) * 100

    # group by portfolio and calculate total pnl
    df_summary = df.groupby("entry_date").agg(
        total_pnl=pd.NamedAgg(column="pnl", aggfunc="sum"),
        trade_count=pd.NamedAgg(column="pnl", aggfunc="count"),
        total_trade_value=pd.NamedAgg(column="sale_value", aggfunc="sum"),
        profit_trades=pd.NamedAgg(column="pnl", aggfunc=lambda x: (x > 0).sum()),
        loss_trades=pd.NamedAgg(column="pnl", aggfunc=lambda x: (x < 0).sum()),
        profits=pd.NamedAgg(column="pnl", aggfunc=lambda x: (x[x > 0]).sum()),
        losses=pd.NamedAgg(column="pnl", aggfunc=lambda x: (x[x < 0]).sum()),
    )

    df_summary["pnlp"] = (
        df_summary["total_pnl"] / df_summary["total_trade_value"]
    ) * 100

    df_summary = df_summary.round(2)

    print(df_summary)
    df_summary.to_clipboard(index=True, header=True)


if __name__ == "__main__":
    main()
