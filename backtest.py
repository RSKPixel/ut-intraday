import pandas as pd
from dependencies import engine
from psycopg2.extras import execute_values
from sqlalchemy.sql import text
import numpy as np
import sys
import os


def main(period: int = 0):

    sql = text("SELECT * FROM tfw_signals WHERE entry = true ORDER BY datetime ASC")

    df = pd.read_sql_query(sql, engine)
    # df = df[df["datetime"].dt.time < pd.to_datetime("15:00").time()]
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
        df_eod = df_eod[: period + 1]

        if df_eod.empty:
            continue

        try:
            df.at[index, "close"] = df_eod["close"].iloc[period]
        except IndexError:
            continue

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
    df["brokerage"] = df["sale_value"] * 0.0004
    df["pnl"] = df["pnl"] - df["brokerage"]  # brokerage cost
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
    df_summary["holding_period"] = period

    df_summary["pnlp"] = (
        df_summary["total_pnl"] / df_summary["total_trade_value"]
    ) * 100

    df_summary = df_summary.round(2)

    summ = round(df_summary["total_pnl"].sum(), 2)
    data = {
        "Overall PnL": summ,
        "Brokerage Cost": round(df["brokerage"].sum(), 2),
        "Total Trades": int(df_summary["trade_count"].sum()),
        "Profitable Trades": int(df_summary["profit_trades"].sum()),
        "Losing Trades": int(df_summary["loss_trades"].sum()),
        "Total Profit": round(df_summary["profits"].sum(), 2),
        "Total Loss": round(df_summary["losses"].sum(), 2),
        "Profit %": round(
            (df_summary["profit_trades"].sum() / df_summary["trade_count"].sum()) * 100,
            2,
        ),
        "Risk of Ruin %": round(ror(df), 2),
    }

    df_overall = pd.DataFrame(list(data.items()), columns=["Metric", "Value"])
    print(
        df_summary[
            [
                "holding_period",
                "total_pnl",
                "pnlp",
                "trade_count",
                "profit_trades",
                "loss_trades",
            ]
        ]
    )
    print(df_overall)

    df_summary.to_clipboard(index=True, header=True)


def ror(df: pd.DataFrame) -> float:
    # ---------------- RISK OF RUIN ---------------- #

    capital = 3_500_000  # 35 lakhs

    wins = (df["pnl"] > 0).sum()
    losses = (df["pnl"] < 0).sum()
    total_trades = wins + losses

    # Safety check
    if total_trades == 0:
        win_rate = 0
        ror = 0
    else:
        p = wins / total_trades
        q = 1 - p

        avg_win = df.loc[df["pnl"] > 0, "pnl"].mean()
        avg_loss = abs(df.loc[df["pnl"] < 0, "pnl"].mean())

        expectancy = (p * avg_win) - (q * avg_loss)

        # If expectancy is negative → ruin is certain
        if expectancy <= 0 or p == 0:
            ror = 1.0
        else:
            ror = (q / p) ** (capital / avg_loss)

        win_rate = p * 100

    return ror


if __name__ == "__main__":
    period = 0
    os.system("cls" if os.name == "nt" else "clear")
    if len(sys.argv) > 1:
        try:
            period = int(sys.argv[1])
        except ValueError:
            period = 0

    main(period=period)
