import pandas as pd
import numpy as np


def signal(df: pd.DataFrame, symbol: str = "") -> pd.DataFrame:
    # KBD1 - Key Breakout Day 1 - Trading Model
    # Author: Brent Penfold
    # When dow_cross occurs in the direction of the trend, enter a trade.
    # Exit with a trailing stop of high if bullish or low if bearish of the previous three candles.

    df_original = df.copy()
    df_original["kbdt"] = None
    df_original["tradingmodel"] = "kbdt"
    df_original["signal_price"] = np.nan
    # df_original["entry_day_sl"] = np.nan

    df = df.copy()

    if df.iloc[-1]["dow_cross"] and df.iloc[-1]["direction"] == 1:
        # Bullish Setup
        # Check for previous dow cross to identify setup candle
        buy_price = df.iloc[-1]["dow_cross"]

        lwv = df.iloc[-1]["lwv"]
        sell_price = np.nan
        profit_points = np.nan
        setup_bar = False

        for i in range(len(df) - 2, -1, -1):
            if df.iloc[i]["direction"] == -1:
                sell_price = df.iloc[i]["dow_cross"]
                break

        if not np.isnan(sell_price):
            profit_points = sell_price - buy_price
            setup_bar = True

        if setup_bar and profit_points < 0 and lwv:
            df_original.at[df.index[-1], "kbdt"] = "buy"
            df_original.at[df.index[-1], "signal_price"] = buy_price
            df_original.at[df.index[-1], "signal"] = True

    if df.iloc[-1]["dow_cross"] and df.iloc[-1]["direction"] == -1:
        # Bearish Setup
        # Check for previous dow cross to identify setup candle

        sell_price = df.iloc[-1]["dow_cross"]
        lwv = df.iloc[-1]["lwv"]
        buy_price = np.nan
        profit_points = np.nan
        setup_bar = False

        for i in range(len(df) - 2, -1, -1):
            if df.iloc[i]["direction"] == 1:
                buy_price = df.iloc[i]["dow_cross"]
                break

        if not np.isnan(buy_price):
            profit_points = sell_price - buy_price
            setup_bar = True

        if setup_bar and profit_points < 0 and lwv:
            df_original.at[df.index[-1], "kbdt"] = "sell"
            df_original.at[df.index[-1], "signal_price"] = sell_price
            df_original.at[df.index[-1], "signal"] = True

    return df_original
