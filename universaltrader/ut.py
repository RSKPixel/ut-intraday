import pandas as pd
from . import tools, ssc
from . import kbdt


def signals2(ohlc: pd.DataFrame, models=["KBD1"]) -> pd.DataFrame:

    df = ohlc.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["signal"] = False

    recent_swings = df[df["swing_point"].notna()].tail(8)

    min_date = recent_swings.index.min()
    df = df[df.index >= min_date]

    if df.empty:
        return df

    df = df.round(4)
    symbol = df.iloc[0].get("symbol", "")
    signals = pd.DataFrame()
    signals["datetime"] = pd.Series([], dtype="datetime64[ns]")

    if "KBDT" in models:
        result = kbdt.signal(df, symbol)
        signals = pd.concat(
            [signals, result[result["kbdt"].notna()]], ignore_index=True
        )
        signals["signal"] = signals["kbdt"]

    return signals
