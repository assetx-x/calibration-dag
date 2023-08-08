import talib
import numpy as np
import pandas as pd

def winsorize(df, cols, by=['date'], alpha=0.01):
    for col in cols:
        lb = df.groupby(by)[col].transform(pd.Series.quantile, alpha / 2.0)
        ub = df.groupby(by)[col].transform(pd.Series.quantile, 1 - alpha / 2.0)
        lb = lb.apply(nonnull_lb)
        ub = ub.apply(nonnull_ub)
        idx = df[col].notnull()
        df.loc[idx, col] = df.loc[idx, col].clip(lb, ub)
    return df

def nonnull_lb(value):
    value = value if pd.notnull(value) else -999999999999999
    return value

def nonnull_ub(value):
    value = value if pd.notnull(value) else 999999999999999
    return value


def talib_STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0, k_or_d="both"):
    index = close.index
    fastk, fastd = pd.Series(talib.STOCHRSI(close.values, timeperiod, fastk_period, fastd_period, fastd_matype))
    k = pd.Series(fastk, index=index)
    d = pd.Series(fastd, index=index)
    if k_or_d=="k":
        return k
    elif k_or_d=="d":
        return d
    else:
        return k, d

def MA(ts, window_size, type='simple', adjust=True):
    if type=='simple':
        return ts.rolling(window_size, min_periods=window_size).mean()
    else:
        return ts.ewm(span=window_size, min_periods=window_size, adjust=adjust).mean()

def talib_PPO(close, fastperiod=12, slowperiod=26, matype=0):
    return pd.Series(talib.PPO(close.values, fastperiod=fastperiod, slowperiod=slowperiod, matype=matype), index=close.index)

def macd(ts, nslow=26, nfast=12):
    ts = ts.fillna(method='ffill')
    emaslow = MA(ts, nslow, type='ewma')
    emafast = MA(ts, nfast, type='ewma')
    return emafast - emaslow

def talib_TRIX(close, timeperiod=30):
    return pd.Series(talib.TRIX(close, timeperiod), index=close.index)