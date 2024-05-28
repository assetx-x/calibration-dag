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


def talib_STOCHRSI(
    close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0, k_or_d="both"
):
    index = close.index
    fastk, fastd = pd.Series(
        talib.STOCHRSI(
            close.values, timeperiod, fastk_period, fastd_period, fastd_matype
        )
    )
    k = pd.Series(fastk, index=index)
    d = pd.Series(fastd, index=index)
    if k_or_d == "k":
        return k
    elif k_or_d == "d":
        return d
    else:
        return k, d


def MA(ts, window_size, type='simple', adjust=True):
    if type == 'simple':
        return ts.rolling(window_size, min_periods=window_size).mean()
    else:
        return ts.ewm(span=window_size, min_periods=window_size, adjust=adjust).mean()


def talib_PPO(close, fastperiod=12, slowperiod=26, matype=0):
    return pd.Series(
        talib.PPO(
            close.values, fastperiod=fastperiod, slowperiod=slowperiod, matype=matype
        ),
        index=close.index,
    )


def macd(ts, nslow=26, nfast=12):
    ts = ts.fillna(method='ffill')
    emaslow = MA(ts, nslow, type='ewma')
    emafast = MA(ts, nfast, type='ewma')
    return emafast - emaslow


def talib_TRIX(close, timeperiod=30):
    return pd.Series(talib.TRIX(close, timeperiod), index=close.index)


def transform_data(ts, transform_type=1):
    if transform_type == 2:
        transformed = ts.diff()
    elif transform_type == 3:
        transformed = ts.diff().diff()
    elif transform_type == 4:
        transformed = np.log(ts)
    elif transform_type == 5:
        transformed = np.log(ts).diff()
    elif transform_type == 6:
        transformed = np.log(ts).diff().diff()
    elif transform_type == 7:
        transformed = ts.pct_change().diff()
    else:
        transformed = ts  # Default is no transform
    return transformed


def factor_standarization(df, X_cols, y_col, exclude_from_standardization):
    df = df.copy(deep=True).set_index(["date", "ticker"])
    not_X_col = [k for k in X_cols if k not in df.columns]
    print("Missing X columns: {0}".format(not_X_col))
    not_y_col = [k for k in y_col if k not in df.columns]
    print("Missing Y columns: {0}".format(not_y_col))
    X_cols = [k for k in X_cols if k not in not_X_col]
    y_col = [k for k in y_col if k not in not_y_col]
    exclude_from_standardization_df = df[
        list(set(df.columns).intersection(set(exclude_from_standardization)))
    ]
    df = df[y_col + X_cols]

    lb = df.groupby('date').transform(pd.Series.quantile, 0.005)
    lb.fillna(-999999999999999, inplace=True)
    ub = df.groupby('date').transform(pd.Series.quantile, 0.995)
    ub.fillna(999999999999999, inplace=True)
    new_df = df.clip(lb, ub)
    df = new_df
    # df[df<=lb]=lb
    # df[df>=ub]=ub
    # new_df = df.copy()
    new_df = new_df.groupby("date").transform(lambda x: (x - x.mean()) / x.std())
    new_df.fillna(0, inplace=True)
    renaming = {k: "{0}_std".format(k) for k in y_col}
    new_df.rename(columns=renaming, inplace=True)
    df = df[y_col]
    new_df = pd.concat([new_df, df, exclude_from_standardization_df], axis=1)
    new_df = new_df.reset_index()
    # new_df["ticker"] = new_df["ticker"].astype(int)
    new_df["Sector"] = new_df["Sector"].astype(str)
    new_df["IndustryGroup"] = new_df["IndustryGroup"].astype(str)
    return new_df
