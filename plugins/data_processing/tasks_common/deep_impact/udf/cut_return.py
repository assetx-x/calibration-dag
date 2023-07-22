import numpy as np

from tasks_common.deep_impact.udf import as_function


@as_function(float, float)
def cut_return(low_thresh, hi_thresh, df):
    low_thresh = 1.0 + low_thresh
    hi_thresh = 1.0 + hi_thresh

    running_max = np.maximum.accumulate(df["high"])
    running_min = np.minimum.accumulate(df["low"])
    location = np.where(
        (
            (df["open"].iloc[0] * hi_thresh - running_max) *
            (running_min - df["open"].iloc[0] * low_thresh)
        ) < 0.)[0]
    location = location[0] if len(location) else -1

    return df["close"].iloc[location] / df["open"].iloc[0] - 1.0
