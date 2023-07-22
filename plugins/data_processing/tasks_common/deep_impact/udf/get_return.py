from tasks_common.deep_impact.udf import as_function

@as_function()
def get_return(df):
    return df["close"].iloc[-1] / df["open"].iloc[0] - 1.0