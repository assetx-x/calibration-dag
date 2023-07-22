import numpy as np
import pandas as pd
import sqlalchemy as sa
from pyspark.sql import Row


def pandas_type_to_python(value):
    if isinstance(value, pd.Series):
        print("Series!!!!")
        print(value)

    if value is pd.NaT or (isinstance(value, np.float) and np.isnan(value)):
        return None
    elif isinstance(value, np.float):
        return float(value)
    elif isinstance(value, np.int):
        return int(value)
    elif isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    else:
        return value


def pandas_dataframe_to_series(df):
    return [
        Row(**dict([(field, pandas_type_to_python(row[1][field])) for field in row[1].keys()]))
        for row in df.iterrows()
    ]


def pandas_dataframe_to_raw_series(df, column_order=None):
    if column_order:
        try:
            df = df[column_order]
        except Exception as exc:
            raise Exception(repr(df) + "\n" + repr(exc) + "\n" + repr(column_order))

    return [
        tuple((pandas_type_to_python(row[1][field]) for field in row[1].keys()))
        for row in df.iterrows()
    ]


def series_to_pandas_dataframe(rows, sort_by=None, drop=None, rename=None, index_name=None, index_by=None):
    rows_dicts = (row.asDict() for row in rows)
    df = pd.DataFrame(rows_dicts)
    df = df.convert_objects()

    try:
        if sort_by:
            df.sort_values(sort_by, inplace=True)
        if drop:
            df.drop(drop, axis=1, inplace=True)
        if rename:
            df.rename(rename, inplace=True)
        if index_by:
            df.set_index(index_by, inplace=True, drop=False)
            df = df.tz_localize("UTC")
        if index_name:
            df.index.name = index_name

    except KeyError as e:
        return df, e

    return df, None


def merge_rows(row1, row2):
    dict1 = row1.asDict()
    dict2 = row2.asDict()
    for key in dict2.keys():
        if key in dict1:
            del dict2[key]

    dict1.update(dict2)

    #for key, value in dict1.items():
    #    if value is None:
    #        del dict1[key]

    return Row(**dict1)


def select_prices_for_ticker(ticker, prices_source, redshift_uri):
    engine = sa.create_engine(redshift_uri, echo=True)

    sql = "SELECT cob,\"open\",high,low,close,volume FROM {} WHERE ticker='{}' AND as_of_end IS NULL".format(
        prices_source,
        ticker
    )
    print(sql)

    return (
        Row(**row)
        for row in engine.execute(sql)
    )


def select_prices_for_ticker_dates(ticker, start_date, end_date, prices_source, redshift_uri):
    engine = sa.create_engine(redshift_uri, echo=True)

    sql = "SELECT cob,\"open\",high,low,close,volume FROM {} WHERE ticker='{}' AND as_of_end IS NULL AND cob >= '%s' and cob <=".format(
        prices_source,
        ticker
    )

    return (
        Row(**row)
        for row in engine.execute(sql)
    )