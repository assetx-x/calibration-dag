from collections import namedtuple
from pydoc import deque
from uuid import uuid4

import numpy as np
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from tasks_common.deep_impact.offsets import get_offset_object
from tasks_common.deep_impact.utils import series_to_pandas_dataframe, select_prices_for_ticker

from enrich_strategies import get_enrichment_strategy
from tasks_common.deep_impact.udf import get_udf_object


def join_prices_with_events_adhoc(spark_context, sql_context, sources, price_data_table, redshift_uri, events_rdd, offsets, udfs):
    import pandas as pd

    offsets = spark_context.broadcast(offsets)
    udf_specifiers = spark_context.broadcast(udfs)

    prices_data_table = spark_context.broadcast(price_data_table)
    redshift_uri = spark_context.broadcast(redshift_uri)

    def broadcast_udf_map():
        udf_map = {}

        for udf_specifier in udfs:
            udf, args = get_udf_object(udf_specifier)
            args = udf.serialize_args(
                args, spark_context, sql_context, sources, prices_data_table.value, redshift_uri.value)
            udf_map[udf_specifier] = args

        return spark_context.broadcast(udf_map)

    udf_map = broadcast_udf_map()

    def get_window_timestamps(timestamp, offset):
         offset_timestamp = pd.to_datetime(get_offset_object(offset).apply(timestamp))

         if offset_timestamp > timestamp:
             return (timestamp, offset_timestamp.to_pydatetime())
         else:
             return (offset_timestamp.to_pydatetime(), timestamp)

    def process_udf(ticker, sliced_df, udf_specifier):
        try:
            value = float(
                get_udf_object(udf_specifier)[0](*udf_map.value[udf_specifier])(sliced_df)
            )

            if np.isnan(value):
                return None
            else:
                return value
        except Exception:
            return None

    def map_prices_to_events((ticker, event_rows)):
        print("fetching prices")
        price_rows = select_prices_for_ticker(ticker, prices_data_table.value, redshift_uri.value)
        print("fetched")

        df = series_to_pandas_dataframe(price_rows, sort_by="cob")

        for event_row in event_rows:
            for offset in offsets.value:
                # Calculate window
                timestamp = event_row.Timestamp
                quarter = event_row.Quarter

                offset_timestamp = get_offset_object(offset).apply(timestamp).to_pydatetime()
                if offset_timestamp > timestamp:
                    window = (timestamp, offset_timestamp)
                else:
                    window = (offset_timestamp, timestamp)

                slice_from = df.cob.searchsorted(window[0], side="left")
                slice_to = df.cob.searchsorted(window[1], side="right")
                sliced_df = df[slice_from:slice_to]

                for udf in udf_specifiers.value:
                    yield (
                        str(uuid4()), ticker, timestamp, quarter, offset, udf, window[0], window[1],
                        process_udf(ticker, sliced_df, udf)
                    )

    def map_prices_to_events1((ticker, event_rows)):
        print("fetching prices")
        price_rows = select_prices_for_ticker(ticker, prices_data_table.value, redshift_uri.value)
        price_rows_iter = iter(price_rows)
        print("fetched")

        window = namedtuple("window", ["event_row", "timestamps", "offset", "result"])

        windows = sorted((
            window(
                event_row,
                get_window_timestamps(event_row.Timestamp, offset),
                offset,
                deque()
            )
            for event_row in event_rows
            for offset in offsets.value
        ), key=lambda window: window.timestamps)
        windows_in_progress = deque()
        windows_iter = iter(windows)

        next_window = next(windows_iter, None)
        price_row = next(price_rows_iter, None)
        while price_row:
            next_price_row = next(price_rows_iter, None)
            while next_window and price_row.cob >= next_window.timestamps[0]:
                # New window reached
                windows_in_progress.append(next_window)
                next_window = next(windows_iter, None)

            while windows_in_progress:
                next_processed_window = windows_in_progress.popleft()

                if price_row.cob >= next_processed_window.timestamps[1] or not next_price_row:
                    # Finished window
                    for udf in udf_specifiers.value:
                        yield (
                            str(uuid4()), ticker, next_processed_window.event_row.Timestamp, next_processed_window.event_row.Quarter, next_processed_window.offset, udf,
                            next_processed_window.timestamps[0], next_processed_window.timestamps[1],
                            process_udf(ticker, window.result, udf)
                        )
                else:
                    windows_in_progress.appendleft(next_processed_window)
                    break

            # Append price_row
            for window in windows_in_progress:
                window.result.append(price_row)

            price_row = next_price_row

    joined_rdd = events_rdd \
        .map(lambda event_row: (event_row.Ticker, event_row)) \
        .groupByKey() \
        .flatMap(map_prices_to_events)

    return joined_rdd


def join_prices_with_events(spark_context, sql_context, sources, prices_rdd, events_rdd, offsets, udfs):
    offsets = spark_context.broadcast(offsets)
    udfs = spark_context.broadcast(udfs)
    tickers = spark_context.broadcast(
        set(events_rdd
            .map(lambda event_row: event_row.Ticker)
            .distinct()
            .collect()
        )
    )
    events_map = spark_context.broadcast(
        events_rdd
            .map(lambda event_row: (event_row.ticker, event_row))
            .groupByKey()
            .collectAsMap()
    )

    udf_args_map = spark_context.broadcast({
        udf: udf.serialize_args(args, sql_context, spark_context, sources, events_rdd)  # FIXME
        for udf, args in map(get_udf_object, udfs.value)
    })

    def process_udf(df, udf):
        try:
            return float(get_udf_object(udf)[0](*udf_args_map.value[udf])(df))
        except Exception:
            return None

    def map_events_to_price_rows((ticker, sorted_price_rows)):
        event_rows = events_map.value[ticker]
        # Calculate dataframe with prices
        df = series_to_pandas_dataframe(sorted_price_rows, sort_by="cob")

        for event_row in event_rows:
            for offset in offsets.value:
                # Calculate window
                timestamp = event_row.Timestamp
                offset_timestamp = get_offset_object(offset).apply(timestamp).to_pydatetime()
                if offset_timestamp > timestamp:
                    window = (timestamp, offset_timestamp)
                else:
                    window = (offset_timestamp, timestamp)

                slice_from = df.cob.searchsorted(window[0], side="left")
                slice_to = df.cob.searchsorted(window[1], side="right")
                sliced_df = df[slice_from:slice_to]

                for udf in udfs.value:
                    yield (ticker, timestamp, offset, udf, window[0], window[1], process_udf(sliced_df, udf))

    return prices_rdd \
        .filter(lambda price_row: price_row.ticker in tickers.value) \
        .map(lambda price_row: (price_row.ticker, price_row)) \
        .groupByKey() \
        .flatMap(map_events_to_price_rows)


def apply_schema(spark_context, sql_context, result_rdd):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("ticker", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("quarter", StringType(), False),
        StructField("offset", StringType(), False),
        StructField("udf_name", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("result", FloatType(), True)
    ])

    return sql_context.createDataFrame(result_rdd, schema)
