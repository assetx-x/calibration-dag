import os
import sys
import traceback
from collections import namedtuple
from uuid import uuid4

import numpy as np
from luigi import DictParameter
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, FloatType

from base import DCMTaskParams, PySparkTask
from tasks_common.deep_impact.offsets import get_offset_object
from tasks_common.deep_impact.udf import get_udf_object
from tasks_common.deep_impact.utils import select_prices_for_ticker, series_to_pandas_dataframe


class DeepImpactTask(DCMTaskParams, PySparkTask):
    date_columns = DictParameter(default={"events_data": ["Timestamp"]})

    def run_spark(self):
        self.logger.info("Creating source data frames")
        price_query = "select * from {table} where as_of_end is null"
        source_dfs = {
            "events_data": self.input()["events_data"].to_spark_dataframe(self.sql_context),
            "price_data": self.input()["price_data"].to_spark_dataframe(self.sql_context, query=price_query)
        }
        for input_name, columns in self.date_columns.items():
            for column in columns:
                source_dfs[input_name] = \
                    source_dfs[input_name].withColumn(column, getattr(source_dfs[input_name], column).cast("timestamp"))

        self.logger.info("Repartitioning event rdd")
        events_df = source_dfs["events_data"]
        events_rdd = events_df.rdd.repartition(128)
        #prices_rdd = source_dfs["price_data"].rdd

        self.logger.info("Calling join_prices_with_events_adhoc")
        result_rdd = self.join_prices_with_events_adhoc(
            source_dfs,
            self.input()["price_data"].table,
            self.input()["price_data"].uri("psycopg2"),
            events_rdd,
            list(self.offsets),
            list(self.udfs)
        )
        # result_rdd = self.join_prices_with_events(
        #     source_dfs,
        #     prices_rdd,
        #     events_rdd,
        #     list(self.offsets),
        #     list(self.udfs)
        # )
        self.logger.info("Creating data frame out of rdd")
        result_df = self.apply_schema(result_rdd)

        self.logger.info("Saving data frame to Redshift")
        self.output().from_spark_dataframe(result_df)

    def broadcast_udf_map(self, udfs, source_dfs, price_data_table=None, redshift_uri=None):
        udf_map = {}

        for udf_specifier in udfs:
            udf, args = get_udf_object(udf_specifier)
            args = udf.serialize_args(
                args, self.spark_context, self.sql_context, source_dfs, price_data_table, redshift_uri)
            udf_map[udf_specifier] = args

        return self.spark_context.broadcast(udf_map)

    def join_prices_with_events_adhoc(self, source_dfs, prices_data_table, redshift_uri, events_rdd, offsets, udfs):
        broadcast = namedtuple(
            "broadcast",
            ["offsets", "udf_specifiers", "udf_map", "prices_data_table", "redshift_uri"]
        )(
            self.spark_context.broadcast(offsets),
            self.spark_context.broadcast(udfs),
            self.broadcast_udf_map(udfs, source_dfs, prices_data_table, redshift_uri),
            self.spark_context.broadcast(prices_data_table),
            self.spark_context.broadcast(redshift_uri)
        )

        def process_udf(sliced_df, udf_specifier):
            try:
                value = float(
                    get_udf_object(udf_specifier)[0](*broadcast.udf_map.value[udf_specifier])(sliced_df)
                )

                if np.isnan(value):
                    return None
                else:
                    return value
            except Exception:
                print("Error during calculating %s udf, setting it to None:" % udf_specifier)
                traceback.print_exc()
                return None

        def map_prices_to_events(ticker, event_rows):
            print("fetching prices using select_prices_for_ticker")
            price_rows = select_prices_for_ticker(ticker, broadcast.prices_data_table.value, broadcast.redshift_uri.value)
            print("fetched from select_prices_for_ticker")

            print("*** ", sys.modules[self.__module__].__file__)
            print("*** ", sys.modules[series_to_pandas_dataframe.__module__].__file__)
            print("*** ", sys.path)
            print("*** ", os.getcwd())
            df = series_to_pandas_dataframe(price_rows, sort_by="cob", index_by="cob", index_name=ticker)
            print("*** ", str(type(df)))
            # if error:
            #     print "*** Error converting to pandas dataframe: columns:", str(df.columns)
            #     raise error

            for event_row in event_rows:
                for offset in broadcast.offsets.value:
                    # Calculate window
                    timestamp = event_row.Timestamp
                    quarter = event_row.Quarter

                    bm_adjuster = get_offset_object("1BT")
                    offset_obj = get_offset_object(offset)
                    if offset.startswith("-"):
                        timestamp = timestamp + bm_adjuster - bm_adjuster
                        window = [timestamp + offset_obj, timestamp]
                    else:
                        timestamp = timestamp - bm_adjuster + bm_adjuster
                        window = [timestamp, timestamp + offset_obj]

                    window[0] = window[0].tz_localize("US/Eastern").tz_convert("UTC")
                    window[1] = window[1].tz_localize("US/Eastern").tz_convert("UTC")

                    sliced_df = df[window[0]:window[1]]

                    # slice_from, = df.cob.searchsorted(window[0], side="left")
                    # slice_to, = df.cob.searchsorted(window[1], side="right")
                    # sliced_df = df.ix[slice_from:slice_to]

                    for udf in broadcast.udf_specifiers.value:
                        # Return nan if window_start < min_cob
                        if window[0] < df.ix[0].name:
                            value = None
                        else:
                            value = process_udf(sliced_df, udf)

                        yield (str(uuid4()), ticker, timestamp, quarter, offset, udf, window[0], window[1], value)

        return events_rdd \
            .map(lambda event_row: (event_row.Ticker, event_row)) \
            .groupByKey() \
            .flatMap(map_prices_to_events)

    def join_prices_with_events(self, source_dfs, prices_rdd, events_rdd, offsets, udfs):
        broadcast = namedtuple(
            "broadcast",
            ["tickers", "offsets", "udf_specifiers", "udf_map", "event_map"]
        )(
            self.spark_context.broadcast(set(events_rdd.map(lambda event_row: event_row.Ticker).distinct().collect())),
            self.spark_context.broadcast(offsets),
            self.spark_context.broadcast(udfs),
            self.broadcast_udf_map(udfs, source_dfs),
            self.spark_context.broadcast(
                events_rdd.map(lambda event_row: (event_row.Ticker, event_row)).groupByKey().collectAsMap()
            )
        )

        def process_udf(sliced_df, udf_specifier):
            try:
                value = float(
                    get_udf_object(udf_specifier)[0](*broadcast.udf_map.value[udf_specifier])(sliced_df)
                )

                if np.isnan(value):
                    return None
                else:
                    return value
            except Exception:
                return None

        def map_events_to_price_rows((ticker, sorted_price_rows)):
            event_rows = broadcast.event_map.value[ticker]
            # Calculate dataframe with prices
            df = series_to_pandas_dataframe(sorted_price_rows, sort_by="cob")

            for event_row in event_rows:
                for offset in broadcast.offsets.value:
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

                    for udf in broadcast.udfs.value:
                        yield (
                            str(uuid4()), ticker, timestamp, quarter, offset, udf, window[0], window[1],
                            process_udf(sliced_df, udf)
                        )

        return prices_rdd \
            .filter(lambda price_row: price_row.as_of_end is None) \
            .filter(lambda price_row: price_row.ticker in broadcast.tickers.value) \
            .map(lambda price_row: (price_row.ticker, price_row)) \
            .groupByKey() \
            .flatMap(map_events_to_price_rows)

    def apply_schema(self, result_rdd):
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

        return self.sql_context.createDataFrame(result_rdd, schema)
