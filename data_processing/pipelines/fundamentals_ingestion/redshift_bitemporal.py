from collections import defaultdict
from datetime import datetime

import pandas as pd
from luigi import DictParameter
from pyspark.rdd import portable_hash

from base import DCMTaskParams, PySparkTask
from pipelines.fundamentals_ingestion.fundamentals_common import BitemporalFundamentalValue

CAPIQ_NA_VALUES = ['NM', 'NA', '(Invalid Identifier)', '(Capability Needed)', '#VALUE!', "#DIV/0!", None]

lost_tickers = ['AHS', 'VMEM', 'RAX', 'RSTI', 'BLOX', 'DTSI', 'DW', 'EDE', 'FFH', 'IM', 'ININ', 'LNKD', 'LXK',
                'STJ', 'VMEM', 'CYNO', 'MENT', 'CBR', 'WWAV', 'LLTC', 'EQY', 'VAL', 'ACAT', 'CFNL', 'VASC',
                'CLC', 'WNR', 'UTEK', 'ISIL', 'SWC', 'HAR', 'CLMS', 'AIRM', 'BEAV', 'XXIA', 'EXAR', 'TSRA',
                'IRG', 'TCB', 'DV', 'PVTB', 'YHOO']


class MergeRedshiftBitemporalData(DCMTaskParams, PySparkTask):
    name_mapping = DictParameter(description="Metric mapping as dict", default={})
    derived_metrics = DictParameter(description="Derived metric mapping to formulas", default={})

    def run_spark(self):
        now = self.spark_context.broadcast(datetime.now())

        input_values = self._load_new_data()

        self.log("creating input_rdd")
        derived_metrics = self.spark_context.broadcast(self.derived_metrics)
        input_rdd = self.spark_context.parallelize(input_values, 64)

        self.log("creating bitemporal_rdd")
        bitemporal_df = self.input()["bitemporal_data"].to_spark_dataframe(self.sql_context)
        bitemporal_rdd = bitemporal_df.rdd

        self.log("repartitioning input_rdd")
        # Repartition RDD by ticker and sort within by dates, to be able to map timeseries
        # in ordered manner to make further forward fill
        input_rdd = input_rdd \
            .keyBy(lambda row: (row["ticker"], row["as_of_end"], row["date"])) \
            .repartitionAndSortWithinPartitions(
                numPartitions=64,
                partitionFunc=lambda key: portable_hash((key[0], key[1])),
                keyfunc=lambda ticker, as_of_end, date: date,
                ascending=True
            )

        def preprocess(iterator):
            # ffill and calculate derived metrics
            print("** *MergeRedshiftBitemporalData: Preprocessing partition")
            last_values = defaultdict(lambda: None)
            calculate_context = defaultdict(dict)
            calculate_date = None

            def calculate_derived():
                print("** *MergeRedshiftBitemporalData: Calculate context")
                for ticker in calculate_context:
                    for metric_name, calculate_code in derived_metrics.value.iteritems():
                        try:
                            value = eval(calculate_code, calculate_context[ticker])
                        except (ZeroDivisionError, TypeError):
                            # Division by zero, NoneType (na) as operand
                            value = None
                        except NameError:
                            # Some of the values still not exists
                            continue

                        yield BitemporalFundamentalValue(
                            metric_name, ticker, calculate_date,
                            value, now.value, None, None
                        )

            for key, row in iterator:
                if calculate_date and row["date"] != calculate_date:
                    # Context is ready
                    for x in calculate_derived():
                        yield x
                calculate_date = row["date"]

                if row["value"] is not None:
                    last_values[(row["ticker"], row["metric"], row["as_of_end"])] = row["value"]

                calculate_context[row["ticker"]][row["metric"]] = \
                    last_values[(row["ticker"], row["metric"], row["as_of_end"])]

                yield BitemporalFundamentalValue(
                    row["metric"], row["ticker"], row["date"],
                    last_values[(row["ticker"], row["metric"], row["as_of_end"])],
                    row["as_of_start"], row["as_of_end"], row["raw_value"]
                )

            # Calculate for last date
            for x in calculate_derived():
                yield x

        self.log("mapping input_rdd partitions")
        input_rdd = input_rdd.mapPartitions(preprocess)

        def get_key(bitemporal_row):
            return (
                bitemporal_row["metric"],
                bitemporal_row["ticker"],
                bitemporal_row["date"],
                bitemporal_row["as_of_end"]
            )
        input_rdd = input_rdd.keyBy(get_key)
        bitemporal_rdd = bitemporal_rdd.keyBy(get_key)

        def merge_values(existing_bitemporal_value, pulled_bitemporal_value):
            if existing_bitemporal_value is not None and pulled_bitemporal_value is not None:
                if existing_bitemporal_value["value"] is None and pulled_bitemporal_value["value"] is not None:
                    yield pulled_bitemporal_value
                elif existing_bitemporal_value["value"] is not None and pulled_bitemporal_value["value"] is None:
                    if pulled_bitemporal_value["ticker"] in lost_tickers:
                        yield existing_bitemporal_value
                    else:
                        raise ValueError("Missing value: " + str(pulled_bitemporal_value))
                elif existing_bitemporal_value["value"] != pulled_bitemporal_value["value"] \
                        and abs(existing_bitemporal_value["value"] - pulled_bitemporal_value["value"]) > 0.001:
                    yield BitemporalFundamentalValue(
                        existing_bitemporal_value["metric"], existing_bitemporal_value["ticker"],
                        existing_bitemporal_value["date"], existing_bitemporal_value["value"],
                        existing_bitemporal_value["as_of_start"], now.value, existing_bitemporal_value["raw_value"]
                    )
                    yield pulled_bitemporal_value
                else:
                    yield existing_bitemporal_value
            elif existing_bitemporal_value is None:
                # New value
                yield pulled_bitemporal_value
            elif pulled_bitemporal_value is None:
                # Value removed
                yield BitemporalFundamentalValue(
                    existing_bitemporal_value["metric"], existing_bitemporal_value["ticker"],
                    existing_bitemporal_value["date"], existing_bitemporal_value["value"],
                    existing_bitemporal_value["as_of_start"], now.value, existing_bitemporal_value["raw_value"]
                )

        self.log("merging values from both RDDs")
        bitemporal_rdd = bitemporal_rdd \
            .fullOuterJoin(input_rdd) \
            .flatMapValues(merge_values) \
            .values()

        bitemporal_df = bitemporal_rdd.toDF(bitemporal_df.schema)
        self.input()["bitemporal_data"].from_spark_dataframe(bitemporal_df)

        self.output()["flag"].open("w").close()

    def _load_new_data(self):
        self.logger.info("Loading new fundamentals data from hdf file")
        input_df = pd.read_hdf(self.input()["new_fundamentals"].path, key="fundamentals_data")
        self.logger.info("Done loading new fundamentals")
        return input_df
