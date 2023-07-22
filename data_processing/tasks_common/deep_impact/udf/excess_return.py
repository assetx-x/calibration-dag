from tasks_common.deep_impact.utils import series_to_pandas_dataframe, select_prices_for_ticker
from tasks_common.deep_impact.udf import Udf


class ExcessReturnUdf(Udf):
    @staticmethod
    def serialize_args(args, spark_context, sql_context, sources, prices_source, redshift_config):
        default_benchmark, = args
        prices_source = spark_context.broadcast(prices_source)
        redshift_config = spark_context.broadcast(redshift_config)

        benchmark_price_data = sources["benchmark"].rdd \
            .map(lambda row: row.benchmark) \
            .distinct() \
            .map(lambda benchmark: (
                benchmark,
                series_to_pandas_dataframe(
                    select_prices_for_ticker(benchmark, prices_source.value, redshift_config.value),
                    sort_by="cob",
                    #index_by="cob",
                    index_name=benchmark
                )
            )) \
            .collectAsMap()

        default_price_data = series_to_pandas_dataframe(
            select_prices_for_ticker(default_benchmark, prices_source.value, redshift_config.value),
            sort_by="cob",
            #index_by="cob",
            index_name=default_benchmark
        )

        benchmark_map = sources["benchmark"].rdd \
            .map(lambda row: (row.ticker, row.benchmark)) \
            .distinct() \
            .collectAsMap()

        print(benchmark_map)

        return benchmark_price_data, default_price_data, benchmark_map

    def __init__(self, benchmark_price_data, default_price_data, benchmark_map):
        self.benchmark_price_data = benchmark_price_data
        self.default_price_data = default_price_data
        self.benchmark_map = benchmark_map

    def get_benchmark_return(self, ticker, start_time, end_time):
        try:
            benchmark = self.benchmark_map[ticker]
            print(benchmark)
            benchmark_data = self.benchmark_price_data[benchmark]
            print("got data")
        except KeyError:
            benchmark_data = self.default_price_data

        slice_from = benchmark_data.cob.searchsorted(start_time, side="left")
        slice_to = benchmark_data.cob.searchsorted(end_time, side="right")
        print(slice_from, slice_to)

        benchmark_data = benchmark_data[slice_from:slice_to]
        print(start_time, end_time)
        print(benchmark_data["cob"].iloc[0], benchmark_data["cob"].iloc[-1])

        return benchmark_data["close"].iloc[-1] / benchmark_data["open"].iloc[0] - 1.0

    def __call__(self, df):
        benchmark_return = self.get_benchmark_return(df.index.name, df["cob"].iloc[0], df["cob"].iloc[-1])
        stock_return = df["close"].iloc[-1] / df["open"].iloc[0] - 1.0

        return (stock_return - benchmark_return)


excess_return = ExcessReturnUdf