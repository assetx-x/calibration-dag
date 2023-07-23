import os
from glob import glob
from abc import abstractmethod

import sys
# Add the path to the "plugins" folder to sys.path
# Assuming the "calibration-dag" directory is the parent directory of your DAGs folder.
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
plugins_folder = os.path.join(parent_directory, "plugins")
data_processing_folder = os.path.join(parent_directory, "data_processing")
#sys.path.append(plugins_folder)
#sys.path.append(data_processing_folder)


import pandas as pd
from pandas.tseries.frequencies import to_offset
from commonlib.timeline_permutation import ReturnTypeForPermutation
from data_processing.commonlib.market_timeline import marketTimeline
from data_processing.calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from data_processing.calibrations.common.calibration_logging import calibration_logger
from data_processing.etl_workflow_steps import StatusType
from data_processing.commonlib.data_requirements import DRDataTypes
from calibrations.core.core_classes import SQLReader, DataReader, S3Reader, H5Reader
from commonlib.util_functions import get_redshift_engine
from pandas_datareader import data as web

from google.cloud import storage
import io
from pylru import lrudecorator
from sqlalchemy import create_engine
from calibrations.common.taskflow_params import TaskflowPipelineRunMode

from fredapi import Fred


class SQLReaderPriceData(SQLReader):
    '''

    Reads daily equity prices from redshift provided a date range and universe date

    '''
    PROVIDES_FIELDS = ["daily_price_data"]

    def __init__(self, table_name="daily_equity_prices", price_limit_filters=None):
        price_limit_filters = price_limit_filters or {}
        base_query = """
        WITH raw_data AS (
            SELECT ticker, date, open, low, high, close, volume
            FROM {table_name}
            WHERE (date >= '{date_st}' AND date < '{date_end}')
            AND ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
            AND as_of_end is NULL
            )
         SELECT *
         FROM raw_data
         WHERE
         ticker IN (
             SELECT ticker
             FROM raw_data GROUP BY ticker
        """
        if price_limit_filters:
            terms = ["""(AVG({0}) BETWEEN {1} AND {2})""".format(k, *price_limit_filters[k])
                     for k in price_limit_filters]
            terms = " AND ".join(terms)
            clause = "\n HAVING ({0})".format(terms)
            base_query += clause
        base_query += "\n)"
        self.table_name = table_name
        SQLReader.__init__(self, base_query, "date", "ticker", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=True, permute_data=True)

    def _get_return_types_for_permutations(self, data):
        return_types = {"close": ReturnTypeForPermutation.LogReturn,
                        "high": ReturnTypeForPermutation.LogReturn,
                        "low": ReturnTypeForPermutation.LogReturn,
                        "open": ReturnTypeForPermutation.LogReturn}

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(table_name=self.table_name, date_st=date_st, date_end=date_end,
                                        universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_default_config(cls):
        return {"table_name": "daily_equity_prices", "price_limit_filters": None}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyEquities]


class SQLReaderAdjustmentFactors(SQLReader):
    '''

    Reads daily equity adjustment factors from redshift provided a date range and universe date

    '''
    PROVIDES_FIELDS = ["adjustment_factor_data"]

    def __init__(self, table_name="equity_adjustment_factors"):
        base_query = """
        select ticker, dcm_security_id, cob as date, split_factor, split_div_factor
        from {table_name}
        where (cob >= '{date_st}' and cob < '{date_end}')
        and as_of_end is NULL
        """
        # and ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
        self.table_name = table_name
        SQLReader.__init__(self, base_query, "date", "ticker", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=True, permute_data=True)

    def _get_return_types_for_permutations(self, data):
        return_types = {"close": ReturnTypeForPermutation.LogReturn,
                        "high": ReturnTypeForPermutation.LogReturn,
                        "low": ReturnTypeForPermutation.LogReturn,
                        "open": ReturnTypeForPermutation.LogReturn}

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(table_name=self.table_name, date_st=date_st, date_end=date_end,
                                        universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_default_config(cls):
        return {"table_name": "equity_adjustment_factors"}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.EquityAdjustmentFactors]


class SQLReaderDailyIndexData(SQLReader):
    '''

    Reads daily index data from redshift. If index_names list is not provided will download all.

    '''
    PROVIDES_FIELDS = ["daily_index_data"]

    def __init__(self, table_name="daily_index", index_names=None):
        self.table_name = table_name
        base_query = """
        SELECT * FROM {table_name}
        WHERE
          (date >= '{date_st}' AND date < '{date_end}')
        AND
          as_of_end is NULL
        """
        if index_names:
            base_query += " AND symbol in ({0})".format(",".join(map(lambda x: "'{0}'".format(x), index_names)))
        SQLReader.__init__(self, base_query, "date", "symbol", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=True, permute_data=True)

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(table_name=self.table_name, date_st=date_st, date_end=date_end)
        return final_query

    @classmethod
    def get_default_config(cls):
        return {"table_name": "daily_index",
                "index_names": ["SPX", "PPUT", "CMBO", "BFLY", "RXM", "VIX", "VXST", "VXMT", "VIX3M"]}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyIndices]


class SQLReaderDailyExtraCBOEIndexData(SQLReader):
    '''

    Reads cboe index data from redshift. If index_names list is not provided will download all.

    '''

    PROVIDES_FIELDS = ["daily_extra_cboe_index_data"]

    def __init__(self, table_name="cboe_auxiliary_indices", index_names=None):
        self.table_name = table_name
        base_query = """
        SELECT * FROM {table_name}
        WHERE
          (date >= '{date_st}' AND date < '{date_end}')
        AND
          as_of_end is NULL
        """
        if index_names:
            base_query += " AND symbol in ({0})".format(",".join(map(lambda x: "'{0}'".format(x), index_names)))
        SQLReader.__init__(self, base_query, "date", "symbol", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=True, permute_data=True)

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(table_name=self.table_name, date_st=date_st, date_end=date_end)
        return final_query

    @classmethod
    def get_default_config(cls):
        return {"table_name": "cboe_auxiliary_indices",
                "index_names": ["SPX", "PPUT", "CMBO", "BFLY", "RXM", "VIX", "VXST", "VXMT", "VIX3M"]}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyIndices]


class SQLReaderFundamentalData(SQLReader):
    '''

    Reads corporate fundamental data from the capiq table on redshift given a date range and universe date.

    '''
    PROVIDES_FIELDS = ["fundamental_data"]

    def __init__(self, table_name="fundamentals_short_digitized", date_column="date"):
        base_query = """
        SELECT * FROM
          {table_name}
        WHERE
          date({date_column}) BETWEEN '{date_st}' AND '{date_end}'
          and ticker in (select ticker from whitelist where run_dt = cast('{universe_dt}' as date))
        """
        self.table_name = table_name
        self.date_column = date_column
        SQLReader.__init__(self, base_query, self.date_column, "ticker", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=False, permute_data=False)

    def _post_process_pulled_data(self, df, **kwargs):
        df = df.drop(["sector", "industry"], axis=1)
        df = SQLReader._post_process_pulled_data(self, df, **kwargs)
        df["this_quarter"] = df["date"].apply(lambda x: str(x.year) + "-Q" + str((x.month - 1) / 3 + 1))
        return df

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(table_name=self.table_name, date_st=date_st, date_end=date_end,
                                        date_column=self.date_column, universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_default_config(cls):
        return {"table_name": "fundamentals_short_digitized", "date_column": "date"}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Fundamentals]


class SQLReaderFundamentalDataQuandl(SQLReader):
    '''

    Reads corporate fundamental data from the quandl table on redshift given a date range and universe date.

    '''

    PROVIDES_FIELDS = ["fundamental_data_quandl"]

    def __init__(self, table_name="fundamental_data", date_column="as_of_start"):
        base_query = """
        SELECT * FROM
          {table_name}
        WHERE
          date({date_column}) BETWEEN date('{date_st}') AND date('{date_end}')
          and ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
        """
        self.table_name = table_name
        self.date_column = date_column
        SQLReader.__init__(self, base_query, self.date_column, "ticker", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=False, permute_data=False)

    def _post_process_pulled_data(self, df, **kwargs):
        df = SQLReader._post_process_pulled_data(self, df, **kwargs)
        return df

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt - to_offset("252B")
        date_end = self.task_params.end_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(table_name=self.table_name, date_st=date_st, date_end=date_end,
                                        date_column=self.date_column, universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_default_config(cls):
        return {"table_name": "fundamental_data", "date_column": "as_of_start"}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Fundamentals]


class SQLReaderRavenpackMomentumIndicator(SQLReader):
    '''

    Reads and computes the Ravenpack sentiment momentum indicator from redshift given a date range and universe date.

    '''

    PROVIDES_FIELDS = ["ravenpack_indicator_data"]

    def __init__(self, ndays_slow=91, ndays_fast=28):
        base_query = """
        with ess_data as
        (
        select
          rcm.ticker, re.rp_entity_id, date(marketdata.convert_timezone('UTC','US/Eastern',re.timestamp_utc)) as date, re.ess, re.category,
          case when re.ess>50 then 1 when re.ess<50 then -1 end as ess_indicator
        from
          ravenpack_equities re
        join
          ravenpack_mapping_simplified rcm
        on
          rcm.rp_entity_id = re.rp_entity_id
          AND rcm.as_of_start <= '{run_dt}'
          AND (rcm.as_of_end IS NULL OR rcm.as_of_end >= '{run_dt}')
        where
          ess is not null
          and relevance=100
          and ess!=50
          and timestamp_utc >='{date_st}'
          and timestamp_utc <='{date_end}'
          and ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
         ),
         sentiment_score_counts as
         (
           select
             rp_entity_id, ticker, date, sum(ess_indicator) as D_t
           from
             ess_data
           group by
             rp_entity_id, ticker, date
         ),
        calendar_timeline as
        (
          select
            tc.calendar_date, tc.is_trading_day,ticker
          from
            trading_calendars tc
          join
            (select distinct(ticker) as ticker from ess_data) on 1=1
           where
             tc.asset_class = 'Equity'
             and tc.MIC='XNYS'
             and tc.calendar_date>=date('{date_st}') and tc.calendar_date<=date('{date_end}')
        ),
        calendar_timeline_with_entity as
        (
          select
            tc.ticker, scc.rp_entity_id, tc.calendar_date as date, tc.is_trading_day
          from
            calendar_timeline tc
          left join
            (select distinct ticker, rp_entity_id from sentiment_score_counts) scc
          on tc.ticker=scc.ticker
        ),
        completed_sentiment_score_counts as
        (
          select
            cte.*, coalesce(scc.d_t,0)
          as
            d_t
          from
            calendar_timeline_with_entity cte
          left join
            sentiment_score_counts scc
          on
            cte.ticker=scc.ticker and cte.date=scc.date
          order by date
        ),
        sentiment_with_rolling_stats as
        (
          select
             rp_entity_id, ticker, date,
             avg(cast(D_t as float64)) over (partition by rp_entity_id,ticker order by date rows {ndays_fast} preceding) as mu_dt_fast,
             avg(cast(D_t as float64)) over (partition by rp_entity_id,ticker order by date rows {ndays_slow} preceding) as mu_dt_slow,
             stddev(cast(D_t as float64)) over (partition by rp_entity_id,ticker order by date rows {ndays_fast} preceding) as sigma_dt_fast
          from
            completed_sentiment_score_counts
         ),
         indicator_values as
         (
         select
           rp_entity_id, ticker, cast(date as datetime) as date,
           case when mu_dt_fast!=0 then (mu_dt_fast - mu_dt_slow)/mu_dt_fast else null end as sentiment_oscilator,
           case when sigma_dt_fast!=0 then (mu_dt_fast - mu_dt_slow)/sigma_dt_fast else null end as sentiment_score
         from
           sentiment_with_rolling_stats
        )
        select
          indicator_values.*
        from
          indicator_values
        order by
          rp_entity_id, date
        """
        self.ndays_slow = ndays_slow
        self.ndays_fast = ndays_fast
        SQLReader.__init__(self, base_query, "date", "ticker", "US/Eastern", "US/Eastern",
                           attach_additional_empty_data_day=False, permute_data=True)

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt.tz_convert(
            "UTC") if self.task_params.start_dt.tzinfo else self.task_params.start_dt
        date_end = self.task_params.end_dt.tz_convert(
            "UTC") if self.task_params.end_dt.tzinfo else self.task_params.end_dt
        run_dt = self.task_params.run_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(date_st=date_st, date_end=date_end, run_dt=run_dt, universe_dt=universe_dt,
                                        ndays_slow=self.ndays_slow, ndays_fast=self.ndays_fast)
        return final_query

    def _post_process_pulled_data(self, df, **kwargs):
        results = df.reset_index().sort_values(["ticker", "date"]).set_index("date").groupby("ticker") \
            .apply(lambda x: x.shift(1).fillna(method="ffill").fillna(method="bfill")).reset_index().set_index("index")
        SQLReader._post_process_pulled_data(self, results, **kwargs)
        return results

    @classmethod
    def get_default_config(cls):
        return {"ndays_slow": 91, "ndays_fast": 28}

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Sentiment]


class SQLReaderRavenpackOvernightVolume(SQLReader):
    '''

    Reads the count of the overnight ravenpack news volume from redshift provided a date range and
    universe date.

    '''

    PROVIDES_FIELDS = ["ravenpack_overnight_volume_data"]

    def __init__(self):
        base_query = """
        with ess_data as
        (
        select
          rcm.ticker, re.rp_entity_id, marketdata.convert_timezone('UTC','US/Eastern',re.timestamp_utc) as timestamp_et, re.timestamp_utc, re.ess, re.category,
          case when re.ess>=50 then 1 when re.ess<50 then -1 end as ess_indicator
        from
          ravenpack_equities re
        join
          ravenpack_mapping_simplified rcm
        on
          rcm.rp_entity_id = re.rp_entity_id
          AND rcm.as_of_start <= '{run_dt}'
          AND (rcm.as_of_end IS NULL OR rcm.as_of_end >= '{run_dt}')
        where
          ess is not null
          and relevance=100
          and g_ens=100
          and timestamp_utc >='{date_st}'
          and timestamp_utc <='{date_end}'
          and ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
         ),
        ess_data_with_calendar as
        (
          select
            *
          from
            ess_data
          left join
            trading_calendars tc
          on
            tc.calendar_date = date(timestamp_et)
          where
            tc.asset_class = 'Equity'
            and tc.MIC='XNYS'
        ),
        ess_dates as
        (
          select
            *, case when is_trading_day=0 then next_trading_date when is_trading_day=1 and timestamp_utc<trading_start_utc then calendar_date when is_trading_day=1 and timestamp_utc>trading_end_utc then next_trading_date else null end as ess_count_date
          from
            ess_data_with_calendar
        )
        select rp_entity_id, ticker, cast(date(ess_count_date) as datetime) as date, avg(cast(ess as float64)) as ess, count(ess) as volume
        from ess_dates
        where ess_count_date is not null
        group by rp_entity_id, cast(date(ess_count_date) as datetime), ticker
        """
        SQLReader.__init__(self, base_query, "date", "ticker", "US/Eastern", "US/Eastern",
                           attach_additional_empty_data_day=False, permute_data=True)

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        run_dt = self.task_params.run_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(date_st=date_st, date_end=date_end, run_dt=run_dt, universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Sentiment]


class SQLReaderRavenpackNewsMomentum(SQLReader):
    '''

    Reads the count of the overnight ravenpack news volume from redshift provided a date range and
    universe date.

    '''

    PROVIDES_FIELDS = ["ravenpack_news_momentum"]

    def __init__(self):
        base_query = """

        select

        rp.time_interval_est,
        rp.rp_entity_id,
        rp.ticker,
        rp.rp_group,
        rp.ess,
        rp.ess_count,
        r.ret

        from
        (

            select

            case

            when date_part('hour', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc))::int < 9 or (date_part('hour', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc))::int = 9 and
                                                                                                              date_part('minute', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc))::int <= 45)
            then date_trunc('day', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc)) + interval '9 hours, 45 minutes'

            when date_part('hour', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc))::int >= 16
            then dateadd(day, 1, date_trunc('day', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc))) + interval '9 hours, 45 minutes'

            else date_trunc('hour', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc)) + (date_part('minute', convert_timezone('UTC', 'US/Eastern', e.timestamp_utc))::int / 15 + 1) * interval '15 min'
            end as time_interval_est,

            e.rp_entity_id,
            m.ticker,
            e.rp_group,
            avg(e.ess) as ess,
            count(e.ess) as ess_count

            from ravenpack_equities e
            join ravenpack_mapping_simplified m on m.rp_entity_id = e.rp_entity_id

            where convert_timezone('UTC', 'US/Eastern', e.timestamp_utc) >= '{date_st}'
            and convert_timezone('UTC', 'US/Eastern', e.timestamp_utc) <= '{date_end}'
            and e.product_key = 'DJ-EQ'
            and e.topic = 'business'
            and e.relevance = 100
            and e.ens = 100
            group by time_interval_est, m.ticker, e.rp_entity_id, e.rp_group

        ) rp

        join (

            select

            convert_timezone('UTC', 'US/Eastern', p.cob) as time_interval_est,
            p.ticker,
            p.close / lag(p.close, 1) over(partition by p.ticker order by p.ticker, p.cob) - 1 as ret

            from equity_prices p
            join (select distinct ticker from ravenpack_mapping_simplified) m on m.ticker = p.ticker

            where date(p.cob) >= '{date_st}'
            and date(p.cob) <= '{date_end}'
            and p.as_of_end is null
            and mod(extract(m from p.cob), 15) = 0
            order by p.ticker, time_interval_est

        ) r on r.time_interval_est = rp.time_interval_est and r.ticker = rp.ticker

        order by rp.time_interval_est, rp.ticker
        """
        SQLReader.__init__(self, base_query, "date", "ticker", "US/Eastern", "US/Eastern",
                           attach_additional_empty_data_day=False, permute_data=False)

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        run_dt = self.task_params.run_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(date_st=date_st, date_end=date_end, run_dt=run_dt, universe_dt=universe_dt)
        return final_query

    def _post_process_pulled_data(self, df, **kwargs):
        df["ret"] = pd.to_numeric(df["ret"])
        df["ess"] = pd.to_numeric(df["ess"])
        return df

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Sentiment]


class SQLReaderETFInformation(SQLReader):
    '''

    Reads mapping file from redshift that contains information of the security_type (Equity/ETF/Fund)

    '''

    PROVIDES_FIELDS = ["etf_information"]

    def __init__(self):
        base_query = """
        SELECT
          ticker, security_type
        FROM
          ravenpack_mapping_simplified rcm
        WHERE
          rcm.as_of_start <= '{run_dt}' AND (rcm.as_of_end IS NULL OR rcm.as_of_end >= '{run_dt}')
          and ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
        """
        SQLReader.__init__(self, base_query, None, "ticker", None, None,
                           attach_additional_empty_data_day=False, permute_data=False)

    def _post_process_pulled_data(self, df, **kwargs):
        df["is_etf"] = df["security_type"].apply(lambda x: 0 if x == "Equity" else 1)
        return df

    def compose_query(self, base_query, **kwargs):
        run_dt = self.task_params.run_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(run_dt=run_dt, universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]


class SQLReaderEarningsDates(SQLReader):
    '''

    Reads the historical and projected earning dates from the earnings table on redshift

    '''

    PROVIDES_FIELDS = ["earnings_dates"]

    def __init__(self):
        base_query = """
        SELECT *
        FROM
          earnings
        WHERE
          earnings_datetime_utc between '{date_st}' and '{date_end}'
          AND as_of_start <= '{run_dt}' AND (as_of_end IS NULL OR as_of_end >= '{run_dt}')
          AND ticker in (select ticker from whitelist where run_dt = date('{universe_dt}'))
        """
        SQLReader.__init__(self, base_query, "earnings_datetime_utc", "ticker", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=False, permute_data=True)

    def compose_query(self, base_query, **kwargs):
        date_st = self.task_params.start_dt
        date_end = self.task_params.end_dt
        run_dt = self.task_params.run_dt
        universe_dt = self.task_params.universe_dt
        final_query = base_query.format(date_st=date_st, date_end=date_end, run_dt=run_dt, universe_dt=universe_dt)
        return final_query

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Earnings]


class SQLReaderSectorBenchmarkMap(SQLReader):
    '''

    Reads from redshift the Sector Industry and Benchmark ETF designation per ticker

    '''

    PROVIDES_FIELDS = ["sector_industry_mapping"]

    def __init__(self):
        base_query = """
        select ticker, sector, industry, etf
        from sector_benchmark_map
        """
        SQLReader.__init__(self, base_query, None, "ticker", None, None,
                           attach_additional_empty_data_day=False, permute_data=False)

    def compose_query(self, base_query, **kwargs):
        return base_query

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]


class S3SecurityMasterReader(S3Reader):
    '''

    Downloads security master from s3 location

    '''

    PROVIDES_FIELDS = ["security_master"]

    def __init__(self, bucket, key):
        self.data = None
        S3Reader.__init__(self, bucket, key)

    def _post_process_pulled_data(self, data, **kwargs):
        data.drop(data.columns[self.index_col], axis=1, inplace=True)
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)

        if self.task_params.run_mode == TaskflowPipelineRunMode.Test:
            query = "select ticker from whitelist where run_dt in ('{0}') and as_of_end is null" \
                .format(self.task_params.universe_dt)
            universe_tickers = list(pd.read_sql(query, get_redshift_engine())["ticker"])
            data = data[data["ticker"].isin(universe_tickers)].reset_index(drop=True)
        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {"bucket": "dcm-data-temp", "key": "jack/security_master.csv"}


class S3GANUniverseReader(S3Reader):
    '''

    Downloads security master from s3 location

    '''

    PROVIDES_FIELDS = ["current_gan_universe"]

    def __init__(self, bucket, key):
        self.data = None
        S3Reader.__init__(self, bucket, key, None)

    def _post_process_pulled_data(self, data, **kwargs):
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {"bucket": "dcm-data-temp", "key": "jack/current_gan_training_universe.csv"}


class S3SASBMappingReader(S3Reader):
    '''

    Downloads security master from s3 location

    '''

    PROVIDES_FIELDS = ["sasb_mapping"]

    def __init__(self, bucket, key):
        self.data = None
        S3Reader.__init__(self, bucket, key, index_col=None)

    def _post_process_pulled_data(self, data, **kwargs):
        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {"bucket": "dcm-data-temp", "key": "jack/security_master.csv"}


class SQLSecurityMasterReader(SQLReader):
    '''

    Downloads security master from s3 location

    '''
    ENGINE = "Snowflake"
    PROVIDES_FIELDS = ["security_master"]

    def __init__(self):
        base_query = """
        select * from SECURITY_MASTER_20190719
        """
        SQLReader.__init__(self, base_query, "date", "dcm_security_id", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=False, permute_data=False)

    def compose_query(self, base_query, **kwargs):
        return base_query

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def _post_process_pulled_data(self, data, **kwargs):
        expected_columns = ["index", "dcm_security_id", "ticker", "cusip", "isin", "exchange", "trading_currency",
                            "company_name", "security_name", "td_security_id", "ciq_company_id", "ciq_trading_item_id",
                            "rp_entity_id", "is_active", "latest_update", "quandl_permaticker", "quandl_id_cusip",
                            "quandl_matched_by_cusip", "quandl_id_name", "quandl_matched_by_name", "quandl_name",
                            "quandl_ticker", "name", "manually_reconciled"]
        data.columns = data.columns.str.lower()
        replacements = {k.lower(): k for k in expected_columns}
        data.rename(columns=replacements, inplace=True)

        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        data.drop("index", axis=1, inplace=True)

        # TODO: This is a hack for unit testing fix later (should use security id instead)
        if self.task_params.run_mode == TaskflowPipelineRunMode.Test:
            additional_query = "select ticker from whitelist where run_dt in ('{0}') and as_of_end is null".format(
                self.task_params.universe_dt)
            engine = get_redshift_engine()
            universe_tickers = list(pd.read_sql(additional_query, engine)["ticker"])
            data = data[data["ticker"].isin(universe_tickers)].reset_index(drop=True)

        return data

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {}


class S3IndustryMappingReader(S3Reader):
    '''

    Downloads industry mapping from s3 location

    '''

    REQUIRES_FIELDS = ["security_master"]
    PROVIDES_FIELDS = ["industry_map"]

    def __init__(self, bucket, key):
        self.data = None
        S3Reader.__init__(self, bucket, key)

    def _post_process_pulled_data(self, data, **kwargs):
        data.drop(data.columns[self.index_col], axis=1, inplace=True)
        sec_master = kwargs["security_master"]
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        data = data[data["dcm_security_id"].isin(list(sec_master["dcm_security_id"]))] \
            .reset_index(drop=True)
        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {"bucket": "dcm-data-temp", "key": "jack/industry_map.csv"}


class SQLIndustryMappingReader(SQLReader):
    REQUIRES_FIELDS = ["security_master"]
    PROVIDES_FIELDS = ["industry_map"]

    def __init__(self):
        base_query = """
        select * from industry_mapping_20190719
        """
        SQLReader.__init__(self, base_query, "date", "dcm_security_id", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=False, permute_data=False)

    def compose_query(self, base_query, **kwargs):
        return base_query

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def _post_process_pulled_data(self, data, **kwargs):
        expected_columns = ["index", "Ticker", "Security", "GICS_Sector", "GICS_Industry", "GICS_IndustryGroup",
                            "GICS_SubIndustry", "Sector", "Industry", "IndustryGroup", "SubIndustry", "dcm_security_id"]
        data.columns = data.columns.str.lower()
        replacements = {k.lower(): k for k in expected_columns}
        data.rename(columns=replacements, inplace=True)

        data.drop("index", axis=1, inplace=True)
        sec_master = kwargs["security_master"]
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        data = data[data["dcm_security_id"].isin(list(sec_master["dcm_security_id"]))].reset_index(drop=True)
        return data

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {}


class S3RussellComponentReader(DataReader):
    '''

    Downloads industry mapping from s3 location

    '''

    REQUIRES_FIELDS = ["security_master"]
    PROVIDES_FIELDS = ["russell_components"]

    def __init__(self, bucket, r3k_key, r1k_key):
        self.data = None
        self.r1k = None
        self.bucket = bucket
        self.r3k_key = r3k_key
        self.r1k_key = r1k_key
        DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def do_step_action(self, **kwargs):
        sec_master = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        s3_client = storage.Client()
        bucket = s3_client.bucket(self.bucket)
        blob = bucket.blob(self.r3k_key)
        file_content = io.BytesIO(blob.download_as_string())
        r3k = pd.read_csv(file_content, index_col=0)
        r3k.columns = r3k.columns.str.lower()
        r3k['date'] = pd.to_datetime(r3k['date'])
        r3k[['wt-r1', 'wt-r1g', 'wt-r1v']] = r3k[['wt-r1', 'wt-r1g', 'wt-r1v']].fillna(0)

        blob = bucket.blob(self.r1k_key)
        file_content = io.BytesIO(blob.download_as_string())
        r1k = pd.read_csv(file_content, index_col=0)
        r1k.columns = r1k.columns.str.lower()
        r1k['date'] = pd.to_datetime(r1k['date'])

        r1k["cusip"] = r1k["cusip"].astype(str)
        r3k["cusip"] = r3k["cusip"].astype(str)
        r1k["dcm_security_id"] = r1k["dcm_security_id"].astype(int)
        df = pd.merge(r3k, r1k, how='left', on=['date', 'cusip'])
        df = df.drop_duplicates()
        df = df[df["dcm_security_id"].isin(list(sec_master["dcm_security_id"]))].reset_index(drop=True)
        df = df.drop_duplicates(["date", "ticker"], keep="last").reset_index(drop=True)
        self.data = df
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        pass

    def _pull_data(self, **kwargs):
        pass

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def _post_process_pulled_data(self, data, **kwargs):
        pass

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.SecurityReference]

    @classmethod
    def get_default_config(cls):
        return {"bucket": "dcm-data-temp", "r3k_key": "jack/r3k.csv", "r1k_key": "jack/r1k.csv"}


class S3RawQuandlDataReader(S3Reader):
    '''

    Downloads raw quandl data from s3 location

    '''
    REQUIRES_FIELDS = ["security_master"]
    PROVIDES_FIELDS = ["raw_quandl_data"]

    def __init__(self, bucket, key):
        self.data = None
        S3Reader.__init__(self, bucket, key, False)

    def _post_process_pulled_data(self, raw, **kwargs):
        raw = raw.reset_index(drop=True)
        sec_master = kwargs["security_master"]
        mapping = sec_master[['dcm_security_id', 'quandl_ticker']]
        mapping.columns = ['dcm_security_id', 'ticker']
        raw = pd.merge(raw, mapping, how='inner', on='ticker')
        raw.drop('ticker', axis=1, inplace=True)
        start_date_str = str(self.task_params.start_dt.date())
        end_date_str = str(self.task_params.end_dt.date())
        raw = raw[(raw['datekey'] >= start_date_str) & (raw['datekey'] <= end_date_str)]
        raw = raw[raw['dimension'].isin(['ARQ', 'ART'])].reset_index(drop=True)
        data = raw
        data["dcm_security_id"] = data["dcm_security_id"].astype(int)
        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Fundamentals]

    @classmethod
    def get_default_config(cls):
        return {"bucket": "dcm-data-temp", "key": "jack/SHARADAR_SF1.csv"}


class SQLMinuteToDailyEquityPrices(SQLReader):
    '''

    Creates daily bars from minute data (with dcm_security_id identifier)

    '''
    PROVIDES_FIELDS = ["daily_price_data"]
    REQUIRES_FIELDS = ["security_master"]

    def __init__(self):
        base_query = """
        with equity_data_with_minute_indicator as
           (
           select distinct
            dcm_security_id,
            ticker, cob, date(cob) as date,
            datetime_diff(datetime_sub(marketdata.convert_timezone('UTC','US/Eastern',cob), interval (9 * 60 + 30) minute), date(cob), minute) as minute_in_day,
            close, open, high, low, volume
           from
            equity_prices
           where
            as_of_end is null
            and date(cob)>=date('{0}')
            and date(cob)<=date('{1}')
            and dcm_security_id in {2}
           ),
           open_quotes as
           (
             select dcm_security_id, ticker, date, open from equity_data_with_minute_indicator where minute_in_day =1
           ),
           close_quotes as
           (
             select dcm_security_id, ticker, date, close from equity_data_with_minute_indicator where minute_in_day =390
           ),
           misc_quotes as
           (
             select dcm_security_id, ticker, date, sum(volume) as volume, max(high) as high, min(low) as low
             from equity_data_with_minute_indicator group by dcm_security_id, ticker, date
           )
            select o.dcm_security_id, o.ticker, cast(o.date as datetime) as date, o.open, c.close, m.high, m.low, m.volume
            from open_quotes o
            left join close_quotes c
            on o.dcm_security_id=c.dcm_security_id and o.ticker=c.ticker and o.date=c.date
            left join misc_quotes m
            on o.dcm_security_id=m.dcm_security_id and  o.ticker=m.ticker and o.date=m.date
            order by dcm_security_id, ticker, date
        """
        SQLReader.__init__(self, base_query, "date", "ticker", "US/Eastern", "UTC",
                           attach_additional_empty_data_day=False, permute_data=True)

    def compose_query(self, base_query, **kwargs):
        sec_master = kwargs["security_master"]
        start_dt = self.task_params.start_dt
        end_dt = self.task_params.end_dt
        universe = sec_master[['dcm_security_id', 'ticker']]
        universe['dcm_security_id'] = universe['dcm_security_id'].astype(int)
        final_query = base_query.format(start_dt, end_dt, tuple(universe["dcm_security_id"]))
        return final_query

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def _post_process_pulled_data(self, price, **kwargs):
        sec_master = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        price = price[price.groupby('dcm_security_id')['date'].transform('count') > 3]
        universe = sec_master[['dcm_security_id', 'ticker']]
        universe['dcm_security_id'] = universe['dcm_security_id'].astype(int)
        price = pd.merge(universe, price, how='inner', on=['dcm_security_id', 'ticker'])
        price['dcm_security_id'] = price['dcm_security_id'].astype(int)
        price['date'] = pd.to_datetime(price['date'])
        price.drop('ticker', axis=1, inplace=True)
        price.rename(columns={"dcm_security_id": "ticker"}, inplace=True)
        return SQLReader._post_process_pulled_data(self, price, **kwargs)

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyEquities]

    @classmethod
    def get_default_config(cls):
        return {}


class H5ESGDataReader(H5Reader):
    '''

    Reads H5 file of ESG data (TruvalueLabs)

    '''
    PROVIDES_FIELDS = ["esg_data"]

    def __init__(self, files_pattern, h5_data_key):
        self.files_pattern = files_pattern
        self.h5_data_key = h5_data_key
        self.files_to_read = glob(self.files_pattern)
        self.data = None
        DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def _get_return_types_for_permutations(self, data, **kwargs):
        raise RuntimeError("{0}._get_return_types_for_permutations cannot be called with "
                           "H5ESGDataReader. Logic has to be developed".format(self.__class__))

    @classmethod
    def get_data_requirement_type(cls):
        return [DRDataTypes.Fundamentals]

    @classmethod
    def get_default_config(cls):
        return {"files_pattern": "tvlid_data_consolidated_dcm_security_id_strict.h5", "h5_data_key": "df"}


class S3EconTransformationReader(S3Reader):
    '''

    Downloads security master from s3 location

    '''

    PROVIDES_FIELDS = ["econ_transformation"]

    def __init__(self, sector_etfs, bucket, key):
        self.data = None
        self.sector_etfs = sector_etfs
        S3Reader.__init__(self, bucket, key)

    def _pull_data(self, **kwargs):
        bucket = self.s3_client.bucket(self.bucket)
        blob = bucket.blob(self.key)
        file_content = io.BytesIO(blob.download_as_string())
        data = pd.read_csv(file_content)
        transform_dict = {}
        for ticker in self.sector_etfs:
            close_col = "{0}_close".format(ticker)
            vol_col = "{0}_volume".format(ticker)
            transform_dict[close_col] = 5
            transform_dict[vol_col] = 5
        df = pd.DataFrame(list(transform_dict.values()), index=list(transform_dict.keys())).reset_index()
        df = df.rename(columns={"index": data.columns[0], 0: data.columns[1]})
        data = pd.concat([data, df], axis=0, ignore_index=True)

        return data

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyEquities]

    @classmethod
    def get_default_config(cls):
        return {"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                "bucket": "dcm-data-temp",
                "key": "jack/econ_transform_definitions.csv"}


class DownloadEconomicData(DataReader):
    '''

    Reads Economic Data From FRED API

    '''
    REQUIRES_FIELDS = ["econ_transformation"]
    PROVIDES_FIELDS = ["econ_data"]

    def __init__(self, start_date, end_date, sector_etfs, use_latest=True):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.data = None
        # self.fred_api_key = get_config()["credentials"]
        self.fred_api_key = "8ef69246bd7866b9476093e6f4141783"
        self.sector_etfs = sector_etfs
        self.use_latest = use_latest
        DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        self.fred_connection = Fred(api_key=self.fred_api_key)

    def _create_data_names(self, econ_trans):
        econ_trans = econ_trans.set_index("Unnamed: 0")
        transform_dict = econ_trans["transform"].to_dict()
        data_names = dict(zip(transform_dict.keys(), transform_dict.keys()))
        data_names["CMRMTSPLx"] = "CMRMTSPL"
        data_names["RETAILx"] = "RRSFS"
        data_names["HWI"] = "TEMPHELPS"
        data_names["CLAIMSx"] = "ICSA"
        data_names["AMDMNOx"] = "DGORDER"
        data_names["ANDENOx"] = "NEWORDER"
        data_names["AMDMUOx"] = "AMDMUO"
        data_names["BUSINVx"] = "BUSINV"
        data_names["ISRATIOx"] = "ISRATIO"
        data_names["CONSPI"] = "NONREVSL"
        data_names["S&P 500"] = "SP500"
        data_names["CP3Mx"] = "DCPF3M"
        data_names["COMPAPFFx"] = "CPFF"
        data_names["EXSZUSx"] = "DEXSZUS"
        data_names["EXJPUSx"] = "DEXJPUS"
        data_names["EXUSUKx"] = "DEXUSUK"
        data_names["EXCAUSx"] = "DEXCAUS"
        data_names["OILPRICEx"] = "DCOILWTICO"
        data_names["UMCSENTx"] = "UMCSENT"
        data_names["VXOCLSx"] = "VXOCLS"
        data_names["CUMFNS"] = "MCUMFN"
        data_names = dict(zip(data_names.values(), data_names.keys()))
        return data_names

    def _pull_data(self, **kwargs):
        econ_trans = kwargs["econ_transformation"]
        data_names = self._create_data_names(econ_trans)
        for etf_name in self.sector_etfs:
            data_names.pop(etf_name + "_close")
            data_names.pop(etf_name + "_volume")
        all_data = []
        for concept in data_names:
            try:
                if self.use_latest:
                    data = self.fred_connection.get_series(concept)
                else:
                    if concept != 'SP500':
                        # try to fetch the first-release data first for a Fred series to avoid any look ahead bias
                        # if some problem, then look for the latest known data for the series
                        try:
                            data = self.fred_connection.get_series_first_release(concept)
                        except:
                            data = self.fred_connection.get_series_latest_release(concept)
                    else:
                        data = self.fred_connection.get_series(concept)
                data = data.ix[self.start_date:self.end_date]
                data.name = data_names[concept]
                data.index = data.index.normalize()
                data = data.to_frame()
                data = data.groupby(data.index).last()
                all_data.append(data)
            except:
                print("****** PROBLEM WITH : {0}".format(concept))
        result = pd.concat(all_data, axis=1)
        result = result.ix[list(map(lambda x: marketTimeline.isTradingDay(x), result.index))]
        result = result.fillna(method="ffill")
        result.index.name = "date"
        return result

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyEquities]

    @classmethod
    def get_default_config(cls):
        return {"start_date": "2015-01-01", "end_date": "2019-10-05",
                "sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"], "use_latest": True}


class YahooDailyPriceReader(DataReader):
    PROVIDES_FIELDS = ["yahoo_daily_price_data"]

    def __init__(self, sector_etfs, start_date, end_date):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.data = None
        self.sector_etfs = sector_etfs
        DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def _prepare_to_pull_data(self, **kwargs):
        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

    def _get_data_lineage(self):
        pass

    def _pull_data(self, **kwargs):
        df_1 = []
        for ticker in self.sector_etfs:
            try:
                close_col = "{0}_close".format(ticker)
                vol_col = "{0}_volume".format(ticker)
                df = web.get_data_yahoo(ticker, self.start_date, self.end_date
                                        ).fillna(method="ffill")[["Adj Close", "Volume"]].rename(
                    columns={"Adj Close": close_col,
                             "Volume": vol_col})
                df = df[~df.index.duplicated(keep='last')]
                df_1.append(df)
            except:
                print("Download failure for {0}".format(ticker))
        etf_prices = pd.concat(df_1, axis=1)

        return etf_prices

    def _get_return_types_for_permutations(self, data, **kwargs):
        pass

    def get_data_requirement_type(cls):
        return [DRDataTypes.DailyEquities]

    @classmethod
    def get_default_config(cls):
        return {"sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
                "start_date": "1997-01-01",
                "end_date": "2019-10-05"}


