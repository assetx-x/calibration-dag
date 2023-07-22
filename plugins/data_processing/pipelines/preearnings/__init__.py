from datetime import datetime, time, timedelta, date
from uuid import uuid4

from luigi import Task
from luigi import WrapperTask, ExternalTask
import pandas as pd

from base import RedshiftTarget, S3Target, DCMTaskParams, credentials_conf
from pipelines.earnings_ingestion import EarningsIngestionPipeline
from pipelines.fundamentals_ingestion import FundamentalsIngestionPipeline, FundamentalsLongStep, FundamentalsLong
from pipelines.preearnings.calculate_rules_stats import CalculateRulesStatsTask
from pipelines.preearnings.calibrate_earnings import CalibrateEarningsTask
from pipelines.preearnings.discover_rules import DiscoverRulesTask
from pipelines.preearnings.enrich_tickers_with_near_earnings import EnrichTickersWithNearEarningsTask
from pipelines.preearnings.prepare_earnings import PrepareEarningsTask
from pipelines.preearnings.quarter_end_events import GenerateQuarterEndEventsTask
from pipelines.preearnings.score_cuts import ScoreCutsTask
from tasks_common.deep_impact import DeepImpactTask
from tasks_common.digitize import DigitizeTask
from tasks_common.enrich import EnrichTask
from tasks_common.redshift_view import RedshiftViewTask

config = {
    "offsets": ["-11B", "-7B"],
    "udfs": ["get_return"],
    "holes_threshold": 10000,
    "max_cut_combination_size": 3
}

s3_url_template = 's3://dcm-data-test/boris/preearnings/{date}/{file}'

def s3_url(date_only, file_name):
    return s3_url_template.format(date=date_only, file=file_name)


class PriceData(ExternalTask):
    def output(self):
        return RedshiftTarget("equity_prices")


class EarningsData(ExternalTask):
    def output(self):
        return RedshiftTarget("earnings")


class TickersUniverse(ExternalTask):
    def output(self):
        # return S3Target("s3://dcm-data-test/fluder/preearnings/whitelist.txt")
        return S3Target("s3://dcm-data-test/chris/universe.txt")


class SectorIndustryMappingData(ExternalTask):
    def output(self):
        return RedshiftTarget("sector_benchmark_map")


class PrepareEarningsStep(PrepareEarningsTask):
    def requires(self):
        return {
            "earnings_data": EarningsData(),
            "universe": TickersUniverse()
        }

    def output(self):
        return {
            "past_quarters_earnings": S3Target(s3_url(self.date_only(), "past_quarters_earnings.csv")),
            "current_quarter_earnings": S3Target(s3_url(self.date_only(), "current_quarter_earnings.csv"))
        }


class GenerateQuarterEndEventsStep(GenerateQuarterEndEventsTask):
    def requires(self):
        return {
            "price_data": PriceData(),
            "ticker_list": TickersUniverse()
        }

    def output(self):
        return S3Target(s3_url(self.date_only(), "quarter_end_events.csv"))


class PastQuartersPerformanceDeepImpactStep(DeepImpactTask):
    executor_memory = "48G"
    executor_cpu_cores = 4

    offsets = ["-1Q", "-2Q", "-3Q", "-4Q"]
    udfs = ["get_return"]
    enrichment_plan = []

    def requires(self):
        return {
            "price_data": PriceData(),
            "events_data": GenerateQuarterEndEventsStep(self.date)
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_past_quarters_performance" % self.date_ymd)


class PastQuartersPerformancePivotStep(DCMTaskParams, RedshiftViewTask):
    query = """
    SELECT
        ticker,
        event_time,
        MAX(CASE WHEN "offset" = '-1Q' THEN result ELSE NULL END) AS Q1,
        MAX(CASE WHEN "offset" = '-2Q' THEN result ELSE NULL END) AS Q2,
        MAX(CASE WHEN "offset" = '-3Q' THEN result ELSE NULL END) AS Q3,
        MAX(CASE WHEN "offset" = '-4Q' THEN result ELSE NULL END) AS Q4
    FROM {deep_impact_table}
    WHERE udf_name = 'get_return'
    GROUP BY ticker, event_time
    """

    def requires(self):
        return PastQuartersPerformanceDeepImpactStep(self.date)

    def input(self):
        return {
            "deep_impact_table": super(PastQuartersPerformancePivotStep, self).input()
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_past_quarters_performance_pivot" % self.date_ymd)


class PastQuartersPerformancePrepareStep(DCMTaskParams, Task):
    def requires(self):
        return {
            "sector_mapping": SectorIndustryMappingData(),
            "pivot": PastQuartersPerformancePivotStep(self.date)
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_past_quarters_performance_prepared" % self.date_ymd)

    def run(self):
        self.logger.info("PQP Prepare: loading sector mapping")
        with self.input()["sector_mapping"].engine() as engine:
            sector_mapping_df = pd.read_sql(self.input()["sector_mapping"].table, engine)
        self.logger.info("PQP Prepare: loading PQP pivot table")
        with self.input()["pivot"].engine() as engine:
            pivot_df = pd.read_sql(self.input()["pivot"].table, engine)

        self.logger.info("PQP Prepare: merging with sector mapping")
        pivot_df = pd.merge(pivot_df, sector_mapping_df, how="left", on="ticker")
        pivot_df["id"] = pivot_df.apply(lambda row: str(uuid4()), axis=1)

        with self.output().engine() as engine:
            self.logger.info("PQP Prepare: saving merged results to Redshift")
            pivot_df.to_redshift(
                self.output().table,
                engine,
                "dcm-data-temp",
                index=False,
                if_exists="replace",
                aws_access_key_id=credentials_conf["s3"]["access_key"],
                aws_secret_access_key=credentials_conf["s3"]["secret_key"]
            )
        self.output().touch()


class PastQuartersPerformanceDigitizeStep(DigitizeTask):
    @property
    def group_mapping(self):
        return {
            "sector_date": {
                "columns": ["q1", "q2", "q3", "q4"],
                "quantiles": [0.0, 0.05, 0.10, 0.20, 0.5, 0.80, 0.90, 0.95, 1.0],
                "group_by": ["sector", "event_time"],
                "suffix": "_sector_date"
            }
        }

    def requires(self):
        return {
            "data": PastQuartersPerformancePrepareStep(self.date)
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_past_quarters_performance_digitized" % self.date_ymd)


class MainDeepImpactStep(DeepImpactTask):
    executor_memory = "20G"
    driver_memory = "20G"
    offsets = config["offsets"]
    udfs = config["udfs"]

    def requires(self):
        return {
            "events_data": PrepareEarningsStep(self.date),
            "price_data": PriceData(),
            "benchmark": SectorIndustryMappingData()
        }

    def input(self):
        return {
            "events_data": super(MainDeepImpactStep, self).input()["events_data"]["past_quarters_earnings"],
            "price_data": super(MainDeepImpactStep, self).input()["price_data"],
            "benchmark": super(MainDeepImpactStep, self).input()["benchmark"]
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_main_deep_impact_result" % self.date_ymd)


class FundamentalsQAStep(DCMTaskParams, RedshiftViewTask):
    @property
    def query(self):
        as_of_end_clause = "as_of_end IS NULL"

        return """
        WITH holes AS (
            SELECT metric, ticker, count(1) AS holes
            FROM {{fundamentals_table}}
            WHERE ({as_of_end_clause}) AND ((raw_value IS NULL AND value IS NULL) OR
                (raw_value IS NOT NULL AND raw_value NOT SIMILAR TO '-?\\\\d+(.\\\\d+)?'))
            GROUP BY metric, ticker
        ),
        total AS (
            SELECT metric, ticker, count(1) AS tot
            FROM {{fundamentals_table}}
            WHERE {as_of_end_clause}
            GROUP BY metric, ticker
        ),
        metrics_ticker_summary AS (
            SELECT total.metric, total.ticker, total.tot, holes.holes
            FROM total
            LEFT JOIN holes ON total.ticker = holes.ticker AND total.metric = holes.metric
            ORDER BY holes desc, metric
        )
        SELECT metric, sum(tot) AS tot, sum(holes) AS holes
        FROM metrics_ticker_summary
        GROUP BY metric
        ORDER BY holes;
        """.format(as_of_end_clause=as_of_end_clause)

    def requires(self):
        return FundamentalsLong()

    def input(self):
        return {
            "fundamentals_table": super(FundamentalsQAStep, self).input()
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_fundamentals_qa" % self.date_ymd)


class MainEnrichStep(EnrichTask):
    executor_memory = "40G"

    @property
    def plan(self):
        with self.input()["fundamentals_qa"].engine() as engine:
            whitelist_fundamentals = [row["metric"] for row in engine.execute("SELECT metric FROM %s WHERE holes < %s" % (
                self.input()["fundamentals_qa"].table,
                config["holes_threshold"]
            ))]
        whitelist_q = ["q1", "q2", "q3", "q4"]

        for x in ["_sector_date"]:
            whitelist_fundamentals += [m.lower() + x for m in whitelist_fundamentals]
        for x in ["_sector_date"]:
            whitelist_q += [m + x for m in whitelist_q]


        return [
            {
                "source": "fundamentals_data",
                "left_on": ["ticker"],
                "right_on": ["ticker"],
                "approximate_left_on": "event_time",
                "approximate_right_on": "date",
                "filter_fields": whitelist_fundamentals
            },
            {
                "source": "sector_industry_mapping",
                "left_on": ["ticker"],
                "right_on": ["ticker"],
                "approximate_left_on": None,
                "approximate_right_on": None,
            },
            {
                "source": "past_quarters_performance_pivot",
                "left_on": ["ticker"],
                "right_on": ["ticker"],
                "approximate_left_on": "event_time",
                "approximate_right_on": "event_time",
                "filter_fields": whitelist_q
            }
        ]

    def requires(self):
        return {
            "deep_impact_result": MainDeepImpactStep(self.date),
            "fundamentals_data": FundamentalsIngestionPipeline(),
            "sector_industry_mapping": SectorIndustryMappingData(),
            "past_quarters_performance_pivot": PastQuartersPerformanceDigitizeStep(self.date),
            "fundamentals_qa": FundamentalsQAStep(self.date)
        }

    def input(self):
        return {
            "data": super(MainEnrichStep, self).input()["deep_impact_result"],
            "fundamentals_qa": super(MainEnrichStep, self).input()["fundamentals_qa"],
            "sources": {
                "fundamentals_data": super(MainEnrichStep, self).input()["fundamentals_data"],
                "sector_industry_mapping": super(MainEnrichStep, self).input()["sector_industry_mapping"],
                "past_quarters_performance_pivot":
                    super(MainEnrichStep, self).input()["past_quarters_performance_pivot"]
            }
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_main_deep_impact_enriched_result" % self.date_ymd)


class ScoreCutsStep(ScoreCutsTask):
    max_size = config["max_cut_combination_size"]
    offsets = config["offsets"]
    udfs = config["udfs"]

    @property
    def fundamentals(self):
        with self.input()["fundamentals_qa"].engine() as engine:
            whitelist_metrics = [
                row["metric"] for row in
                engine.execute("SELECT metric FROM %s WHERE holes < %s" % (
                    self.input()["fundamentals_qa"].table,
                    config["holes_threshold"]
                ))
            ]
        print(whitelist_metrics)

        return ["%s_sector_date" % metric.lower() for metric in whitelist_metrics] + [
            "q1_sector_date",
            "q2_sector_date",
            "q3_sector_date",
            "q4_sector_date"
        ]

    def requires(self):
        return {
            "digitized_result": MainEnrichStep(self.date),
            "fundamentals_qa": FundamentalsQAStep(self.date),
            "sector_mapping": SectorIndustryMappingData()
        }

    def output(self):
        return S3Target(s3_url(self.date_only(), "scored_cuts.csv"))


class DiscoverRulesStep(DiscoverRulesTask):
    def requires(self):
        return ScoreCutsStep(self.date)

    def output(self):
        return S3Target(s3_url(self.date_only(), "rules.csv"))


class CalculateRulesStatsStep(CalculateRulesStatsTask):
    def requires(self):
        return {
            "rules": DiscoverRulesStep(self.date),
            "digitized_deep_impact_data": DigitizeStep(self.date)
        }

    def output(self):
        return S3Target(s3_url(self.date_only(), "rules_with_stats.csv"))


class EnrichTickersWithNearEarningsStep(EnrichTask):
    executor_memory = "40G"

    @property
    def plan(self):
        with self.input()["fundamentals_qa"].engine() as engine:
            whitelist_fundamentals = [row["metric"].lower() for row in engine.execute("SELECT metric FROM %s WHERE holes < %s" % (
                self.input()["fundamentals_qa"].table,
                config["holes_threshold"]
            ))]

        whitelist_q = ["q1", "q2", "q3", "q4"]

        for x in ["_sector_date"]:
            whitelist_fundamentals += [m.lower() + x for m in whitelist_fundamentals]
        for x in ["_sector_date"]:
            whitelist_q += [m + x for m in whitelist_q]

        return [
            {
                "source": "fundamentals_data",
                "left_on": ["Ticker"],
                "right_on": ["ticker"],
                "approximate_left_on": "Timestamp",
                "approximate_right_on": "date",
                "filter_fields": whitelist_fundamentals
            },
            {
                "source": "sector_industry_mapping",
                "left_on": ["Ticker"],
                "right_on": ["ticker"],
                "approximate_left_on": None,
                "approximate_right_on": None,
            },
            {
                "source": "past_quarters_performance_pivot",
                "left_on": ["Ticker"],
                "right_on": ["ticker"],
                "approximate_left_on": "Timestamp",
                "approximate_right_on": "event_time",
            }
        ]

    def requires(self):
        return {
            "earnings_data": PrepareEarningsStep(self.date),
            "past_quarters_performance_pivot": PastQuartersPerformancePivotStep(self.date),
            "sector_industry_mapping": SectorIndustryMappingData(),
            "fundamentals_data": FundamentalsIngestionPipeline(date=self.date),
            "fundamentals_qa": FundamentalsQAStep(self.date)
        }

    def input(self):
        return {
            "data": super(EnrichTickersWithNearEarningsStep, self).input()["earnings_data"]["current_quarter_earnings"],
            "fundamentals_qa": super(EnrichTickersWithNearEarningsStep, self).input()["fundamentals_qa"],
            "sources": {
                "fundamentals_data":
                    super(EnrichTickersWithNearEarningsStep, self).input()["fundamentals_data"],
                "past_quarters_performance_pivot":
                    super(EnrichTickersWithNearEarningsStep, self).input()["past_quarters_performance_pivot"],
                "sector_industry_mapping":
                    super(EnrichTickersWithNearEarningsStep, self).input()["sector_industry_mapping"]
            }
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_enriched_tickers" % self.date_ymd)


class DigitizeEnrichedTickersStep(DigitizeTask):
    @property
    def group_mapping(self):
        with self.input()["fundamentals_qa"].engine() as engine:
            whitelist_metrics = [
                row["metric"].lower() for row in
                engine.execute("SELECT metric FROM %s WHERE holes < %s" % (
                    self.input()["fundamentals_qa"].table,
                    config["holes_threshold"]
                ))
            ]

        return {
            # "fundamentals": {
            #     "columns": whitelist_metrics,
            #     "quantiles": [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
            #     "group_by": ["sector", "quarter"],
            #     "suffix": "_sector_date"
            # },
            "past_quarters_performance/sector_date": {
                "columns": ["q1", "q2", "q3", "q4"],
                "quantiles": [0.0, 0.05, 0.10, 0.20, 0.5, 0.80, 0.90, 0.95, 1.0],
                "group_by": ["sector", "quarter"],
                "suffix": "_sector_date"
            },
        }

    def requires(self):
        return {
            "data": EnrichTickersWithNearEarningsStep(self.date),
            "fundamentals_qa": FundamentalsQAStep(self.date)
        }

    def output(self):
        return RedshiftTarget("preearnings_asof_%s_enriched_tickers_digitized" % self.date_ymd)


class CalibrateEarningsStep(CalibrateEarningsTask):
    def requires(self):
        return {
            "rules": CalculateRulesStatsStep(self.date),
            "enriched_tickers": DigitizeEnrichedTickersStep(self.date)
        }

    def output(self):
        return {
            "calibration": S3Target(s3_url(self.date_only(), "calibration.csv")),
            "possible_best_rules_for_each_sample": S3Target(
                s3_url(self.date_only(), "possible_best_rules_for_each_sample.csv"))
        }


class PreEarningsPipeline(DCMTaskParams, WrapperTask):
    @staticmethod
    def _get_quarter_start_date(_date=pd.Timestamp.today()):
        return pd.to_datetime(pd.Timestamp(_date) - pd.offsets.QuarterEnd() + timedelta(1))

    def __init__(self, *args, **kwargs):
        kwargs["date"] = kwargs.get("date", self._get_quarter_start_date())

        super(PreEarningsPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        # yield PrepareEarningsStep(self.date)
        #   yield GenerateQuarterEndEventsStep(self.date)
        #   yield PastQuartersPerformanceDeepImpactStep(self.date)
        yield PastQuartersPerformanceDigitizeStep(self.date)
        #     yield MainDeepImpactStep(self.date)
        #   yield MainEnrichStep(self.date)
        #     yield ScoreCutsStep(self.date)
        #   yield EnrichTickersWithNearEarningsStep(self.date)
        # yield DigitizeEnrichedTickersStep(self.date)

        # yield FundamentalsIngestionPipeline()

        # yield FundamentalsQAStep(self.date)
        # yield MainEnrichStep(self.date)
        # yield PastQuartersPerformanceDigitizeStep(self.date)
        # yield CalibrateEarningsStep(self.date)
        # yield CalibrateEarningsStep(self.date)
