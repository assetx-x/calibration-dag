import os
from datetime import datetime, timedelta

from luigi import WrapperTask, ExternalTask, LocalTarget, Task

from base import DCMTaskParams, RedshiftTarget, S3Target
from pipelines.fundamentals_ingestion.convert_raw_fundamentals import ConvertRawFundamentalsTask
from pipelines.fundamentals_ingestion.fundamentals_long import FundamentalsLongTask
from pipelines.fundamentals_ingestion.merge_fundamentals import MergeFundamentalsTask
from pipelines.fundamentals_ingestion.pull_fundamentals import PullFundamentalsTask
from pipelines.fundamentals_ingestion.redshift_bitemporal import MergeRedshiftBitemporalData
from pipelines.fundamentals_ingestion.short import FundamentalsShortTask, FundamentalsShortDigitizedJoinTask
from pipelines.prices_ingestion import get_config
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_bitemporal_s3_path
from tasks_common.digitize import DigitizeTask

static_template = """parm1="{ticker}" parm2="{metric}" parm3="5000" """
with_date_cq_template = """parm1="{ticker}" parm2="{metric}" parm3="5000" parm4="{date}" parm7="USD" """
with_date_ltm_template = """parm1="{ticker}" parm2="{metric}" parm3="2000" parm4="{date}" parm7="USD" """
with_date_ntm_template = """parm1="{ticker}" parm2="{metric}" parm3="6000" parm4="{date}" parm7="USD" """
with_date_template = """parm1="{ticker}" parm2="{metric}" parm3="{date}" parm7="USD" """

# noinspection SpellCheckingInspection
required_metrics = {
    "company_information": {
        "yearfounded": ("IQ_YEAR_FOUNDED", static_template),
        "companyname": ("IQ_COMPANY_NAME", static_template)
    },
    "balance_sheet": {
        "total_cashstinv": ("IQ_CASH_ST_INVEST", with_date_cq_template),
        "total_currentassets": ("IQ_TOTAL_CA", with_date_cq_template),
        "total_currentliab": ("IQ_TOTAL_CL", with_date_cq_template),
        "total_liabilities": ("IQ_TOTAL_LIAB", with_date_cq_template),
        "total_enterprisevalue": ("IQ_TEV", with_date_template),
        "longtermdebt": ("IQ_LT_DEBT", with_date_cq_template),
        "marketcap": ("IQ_MARKETCAP", with_date_template),
        "netdebt": ("IQ_NET_DEBT", with_date_cq_template),
        "totaldebt": ("IQ_TOTAL_DEBT", with_date_cq_template),
        "total_equity": ("IQ_TOTAL_EQUITY", with_date_cq_template),
        "total_assets": ("IQ_TOTAL_ASSETS", with_date_cq_template)
    },
    "per_share_metrics_ltm": {
        "diluted_sharesoutstanding_ltm": ("IQ_DILUT_WEIGHT", with_date_ltm_template),
        "diluted_earningspershare_ltm": ("IQ_DILUT_EPS_EXCL", with_date_ltm_template),
        "cashflowpershare_ltm": ("IQ_DISTRIBUTABLE_CASH_SHARE", with_date_ltm_template),
    },
    "income_statement_ltm": {
        "revenue_ltm": ("IQ_TOTAL_REV", with_date_ltm_template),
        "grossprofit_ltm": ("IQ_GP", with_date_ltm_template),
        "totaloperatingexpense_ltm": ("IQ_TOTAL_OPER_EXPEN", with_date_ltm_template),
        "operatingincome_ltm": ("IQ_OPER_INC", with_date_ltm_template),
        "netinterestexpense_ltm": ("IQ_NET_INTEREST_EXP", with_date_ltm_template),
        "netincome_ltm": ("IQ_NI", with_date_ltm_template),
        "ebitda_ltm": ("IQ_EBITDA", with_date_ltm_template),
        "ebit_ltm": ("IQ_EBIT", with_date_ltm_template),
    },
    "income_statement_cq": {
        "revenue_cq": ("IQ_TOTAL_REV", with_date_cq_template),
        "ebitda_cq": ("IQ_EBITDA", with_date_cq_template),
    },
    "cashflow_statement_ltm": {
        "capex_ltm": ("IQ_CAPEX", with_date_ltm_template),
    },
    "margin_metrics_ltm": {
        "grossprofit_margin_ltm": ("IQ_GROSS_MARGIN", with_date_ltm_template),
        "ebitda_margin_ltm": ("IQ_EBITDA_MARGIN", with_date_ltm_template),
        "ebit_margin_ltm": ("IQ_EBIT_MARGIN", with_date_ltm_template),
        "earningsfromcontinuingoperations_margin_ltm": ("IQ_EARNING_CO_MARGIN", with_date_ltm_template),
        "netincome_margin_ltm": ("IQ_NI_MARGIN", with_date_ltm_template),
        "leveredfreecashflow_margin_ltm": ("IQ_LFCF_MARGIN", with_date_ltm_template),
        "unleveredfreecashflow_margin_ltm": ("IQ_UFCF_MARGIN", with_date_ltm_template),
    },
    "profitability_metrics_ltm": {
        "return_on_assets_ltm": ("IQ_RETURN_ASSETS", with_date_ltm_template),
        "return_on_equity_ltm": ("IQ_RETURN_EQUITY", with_date_ltm_template),
        "return_on_capital_ltm": ("IQ_RETURN_CAPITAL", with_date_ltm_template),
        "return_on_investment_capital": ("IQ_RETURN_INVESTMENT_CAPITAL", with_date_ltm_template),
        "cashflow_aspercentof_sales": ("IQ_CF_TOTAL_REV", with_date_ltm_template),
    },
    "growth_metrics_ltm": {
        "revenue_growth_2yr_cagr_ltm": ("IQ_TOTAL_REV_2YR_ANN_CAGR", with_date_ltm_template),
        "eps_growth_2yr_cagr_ltm": ("IQ_EPS_2YR_ANN_CAGR", with_date_ltm_template),
        "revenue_growth_1yr_ltm": ("IQ_TOTAL_REV_1YR_ANN_GROWTH", with_date_ltm_template),
        "ebitda_growth_1yr_ltm": ("IQ_EBITDA_1YR_ANN_GROWTH", with_date_ltm_template),
        "netincome_growth_1yr_ltm": ("IQ_NI_1YR_ANN_GROWTH", with_date_ltm_template),
        "revenue_growth_5yr_cagr_ltm": ("IQ_TOTAL_REV_5YR_ANN_CAGR", with_date_ltm_template),
        "netincome_growth_5yr_cagr_ltm": ("IQ_NI_5YR_ANN_CAGR", with_date_ltm_template),
        "dividend_per_share_growth_1YR": ("IQ_DPS_1YR_ANN_GROWTH", with_date_ltm_template),
        "inventory_growth_1YR": ("IQ_INV_1YR_ANN_GROWTH", with_date_ltm_template),
        "capex_growth_1YR": ("IQ_CAPEX_1YR_ANN_GROWTH", with_date_ltm_template),
    },
    "valuation_metrics_ltm": {
        "marketcap_to_revenue_ltm": """marketcap / revenue_ltm""",
        "enterprisevalue_to_revenue_ltm": """total_enterprisevalue / revenue_ltm""",
        "price_to_cashflowpershare_ltm": ("IQ_PRICE_CFPS_FWD", with_date_ltm_template),
        "price_to_earningspershare_ltm": ("IQ_PE_EXCL", with_date_ltm_template),
        "enterprisevalue_to_ebitda_ltm": """total_enterprisevalue / ebitda_ltm"""
    },
    "credit_metrics": {
        "ebitda_minus_capex_ltm": """ebitda_ltm - capex_ltm""",
        "netdebt_to_ebitda_ltm": """netdebt / ebitda_ltm""",
        "netdebt_to_ebitda_minus_capex_ltm": """netdebt / (ebitda_ltm - capex_ltm)""",
        "interestcoverage_ratio_ltm": ("IQ_COVERAGE_RATIO", with_date_ltm_template),
        "netdebt_to_ebitda_cq": """netdebt / ebitda_cq""",
        "interestcoverage_ratio_cq": ("IQ_COVERAGE_RATIO", with_date_cq_template),
        "totaldebt_to_equity": """totaldebt / total_equity""",
        "currentliabilities_to_equity": """total_currentliab / total_equity""",
        "totalliab_to_assets": """total_liabilities / total_assets""",
    },
    "retail_metrics_cq": {
        "total_store_growth_1YR_cq": ("IQ_Total_Store_Change_%", with_date_cq_template),
        "same_store_sales_growth_1YR_cq": ("IQ_SAME_STORE", with_date_cq_template),
        "stores_total_cq": ("IQ_RETAIL_TOTAL_STORES", with_date_cq_template),
        "stores_franchise_total_cq": ("IQ_RETAIL_TOTAL_FRANCHISE_STORES", with_date_cq_template),
        "stores_franchise_opened_cq": ("IQ_RETAIL_OPENED_FRANCHISE_STORES", with_date_cq_template),
        "stores_owned_opened_cq": ("IQ_RETAIL_OPENED_STORES", with_date_cq_template),
        "stores_owned_closed_cq": ("IQ_RETAIL_CLOSED_STORES", with_date_cq_template),
        "stores_franchise_closed_cq": ("IQ_RETAIL_CLOSED_FRANCHISE_STORES", with_date_cq_template),
    },
    "retail_metrics_ltm": {
        "total_store_growth_1YR_ltm": ("IQ_Total_Store_Change_%", with_date_ltm_template),
        "same_store_sales_growth_1YR_ltm": ("IQ_SAME_STORE", with_date_ltm_template),
        "stores_total_ltm": ("IQ_RETAIL_TOTAL_STORES", with_date_ltm_template),
        "stores_franchise_total_ltm": ("IQ_RETAIL_TOTAL_FRANCHISE_STORES", with_date_ltm_template),
        "stores_franchise_opened_ltm": ("IQ_RETAIL_OPENED_FRANCHISE_STORES", with_date_ltm_template),
        "stores_owned_opened_ltm": ("IQ_RETAIL_OPENED_STORES", with_date_ltm_template),
        "stores_owned_closed_ltm": ("IQ_RETAIL_CLOSED_STORES", with_date_ltm_template),
        "stores_franchise_closed_ltm": ("IQ_RETAIL_CLOSED_FRANCHISE_STORES", with_date_ltm_template),
    },
    "estimate_metrics": {
        "estimates_price_target_median_cq": ("IQ_MEDIAN_TARGET_PRICE", with_date_cq_template),
        "estimates_price_target_high_cq": ("IQ_HIGH_TARGET_PRICE", with_date_cq_template),
        "estimates_price_target_low_cq": ("IQ_LOW_TARGET_PRICE", with_date_cq_template),
        "earning_announce_date_cq": ("IQ_EARNINGS_ANNOUNCE_DATE", with_date_cq_template),
        "estimates_fical_Q_cq": ("IQ_FISCAL_Q_EST", with_date_cq_template),
        "estimtes_fiscal_Y_cq": ("IQ_FISCAL_Y_EST", with_date_cq_template),
        "estimates_cal_Q_cq": ("IQ_CAL_Q_EST", with_date_cq_template),
        "estimates_same_store_growth_cq": ("IQ_SAME_STORE_MEDIAN_EST", with_date_cq_template),
        "estimates_eps_growth_1YR_cq": ("IQ_EST_EPS_GROWTH_1YR", with_date_cq_template),
    },
    "liquidity_metrics_cq": {
        "turnover_assets_cq": ("IQ_ASSET_TURNS", with_date_cq_template),
        "turnover_fixed_assets_cq": ("IQ_FIXED_ASSET_TURNS", with_date_cq_template),
        "turnover_accountsreceivable_cq": ("IQ_AR_TURNS", with_date_cq_template),
        "turnover_inventory_cq": ("IQ_INVENTORY_TURNS", with_date_cq_template),
        "current_ratio_cq": ("IQ_CURRENT_RATIO", with_date_cq_template),
        "quick_ratio_cq": ("IQ_QUICK_RATIO", with_date_cq_template),
        "days_sales_out_cq": ("IQ_DAYS_SALES_OUT", with_date_cq_template),
        "days_inventory_out_cq": ("IQ_DAYS_INVENTORY_OUT", with_date_cq_template),
        "days_payable_out_cq": ("IQ_DAYS_PAYABLE_OUT", with_date_cq_template),
        "days_cashconversion_cq": ("IQ_CASH_CONVERSION", with_date_cq_template),
        "cashflow_to_current_liabilities_cq": ("IQ_CFO_CURRENT_LIAB", with_date_cq_template),
        "revoliving_credit_pct_cq": ("IQ_RC_PCT", with_date_cq_template),
        "current_portionoflongtermdebt_pct_cq": ("IQ_CURRENT_PORT_PCT", with_date_cq_template),
        "undrawn_credit_cq": ("IQ_UNDRAWN_CREDIT", with_date_cq_template),
    },
    "new_metrics": {
        "operating_cash_cq": ("IQ_CASH_OPER", with_date_cq_template),
        "capex_cq": ("IQ_CAPEX", with_date_cq_template),
        "sale_intangible_cq": ("IQ_SALE_INTAN_CF", with_date_cq_template),
        "grossprofit_cq": ("IQ_GP", with_date_cq_template),
        "dividend_yield": ("IQ_DIVIDEND_YIELD", with_date_template),
        "total_dividend_cq": ("IQ_TOTAL_DIV_PAID_CF", with_date_cq_template),
        "pe_forward": ("IQ_PE_EXCL_FWD", with_date_ntm_template),
        "repurchase_commonshares_cq": ("IQ_COMMON_REP", with_date_cq_template),
        "netdebt_issued_cq": ("IQ_NET_DEBT_ISSUED", with_date_cq_template),
        "dividend_pershare": ("IQ_ANNUALIZED_DIVIDEND", with_date_template),
        "eps_diluted_cq": ("IQ_DILUT_EPS_INCL", with_date_cq_template),
        "total_shares_outstanding_filing": ("IQ_TOTAL_OUTSTANDING_FILING_DATE", with_date_cq_template),
        "total_shares_outstanding_bs": ("IQ_TOTAL_OUTSTANDING_BS_DATE", with_date_cq_template),
        "roi_cq": ("IQ_RETURN_INVESTED_CAPITAL", with_date_cq_template),
    }
}

config = get_config()
data_lake_bucket = config["data_lake_bucket"]
temp_data_bucket = config["data_processing_bucket"]

if os.name == 'nt':
    default_df_data_dir = r"d:\tmp\fundamentals_ingestion\%s"
    # default_df_data_dir = r"d:\tmp\fundamentals_ingestion"
else:
    default_df_data_dir = "/efs/data_processing/fundamentals_ingestion/%s"


class TickerUniverse(ExternalTask):
    def output(self):
        return S3Target("s3://dcm-data-test/chris/universe.txt")


class ETFList(ExternalTask):
    def output(self):
        return S3Target("s3://dcm-data-test/fluder/etf.csv")


class DailyEquityPrices(ExternalTask):
    def output(self):
        return RedshiftTarget("daily_equity_prices")


class SectorIndustryMappingData(ExternalTask):
    def output(self):
        return RedshiftTarget("sector_benchmark_map")


class FundamentalsLong(ExternalTask):
    def output(self):
        return RedshiftTarget("fundamentals_long")


class PullFundamentalsStep(PullFundamentalsTask):
    # Filter out derived metrics
    # noinspection PyTypeChecker
    required_metrics = {
        group: {
            metric_name: template
            for metric_name, template in mapping.items()
            if not isinstance(template, str)
        }
        for group, mapping in required_metrics.items()
    }

    def __init__(self, *args, **kwargs):
        super(PullFundamentalsStep, self).__init__(*args, **kwargs)
        self.df_data_dir = default_df_data_dir % self.date_only()

    def requires(self):
        return {
            "universe": TickerUniverse(),
            "etf": ETFList(),
            "daily_equity_prices": DailyEquityPrices()
        }

    def run(self):
        super(PullFundamentalsStep, self).run()
        # This is done for efficiency since we already have the data in memory
        # Otherwise ConvertRawFundamentalsTask can (and will) pull the raw data from S3
        ConvertRawFundamentalsStep(date=self.date).process_raw_data(self.raw_json)

    def output(self):
        # return S3Target("s3://dcm-data-test/fluder/fundamentals_ingestion/asof_%s_raw_data.json" % self.date_only())
        return LocalTarget(os.path.join(self.df_data_dir, "fundamentals_raw.json"))
        # s3_path = get_bitemporal_s3_path("dcm-data-test", "boris/capiq/fundamentals", "all_fundamentals", self.date, "json")
        # s3_path = "s3://dcm-data-test/boris/capiq/fundamentals/all_fundamentals/2017/09/27/all_fundamentals_1507068147.json"
        # return S3Target(s3_path)


class ConvertRawFundamentalsStep(ConvertRawFundamentalsTask):
    # Collect derived metrics only
    # noinspection PyTypeChecker
    derived_metrics = {
        metric_name: template
        for group, mapping in required_metrics.items()
        for metric_name, template in mapping.items()
        if isinstance(template, str)
    }

    def __init__(self, *args, **kwargs):
        super(ConvertRawFundamentalsStep, self).__init__(*args, **kwargs)
        self.df_data_dir = default_df_data_dir % self.date_only()

    def requires(self):
        return PullFundamentalsStep(date=self.date)

    def output(self):
        # fundamentals_data.h5 = converted input json (fundamentals_input.h5) + derived values
        return LocalTarget(os.path.join(self.df_data_dir, "fundamentals_data.h5"))


class MergeFundamentalsStep(MergeFundamentalsTask):
    def __init__(self, *args, **kwargs):
        super(MergeFundamentalsStep, self).__init__(*args, **kwargs)
        self.df_data_dir = default_df_data_dir % self.date_only()

    def requires(self):
        return {
            "new_fundamentals": ConvertRawFundamentalsStep(date=self.date),
            "bitemporal_data": FundamentalsLong()
        }

    def output(self):
        return {
            # "new_records": S3Target("s3://dcm-data-test/boris/capiq/fundamentals/all_fundamentals/2017/09/27/new_records.h5"),
            # "data_to_update": S3Target("s3://dcm-data-test/boris/capiq/fundamentals/all_fundamentals/2017/09/27/data_to_update.h5"),
            # "records_to_retry": S3Target("s3://dcm-data-test/boris/capiq/fundamentals/all_fundamentals/2017/09/27/records_to_retry.h5")
            "new_records": LocalTarget(os.path.join(self.df_data_dir, "new_records.h5")),
            "data_to_update": LocalTarget(os.path.join(self.df_data_dir, "data_to_update.h5")),
            "records_to_retry": LocalTarget(os.path.join(self.df_data_dir, "records_to_retry.h5"))
        }


class FundamentalsLongStep(FundamentalsLongTask):
    def __init__(self, *args, **kwargs):
        super(FundamentalsLongStep, self).__init__(*args, **kwargs)
        self.df_data_dir = default_df_data_dir % self.date_only()

    def requires(self):
        return {
            "new_fundamentals": MergeFundamentalsStep(date=self.date),
            "bitemporal_data": FundamentalsLong()
        }

    def input(self):
        return {
            "new_records": super(FundamentalsLongStep, self).input()["new_fundamentals"]["new_records"],
            "data_to_update": super(FundamentalsLongStep, self).input()["new_fundamentals"]["data_to_update"],
            "records_to_retry": super(FundamentalsLongStep, self).input()["new_fundamentals"]["records_to_retry"],
            "bitemporal_data": super(FundamentalsLongStep, self).input()["bitemporal_data"]
        }

    def output(self):
        return {
            "flag": S3Target(
                "s3://dcm-data-test/boris/fundamentals_ingestion/%s/fundamentals_long_merge.flag" % self.date_only()),
            "bitemporal_data": self.input()["bitemporal_data"]
        }


class FundamentalsShortStep(FundamentalsShortTask):
    def requires(self):
        return {
            "long": FundamentalsLongStep(date=self.date),
            "sector_mapping": SectorIndustryMappingData()
        }

    def input(self):
        return {
            "long": super(FundamentalsShortStep, self).input()["long"]["bitemporal_data"],
            "sector_mapping": super(FundamentalsShortStep, self).input()["sector_mapping"]
        }

    def output(self):
        return RedshiftTarget("fundamentals_short")


class FundamentalsShortDigitizeSectorDateStep(DigitizeTask):
    @property
    def group_mapping(self):
        names = [name.lower() for group in required_metrics for name in required_metrics[group]]
        print(names)
        return {
            "sector_date": {
                "columns": names,
                "quantiles": [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
                "group_by": ["sector", "quarter"],
                "suffix": "_sector_date"
            }
        }

    def requires(self):
        return FundamentalsShortStep(date=self.date)

    def input(self):
        return {
            "data": super(FundamentalsShortDigitizeSectorDateStep, self).input()
        }

    def output(self):
        return RedshiftTarget("fundamentals_short_digitized_sector_date")


class FundamentalsShortDigitizeDateStep(DigitizeTask):
    @property
    def group_mapping(self):
        names = [name.lower() for group in required_metrics for name in required_metrics[group]]
        print(names)
        return {
            "date_only": {
                "columns": names,
                "quantiles": [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
                "group_by": ["quarter"],
                "suffix": "_date"
            }
        }

    def requires(self):
        return FundamentalsShortStep(date=self.date)

    def input(self):
        return {
            "data": super(FundamentalsShortDigitizeDateStep, self).input()
        }

    def output(self):
        return RedshiftTarget("fundamentals_short_digitized_date")


class FundamentalsShortDigitizedJoinStep(FundamentalsShortDigitizedJoinTask):
    def requires(self):
        return {
            "date": FundamentalsShortDigitizeDateStep(date=self.date),
            "sector_date": FundamentalsShortDigitizeSectorDateStep(date=self.date)
        }

    def output(self):
        return RedshiftTarget("fundamentals_short_digitized")


class FundamentalsIngestionCleanupStep(DCMTaskParams, Task):
    def __init__(self, *args, **kwargs):
        super(FundamentalsIngestionCleanupStep, self).__init__(*args, **kwargs)
        self.temp_data_dir = default_df_data_dir % self.date_only()
        self.performed_cleanup = False

    def requires(self):
        # return PullFundamentalsStep(date=self.date)
        # return ConvertRawFundamentalsStep(date=self.date)
        # return MergeFundamentalsStep(date=self.date)
        # return FundamentalsLongStep(date=self.date)
        # return FundamentalsShortStep(date=self.date)
        # return FundamentalsShortDigitizeDateStep(date=self.date)
        # return FundamentalsShortDigitizeSectorDateStep(date=self.date)
        return FundamentalsShortDigitizedJoinStep(date=self.date)
        # return FundamentalsShortDigitizeStep()

    def run(self):
        # if os.path.exists(self.temp_data_dir):
        #     shutil.rmtree(self.temp_data_dir)
        self.performed_cleanup = True

    def complete(self):
        return self.performed_cleanup  # and not os.path.exists(self.temp_data_dir)

    def output(self):
        return self.input()


class FundamentalsIngestionPipeline(DCMTaskParams, WrapperTask):
    def __init__(self, *args, **kwargs):
        if "date" not in kwargs:
            # kwargs["date"] = kwargs.get("date", datetime.now())  # - timedelta(days=1)
            kwargs["date"] = kwargs.get("date", datetime(2017, 11, 30))
            # kwargs["date"] = datetime.combine(kwargs["date"].date(), time(0, 0))  # - timedelta(days=2)

        super(FundamentalsIngestionPipeline, self).__init__(*args, **kwargs)

    def requires(self):
        return FundamentalsIngestionCleanupStep(date=self.date)

    def output(self):
        return self.input()
