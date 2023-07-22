import gc
import pandas as pd

from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from etl_workflow_steps import StatusType

class MergeAllData(CalibrationTaskflowTask):
    '''

    Merges all data elements for the market view calibration pipeline

    '''
    PROVIDES_FIELDS = ["merged_data"]
    REQUIRES_FIELDS = ["names_performance_over_intervals",
                       "overnight_return_data", "quarterly_return_data",
                       "correlation_data","technical_indicator_data", "sector_industry_mapping", "etf_information",
                       "ravenpack_indicator_data","ravenpack_overnight_volume_data", "fundamental_data",
                       "earnings_block_dates", "vxx_return_data", "dollar_volume_data",
                       "spy_indicator_peer_rank","indicator_comparisons",
                       "indicator_comparisons", "derived_fundamental_data"]

    def __init__(self, slippage_pct=None, addtive_tc=None, ranking_benchmarks=None, sector_benchmarks=None,
                 other_benchmarks=None):
        self.data = None
        self.slippage_pct = slippage_pct if slippage_pct else 0.001
        self.addtive_tc = addtive_tc if addtive_tc else 0.01
        self.ranking_benchmarks = ranking_benchmarks
        self.sector_benchmarks = sector_benchmarks
        self.other_benchmarks = other_benchmarks
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        gc.collect()
        try:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl[k] = kwargs[k]

            new_sector_industry_mapping = sector_industry_mapping.rename(columns = {"etf" : "benchmark_etf"})

            merged_data = names_performance_over_intervals

            new_overnight_return_data = overnight_return_data.copy(deep=True)
            new_overnight_return_data["date"] = pd.DatetimeIndex(new_overnight_return_data["date"]).normalize()
            merged_data = pd.merge(merged_data, new_overnight_return_data, how="left",
                                   left_on=["entry_date","ticker"], right_on=["date","ticker"]).drop(["date"],axis=1)

            correlation_data_subset = correlation_data.copy(deep=True)
            correlation_data_subset["date"] = pd.DatetimeIndex(correlation_data_subset["date"]).normalize()

            correlation_data_subset = correlation_data_subset[correlation_data_subset["date"].isin(names_performance_over_intervals["entry_date"].unique())]

            
            
            technical_indicator_subset = technical_indicator_data.copy(deep=True)
            technical_indicator_subset["date"] = pd.DatetimeIndex(technical_indicator_subset["date"]).normalize()

            technical_indicator_subset = technical_indicator_subset[technical_indicator_subset["date"].isin(names_performance_over_intervals["entry_date"].unique())]

                        
            correlation_indicator_data = pd.merge(correlation_data_subset,technical_indicator_subset, how="left" ,
                                                  on= ["date","ticker"])
            merged_data = pd.merge(merged_data,correlation_indicator_data, how="left",  left_on=["entry_date","ticker"],
                                   right_on=["date","ticker"]).drop(["date"],axis=1)

            for df in [new_sector_industry_mapping, etf_information]:
                merged_data = pd.merge(merged_data, df, how="left", on=["ticker"])


            for df in [ravenpack_indicator_data,ravenpack_overnight_volume_data]:
                merged_data = pd.merge(merged_data, df.rename(columns = {"date" : "entry_date"}).drop(["rp_entity_id"],axis=1),
                                           how ="left", on=["entry_date", "ticker"])

            #TODO: 20180612 replaced capiq marketcap with quandl
            new_fundamental_data = fundamental_data.drop("marketcap", axis=1)
            for df in [quarterly_return_data, new_fundamental_data]:
                merged_data = pd.merge(merged_data, df, how="left",
                                       on = ["ticker","this_quarter"]).drop(["date"],axis=1)
            #derived_fundamental_data["marketcap"] = derived_fundamental_data["marketcap"]/1000000.0
            merged_data = pd.merge(merged_data, derived_fundamental_data.dropna(subset = ["marketcap"]), how="left", on=["entry_date", "ticker"])

            merged_data = pd.merge(merged_data, earnings_block_dates.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date", "ticker"])

            merged_data["sector"].fillna(value = "None", inplace= True)

            # Add indicator_benchmark
            merged_data = pd.merge(merged_data,
                                   merged_data[['entry_date', 'ticker', 'indicator']].rename(columns={'entry_date': 'date'}),
                                   how='left', left_on=['entry_date', 'benchmark_etf'], right_on=['date', 'ticker'],
                                   suffixes=("", "_benchmark")).drop(['date', 'ticker_benchmark'], axis=1)

            # Add here (Check column name)
            merged_data = pd.merge(merged_data, spy_indicator_peer_rank.rename(columns = {"SPY" : "spy_indicator_peer_rank",
                                                                                          "date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, indicator_comparisons.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            tlt = merged_data[merged_data["ticker"]=="TLT"][["entry_date", "SPY_correl", "SPY_correl_rank"]]
            merged_data = pd.merge(merged_data, tlt.rename(columns = {"SPY_correl" : "SPY_TLT_correl", "SPY_correl_rank": "SPY_TLT_correl_rank"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, vxx_return_data.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date"])

            merged_data = pd.merge(merged_data, dollar_volume_data.rename(columns = {"date" : "entry_date"}),
                                   how ="left", on=["entry_date", "ticker"])
            merged_data.columns = merged_data.columns.astype(str)
            self.data = merged_data
        finally:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl.pop(k,None)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"slippage_pct": 0.001, "addtive_tc": 0.01,
                "ranking_benchmarks": ['DIA', 'DVY', 'IJH', 'IJJ', 'IJK', 'IJR', 'IVE', 'IVV', 'IVW', 'IWB',
                                       'IWD', 'IWM', 'IWN', 'IWO', 'IWR', 'IWV', 'MDY', 'RSP', 'SCHA', 'SPY',
                                       'VTI', 'VTV', 'VV'],
                "sector_benchmarks": ["XLI", "XLV", "XLF", "XLK", "XLY", "XLE", "VDC", "XLB", "XLU"],
                "other_benchmarks": ["TLT", "RSP", "EEM", "ACWI"]}
