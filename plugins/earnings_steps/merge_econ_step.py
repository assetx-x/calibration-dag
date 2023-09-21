from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from market_timeline import pick_trading_week_dates,pick_trading_month_dates
from commonlib import talib_STOCHRSI, MA,talib_PPO, talib_TRIX
import talib


current_date = datetime.datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

from core_classes import construct_required_path_earnings as construct_required_path
from core_classes import construct_destination_path_earnings as construct_destination_path


class QuantamentalMergeEconIndustry(DataReaderClass):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data_econ_industry", "econ_data_final"]
    REQUIRES_FIELDS = ["monthly_merged_data_single_names", "security_master", "industry_average",
                       "sector_average", "transformed_econ_data"]

    def __init__(self, industry_cols, security_master_cols, sector_cols, key_sectors, econ_cols, start_date, end_date,
                 normalize_econ=True):
        self.data = None
        self.econ_data_final = None
        self.sector_cols = sector_cols
        self.key_sectors = key_sectors
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.industry_cols = industry_cols
        self.econ_cols = econ_cols
        self.security_master_cols = security_master_cols
        self.normalize_econ = normalize_econ

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _remove_bad_cols_from_econ_data(self, econ_data):
        econ_data = econ_data.drop("CLAIMSx", axis=1)  # bad column
        econ_data = econ_data.drop("S&P 500", axis=1)  # bad column
        econ_data = econ_data.drop("AWOTMAN", axis=1)
        econ_data = econ_data.set_index("date")
        econ_data[econ_data == np.inf] = 0.0
        econ_data[econ_data == -np.inf] = 0.0
        econ_data = econ_data.reset_index()
        econ_data = econ_data.fillna(0.0)
        return econ_data

    def _generate_econ_data(self, transformed_econ_data, max_end_date):
        econ_data = transformed_econ_data[(transformed_econ_data["date"] >= self.start_date) &
                                          (transformed_econ_data["date"] <= max_end_date)].reset_index(drop=True)
        econ_data = econ_data.sort_values(["date"])

        """sector_averages_summary = []
        for sector in self.key_sectors:
            short_name = sector.replace(" ", "")
            df = sector_average[sector_average["Sector"]==sector].drop("Sector", axis=1).set_index("date")[self.sector_cols]

            df.columns = list(map(lambda x:"{0}_{1}".format(x, short_name), df.columns))
            sector_averages_summary.append(df)

        sector_averages_with_renamed_cols = pd.concat(sector_averages_summary, axis=1)
        econ_data = pd.merge(econ_data, sector_averages_with_renamed_cols.reset_index(), how="left", on=["date"])"""
        econ_data = self._remove_bad_cols_from_econ_data(econ_data)
        return econ_data

    def _normalize_econ_data(self, econ_data):
        econ_data = econ_data  # [["date"] + self.econ_cols] # Select key features
        if self.normalize_econ:
            for col in self.econ_cols:
                econ_data[col] = econ_data[col].transform(lambda x: (x - x.mean()) / x.std())
        return econ_data

    def _merge_data(self, monthly_merged_data, security_master, econ_data):

        merged_data = pd.merge(monthly_merged_data,
                               security_master[self.security_master_cols],
                               how="left", on=["ticker"])

        """industry_average.set_index(['date','IndustryGroup'],inplace=True)
        merged_data = pd.merge(merged_data, industry_average[self.industry_cols].rename(columns={col: col+"_indgrp"
                                                                                                 for col in industry_average.columns}).reset_index(),
                               how="left", on=["date", "IndustryGroup"])"""

        # merged_data["dcm_security_id"] = merged_data["dcm_security_id"].astype(int)
        # merged_data = merged_data.rename(columns={"dcm_security_id" : "ticker"})

        merged_data = pd.merge(merged_data, econ_data, how="left", on=["date"])
        merged_data = merged_data.set_index(["date", "ticker"])
        merged_data[merged_data == np.inf] = np.nan
        merged_data[merged_data == -np.inf] = np.nan
        return merged_data

    def do_step_action(self, **kwargs):
        monthly_merged_data = kwargs["monthly_merged_data_single_names"].copy(deep=True)
        security_master = kwargs["security_master"]
        transformed_econ_data = kwargs["transformed_econ_data"]

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        max_end_date = min(monthly_merged_data["date"].max(), self.end_date)
        econ_data = self._generate_econ_data(transformed_econ_data, max_end_date)
        self.econ_data_final = econ_data.copy()  # Has all features (for gan)
        econ_data = self._normalize_econ_data(econ_data)

        merged_data = self._merge_data(monthly_merged_data, security_master, econ_data)
        self.data = merged_data.reset_index()
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data,
                self.__class__.PROVIDES_FIELDS[1]: self.econ_data_final}


class QuantamentalMergeEconIndustryQuarterly(QuantamentalMergeEconIndustry):
    '''

    Merges the data elements on both monthly/weekly timeline for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data_econ_industry", "merged_data_econ_industry_quarterly", "econ_data_final",
                       "econ_data_final_quarterly"]
    REQUIRES_FIELDS = ["monthly_merged_data_single_names", "weekly_merged_data_single_names", "security_master",
                       "transformed_econ_data", "transformed_econ_data_quarterly"]

    def __init__(self, industry_cols, security_master_cols, sector_cols, key_sectors, econ_cols, start_date, end_date,
                 normalize_econ=True):
        self.weekly_data = None
        self.econ_data_final_weekly = None
        QuantamentalMergeEconIndustry.__init__(self, industry_cols, security_master_cols, sector_cols,
                                               key_sectors, econ_cols, start_date, end_date, normalize_econ)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        monthly_merged_data = kwargs["monthly_merged_data_single_names"].copy(deep=True)
        monthly_merged_data['date'] = monthly_merged_data['date'].apply(pd.Timestamp)
        weekly_merged_data = kwargs["quarterly_merged_data_single_names"].copy(deep=True)
        weekly_merged_data['date'] = weekly_merged_data['date'].apply(pd.Timestamp)
        security_master = kwargs["security_master"]
        transformed_econ_data = kwargs["transformed_econ_data"]
        transformed_econ_data['date'] = transformed_econ_data['date'].apply(pd.Timestamp)
        transformed_econ_data_weekly = kwargs["transformed_econ_data_quarterly"]
        transformed_econ_data_weekly['date'] = transformed_econ_data_weekly['date'].apply(pd.Timestamp)

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        max_end_date = min(monthly_merged_data["date"].max(), self.end_date)
        econ_data = self._generate_econ_data(transformed_econ_data, max_end_date)
        # return econ_data
        self.econ_data_final = econ_data.copy()  # Has all features (for gan)
        econ_data = self._normalize_econ_data(econ_data)
        monthly_data = self._merge_data(monthly_merged_data, security_master, econ_data)

        self.data = monthly_data.reset_index()

        max_end_date = min(weekly_merged_data["date"].max(), self.end_date)
        econ_data_weekly = self._generate_econ_data(transformed_econ_data_weekly, max_end_date)
        self.econ_data_final_weekly = econ_data_weekly.copy()
        econ_data_weekly = self._normalize_econ_data(econ_data_weekly)
        weekly_data = self._merge_data(weekly_merged_data, security_master, econ_data_weekly)
        self.weekly_data = weekly_data.reset_index()

        return {self.__class__.PROVIDES_FIELDS[0]: self.data, self.__class__.PROVIDES_FIELDS[1]: self.weekly_data,
                self.__class__.PROVIDES_FIELDS[2]: self.econ_data_final,
                self.__class__.PROVIDES_FIELDS[3]: self.econ_data_final_weekly}

        # return self.data,self.weekly_data,self.econ_data_final,self.econ_data_final_weekly




###############

WMEIW_params = {'industry_cols':["volatility_126", "PPO_12_26", "PPO_21_126", "netmargin", "macd_diff", "pe", "debt2equity", "bm", "ret_63B", "ebitda_to_ev", "divyield"],
 'security_master_cols':["ticker", "Sector", "IndustryGroup"],
 'sector_cols':["volatility_126", "PPO_21_126", "macd_diff", "divyield", "bm"],
 'key_sectors':["Energy", "Information Technology", "Financials", "Utilities", "Consumer Discretionary", "Industrials", "Consumer Staples"],
 'econ_cols':["RETAILx", "USTRADE", "SPY_close", "bm_Financials", "T10YFFM", "T5YFFM", "CPITRNSL", "EWJ_volume", "HWI", "CUSR0000SA0L2", "CUSR0000SA0L5",
              "T1YFFM", "DNDGRG3M086SBEA", "AAAFFM", "RPI", "macd_diff_ConsumerStaples", "PPO_21_126_Industrials", "PPO_21_126_Financials", "CP3Mx", "divyield_ConsumerStaples",
               "GS10", "bm_Utilities", "EWG_close", "CUSR0000SAC", "GS5", "divyield_Industrials", "WPSID62", "IPDCONGD", "PPO_21_126_InformationTechnology", "PPO_21_126_Energy",
              "PPO_21_126_ConsumerDiscretionary"],
 'start_date': "1997-12-15",
 'end_date': RUN_DATE,
 'normalize_econ':False
    }

QuantamentalMergeEconIndustryWeekly_params = {'params':WMEIW_params,
                                   'class':QuantamentalMergeEconIndustryQuarterly,
                                       'start_date':RUN_DATE,
                                'provided_data': {'merged_data_econ_industry': construct_destination_path('merge_econ'),
                                                  'merged_data_econ_industry_quarterly': construct_destination_path('merge_econ'),
                                                  'econ_data_final': construct_destination_path('merge_econ'),
                                                  'econ_data_final_quarterly': construct_destination_path('merge_econ'),
                                                 },
                                'required_data': {'monthly_merged_data_single_names':construct_required_path('filter_dates_single_name','monthly_merged_data_single_names'),
                                                  'security_master': construct_required_path('data_pull','security_master'),
                                                  'quarterly_merged_data_single_names': construct_required_path('filter_dates_single_name','quarterly_merged_data_single_names'),
                                                  'transformed_econ_data': construct_required_path('transformation','transformed_econ_data'),
                                                  'transformed_econ_data_quarterly': construct_required_path('transformation','transformed_econ_data_quarterly')
                                                 }}