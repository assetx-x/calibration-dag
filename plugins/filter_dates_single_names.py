from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from market_timeline import pick_trading_week_dates,pick_trading_month_dates
from commonlib import talib_STOCHRSI, MA,talib_PPO, talib_TRIX
import talib
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path


class FilterMonthlyDatesFullPopulation(DataReaderClass):
    '''

    Filters data for month start or month end observations

    '''

    PROVIDES_FIELDS = ["monthly_merged_data"]
    REQUIRES_FIELDS = ["merged_data"]

    def __init__(self, start_date, end_date, mode):
        self.data = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.mode = mode or "bme"
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        df['date'] = df['date'].apply(pd.Timestamp)
        # df.set_index(['ticker','date'],inplace=True)
        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt

        max_end_date = min(self.end_date, df["date"].max())
        if self.mode in ["bme", "bms"]:
            chosen_days = list(pick_trading_month_dates(self.start_date, max_end_date, self.mode))
            filtered_data = df[df["date"].isin(chosen_days)].reset_index(drop=True)
        else:
            filtered_data = df[(df["date"] >= self.start_date) & (df["date"] <= max_end_date)]
        # filtered_data["log_avg_dollar_volume"] = pd.np.log(filtered_data["avg_dollar_volume"])
        self.data = filtered_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class FilterMonthlyDatesFullPopulationWeekly(FilterMonthlyDatesFullPopulation):
    '''

    Filters data for month start or month end, as well as weekly observations

    '''

    PROVIDES_FIELDS = ["monthly_merged_data", "weekly_merged_data"]
    REQUIRES_FIELDS = ["merged_data"]

    def __init__(self, start_date, end_date, monthly_mode, weekly_mode):
        self.weekly_data = None
        self.weekly_mode = weekly_mode or "w-mon"
        FilterMonthlyDatesFullPopulation.__init__(self, start_date, end_date, monthly_mode)

    def do_step_action(self, **kwargs):
        FilterMonthlyDatesFullPopulation.do_step_action(self, **kwargs)
        df = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        # df['date'] = df['date'].apply(pd.Timestamp)
        # df.set_index(['ticker','date'],inplace=True)

        max_end_date = min(self.end_date, df["date"].max())
        chosen_days = list(pick_trading_week_dates(self.start_date, max_end_date, self.weekly_mode))
        filtered_data = df[df["date"].isin(chosen_days)].reset_index(drop=True)
        self.weekly_data = filtered_data

        return {"monthly_merged_data": self.data, "weekly_merged_data": self.weekly_data}


class CreateMonthlyDataSingleNames(DataReaderClass):
    PROVIDES_FIELDS = ["monthly_merged_data_single_names"]
    REQUIRES_FIELDS = ["monthly_merged_data", "security_master"]

    def __init__(self):
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        security_master = kwargs[self.REQUIRES_FIELDS[1]]
        monthly_merged_data = kwargs[self.REQUIRES_FIELDS[0]]
        singlename_tickers = list(security_master.dropna(subset=["IndustryGroup"])["dcm_security_id"].unique())
        res = monthly_merged_data[monthly_merged_data["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        self.data = res
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class CreateMonthlyDataSingleNamesWeekly(CreateMonthlyDataSingleNames):

    PROVIDES_FIELDS = ["monthly_merged_data_single_names", "weekly_merged_data_single_names"]
    REQUIRES_FIELDS = ["monthly_merged_data", "weekly_merged_data", "security_master"]

    def __init__(self):
        self.weekly_data = None
        CreateMonthlyDataSingleNames.__init__(self)

    def do_step_action(self, **kwargs):
        security_master = kwargs["security_master"]
        monthly_merged_data = kwargs["monthly_merged_data"]
        weekly_merged_data = kwargs["weekly_merged_data"]
        singlename_tickers = list(security_master.dropna(subset=["IndustryGroup"])["ticker"].unique())

        self.data = monthly_merged_data[monthly_merged_data["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        self.weekly_data = weekly_merged_data[weekly_merged_data["ticker"].isin(singlename_tickers)].reset_index(drop=True)
        return {'monthly_merged_data_single_names':self.data,'weekly_merged_data_single_names':self.weekly_data}

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.weekly_data}





############# AIRFLOW FUNCTIONS



FMDP_params = {'monthly_mode':"bme",
        'weekly_mode':"w-mon",
        'start_date':"2000-03-15",
        'end_date':RUN_DATE}


FilterMonthlyDatesFullPopulationWeekly_params = {'params':FMDP_params,
                                   'class':FilterMonthlyDatesFullPopulationWeekly,'start_date':RUN_DATE,
                                'provided_data': {'monthly_merged_data': construct_destination_path('filter_dates_single_name'),
                                                 'weekly_merged_data': construct_destination_path('filter_dates_single_name')},
                                'required_data': {'merged_data': construct_required_path('merge_step','merged_data')}}





CreateMonthlyDataSingleNamesWeekly_params = {'params':{},
                                   'class':CreateMonthlyDataSingleNamesWeekly,'start_date':RUN_DATE,
                                'provided_data': {'monthly_merged_data_single_names': construct_destination_path('filter_dates_single_name'),
                                                 'weekly_merged_data_single_names': construct_destination_path('filter_dates_single_name')},
                                'required_data': {'monthly_merged_data': construct_required_path('filter_dates_single_name','monthly_merged_data'),
                                                  'weekly_merged_data': construct_required_path('filter_dates_single_name','weekly_merged_data'),
                                                  'security_master': construct_required_path('data_pull','security_master'),
                                                 }}




