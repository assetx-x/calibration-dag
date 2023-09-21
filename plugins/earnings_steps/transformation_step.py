from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from market_timeline import pick_trading_week_dates, pick_trading_month_dates, transform_wrapper
from commonlib import talib_STOCHRSI, MA,talib_PPO, talib_TRIX
import talib


current_date = datetime.datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

from core_classes import construct_required_path_earnings as construct_required_path
from core_classes import construct_destination_path_earnings as construct_destination_path
from core_classes import pick_trading_quarterly_dates,pick_trading_month_dates


class CreateYahooDailyPriceRolling(DataReaderClass):
    REQUIRES_FIELDS = ["yahoo_daily_price_data"]
    PROVIDES_FIELDS = ["yahoo_daily_price_rolling"]

    def __init__(self, rolling_interval):
        self.rolling_interval = 5
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        yahoo_daily_price = kwargs["yahoo_daily_price_data"]
        self.data = yahoo_daily_price.rolling(self.rolling_interval).mean()
        return self.data


class TransformEconomicData(DataReaderClass):
    PROVIDES_FIELDS = ["transformed_econ_data"]
    REQUIRES_FIELDS = ["econ_data", "yahoo_daily_price_rolling", "econ_transformation"]

    def __init__(self, start_date, end_date, mode, shift_increment="month"):
        self.data = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.mode = mode
        self.shift_increment = shift_increment

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _transform_data(self, etf_rolling, econ_data, transform_dict, trading_days, date_shift):
        if self.shift_increment == "day":  # Shifts 1-day
            etf_rolling = etf_rolling.shift(date_shift).loc[trading_days].fillna(method="ffill")
            econ_data = econ_data.shift(date_shift).loc[trading_days].fillna(method="ffill")
        else:  # Shifts 1-month
            etf_rolling = etf_rolling.loc[trading_days].fillna(method="ffill").shift(date_shift)
            econ_data = econ_data.loc[trading_days].fillna(method="ffill").shift(date_shift)

        etf_rolling = etf_rolling.apply(lambda x: transform_wrapper(x, transform_dict), axis=0)
        econ_data = econ_data.apply(lambda x: transform_wrapper(x, transform_dict), axis=0)
        etf_rolling = etf_rolling.reset_index().rename(columns={"Date": "date"})
        econ_data = econ_data.reset_index()
        econ_data["date"] = pd.DatetimeIndex(econ_data["date"]).normalize()
        etf_rolling["date"] = pd.DatetimeIndex(etf_rolling["date"]).normalize()
        econ_data = pd.merge(econ_data, etf_rolling, how="left", on=["date"])
        dates = pd.DatetimeIndex(etf_rolling["date"])
        date_intersection = dates.intersection(pd.DatetimeIndex(econ_data["date"]))
        econ_data = econ_data[econ_data["date"].isin(date_intersection)].sort_values("date").reset_index(drop=True)
        return econ_data[2:]

    def do_step_action(self, **kwargs):
        etf_rolling = kwargs["yahoo_daily_price_rolling"]
        econ_data = kwargs["econ_data"]
        transform_dict_org = kwargs["econ_transformation"]
        transform_dict = transform_dict_org.set_index("Unnamed: 0")["transform"].to_dict()

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        # trading_days = self._pick_trading_month_dates()
        max_end_date = min(min(etf_rolling.index.max(), econ_data.index.max()), self.end_date)
        trading_days = list(pick_trading_month_dates(self.start_date, max_end_date, self.mode))

        date_shift = 1

        self.data = self._transform_data(etf_rolling, econ_data, transform_dict, trading_days, date_shift)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class TransformEconomicDataQuarterly(TransformEconomicData):
    PROVIDES_FIELDS = ["transformed_econ_data", "transformed_econ_data_quarterly"]
    REQUIRES_FIELDS = ["econ_data", "yahoo_daily_price_rolling", "econ_transformation"]

    def __init__(self, start_date, end_date, monthly_mode, weekly_mode, shift_increment="month"):
        self.weekly_data = None
        self.weekly_mode = weekly_mode
        TransformEconomicData.__init__(self, start_date, end_date, monthly_mode, shift_increment)

    def do_step_action(self, **kwargs):
        etf_rolling = kwargs["yahoo_daily_price_rolling"]
        etf_rolling.index = etf_rolling.index.map(pd.Timestamp)
        econ_data = kwargs["econ_data"]
        econ_data.index = econ_data.index.map(pd.Timestamp)
        transform_dict_org = kwargs["econ_transformation"]
        transform_dict = transform_dict_org.set_index("Unnamed: 0")["transform"].to_dict()

        self.start_date = self.start_date if pd.notnull(self.start_date) else self.task_params.start_dt
        self.end_date = self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        """if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
            date_shift = 0
        else:
            date_shift = 1"""
        date_shift = 0
        max_end_date = min(min(etf_rolling.index.max(), econ_data.index.max()), self.end_date)
        monthly_trading_days = list(pick_trading_month_dates(self.start_date, max_end_date, self.mode))
        self.data = self._transform_data(etf_rolling, econ_data, transform_dict, monthly_trading_days, date_shift)

        weekly_trading_days = list(pick_trading_quarterly_dates(self.start_date, max_end_date, self.weekly_mode))
        weekly_dated_df = pd.DataFrame({'date': weekly_trading_days})
        self.weekly_data = pd.merge_asof(weekly_dated_df, self.data, on='date', direction='backward')

        return {self.__class__.PROVIDES_FIELDS[0]: self.data,
                self.__class__.PROVIDES_FIELDS[1]: self.weekly_data}


CreateYahooDailyPriceRolling_params = {'params':{'rolling_interval':5},
                                   'class':CreateYahooDailyPriceRolling,'start_date':RUN_DATE,
                                'provided_data': {'yahoo_daily_price_rolling': construct_destination_path('transformation')},
                                'required_data': {'yahoo_daily_price_data': construct_required_path('data_pull','etf_prices'),
                                                 }}


#####################################
TWDW_params = {"monthly_mode" : "bme",
                "weekly_mode" : "BQ",
                "start_date" : "1997-01-01",
                "end_date" : "2023-06-28",
                "shift_increment": "month"}
TransformEconomicDataWeekly_params = {'params':TWDW_params,
                                   'class':TransformEconomicDataQuarterly,
                                       'start_date':RUN_DATE,
                                'provided_data': {'transformed_econ_data': construct_destination_path('transformation'),
                                                  'transformed_econ_data_quarterly': construct_destination_path('transformation'),
                                                 },
                                'required_data': {'yahoo_daily_price_rolling':construct_required_path('transformation','yahoo_daily_price_rolling'),
                                                  'econ_transformation': construct_required_path('data_pull','econ_transformation'),
                                                  'econ_data': construct_required_path('econ_data','econ_data')
                                                 }}
