from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from market_timeline import pick_trading_week_dates,pick_trading_month_dates
from commonlib import factor_standarization
import talib
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path


class GenerateBMEReturns(DataReaderClass):
    PROVIDES_FIELDS = ["past_returns_bme", "future_returns_bme"]
    REQUIRES_FIELDS = ["active_matrix", "daily_price_data"]

    '''
    Consolidate_data_industry_average_201910.ipynb
    Generate Target BME Returns section
    '''

    def __init__(self):
        self.data = None
        self.future_data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _generate_raw_returns(self, daily_price_data, gan_universe, trading_freq):
        pivot_data = pd.pivot_table(daily_price_data[["date", "ticker", "close"]], index="date", values="close", \
                                    columns="ticker", dropna=False).sort_index()
        pivot_data = pivot_data.fillna(method="ffill")
        pivot_data.index = pd.DatetimeIndex(list(map(pd.Timestamp, pivot_data.index))).normalize()

        start_date = pivot_data.index.min()
        end_date = pivot_data.index.max()
        if trading_freq == "monthly":
            trading_dates = pick_trading_month_dates(start_date, end_date, "bme")
        elif trading_freq == "weekly":
            trading_dates = pick_trading_week_dates(start_date, end_date, "w-mon")
        else:
            raise ValueError(f"Unsupported trading frequency passed to generate raw returns data - {trading_freq}")
        trading_days = pd.DatetimeIndex(list(trading_dates))
        pivot_data = pivot_data.loc[trading_days]

        raw_returns = pivot_data.pct_change()
        # print(raw_returns)
        for dt in raw_returns.index:
            lb = raw_returns.loc[dt, :].quantile(0.005)
            lb = lb if pd.notnull(lb) else -0.999999999
            ub = raw_returns.loc[dt, :].quantile(0.995)
            ub = ub if pd.notnull(ub) else 2.0
            raw_returns.loc[dt, :] = raw_returns.loc[dt, :].clip(lb, ub)

        raw_returns = raw_returns.fillna(0.0)
        residual_tickers = list(set(gan_universe) - set(raw_returns.columns))
        for rt in residual_tickers:
            raw_returns[rt] = 0.0
        return raw_returns

    def do_step_action(self, **kwargs):
        active_matrix = kwargs[self.REQUIRES_FIELDS[0]]
        daily_price_data = kwargs[self.REQUIRES_FIELDS[1]]

        gan_universe = list(active_matrix.columns)
        raw_returns = self._generate_raw_returns(daily_price_data, gan_universe, "monthly")
        future_returns = raw_returns.shift(-1)
        future_returns = future_returns.fillna(0.0)

        self.data = raw_returns.loc[active_matrix.index, gan_universe]
        self.future_data = future_returns.loc[active_matrix.index, gan_universe]
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data,
                self.__class__.PROVIDES_FIELDS[1]: self.future_data}


class GenerateBMEReturnsWeekly(GenerateBMEReturns):
    PROVIDES_FIELDS = ["past_returns_bme", "future_returns_bme", "future_returns_weekly"]
    REQUIRES_FIELDS = ["active_matrix", "active_matrix_weekly", "daily_price_data"]

    def __init__(self):
        self.future_data_weekly = None
        GenerateBMEReturns.__init__(self)

    def do_step_action(self, **kwargs):
        active_matrix = kwargs[self.REQUIRES_FIELDS[0]]
        active_matrix_weekly = kwargs[self.REQUIRES_FIELDS[1]]
        daily_price_data = kwargs[self.REQUIRES_FIELDS[2]]
        gan_universe = list(active_matrix.columns)

        raw_returns = self._generate_raw_returns(daily_price_data, gan_universe, "monthly")
        future_returns = raw_returns.shift(-1).fillna(0.0)
        self.data = raw_returns.loc[active_matrix.index, gan_universe]
        self.future_data = future_returns.loc[active_matrix.index, gan_universe]

        raw_returns_weekly = self._generate_raw_returns(daily_price_data, gan_universe, "weekly")
        future_returns_weekly = raw_returns_weekly.shift(-1).fillna(0.0)
        self.future_data_weekly = future_returns_weekly.loc[active_matrix_weekly.index, gan_universe]
        return {'past_returns_bme': self.data, 'future_returns_bme': self.future_data,
                'future_returns_weekly': self.future_data_weekly}
        # return self.data,self.future_data,self.future_data_weekly



GenerateBMEReturnsWeekly_params = {'params':{},
                                   'class':GenerateBMEReturnsWeekly,
                                   'start_date':RUN_DATE,
                                'provided_data': {'past_returns_bme': construct_destination_path('additional_gan_features'),
                                                  'future_returns_bme': construct_destination_path('additional_gan_features'),
                                                  'future_returns_weekly': construct_destination_path('additional_gan_features')
                                                 },
                                'required_data': {'active_matrix':construct_required_path('active_matrix','active_matrix'),
                                                  'active_matrix_weekly': construct_required_path('active_matrix','active_matrix_weekly'),
                                                 'daily_price_data': construct_required_path('data_pull','daily_price_data')}}


