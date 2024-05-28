from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from market_timeline import pick_trading_week_dates, pick_trading_month_dates
from commonlib import talib_STOCHRSI, MA, talib_PPO, talib_TRIX
import talib

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path


class GenerateActiveMatrix(DataReaderClass):
    PROVIDES_FIELDS = ["active_matrix"]
    REQUIRES_FIELDS = ["current_gan_universe", "monthly_merged_data_single_names"]

    '''
    Consolidate_data_industry_average_201910.ipynb
    Generate Active Matrix section
    '''

    def __init__(self, start_date, end_date, ref_col):
        self.data = None
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        self.ref_col = ref_col

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _generate_active_matrix(self, singlename_tickers, data):
        dates = pd.DatetimeIndex(list(data["date"].unique()))
        dates.sort_values()
        indicator = data[
            data["date"].isin(dates) & data["ticker"].isin(singlename_tickers)
        ][["date", "ticker", self.ref_col]]
        indicator["is_active"] = (
            pd.notnull(indicator[self.ref_col]) & abs(indicator[self.ref_col]) > 1e-6
        ).astype(int)

        result = pd.pivot_table(
            indicator,
            values="is_active",
            index=["date"],
            columns=["ticker"],
            fill_value=0,
        )
        result = result.sort_index()
        residual_tickers = list(set(singlename_tickers) - set(result.columns))
        for rt in residual_tickers:
            result[rt] = 0

        result = result.loc[self.start_date : self.end_date, singlename_tickers]
        result = result.fillna(0)
        result = result.astype(int)
        return result

    def do_step_action(self, **kwargs):
        gan_universe = kwargs[self.REQUIRES_FIELDS[0]]
        data = kwargs[self.REQUIRES_FIELDS[1]]

        self.start_date = (
            self.start_date
            if pd.notnull(self.start_date)
            else self.task_params.start_dt
        )
        self.end_date = (
            self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        )
        singlename_tickers = list(gan_universe["dcm_security_id"].unique())
        singlename_tickers.sort()

        self.data = self._generate_active_matrix(singlename_tickers, data)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}


class GenerateActiveMatrixWeekly(GenerateActiveMatrix):

    PROVIDES_FIELDS = ["active_matrix", "active_matrix_weekly"]
    REQUIRES_FIELDS = [
        "current_gan_universe",
        "monthly_merged_data_single_names",
        "weekly_merged_data_single_names",
    ]

    def __init__(self, start_date, end_date, ref_col):
        self.weekly_data = None
        GenerateActiveMatrix.__init__(self, start_date, end_date, ref_col)

    def do_step_action(self, **kwargs):
        gan_universe = kwargs[self.REQUIRES_FIELDS[0]]
        monthly_data = kwargs[self.REQUIRES_FIELDS[1]]
        monthly_data['date'] = monthly_data['date'].apply(pd.Timestamp)
        weekly_data = kwargs[self.REQUIRES_FIELDS[2]]
        weekly_data['date'] = weekly_data['date'].apply(pd.Timestamp)

        self.start_date = (
            self.start_date
            if pd.notnull(self.start_date)
            else self.task_params.start_dt
        )
        self.end_date = (
            self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        )
        singlename_tickers = list(gan_universe["dcm_security_id"].unique())
        singlename_tickers.sort()

        self.data = self._generate_active_matrix(singlename_tickers, monthly_data)
        self.weekly_data = self._generate_active_matrix(singlename_tickers, weekly_data)
        return {'active_matrix': self.data, 'active_matrix_weekly': self.weekly_data}
        # return self.data,self.weekly_data

    def _get_additional_step_results(self):
        return {
            self.__class__.PROVIDES_FIELDS[0]: self.data,
            self.__class__.PROVIDES_FIELDS[1]: self.weekly_data,
        }


active_matrix_params = {
    'start_date': "2000-01-01",
    'end_date': RUN_DATE,
    'ref_col': 'ret_5B',
}


GenerateActiveMatrixWeekly_params = {
    'params': active_matrix_params,
    'class': GenerateActiveMatrixWeekly,
    'start_date': RUN_DATE,
    'provided_data': {
        'active_matrix': construct_destination_path('active_matrix'),
        'active_matrix_weekly': construct_destination_path('active_matrix'),
    },
    'required_data': {
        'current_gan_universe': construct_required_path(
            'data_pull', 'current_gan_universe'
        ),
        'monthly_merged_data_single_names': construct_required_path(
            'filter_dates_single_name', 'monthly_merged_data_single_names'
        ),
        'weekly_merged_data_single_names': construct_required_path(
            'filter_dates_single_name', 'weekly_merged_data_single_names'
        ),
    },
}
