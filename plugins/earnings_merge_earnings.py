from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

from core_classes import construct_required_path_earnings as construct_required_path
from core_classes import construct_destination_path_earnings as construct_destination_path


class MergeEarnings(DataReaderClass):
    '''

    Merge the earnings data with the fundamental data

    '''

    PROVIDES_FIELDS = ["merged_earnings_data"]
    REQUIRES_FIELDS = ["merged_data_econ_industry_quarterly", "earnings_data"]

    def __init__(self):
        self.data = None
        # self.tickers = tickers
        # self.return_column = return_column or "close"
        # self.periods = periods or [1, 5, 10, 21]
        # self.winsorize_alpha = winsorize_alpha or 0.01

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        fundamental_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        earnings_data = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)[['date', 'ticker', 'Reported EPS']]
        target = earnings_data.pivot_table(index='date', columns='ticker', values='Reported EPS', dropna=False)
        target = target.sort_index().fillna(method='ffill', limit=3)
        future_earnings = pd.concat((target.shift(-d).stack() for d in [1]),
                                    axis=1, keys=['future_earnings_%dQ' % d for d in [1]]).reset_index()
        # future_earnings['date'] = future_earnings['date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        earnings_w_future = earnings_data.merge(future_earnings, on=['ticker', 'date'], how='left')
        self.data = fundamental_data.merge(earnings_w_future, on=['ticker', 'date'], how='left')

        return self.data


MergeEarnings_params = {'params': {},
                        'class': MergeEarnings,
                        'start_date': RUN_DATE,
                        'provided_data': {'merged_earnings_data': construct_destination_path('merged_earnings_data'),

                                          },
                        'required_data': {'merged_data_econ_industry_quarterly': construct_required_path('merge_econ',
                                                                                                         'merged_data_econ_industry_quarterly'),
                                          'earnings_data': construct_required_path('earnings_data', 'earnings_data')

                                          }}