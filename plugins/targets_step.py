from core_classes import DataReaderClass
import pandas as pd
from commonlib import winsorize
from datetime import datetime

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path


class CalculateTargetReturns(DataReaderClass):
    '''

    Calculates the talib version of OBV indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["target_returns"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, return_column, periods, winsorize_alpha):
        self.data = None
        self.return_column = return_column or "close"
        self.periods = periods or [1, 5, 10, 21]
        self.winsorize_alpha = winsorize_alpha or 0.01

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        price = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        target = price.pivot_table(
            index='date', columns='ticker', values=self.return_column, dropna=False
        )
        target = target.sort_index().fillna(method='ffill', limit=3)
        ret = pd.concat(
            (target.pct_change(d).shift(-d).stack() for d in self.periods),
            axis=1,
            keys=['future_ret_%dB' % d for d in self.periods],
        )
        if self.winsorize_alpha:
            ret = winsorize(ret, ret.columns, ['date'], self.winsorize_alpha)
        self.data = ret.reset_index()
        return self.data


TARGETS_PARAMS = {
    'params': {
        'return_column': 'close',
        'periods': [1, 5, 10, 21],
        'winsorize_alpha': 0.01,
    },
    'class': CalculateTargetReturns,
    'start_date': RUN_DATE,
    'provided_data': {'target_returns': construct_destination_path('targets')},
    'required_data': {
        'daily_price_data': construct_required_path('data_pull', 'daily_price_data')
    },
}
