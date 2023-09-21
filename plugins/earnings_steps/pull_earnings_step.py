from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf

current_date = datetime.datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')

from core_classes import construct_required_path_earnings as construct_required_path
from core_classes import construct_destination_path_earnings as construct_destination_path

def move_date_to_forward_bme(date):
    return pd.offsets.BQuarterEnd().rollforward(date)

def move_date_to_backwards_bme(date):
    return pd.offsets.BQuarterEnd().rollback(date)
def get_closest_bme(date):
    date = pd.to_datetime(date)
    forward_bme = move_date_to_forward_bme(date)
    backward_bme = move_date_to_backwards_bme(date)

    if abs(date - forward_bme) < abs(date - backward_bme):
        return forward_bme
    else:
        return backward_bme

def convert_string_to_datetime(date_string):
    date_part = ' '.join(date_string.split(', ')[:2])
    date_datetime = pd.to_datetime(date_part)
    date_formatted = date_datetime.strftime('%Y-%m-%d')
    return date_formatted

def gather_earnings_data(tickers):
    all_data = pd.DataFrame([])

    for individual_ticker in tickers:
        try:
            df = yf.Ticker(individual_ticker)
            earnings_history = df.get_earnings_history()
            earnings_history['Earnings Date'] = earnings_history['Earnings Date'].apply(convert_string_to_datetime)
            earnings_history = earnings_history.drop_duplicates()


            all_data = pd.concat([all_data ,earnings_history])
        except Exception as e:
            print(e)
            pass
        all_data['QE Date'] = all_data['Earnings Date'].apply(get_closest_bme)

    return all_data

class PullEarnings(DataReaderClass):
    '''

    Calculates the talib version of OBV indicator based on daily prices

    '''

    PROVIDES_FIELDS = ["earnings_data"]
    REQUIRES_FIELDS = ["merged_data_econ_industry_quarterly"]

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
        earnings_history = gather_earnings_data(fundamental_data['ticker'].unique().tolist())
        earnings_history['QE Date'] = [i.strftime('%Y-%m-%d') for i in earnings_history['QE Date']]
        earnings_history.rename(columns={'Symbol': 'ticker', 'QE Date': 'date'}, inplace=True)
        self.data = earnings_history
        return self.data


PullEarnings_params = {'params': {},
                       'class': PullEarnings,
                       'start_date': RUN_DATE,
                       'provided_data': {'earnings_data': construct_destination_path('earnings_data'),

                                         },
                       'required_data': {'merged_data_econ_industry_quarterly': construct_required_path('merge_econ',
                                                                                                        'merged_data_econ_industry_quarterly'),

                                         }}