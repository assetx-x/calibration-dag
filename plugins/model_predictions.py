import pandas as pd
from core_classes import GCPReader, download_yahoo_data, DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path, construct_destination_path




class Predictions(DataReaderClass):

    def __init__(self):
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        value_signals = kwargs['signals_value']
        growth_signals = kwargs['signals_growth']
        largecap_value_signals = kwargs['signals_largecap_value']
        largecap_growth_signals = kwargs['signals_largecap_growth']
        security_master = kwargs['security_master']
        security_master_dict = security_master.set_index(['dcm_security_id'])['ticker'].to_dict()
        self.final_data = []

        k = 'ensemble'
        for df in [growth_signals,value_signals,largecap_value_signals,largecap_growth_signals]:
            df['date'] = df['date'].apply(pd.Timestamp)
            df[k + '_qt'] = df.groupby('date')[k].apply(pd.qcut, 5, labels=False,duplicates='drop')
            df['ticker'] = df['ticker'].replace(security_master_dict)
            df = df[(df.date > '2024-02-01')]

            df = df[['date', 'ticker', k+'_qt', 'future_ret_5B']].rename(
                columns={k+'_qt': 'cross_sectional_rank'})
            self.final_data.append(df)

        return self._get_additional_step_results()

    def _get_additional_step_results(self):

        return {
            "signals_growth_predictions": self.final_data[0],
            "signals_value_predictions": self.final_data[1],
            "signals_largecap_value_predictions": self.final_data[2],
            "signals_largecap_growth_predictions": self.final_data[3],
        }

