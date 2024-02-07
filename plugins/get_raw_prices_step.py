import pandas as pd
from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path

class CalculateRawPrices(DataReaderClass):
    '''

    Calculates raw entry price (unadjusted using the adjustment factors) at the beginning of intervals

    '''

    PROVIDES_FIELDS = ["raw_price_data"]
    REQUIRES_FIELDS = ["adjustment_factor_data", "daily_price_data"]

    def __init__(self):
        self.data = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        adjustment_factor_data = kwargs["adjustment_factor_data"].drop("ticker", axis=1).rename(columns={"dcm_security_id": "ticker"})
        adjustment_factor_data = adjustment_factor_data[pd.notnull(adjustment_factor_data["ticker"])].reset_index(drop=True)
        adjustment_factor_data["ticker"] = adjustment_factor_data["ticker"].astype(int)
        daily_price_data = kwargs["daily_price_data"]
        adjustment_factor_data["date"] = pd.DatetimeIndex(adjustment_factor_data["date"]).normalize()
        daily_price_data['date'] = daily_price_data['date'].apply(pd.Timestamp)
        data_to_use = daily_price_data[["close", "ticker", "date"]]
        merged_data = pd.merge(data_to_use, adjustment_factor_data, how="left", on=["date", "ticker"])
        merged_data = merged_data.sort_values(["ticker", "date"])
        merged_data["split_div_factor"] = merged_data["split_div_factor"].fillna(1.0)
        merged_data["raw_close_price"] = merged_data["close"]*merged_data["split_div_factor"]
        merged_data = merged_data[["ticker", "date", "raw_close_price"]]
        merged_shifted_data = merged_data.sort_values(["ticker", "date"]).set_index("date").groupby("ticker")\
            .apply(lambda x:x.shift(1)).reset_index()
        merged_shifted_data = merged_shifted_data.dropna(subset=["ticker"])
        merged_shifted_data["ticker"] = merged_shifted_data["ticker"].astype(int)
        self.data = merged_shifted_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {}


CalculateRawPrices_params = {'params':{},
                                            'class':CalculateRawPrices,
                                      'start_date':RUN_DATE,
                                        'provided_data': {'raw_price_data': construct_destination_path('get_raw_prices')},
                                            'required_data': {
                                                'adjustment_factor_data': construct_required_path(
                                                    'get_adjustment_factors', 'adjustment_factor_data'),
                                                'daily_price_data': construct_required_path(
                                                    'data_pull', 'daily_price_data')}

                                            }


