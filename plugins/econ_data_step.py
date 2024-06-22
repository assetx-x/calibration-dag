from core_classes import GCPReader, download_yahoo_data, DataReaderClass
from market_timeline import marketTimeline
from abc import ABC, ABCMeta, abstractmethod
from google.cloud import storage
from enum import Enum
import pandas as pd
import io
import os
from pandas_datareader import data as web
import yfinance as yf

# import fix_yahoo_finance as fyf
import sys
from fredapi import Fred
import pandas
from google.cloud import bigquery
import gc
import numpy as np
from fredapi import Fred
from datetime import datetime
from core_classes import construct_required_path, construct_destination_path


current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')


class DownloadEconomicData(DataReaderClass):
    '''

    Reads Economic Data From FRED API

    '''

    REQUIRES_FIELDS = ["econ_transformation"]
    PROVIDES_FIELDS = ["econ_data"]

    def __init__(self, start_date, end_date, sector_etfs, use_latest=True):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.data = None
        # self.fred_api_key = get_config()["credentials"]
        self.fred_api_key = "8ef69246bd7866b9476093e6f4141783"
        self.sector_etfs = sector_etfs
        self.use_latest = use_latest
        # DataReader.__init__(self, "date", "dcm_security_id", "US/Eastern", "UTC", False, False)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        self.start_date = (
            self.start_date
            if pd.notnull(self.start_date)
            else self.task_params.start_dt
        )
        self.end_date = (
            self.end_date if pd.notnull(self.end_date) else self.task_params.end_dt
        )
        self.fred_connection = Fred(api_key=self.fred_api_key)

    def _create_data_names(self, econ_trans):
        econ_trans = econ_trans.set_index("Unnamed: 0")
        transform_dict = econ_trans["transform"].to_dict()
        data_names = dict(zip(transform_dict.keys(), transform_dict.keys()))
        data_names["CMRMTSPLx"] = "CMRMTSPL"
        data_names["RETAILx"] = "RRSFS"
        data_names["HWI"] = "TEMPHELPS"
        data_names["CLAIMSx"] = "ICSA"
        data_names["AMDMNOx"] = "DGORDER"
        data_names["ANDENOx"] = "NEWORDER"
        data_names["AMDMUOx"] = "AMDMUO"
        data_names["BUSINVx"] = "BUSINV"
        data_names["ISRATIOx"] = "ISRATIO"
        data_names["CONSPI"] = "NONREVSL"
        data_names["S&P 500"] = "SP500"
        data_names["CP3Mx"] = "DCPF3M"
        data_names["EXSZUSx"] = "DEXSZUS"
        data_names["EXJPUSx"] = "DEXJPUS"
        data_names["EXCAUSx"] = "DEXCAUS"
        data_names["UMCSENTx"] = "UMCSENT"
        data_names["CUMFNS"] = "MCUMFN"
        data_names = dict(zip(data_names.values(), data_names.keys()))
        return data_names

    def _pull_data(self, **kwargs):
        econ_trans = kwargs["econ_transformation"]
        data_names = self._create_data_names(econ_trans)
        required_cols = ['WPSFD49502', 'WPSID62', 'EXCAUSx', 'M2REAL', 'M2SL']

        for etf_name in self.sector_etfs:
            data_names.pop(
                etf_name + "_close", None
            )  # Use .pop(key, None) to avoid KeyError if key does not exist
            data_names.pop(etf_name + "_volume", None)
        all_data = []
        for concept in data_names:
            try:
                if self.use_latest:
                    data = self.fred_connection.get_series(concept)
                else:
                    # Special handling for 'SP500'
                    if concept != 'SP500':
                        try:
                            data = self.fred_connection.get_series_first_release(
                                concept
                            )
                        except Exception as e:
                            data = self.fred_connection.get_series_latest_release(
                                concept
                            )
                    else:
                        data = self.fred_connection.get_series(concept)
                data = data.loc[self.start_date : self.end_date]
                data.name = data_names[concept]
                data.index = data.index.normalize()
                data = data.to_frame()
                data = data.groupby(data.index).last()
                all_data.append(data)
            except Exception as e:
                print("****** PROBLEM WITH : {0}".format(concept))
        # TODO get rid of the below logic and consolidate to look for lists of missing tickers instead

        for series in required_cols:
            # Check if 'WPSFD49502' data is missing after the initial download attempts
            if series not in [d.columns[0] for d in all_data]:
                # Try to download 'WPSFD49502' one more time
                try:
                    data = (
                        self.fred_connection.get_series(series)
                        if self.use_latest
                        else self.fred_connection.get_series_first_release(series)
                    )
                    data = data.loc[self.start_date : self.end_date]
                    data.name = series
                    data.index = data.index.normalize()
                    data = data.to_frame()
                    data = data.groupby(data.index).last()
                    all_data.append(data)
                except Exception as e:
                    # If there's an error in the second attempt, raise an exception
                    raise Exception(
                        "Failed to download {} on the second attempt.".format(series)
                    )


        result = pd.concat(all_data, axis=1)
        result = result.loc[
            list(map(lambda x: marketTimeline.isTradingDay(x), result.index))
        ]
        result = result.fillna(method="ffill")
        result.index.name = "date"
        return result

    def _post_process_pulled_data(self, result, **kwargs):
        return result

    def _get_data_lineage(self):
        pass

    def do_step_action(self, **kwargs):
        self._prepare_to_pull_data(**kwargs)
        result = self._pull_data(**kwargs)
        self.data = self._post_process_pulled_data(result, **kwargs)
        return self.data


################ AIRFLOW ##################

DOWNLOAD_ECONOMIC_DATA_PARAMS = {
    "params": {
        "sector_etfs": ["SPY", "MDY", "EWG", "EWH", "EWJ", "EWW", "EWS", "EWU"],
        "start_date": "1997-01-01",
        "end_date": RUN_DATE,
    },
    "start_date": RUN_DATE,
    'class': DownloadEconomicData,
    'provided_data': {'econ_data': construct_destination_path('econ_data')},
    'required_data': {
        'econ_transformation': construct_required_path(
            'data_pull', 'econ_transformation'
        )
    },
}
