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
import concurrent.futures
from tenacity import retry, stop_after_attempt,wait_fixed

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
        required_cols = ["RETAILx","USTRADE","T10YFFM","T5YFFM",
                         "CPITRNSL","DCOILWTICO",'HWI',"CUSR0000SA0L2",
                         "CUSR0000SA0L5","T1YFFM","DNDGRG3M086SBEA",
                         "AAAFFM","RPI","DEXUSUK","CPFF","CP3Mx",
                         "VIXCLS","GS10","CUSR0000SAC","GS5",
                         "WPSID62","IPDCONGD","WPSFD49502",'WPSFD49207']


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
        #print('first pass data shape : {}'.format(all_data.shape))
        print('missing data columns : {}'.format(set(required_cols) - set([d.columns[0] for d in all_data])))
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

##### REFACTORED VERSION ########
class DownloadEconomicDataR(DataReaderClass):
    '''

    Reads Economic Data From FRED API

    '''

    REQUIRES_FIELDS = ["econ_transformation"]
    PROVIDES_FIELDS = ["econ_data"]

    def __init__(self, start_date, end_date, sector_etfs, use_latest=True):
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.fred_api_key = '8ef69246bd7866b9476093e6f4141783'
        self.sector_etfs = sector_etfs
        self.use_latest = use_latest
        self.fred_connection = Fred(api_key=self.fred_api_key)
        # Required Columns
        self.required_cols = ["RETAILx", "USTRADE", "T10YFFM", "T5YFFM",
                              "CPITRNSL", "DCOILWTICO", 'HWI', "CUSR0000SA0L2",
                              "CUSR0000SA0L5", "T1YFFM", "DNDGRG3M086SBEA",
                              "AAAFFM", "RPI", "DEXUSUK", "CPFF", "CP3Mx",
                              "VIXCLS", "GS10", "CUSR0000SAC", "GS5",
                              "WPSID62", "IPDCONGD", "WPSFD49502", 'WPSFD49207']

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self, **kwargs):
        # When we need a value or none and it is inside of a dictionary, use the .get method
        self.start_date = pd.Timestamp(kwargs.get('start_date', self.start_date))
        self.end_date = pd.Timestamp(kwargs.get('end_date', self.end_date))

    def _remove_etf_keys(self, data_names):
        # Remove ETF specific close/volume keys to avoid errors

        for etf_name in self.sector_etfs:
            data_names.pop(etf_name + '_close', None)
            data_names.pop(etf_name + '_volume', None)

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
        # For error handling on retries
        self.reverse_map = {k: v for v, k in data_names.items()}
        print(self.reverse_map)

        return data_names

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0))
    def fetch_fred_data(self, concept):
        ''' Fetch data from FRED API with retry on failure. '''

        try:
            if self.use_latest:
                data = self.fred_connection.get_series(concept)
            else:
                # Special handling for 'SP500'
                if concept != 'SP500':
                    try:
                        data = self.fred_connection.get_series_first_release(concept)
                    except Exception as e:
                        data = self.fred_connection.get_series_latest_release(concept)
                else:
                    data = self.fred_connection.get_series(concept)
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

        return data

    def _pull_data_for_concept(self, concept, data_names):
        """Pulls data for single concept and processes it"""
        data = self.fetch_fred_data(concept)
        data = data.loc[self.start_date:self.end_date]
        df = data.to_frame(name=data_names[concept])
        df.index = df.index.normalize()
        return df

    def _parallel_pull_data(self, data_names):
        """ Parallelizes data pulling from FRED API"""

        self._remove_etf_keys(data_names)  # Remove ETF close/volume keys
        # Put future placeholders for each concept that is being pulled from fred

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_concept = {executor.submit(self._pull_data_for_concept, concept, data_names): concept for
                                 concept in data_names}

            all_data = {}
            # This loops through submitted tasks as they complete (as_complete)
            for future in concurrent.futures.as_completed(future_to_concept):
                # as each concept complete, we fetch the result of it and put it in the all_data dictionary
                concept = future_to_concept[future]
                try:
                    all_data[concept] = future.result()
                except Exception as exc:
                    print(f"{concept} generated an exception: {exc}")
        return all_data

    def _manually_retry_missing_series(self, all_data):
        # Check for missing columns after the initial pull
        missing_cols = set(self.required_cols) - set([d.columns[0] for d in all_data.values()])
        if missing_cols:
            print(f'missing data columns : {missing_cols}')
            for series in missing_cols:
                if series in self.reverse_map:
                    name = series
                    series = self.reverse_map[series]
                name = series

                try:
                    data = (
                        self.fred_connection.get_series(series)
                        if self.use_latest
                        else self.fred_connection.get_series_first_release(series)
                    )
                    data = data.loc[self.start_date: self.end_date]
                    data.name = name
                    data.index = data.index.normalize()
                    data = data.to_frame()
                    data = data.groupby(data.index).last()
                    all_data[series] = data
                except Exception as e:
                    print(e)
        return all_data

    def _pull_data(self, **kwargs):
        econ_trans = kwargs['econ_transformation']
        data_names = self._create_data_names(econ_trans)

        # Perform parallel fetching
        all_data = self._parallel_pull_data(data_names)

        # Retry any missing columns
        all_data = self._manually_retry_missing_series(all_data)

        # Combine all data into one DataFrame
        result = pd.concat([i for i in all_data.values()], axis=1)

        result.fillna(method="ffill", inplace=True)
        result.index.name = "date"

        return result

    def do_step_action(self, **kwargs):
        start = time()
        self._prepare_to_pull_data(**kwargs)
        result = self._pull_data(**kwargs)
        self.data = result
        end = time()
        print('ELAPSED TIME')
        print(end - start)
        return self.data

    def _post_process_pulled_data(self, result, **kwargs):
        return result


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
