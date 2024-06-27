import pandas as pd
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from datetime import datetime
import os
from google.cloud import storage


from datetime import datetime
import os
from google.cloud import storage
import gcsfs
from collections import defaultdict

def construct_required_path(step, file_name):
    return (
        "gs://{}/calibration_data/live"
        + "/{}/".format(step)
        + "{}.csv".format(file_name)
    )


def construct_destination_path(step):
    return "gs://{}/calibration_data/live" + "/{}/".format(step) + "{}.csv"


class DataFormatter(object):

    def __init__(self, class_, class_parameters, provided_data, required_data):

        self.class_ = class_
        self.class_parameters = class_parameters
        self.provided_data = provided_data
        self.required_data = required_data

    def construct_data(self):
        return {
            'class': self.class_,
            'params': self.class_parameters,
            # 'start_date':self.start_date,
            'provided_data': self._provided_data_construction(),
            'required_data': self._required_data_construction(),
        }

    def __call__(self):
        # When an instance is called, return the result of construct_data
        return self.construct_data()

    def _required_data_construction(self):

        if len(self.required_data) == 0:
            return {}
        elif len(self.required_data) == 1:
            directory = list(self.required_data.keys())[0]
            return {
                k: construct_required_path(directory, k)
                for k in self.required_data[directory]
            }

        else:
            path_dictionary = {}
            directories = list(self.required_data.keys())
            for directory in directories:
                path_dictionary.update(
                    {
                        k: construct_required_path(directory, k)
                        for k in self.required_data[directory]
                    }
                )
            return path_dictionary

    def _provided_data_construction(self):
        directory = list(self.provided_data.keys())[0]
        return {
            k: construct_destination_path(directory)
            for k in self.provided_data[directory]
        }


class DataReaderClass(ABC):

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key

    @abstractmethod
    def _prepare_to_pull_data(self, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def _get_data_lineage(self):
        raise NotImplementedError()

def read_csv_in_chunks(gcs_path, batch_size=10000, project_id='dcm-prod-ba2f'):
    """
    Reads a CSV file from Google Cloud Storage in chunks.
    Parameters:
    - gcs_path (str): The path to the CSV file on GCS.
    - batch_size (int, optional): The number of rows per chunk. Default is 10,000.
    - project_id (str, optional): The GCP project id. Default is 'dcm-prod-ba2f'.
    Yields:
    - pd.DataFrame: The data chunk as a DataFrame.
    """
    # Create GCS file system object
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Open the GCS file for reading
    with fs.open(gcs_path, 'r') as f:
        # Yield chunks from the CSV
        for chunk in pd.read_csv(f, chunksize=batch_size, index_col=0):
            yield chunk


def airflow_wrapper(**kwargs):
    params = kwargs['params']

    # Read all required data into step_action_args dictionary
    step_action_args = {
        k: pd.read_csv(v.format(os.environ['GCS_BUCKET']), index_col=0)
        for k, v in kwargs['required_data'].items()
    }
    #print(f'Executing step action with args {step_action_args}')

    # Execute do_step_action method
    data_outputs = kwargs['class'](**params).do_step_action(**step_action_args)

    print('Your Params are')
    print(params)

    # If the method doesn't return a dictionary (for classes returning just a single DataFrame)
    # convert it into a dictionary for consistency
    if not isinstance(data_outputs, dict):
        data_outputs = {list(kwargs['provided_data'].keys())[0]: data_outputs}

    # Save each output data to its respective path on GCS
    for data_key, data_value in data_outputs.items():
        if data_key in kwargs['provided_data']:
            gcs_path = kwargs['provided_data'][data_key].format(
                os.environ['GCS_BUCKET'], data_key
            )
            print('Here is the path')
            print(gcs_path)
            data_value.to_csv(gcs_path)


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
        adjustment_factor_data = (
            kwargs["adjustment_factor_data"]
            .drop("ticker", axis=1)
            .rename(columns={"dcm_security_id": "ticker"})
        )
        adjustment_factor_data = adjustment_factor_data[
            pd.notnull(adjustment_factor_data["ticker"])
        ].reset_index(drop=True)
        adjustment_factor_data["ticker"] = adjustment_factor_data["ticker"].astype(int)
        daily_price_data = kwargs["daily_price_data"]
        adjustment_factor_data["date"] = pd.DatetimeIndex(
            adjustment_factor_data["date"]
        ).normalize()
        daily_price_data['date'] = daily_price_data['date'].apply(pd.Timestamp)
        data_to_use = daily_price_data[["close", "ticker", "date"]]
        merged_data = pd.merge(
            data_to_use, adjustment_factor_data, how="left", on=["date", "ticker"]
        )
        merged_data = merged_data.sort_values(["ticker", "date"])
        merged_data["split_div_factor"] = merged_data["split_div_factor"].fillna(1.0)
        merged_data["raw_close_price"] = (
            merged_data["close"] * merged_data["split_div_factor"]
        )
        merged_data = merged_data[["ticker", "date", "raw_close_price"]]

        merged_shifted_data = merged_data.sort_values(["ticker", "date"]).set_index("date").groupby("ticker") \
            .apply(lambda x: x.shift(1)).reset_index()
        merged_shifted_data = merged_shifted_data.dropna(subset=["ticker"])
        merged_shifted_data["ticker"] = merged_shifted_data["ticker"].astype(int)
        self.data = merged_shifted_data
        return {'raw_price_data': self.data}

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {}


calculate_raw_prices = DataFormatter(
    class_=CalculateRawPrices,
    class_parameters={},
    provided_data={'GetRawPrices': ['raw_price_data']},
    required_data={
        'GetAdjustmentFactors': ['adjustment_factor_data'],
        'DataPull': ['daily_price_data'],
    },
)



if __name__ == "__main__":
    os.environ['GCS_BUCKET'] = 'dcm-prod-ba2f-us-dcm-data-test'

    raw_price_data = calculate_raw_prices()
    airflow_wrapper(**raw_price_data)