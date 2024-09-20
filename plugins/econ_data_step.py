import logging
from time import time

from core_classes import DataReaderClass
import pandas as pd

from fredapi import Fred
from datetime import datetime
from core_classes import construct_required_path, construct_destination_path
from tenacity import retry, stop_after_attempt, wait_fixed

from task_queue_executor import JobExecutor

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')


class DownloadEconomicData(DataReaderClass):
    """
    A class that reads economic data from the FRED API, processes it, and
    performs parallel fetching using the JobExecutor.

    Attributes:
        start_date (pd.Timestamp): The start date for data retrieval.
        end_date (pd.Timestamp): The end date for data retrieval.
        sector_etfs (list): A list of ETF symbols.
        use_latest (bool): Whether to fetch the latest release of data.
        fred_connection (Fred): A connection to the FRED API.
        reverse_map (dict): A dictionary mapping data names to their transformations.
    """

    REQUIRES_FIELDS = ["econ_transformation"]
    PROVIDES_FIELDS = ["econ_data"]
    FRED_API_KEY = '8ef69246bd7866b9476093e6f4141783'

    REQUIRED_COLS = [
        "RETAILx",
        "USTRADE",
        "T10YFFM",
        "T5YFFM",
        "CPITRNSL",
        "DCOILWTICO",
        "HWI",
        "CUSR0000SA0L2",
        "CUSR0000SA0L5",
        "T1YFFM",
        "DNDGRG3M086SBEA",
        "AAAFFM",
        "RPI",
        "DEXUSUK",
        "CPFF",
        "CP3Mx",
        "VIXCLS",
        "GS10",
        "CUSR0000SAC",
        "GS5",
        "WPSID62",
        "IPDCONGD",
        "WPSFD49502",
        "WPSFD49207",
    ]

    DATA_NAME_MAPPINGS = {
        "CMRMTSPLx": "CMRMTSPL",
        "RETAILx": "RRSFS",
        "HWI": "TEMPHELPS",
        "CLAIMSx": "ICSA",
        "AMDMNOx": "DGORDER",
        "ANDENOx": "NEWORDER",
        "AMDMUOx": "AMDMUO",
        "BUSINVx": "BUSINV",
        "ISRATIOx": "ISRATIO",
        "CONSPI": "NONREVSL",
        "S&P 500": "SP500",
        "CP3Mx": "DCPF3M",
        "EXSZUSx": "DEXSZUS",
        "EXJPUSx": "DEXJPUS",
        "EXCAUSx": "DEXCAUS",
        "UMCSENTx": "UMCSENT",
        "CUMFNS": "MCUMFN",
    }

    def __init__(self, start_date, end_date, sector_etfs, bucket, key, use_latest=True):
        """
        Initializes the DownloadEconomicData class with the required parameters.

        Args:
            start_date (str): The start date for retrieving data.
            end_date (str): The end date for retrieving data.
            sector_etfs (list): A list of ETF symbols.
            bucket (str): The bucket where the data will be saved.
            key (str): The key for saving data.
            use_latest (bool): A flag indicating whether to use the latest data release.
        """
        super().__init__(bucket, key)
        self.data = None
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.sector_etfs = sector_etfs
        self.use_latest = use_latest
        self.fred_connection = Fred(api_key=self.FRED_API_KEY)
        self.reverse_map = {v: k for k, v in self.DATA_NAME_MAPPINGS.items()}

    def _get_data_lineage(self):
        raise NotImplementedError(f"Method _get_data_lineage not implemented in {self.__class__.__name__}")

    def _prepare_to_pull_data(self, **kwargs):
        """
        Prepares the object to pull data by updating the start and end dates if necessary.

        Args:
            **kwargs: Optional keyword arguments containing 'start_date' and 'end_date'.
                      If provided, they will update the existing start and end dates.
        """
        self.start_date = pd.Timestamp(kwargs.get('start_date', self.start_date))
        self.end_date = pd.Timestamp(kwargs.get('end_date', self.end_date))

    def _remove_etf_keys(self, data_names):
        """
        Removes ETF-related keys from the data names dictionary.

        Args:
            data_names (dict): The dictionary containing data names.
        """
        for etf_name in self.sector_etfs:
            data_names.pop(f"{etf_name}_close", None)
            data_names.pop(f"{etf_name}_volume", None)

    def _create_data_names(self, econ_trans):
        """
        Creates the data names based on economic transformation mappings.

        Args:
            econ_trans (pd.DataFrame): A DataFrame containing transformation data.

        Returns:
            dict: A dictionary mapping economic transformations to data names.
        """
        econ_trans = econ_trans.set_index("Unnamed: 0")
        transform_dict = econ_trans["transform"].to_dict()
        data_names = {value: key for key, value in transform_dict.items()}
        data_names.update(self.DATA_NAME_MAPPINGS)
        self.reverse_map = {v: k for k, v in data_names.items()}
        return data_names

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(0))
    def fetch_fred_data(self, concept):
        """
        Fetches data from the FRED API with retries on failure.

        Args:
            concept (str): The data concept to be fetched from FRED.

        Returns:
            pd.Series: A pandas Series containing the requested data.
            None: If an error occurs or data is unavailable.
        """
        try:
            if self.use_latest or concept == 'SP500':
                return self.fred_connection.get_series(concept)
            try:
                return self.fred_connection.get_series_first_release(concept)
            except Exception as e:
                logging.error(f"First release data unavailable for {concept}: {e}")
                return self.fred_connection.get_series_latest_release(concept)
        except Exception as e:
            logging.error(f"An error occurred while fetching {concept}: {e}")
            return None

    def _pull_data_for_concept(self, concept, data_names):
        """
        Pulls and processes data for a single concept.

        Args:
            concept (str): The concept for which data needs to be pulled.
            data_names (dict): A dictionary mapping concept names to column names.

        Returns:
            pd.DataFrame: A DataFrame containing the data for the concept.
            None: If no data is available or the data is empty.
        """
        data = self.fetch_fred_data(concept)
        if data is None:
            return None
        data = data.loc[self.start_date : self.end_date]
        if data.empty:
            logging.warning(
                f"No data found for {concept} between {self.start_date} and {self.end_date}"
            )
            return None
        df = data.to_frame(name=data_names.get(concept, concept))
        df.index = df.index.normalize()
        return df

    def _parallel_pull_data(self, data_names):
        """
        Uses JobExecutor to parallelize data pulling from the FRED API.

        Args:
            data_names (dict): A dictionary mapping concept names to data columns.

        Returns:
            dict: A dictionary of all fetched data, where keys are concept names
                  and values are pandas DataFrames.
        """
        self._remove_etf_keys(data_names)
        all_data = {}

        for concept in data_names:
            JobExecutor.submit_job(self._execute_job, concept, data_names)

        results = JobExecutor.get_completed_jobs()
        for result in results:
            if result is not None:
                concept, df = result
                all_data[concept] = df
        return all_data

    def _execute_job(self, concept, data_names):
        """
        Helper method to execute a job and return the result.

        Args:
            concept (str): The concept for which data is being pulled.
            data_names (dict): A dictionary mapping concept names to data columns.

        Returns:
            tuple: A tuple of (concept, pd.DataFrame) if successful.
            None: If the data could not be fetched or processed.
        """
        df = self._pull_data_for_concept(concept, data_names)
        return (concept, df) if df is not None else None

    def _manually_retry_missing_series(self, all_data):
        """
        Retries fetching any missing data series that were not successfully pulled.

        Args:
            all_data (dict): A dictionary containing the fetched data.

        Returns:
            dict: An updated dictionary with missing data series retried.
        """
        existing_columns = {df.columns[0] for df in all_data.values()}
        missing_cols = set(self.REQUIRED_COLS) - existing_columns
        if not missing_cols:
            return all_data

        for series in missing_cols:
            concept = self.reverse_map.get(series, series)
            data = self.fetch_fred_data(concept)
            if data is None:
                logging.error(f"Could not fetch data for missing series {series}")
                continue
            data = data.loc[self.start_date : self.end_date]
            if data.empty:
                logging.warning(
                    f"No data found for missing series {series} between {self.start_date} and {self.end_date}"
                )
                continue
            df = data.to_frame(name=series)
            df.index = df.index.normalize()
            all_data[concept] = df
        return all_data

    def _pull_data(self, **kwargs):
        """
        Orchestrates the data pulling process by first parallelizing the data pull
        and then retrying any missing series manually.

        Args:
            **kwargs: Keyword arguments, including the 'econ_transformation' DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the combined data for all concepts.
        """
        econ_trans = kwargs['econ_transformation']
        data_names = self._create_data_names(econ_trans)
        all_data = self._parallel_pull_data(data_names)
        all_data = self._manually_retry_missing_series(all_data)
        if not all_data:
            logging.error("No data was pulled.")
            return pd.DataFrame()
        result = pd.concat(all_data.values(), axis=1)
        result.fillna(method="ffill", inplace=True)
        result.index.name = "date"
        return result

    def do_step_action(self, **kwargs):
        """
        Performs the entire data retrieval step, including preparing data,
        pulling data, and post-processing.

        Args:
            **kwargs: Keyword arguments to be passed for data preparation and pulling.

        Returns:
            pd.DataFrame: The final processed data.
        """
        start_time = time()
        self._prepare_to_pull_data(**kwargs)
        self.data = self._pull_data(**kwargs)
        logging.info(f"Data pulled in {time() - start_time} seconds")
        return self.data

    def _post_process_pulled_data(self, result, **kwargs):
        """
        Post-processes the pulled data after it has been retrieved.

        Args:
            result (pd.DataFrame): The data to be processed.
            **kwargs: Optional keyword arguments for additional processing.

        Returns:
            pd.DataFrame: The processed data.
        """
        return result


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
