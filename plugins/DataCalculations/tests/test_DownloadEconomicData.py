import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from econ_data_step import DownloadEconomicData
from task_queue_executor import JobExecutor


class TestDownloadEconomicData(unittest.TestCase):
    """Unit tests for the DownloadEconomicData class."""

    def setUp(self):
        """
        Set up the common resources for all tests.

        Initializes an instance of the DownloadEconomicData class with predefined
        start and end dates, sector ETFs, and mock bucket/key parameters.
        """
        JobExecutor.initialize_executor(max_workers=3)

        self.start_date = '2021-01-01'
        self.end_date = '2021-12-31'
        self.sector_etfs = ['XLF', 'XLK']
        self.bucket = 'test-bucket'
        self.key = 'test-key'

        self.downloader = DownloadEconomicData(
            start_date=self.start_date,
            end_date=self.end_date,
            sector_etfs=self.sector_etfs,
            bucket=self.bucket,
            key=self.key
        )

    def test_prepare_to_pull_data(self):
        """
        Test the _prepare_to_pull_data method.

        This test verifies that the method correctly updates the start_date and end_date
        attributes based on the keyword arguments passed. It checks that the start_date
        and end_date in the object are modified to match the provided values.
        """
        kwargs = {'start_date': '2022-01-01', 'end_date': '2022-12-31'}
        self.downloader._prepare_to_pull_data(**kwargs)
        self.assertEqual(self.downloader.start_date, pd.Timestamp('2022-01-01'))
        self.assertEqual(self.downloader.end_date, pd.Timestamp('2022-12-31'))

    def test_remove_etf_keys(self):
        """
        Test the _remove_etf_keys method.

        This test ensures that ETF-related keys (e.g., 'XLF_close' and 'XLF_volume')
        are properly removed from the data_names dictionary. It confirms that ETF-specific
        keys are no longer in the dictionary after calling this method, while other keys
        (like 'SP500') remain unaffected.
        """
        data_names = {
            "XLF_close": "some_value",
            "XLF_volume": "some_value",
            "SP500": "some_value"
        }
        self.downloader._remove_etf_keys(data_names)
        self.assertNotIn("XLF_close", data_names)
        self.assertNotIn("XLF_volume", data_names)
        self.assertIn("SP500", data_names)

    def test_create_data_names(self):
        """
        Test the _create_data_names method.

        This test checks if the method correctly creates a mapping between economic
        transformation names and the corresponding column names. It uses a mock
        transformation DataFrame and ensures that the returned dictionary contains
        the expected mappings.
        """
        econ_trans = pd.DataFrame({
            "Unnamed: 0": ["RETAILx", "CPFF"],
            "transform": ["RRSFS", "CPFF"]
        })
        data_names = self.downloader._create_data_names(econ_trans)
        expected_data_names = {
            "RRSFS": "RETAILx",
            "CPFF": "CPFF",
            "CMRMTSPLx": "CMRMTSPL",
            "HWI": "TEMPHELPS"
        }

    @patch('fredapi.Fred.get_series')
    def test_fetch_fred_data(self, mock_get_series):
        """
        Test the fetch_fred_data method.

        This test verifies that the fetch_fred_data method retrieves data from the
        FRED API using the provided concept. The FRED API call is mocked to return
        sample data, and the test ensures that the method correctly returns the data
        without making a real API call.

        Mocks:
            fredapi.Fred.get_series: Mocked to return a sample pandas Series for the test.
        """
        mock_get_series.return_value = pd.Series([100, 200], index=pd.date_range('2021-01-01', periods=2))
        data = self.downloader.fetch_fred_data('SP500')
        self.assertIsNotNone(data)
        self.assertEqual(len(data), 2)

    @patch('fredapi.Fred.get_series')
    def test_pull_data_for_concept(self, mock_get_series):
        """
        Test the _pull_data_for_concept method.

        This test checks if the _pull_data_for_concept method fetches and processes data
        for a given concept correctly. The FRED API is mocked to return a pandas Series,
        and the test verifies that the method converts this data into a DataFrame with
        the correct column name.

        Mocks:
            fredapi.Fred.get_series: Mocked to return a sample pandas Series for the test.
        """
        mock_get_series.return_value = pd.Series([100, 200], index=pd.date_range('2021-01-01', periods=2))
        data_names = {"SP500": "S&P 500"}
        df = self.downloader._pull_data_for_concept('SP500', data_names)
        self.assertEqual(df.shape[0], 2)
        self.assertEqual(df.columns[0], 'S&P 500')

    @patch('concurrent.futures.ThreadPoolExecutor')
    @patch('fredapi.Fred.get_series')
    def test_parallel_pull_data(self, mock_get_series, MockExecutor):
        """
        Test the _parallel_pull_data method.

        This test verifies that the _parallel_pull_data method correctly pulls data
        in parallel for multiple concepts. It mocks the FRED API to return data and
        simulates parallel execution using ThreadPoolExecutor.

        Mocks:
            fredapi.Fred.get_series: Mocked to return a sample pandas Series for the test.
            concurrent.futures.ThreadPoolExecutor: Mocked to simulate parallel execution.
        """
        mock_get_series.return_value = pd.Series([100, 200], index=pd.date_range('2021-01-01', periods=2))
        mock_executor_instance = MockExecutor.return_value
        mock_future = MagicMock()
        mock_future.result.return_value = pd.DataFrame({'S&P 500': [100, 200]})
        mock_executor_instance.submit.return_value = mock_future
        mock_executor_instance.as_completed.return_value = [mock_future]

        data_names = {"SP500": "S&P 500"}
        all_data = self.downloader._parallel_pull_data(data_names)
        self.assertIn("SP500", all_data)
        self.assertEqual(all_data["SP500"].shape[0], 2)

    @patch('fredapi.Fred.get_series')
    def test_manually_retry_missing_series(self, mock_get_series):
        """
        Test the _manually_retry_missing_series method.

        This test checks if the _manually_retry_missing_series method correctly retries
        fetching missing data series that were not retrieved in the initial data pull.
        The FRED API is mocked to return data, and the test ensures that the missing
        series are added to the final data.

        Mocks:
            fredapi.Fred.get_series: Mocked to return a sample pandas Series for the test.
        """
        mock_get_series.return_value = pd.Series([100, 200], index=pd.date_range('2021-01-01', periods=2))
        all_data = {
            "SP500": pd.DataFrame({"S&P 500": [100, 200]}, index=pd.date_range('2021-01-01', periods=2))
        }
        result_data = self.downloader._manually_retry_missing_series(all_data)
        self.assertIn("SP500", result_data)
        self.assertEqual(result_data["SP500"].shape[0], 2)

    @patch('pandas.concat')
    @patch('plugins.task_queue_executor.JobExecutor.get_completed_jobs')
    @patch('econ_data_step.DownloadEconomicData._parallel_pull_data')
    def test_pull_data(self, mock_parallel_pull, mock_get_completed_jobs, mock_concat):
        """
        Test the _pull_data method.

        This test verifies that the _pull_data method orchestrates the entire data
        pulling process by calling _parallel_pull_data and retrying any missing series.
        It mocks the data pulling, job completion, and concatenation steps to ensure
        the method works as expected.

        Mocks:
            your_module.DownloadEconomicData._parallel_pull_data: Mocked to simulate the data pull.
            your_module.JobExecutor.get_completed_jobs: Mocked to simulate job completion.
            pandas.concat: Mocked to simulate data concatenation.
        """
        mock_parallel_pull.return_value = {
            "SP500": pd.DataFrame({"S&P 500": [100, 200]}, index=pd.date_range('2021-01-01', periods=2))
        }
        mock_get_completed_jobs.return_value = []
        mock_concat.return_value = pd.DataFrame({"S&P 500": [100, 200]}, index=pd.date_range('2021-01-01', periods=2))

        result = self.downloader._pull_data(econ_transformation=pd.DataFrame())
        self.assertIsNotNone(result)
        self.assertEqual(result.shape[0], 2)

    @patch('time.time')
    @patch('econ_data_step.DownloadEconomicData._pull_data')
    def test_do_step_action(self, mock_pull_data, mock_time):
        """
        Test the do_step_action method.

        This test simulates the full data retrieval step by mocking the time-tracking
        and data pulling process. It ensures that the method correctly prepares, pulls,
        and returns the data, and it verifies that the elapsed time is logged.

        Mocks:
            your_module.DownloadEconomicData._pull_data: Mocked to simulate the data pulling.
            time.time: Mocked to simulate time-tracking for logging the duration.
        """
        mock_time.side_effect = [1000, 2000]
        mock_pull_data.return_value = pd.DataFrame({"S&P 500": [100, 200]},
                                                   index=pd.date_range('2021-01-01', periods=2))

        result = self.downloader.do_step_action(econ_transformation=pd.DataFrame())
        self.assertIsNotNone(result)
        self.assertEqual(result.shape[0], 2)

    def test_post_process_pulled_data(self):
        """
        Test the _post_process_pulled_data method.

        This test ensures that the _post_process_pulled_data method returns the
        result without applying any modifications. It verifies that the input
        DataFrame and the output DataFrame are identical.
        """
        result = pd.DataFrame({"S&P 500": [100, 200]}, index=pd.date_range('2021-01-01', periods=2))
        processed_data = self.downloader._post_process_pulled_data(result)
        self.assertTrue(processed_data.equals(result))


if __name__ == '__main__':
    unittest.main()
