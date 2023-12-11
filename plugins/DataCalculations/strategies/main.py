import io

import pandas as pd
from pandas import read_csv

from plugins.DataCalculations.strategies.accuracy_metrics import AccuracyMetrics
from plugins.DataCalculations.strategies.return_summary import ReturnSummary


class PerformanceCalculator:
    calculators = [
        AccuracyMetrics,
        # ReturnSummary,
    ]

    def __init__(self, strategy_name, file_content):
        self.strategy_name = strategy_name
        self.dataframe = self._read_strategy_dataframe(file_content)

    def _read_strategy_dataframe(self, file_content) -> pd.DataFrame:
        """
        Read strategy CSV file and return a dataframe with the data to be calculated
        :param file_path (str): path to the strategy CSV file
        :return: dataframe with the data to be calculated
        """
        try:
            return read_csv(io.BytesIO(file_content), index_col=[0])
        except (
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
            FileNotFoundError,
        ) as e:
            print(f"Error reading strategy CSV file. {e}")  # {file_path.name} - {e}")
            raise ValueError(f"Error reading strategy CSV file. {e}")  # {file_path.name}") from e

    def run(self) -> dict:
        """
        Calculates every calculate_* method from every calculator class in the calculators list
        and returns a dictionary with the results
        :return: dictionary with the results

        Example output:
        {
            'AccuracyMetrics': {
                'calculate_correct_predictions': 0.5
            }
        }
        """
        return {
            Calculator.__name__: Calculator(self.dataframe).calculate()
            for Calculator in self.calculators
        }
