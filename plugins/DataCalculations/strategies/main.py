import pandas as pd
import io

from plugins.DataCalculations.strategies.accuracy_metrics import AccuracyMetrics
from plugins.DataCalculations.strategies.return_quantile_metrics import ReturnQuantileMetrics
from plugins.DataCalculations.strategies.volatility_metrics import VolatilityMetrics


class PerformanceCalculator:
    calculators = [
        AccuracyMetrics,
        # ReturnSummary,
        ReturnQuantileMetrics,
        VolatilityMetrics,
    ]

    @classmethod
    def get_name(cls):
        return cls.__name__

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
            return pd.read_csv(io.BytesIO(file_content), index_col=[0])
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
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
            Calculator.get_name(): Calculator(self.dataframe).calculate()
            for Calculator in self.calculators
        }
