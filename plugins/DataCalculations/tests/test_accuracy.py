import unittest
from pandas import DataFrame

from plugins.DataCalculations.strategies.accuracy_metrics import AccuracyMetrics


class TestAccuracyMetrics(unittest.TestCase):
    def test_empty_dataframe_exception(self):
        # Test if exception is raised when an empty DataFrame is passed to the constructor
        empty_df = DataFrame()
        with self.assertRaises(Exception) as context:
            AccuracyMetrics(empty_df)
        self.assertTrue('Empty dataframe AccuracyMetrics cannot be calculated' in str(context.exception))

    def test_no_predictions(self):
        df_no_predictions = DataFrame({'ensemble': [1, 2, 3], 'future_ret_5B': [1, 2, 3]})
        accuracy_metrics_no_predictions = AccuracyMetrics(df_no_predictions)
        self.assertEqual(accuracy_metrics_no_predictions.calculate_correct_predictions(), 0)


if __name__ == '__main__':
    unittest.main()
