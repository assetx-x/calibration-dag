import os
import unittest

import pandas as pd
from pandas import DataFrame

from plugins.DataCalculations.strategies.accuracy_metrics import AccuracyMetrics


class TestAccuracyMetrics(unittest.TestCase):
    def setUp(self):
        csv_file_path = os.path.join(os.getcwd(), 'value_predictions.csv')
        self.df = pd.read_csv(csv_file_path)
        self.accuracy_metrics = AccuracyMetrics(self.df)
        print(f'[*] {self.__class__.__name__} class started')

    def test_calculate_correct_predictions(self):
        result = self.accuracy_metrics.calculate_correct_predictions()
        self.assertEqual(result, 8)

    def test_calculate_number_of_predictions(self):
        result = self.accuracy_metrics.calculate_number_of_predictions()
        self.assertEqual(result, 9)

    def test_calculate_number_of_correct_predictions(self):
        result = self.accuracy_metrics.calculate_number_of_correct_predictions()
        self.assertEqual(result, 8)

    def test_calculate_up_periods(self):
        result = self.accuracy_metrics.calculate_up_periods()
        self.assertEqual(result, 3)

    # def test_calculate_down_periods(self):
    #     result = self.accuracy_metrics.calculate_down_periods()
    #     self.assertEqual(result, 2)

    def test_calculate_up_down_ratio(self):
        result = self.accuracy_metrics.calculate_up_down_ratio()
        self.assertEqual(result, 1.5)


if __name__ == '__main__':
    unittest.main()
