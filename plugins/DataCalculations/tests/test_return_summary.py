import unittest

import numpy as np
import pandas as pd
from plugins.DataCalculations.strategies.return_summary import ReturnSummary


class TestReturnSummary(unittest.TestCase):
    def setUp(self):
        self.test_data = pd.read_csv('value_predictions.csv')
        self.return_summary = ReturnSummary(self.test_data)

    def test_validate(self):
        self.assertTrue('ensemble_qt' in self.return_summary.dataframe.columns)

    def test_annualized_return(self):
        expected_return = self.return_summary.rets.mean() * 12
        self.assertAlmostEqual(
            self.return_summary._calculate_annualized_return(), expected_return
        )

    def test_annualized_volatility(self):
        expected_volatility = self.return_summary.rets.std() * np.sqrt(12)
        self.assertAlmostEqual(
            self.return_summary._calculate_annualized_volatility(), expected_volatility
        )

    def test_calculate_sharpe_ratio(self):
        ann_ret = self.return_summary._calculate_annualized_return()
        excess_return = ann_ret - 0.02
        ann_std = self.return_summary._calculate_annualized_volatility()
        expected_sharpe = excess_return / ann_std

        self.assertAlmostEqual(
            self.return_summary._calculate_sharpe_ratio(), expected_sharpe
        )

    def test_calculate_treynor_ratio(self):
        beta = 1.5
        ann_ret = self.return_summary._calculate_annualized_return()
        excess_return = ann_ret - 0.02
        expected_treynor = excess_return / beta if beta != 0 else np.nan

        self.assertAlmostEqual(
            self.return_summary._calculate_treynor_ratio(beta=beta), expected_treynor
        )

    def test_get_long_short_strategy_performance(self):
        start_date = pd.Timestamp('2023-01-01')
        end_date = pd.Timestamp('2023-06-30')

        result = self.return_summary._get_long_short_strategy_performance(
            start_date, end_date
        )

        self.assertIsInstance(result, pd.DataFrame)
        # self.assertTrue('ensemble_qt' in result.columns)
        # self.assertTrue('future_ret_5B' in result.columns)

    # def test_calculate_hit_ratio(self):
    #     result = self.return_summary.calculate_hit_ratio()
    #
    #     self.assertIsInstance(result, pd.Series)


if __name__ == '__main__':
    unittest.main()
