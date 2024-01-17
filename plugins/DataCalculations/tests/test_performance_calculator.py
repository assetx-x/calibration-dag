import unittest
import pandas as pd
from unittest.mock import patch, Mock

from plugins.DataCalculations.strategies.main import PerformanceCalculator


class TestPerformanceCalculator(unittest.TestCase):
    def setUp(self):
        strategy_name = "Test Strategy"
        file_content = b"Test Content"
        Calculators = [
            Mock(spec=PerformanceCalculator.calculators[0], **{"calculate.return_value": 1}),
            Mock(spec=PerformanceCalculator.calculators[1], **{"calculate.return_value": 2}),
            Mock(spec=PerformanceCalculator.calculators[2], **{"calculate.return_value": 3}),
        ]
        self.performance_calculator = PerformanceCalculator(strategy_name, file_content)
        self.performance_calculator.calculators = Calculators

    def test_get_name(self):
        """
        Testing the 'get_name' function
        """
        self.assertEqual(self.performance_calculator.get_name(), "PerformanceCalculator")

    @patch("plugins.DataCalculations.strategies.main.pd.read_csv")
    def test_read_strategy_dataframe(self, mock_read_csv):
        """
        Testing the '_read_strategy_dataframe' function
        """
        file_content = b"Date,Value\n2022-01-01,100.0"
        expected_df = pd.DataFrame({"Value": [100.0]}, index=pd.to_datetime(["2022-01-01"]))

        mock_read_csv.return_value = expected_df
        strategy_name = "Test Strategy"
        performance_calculator = PerformanceCalculator(strategy_name, file_content)

        result = performance_calculator._read_strategy_dataframe(file_content)
        pd.testing.assert_frame_equal(result, expected_df)

    @patch("plugins.DataCalculations.strategies.main.pd.read_csv", side_effect=pd.errors.EmptyDataError("Empty data"))
    def test_read_strategy_dataframe_empty_error(self, mock_read_csv):
        """
        Testing the '_read_strategy_dataframe' function with an empty data error
        """
        file_content = b""

        with self.assertRaises(ValueError):
            self.performance_calculator._read_strategy_dataframe(file_content)

    def test_run(self):
        """
        Testing the 'run' function
        """
        mock_calculator = Mock()
        mock_calculator.get_name.return_value = "MockCalculator"
        mock_calculator.calculate.return_value = 1.23

        original_calculators = PerformanceCalculator.calculators
        PerformanceCalculator.calculators = [mock_calculator]

        expected_results = {"MockCalculator": 1.23}
        result = self.performance_calculator.run()

        self.assertEqual(result, expected_results)

        PerformanceCalculator.calculators = original_calculators


    def test_run_multiple_calculators(self):
        """
        Testing the 'run' function with multiple calculators
        """
        mock_calculator1 = Mock()
        mock_calculator1.get_name.return_value = "MockCalculator1"
        mock_calculator1.calculate.return_value = 1.23

        mock_calculator2 = Mock()
        mock_calculator2.get_name.return_value = "MockCalculator2"
        mock_calculator2.calculate.return_value = 4.56

        original_calculators = PerformanceCalculator.calculators
        PerformanceCalculator.calculators = [mock_calculator1, mock_calculator2]

        expected_results = {"MockCalculator1": 1.23, "MockCalculator2": 4.56}
        result = self.performance_calculator.run()
        for calculator in PerformanceCalculator.calculators:
            calculator_instance = calculator()
            result[calculator_instance.get_name()] = calculator_instance.calculate()

        self.assertEqual(result, expected_results)

        PerformanceCalculator.calculators = original_calculators


if __name__ == "__main__":
    unittest.main()
