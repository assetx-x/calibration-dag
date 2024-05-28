import unittest
from unittest.mock import MagicMock, patch

from plugins.DataCalculations.strategies.base_metrics_abc import BaseMetrics


class TestBaseMetrics(unittest.TestCase):
    def setUp(self):
        self.base_metrics = BaseMetrics()

    def test_validate_empty_dataframe(self):
        with self.assertRaises(Exception) as context:
            self.base_metrics._validate()
        self.assertTrue(
            'Please implement this method in your subclass' in str(context.exception)
        )

    def test_calculate_method(self):
        self.base_metrics.calculate_calculate_example = MagicMock(return_value=42)
        with patch('logging.info') as mock_logging:
            results = self.base_metrics.calculate()
            self.assertEqual(results['calculate_calculate_example'], 42)
            mock_logging.assert_called_with(
                '[*] Calculating calculate_calculate_example from BaseMetrics'
            )


if __name__ == '__main__':
    unittest.main()
