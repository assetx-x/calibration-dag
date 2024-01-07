from abc import ABC
import logging


def save_on_database(func):
    def wrapper(self, *args, **kwargs):
        results = func(self, *args, **kwargs)
        self._save_results(results)
        return results

    return wrapper


class BaseMetrics(ABC):
    def _validate(self):
        raise NotImplementedError(
            f'Please implement this method in your subclass {self.__class__.__name__}'
        )

    def _normalize(self):
        return NotImplementedError(
            f'Please implement this method in your subclass {self.__class__.__name__}'
        )

    @classmethod
    def get_name(cls):
        return cls.__name__

    @save_on_database
    def calculate(self):
        calculation_methods = self._get_calculation_methods()
        results = self._calculate_results(calculation_methods)
        return results

    def _get_calculation_methods(self):
        return [
            method
            for method in dir(self)
            if method.startswith('calculate_') and callable(getattr(self, method))
        ]

    def _calculate_results(self, methods):
        results = {}
        for method_name in methods:
            logging.info(
                f'[*] Calculating {method_name} from {self.__class__.__name__}'
            )
            method = getattr(self, method_name)
            results[method_name] = method()
        return results

    def _save_results(self, results):
        # logging.info(f'[MOCK] Saving {self.__class__.__name__} results on database here ...')
        # TODO save results on database
        pass


"""
-- README --

Creating a new strategy:
1. Create a new class in DataCalculations/strategies/base_metrics_abc.py file
2. Inherit from BaseMetrics
3. Define columns attribute
4. Define calculate methods
5. Add the new class to the calculators list in PerformanceCalculator class
6. Create the tests in DataCalculations/tests/test_strategy.py to ensure the new class works as expected

Template example:

    class MyCustomMetric(BaseMetrics):
        columns = ['column1', 'column2', 'column3']

        def __init__(self, dataframe):
            super().__init__(dataframe)
            self._validate()

        def calculate_correct_predictions(self):
            ''' perform calculations here '''
            your_result = 0
            return your_result

"""
