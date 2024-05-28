from pandas import qcut

from plugins.DataCalculations.strategies.base_metrics_abc import BaseMetrics


class ReturnQuantileMetrics(BaseMetrics):
    columns = ['future_ret_5B']

    def __init__(self, dataframe):
        self.dataframe = dataframe
        self._validate()

    def _validate(self):
        if len(self.dataframe) == 0:
            raise Exception(
                f'[*] Empty dataframe {self.__class__.__name__} cannot be calculated'
            )
        if not all([col in self.dataframe.columns for col in self.columns]):
            raise Exception(
                f'[*] Columns not found in dataframe {self.__class__.__name__} cannot be calculated'
            )

    def calculate_quantiles(self):
        self.dataframe['quantiles'] = (
            qcut(self.dataframe['future_ret_5B'], 5, labels=False) + 1
        )
        return self.dataframe.to_dict()
