from plugins.DataCalculations.strategies.base_metrics_abc import BaseMetrics
from pandas import DataFrame
import numpy as np


class VolatilityMetrics(BaseMetrics):
    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe

    def calculate_correlation(self):
        correlation = self.dataframe.corr()
        return correlation

    def calculate_std_dev_portfolio(self):
        std_dev_portfolio = np.std(self.dataframe)
        return std_dev_portfolio

    def calculate_upside_beta(self):
        mask = self.dataframe["returns"] > 0
        upside_beta = self.dataframe[mask].std() / self.dataframe['returns'].std()
        return upside_beta

    def calculate_downside_beta(self):
        mask = self.dataframe["returns"] < 0
        downside_beta = self.dataframe[mask].std() / self.dataframe['returns'].std()
        return downside_beta
