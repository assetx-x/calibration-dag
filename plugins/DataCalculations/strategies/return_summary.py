import numpy as np
import pandas as pd

from plugins.DataCalculations.strategies.base_metrics_abc import BaseMetrics


class ReturnSummary(BaseMetrics):
    columns = ['ensemble']

    def __init__(self, dataframe):
        self.dataframe = dataframe
        self._validate()
        self.quantile_returns = self.get_long_short_strategy_performance(dataframe)
        self._top_quantile = max(self.quantile_returns.columns.tolist())
        self._bottom_quantile = min(self.quantile_returns.columns.tolist())
        self.rets = (
            self.quantile_returns[self._top_quantile] - self.quantile_returns[self._bottom_quantile]
        )

    def _validate(self):
        for col in self.columns:
            if f'{col}_qt' not in self.dataframe.columns:
                self.dataframe[f'{col}_qt'] = self.dataframe.groupby('date')[
                    col
                ].apply(pd.qcut, 5, labels=False, duplicates='drop')
                print(f'[*] Creating quantile column {col}_qt')

    def calculate_annualized_return(self):
        return self.rets.mean() * 12

    def calculate_annualized_volatility(self):
        return self.rets.std() * np.sqrt(12)

    def calculate_sharpe_ratio(self):
        ann_ret = self.calculate_annualized_return()
        excess_return = ann_ret - 0.02
        ann_std = self.calculate_annualized_return()
        sharpe = excess_return / ann_std
        return sharpe

    def calculate_treynor_ratio(self):
        beta = 1
        ann_ret = self.calculate_annualized_return()
        # 0.02 = risk_free_rate
        excess_return = ann_ret - 0.02
        treynor_ratio = excess_return / beta if beta != 0 else np.nan
        return treynor_ratio

    def calculate_calmar_ratio(self):
        ann_ret = self.calculate_annualized_return()
        # 0.02 = risk_free_rate
        excess_return = ann_ret - 0.02
        ann_std = self.calculate_annualized_return()
        # sharpe = excess_return / ann_std
        # pnl = (1 + x).cumprod()
        # mdd = (1 - pnl / pnl.cummax()).max()
        # calmar_ratio = ann_ret / mdd

    def get_long_short_strategy_performance(
            self,
            start_date=None,
            end_date=None,
            algo=0,
    ):
        column = "{0}_qt".format(algo)
        portfolio = self.dataframe.copy()
        if start_date:
            df = portfolio[(portfolio["date"] >= pd.Timestamp(start_date))]
        if end_date:
            df = df[(df["date"] <= pd.Timestamp(end_date))]
        long_port = portfolio[column].max()
        short_port = portfolio[column].min()
        rets = df.groupby(['date', column])['future_ret_5B'].mean().unstack()
        return rets
