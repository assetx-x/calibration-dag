import numpy as np
import pandas as pd

from plugins.DataCalculations.strategies.base_metrics_abc import BaseMetrics


class ReturnSummary(BaseMetrics):
    COLUMNS = ['ensemble']

    def __init__(self, dataframe):
        self.dataframe = dataframe
        self._validate()
        self._normalize()
        self.quantile_returns = self._get_long_short_strategy_performance(dataframe)
        self._top_quantile = max(self.quantile_returns.columns.tolist())
        self._bottom_quantile = min(self.quantile_returns.columns.tolist())
        _top_quantile = self.quantile_returns[self._top_quantile]
        _bottom_quantile = self.quantile_returns[self._bottom_quantile]
        self.rets = _top_quantile - _bottom_quantile

    def __dict__(self):
        return vars(self)

    def _normalize(self):
        if 'date' in self.dataframe.columns:
            self.dataframe['date'] = pd.to_datetime(self.dataframe['date'])
        # if 'ensemble' in self.dataframe.columns:
        #     self.dataframe['ensemble'] = self.dataframe['ensemble'].astype(int)
        #     print('[*] Normalizing ensemble column')

    def _validate(self):
        for col in self.COLUMNS:
            if f'{col}_qt' not in self.dataframe.columns:
                self.dataframe[f'{col}_qt'] = self.dataframe.groupby('date')[col].apply(
                    pd.qcut, 5, labels=False, duplicates='drop'
                )
                print(f'[*] Creating quantile column {col}_qt')

    def _calculate_annualized_return(self):
        return self.rets.mean() * 12

    def _calculate_annualized_volatility(self):
        return self.rets.std() * np.sqrt(12)

    def _calculate_sharpe_ratio(self):
        ann_ret = self._calculate_annualized_return()
        excess_return = ann_ret - 0.02
        ann_std = self._calculate_annualized_volatility()
        sharpe = excess_return / ann_std
        return sharpe

    def _calculate_treynor_ratio(self, beta=1):
        ann_ret = self._calculate_annualized_return()
        excess_return = ann_ret - 0.02
        treynor_ratio = excess_return / beta if beta != 0 else np.nan
        return treynor_ratio

    def _calculate_calmar_ratio(self):
        ann_ret = self._calculate_annualized_return()
        excess_return = ann_ret - 0.02

        rets = self.rets
        cum_rets = (1 + rets).cumprod()
        peak = cum_rets.expanding(min_periods=1).max()
        drawdown = (cum_rets - peak) / peak
        max_drawdown = drawdown.min()

        calmar_ratio = excess_return / abs(max_drawdown) if max_drawdown != 0 else np.nan

        return calmar_ratio

    def _get_long_short_strategy_performance(self, start_date=None, end_date=None):
        portfolio = self.dataframe.copy()

        if isinstance(start_date, pd.Timestamp):
            portfolio = portfolio[portfolio["date"] >= start_date]
        if isinstance(end_date, pd.Timestamp):
            portfolio = portfolio[portfolio["date"] <= end_date]

        rets = portfolio.groupby(['date', 'ensemble_qt'])['future_ret_5B'].mean().unstack()

        return rets

    def calculate_hit_ratio(self):
        quarterly_data = self.dataframe.groupby(pd.Grouper(key='date', freq='Q')).apply(
            lambda x: self._calculate_quarterly_hit_ratio(x)
        )
        return quarterly_data

    def _calculate_quarterly_hit_ratio(self, quarter_data):
        # Ensure that the index is a datetime index
        quarter_data.index = pd.to_datetime(quarter_data.index)

        long_short_dataframe = self._get_long_short_dataframe(quarter_data)

        hit_ratios = {}
        for year_quarter, quarter in long_short_dataframe.groupby(long_short_dataframe.index.to_period('Q')):
            hit_ratio = self._calculate_hit_ratio_for_quarter(quarter)
            hit_ratios[f"{year_quarter}"] = f"{hit_ratio:.1f}%"

        return hit_ratios

    def _get_long_short_dataframe(self, quarter_data):
        long_short_dataframe = pd.DataFrame()
        for year, quarter in quarter_data.groupby(quarter_data.index.year):
            if len(quarter) >= 4:  # Ensuring at least 4 records for a quarter
                longs = quarter[quarter['ensemble_qt'] == 4]
                shorts = quarter[quarter['ensemble_qt'] == 0]

                long_short_dataframe = pd.concat([long_short_dataframe, longs])
                long_short_dataframe = pd.concat([long_short_dataframe, shorts])

        return long_short_dataframe

    def _calculate_hit_ratio_for_quarter(self, quarter):
        hit_count = (
                ((quarter['ensemble_qt'] == 4) & (quarter['future_ret_5D'] > 0)).sum() +
                ((quarter['ensemble_qt'] == 0) & (quarter['future_ret_5D'] < 0)).sum()
        )
        total_entries = len(quarter)

        if total_entries > 0:
            hit_ratio = (hit_count / total_entries) * 100
            return hit_ratio
        else:
            return 0

