from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
from core_classes import StatusType
from commonlib import talib_STOCHRSI, MA,talib_PPO, talib_TRIX
import talib
import gc
from market_timeline import marketTimeline
current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path


class QuantamentalMerge(DataReaderClass):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["merged_data"]
    REQUIRES_FIELDS = ["daily_price_data", "fundamental_features", "target_returns", "talib_stochrsi_indicator_data",
                       "volatility_data", "talib_willr_indicator_data", "talib_adx_indicator_data",
                       "talib_ppo_indicator_data", "beta_data", "macd_indicator_data", "correlation_data",
                       "dollar_volume_data", "overnight_return_data", "past_return_data", "talib_stoch_indicator_data",
                       "talib_stochf_indicator_data", "talib_trix_indicator_data", "talib_ultosc_indicator_data",
                       "security_master"]

    def __init__(self, apply_log_vol=True, start_date=None, end_date=None):
        self.data = None
        self.apply_log_vol = apply_log_vol
        self.start_date = pd.Timestamp(start_date) if start_date else None
        self.end_date = pd.Timestamp(end_date) if end_date else None
        # CalibrationTaskflowTask.__init__(self)

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        gc.collect()
        try:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl[k] = kwargs[k]

            merged_data = overnight_return_data.copy(deep=True)
            merged_data["date"] = merged_data["date"].apply(pd.Timestamp)
            merged_data["date"] = pd.DatetimeIndex(merged_data["date"]).normalize()
            self.start_date = self.start_date or merged_data["date"].min()
            self.end_date = self.end_date or merged_data["date"].max()
            merged_data = merged_data[(merged_data["date"] >= self.start_date) & (merged_data["date"] <= self.end_date)]
            for i, df in enumerate([talib_stochrsi_indicator_data, volatility_data, talib_willr_indicator_data,
                                    talib_adx_indicator_data, talib_ppo_indicator_data, beta_data, macd_indicator_data,
                                    correlation_data, dollar_volume_data, past_return_data,
                                    talib_stoch_indicator_data, talib_stochf_indicator_data, talib_trix_indicator_data,
                                    talib_ultosc_indicator_data, fundamental_features, target_returns]):
                current_df = df.copy(deep=True)
                if isinstance(current_df.index, pd.MultiIndex):
                    current_df = current_df.reset_index()
                current_df["date"] = pd.DatetimeIndex(current_df["date"]).normalize()

                # merged_data["ticker"] = merged_data["ticker"].astype(int)
                # current_df["ticker"] = current_df["ticker"].astype(int)
                merged_data = pd.merge(merged_data, current_df, how="left", on=["date", "ticker"])
            # TODO: chang eback to spy
            #merged_data = merged_data.drop(["8554_correl_rank"], axis=1)
            #merged_data = merged_data.drop(["8554_beta_rank"], axis=1)

            merged_data = merged_data.drop(["SPY_correl_rank"], axis=1)
            merged_data = merged_data.drop(["SPY_beta_rank"], axis=1)


            if self.apply_log_vol:
                for col in [c for c in merged_data.columns if str(c).startswith("volatility_")]:
                    merged_data[col] = np.log(merged_data[col])
            merged_data["log_avg_dollar_volume"] = np.log(merged_data["avg_dollar_volume"])

            # merged_data["ticker"] = merged_data["ticker"].astype(int)
            merged_data["momentum"] = merged_data["ret_252B"] - merged_data["ret_21B"]
            index_cols = [c for c in merged_data.columns if str(c).startswith("index")]
            good_cols = list(set(merged_data.columns) - set(index_cols))
            merged_data = merged_data[good_cols]
            # TODO: change back to SPY
            # merged_data.columns = map(lambda x:x.replace("8554", "SPY"), merged_data.columns)
            merged_data["SPY_beta"] = merged_data["SPY_beta"].astype(float)
            #merged_data["ABCB_beta"] = merged_data["ABCB_beta"].astype(float)
            # TODO: added by Jack 2020/04/30 for Live mode. Live mode should use the latest available data without date shift
            # if self.task_params.run_mode==TaskflowPipelineRunMode.Calibration:
            merged_data["date"] = merged_data["date"].apply(lambda x: marketTimeline.get_trading_day_using_offset(x, 1))
            self.data = merged_data
        finally:
            lcl = globals()
            for k in self.REQUIRES_FIELDS:
                lcl.pop(k, None)
        return self.data


QM_datasets = {'daily_price_data': construct_required_path('data_pull', 'daily_price_data'),
               'fundamental_features': construct_required_path('derived_fundamental', 'fundamental_features'),
               'target_returns': construct_required_path('targets', 'target_returns'),
               'talib_stochrsi_indicator_data': construct_required_path('derived_technical',
                                                                        'talib_stochrsi_indicator_data'),
               'volatility_data': construct_required_path('derived_technical', 'volatility_data'),
               'talib_willr_indicator_data': construct_required_path('derived_technical', 'talib_willr_indicator_data'),
               'talib_adx_indicator_data': construct_required_path('derived_technical', 'talib_adx_indicator_data'),
               'talib_ppo_indicator_data': construct_required_path('derived_technical', 'talib_ppo_indicator_data'),
               'beta_data': construct_required_path('derived_simple', 'beta_data'),
               'macd_indicator_data': construct_required_path('derived_simple', 'macd_indicator_data'),
               'correlation_data': construct_required_path('derived_simple', 'correlation_data'),
               'dollar_volume_data': construct_required_path('derived_simple', 'dollar_volume_data'),
               'overnight_return_data': construct_required_path('derived_simple', 'overnight_return_data'),
               'past_return_data': construct_required_path('derived_simple', 'past_return_data'),
               'talib_stoch_indicator_data': construct_required_path('derived_simple', 'talib_stoch_indicator_data'),
               'talib_stochf_indicator_data': construct_required_path('derived_simple', 'talib_stochf_indicator_data'),
               'talib_trix_indicator_data': construct_required_path('derived_simple', 'talib_trix_indicator_data'),
               'talib_ultosc_indicator_data': construct_required_path('derived_simple', 'talib_ultosc_indicator_data'),
               'security_master': construct_required_path('data_pull', 'security_master'),

               }

QM_configs = {'apply_log_vol': True,
              'start_date': "2001-01-02",
              'end_date': RUN_DATE
              }

QuantamentalMerge_params = {'params':QM_configs,
                                   'class':QuantamentalMerge,'start_date':RUN_DATE,
                                'provided_data': {'merged_data': construct_destination_path('merge_step')},
                                'required_data': QM_datasets}
