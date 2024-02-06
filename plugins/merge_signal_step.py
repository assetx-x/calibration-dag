import pandas as pd
from core_classes import GCPReader,download_yahoo_data,DataReaderClass
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path


class QuantamentalMergeSignals(DataReaderClass):
    '''

    Merges the data elements for the Quantamental ML calibration pipeline

    '''

    PROVIDES_FIELDS = ["data_full_population_signal"]
    REQUIRES_FIELDS = ["normalized_data_full_population", "intermediate_signals"]

    def __init__(self, drop_column):
        self.data = None
        self.drop_column = drop_column

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def do_step_action(self, **kwargs):
        all_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        all_signal_ranks = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        merged_data = pd.merge(all_data, all_signal_ranks.drop(self.drop_column, axis=1), how="left",
                               on=["date", "ticker"])
        self.data = merged_data
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data}

    @classmethod
    def get_default_config(cls):
        return {"drop_column": "future_ret_21B"}


class QuantamentalMergeSignalsWeekly(QuantamentalMergeSignals):
    PROVIDES_FIELDS = ["data_full_population_signal", "data_full_population_signal_weekly"]
    REQUIRES_FIELDS = ["normalized_data_full_population", "normalized_data_full_population_weekly",
                       "intermediate_signals", "intermediate_signals_weekly"]

    def __init__(self, drop_column):
        self.weekly_data = None
        QuantamentalMergeSignals.__init__(self, drop_column)

    def do_step_action(self, **kwargs):
        all_data = kwargs[self.__class__.REQUIRES_FIELDS[0]].copy(deep=True)
        all_data_weekly = kwargs[self.__class__.REQUIRES_FIELDS[1]].copy(deep=True)
        all_signal_ranks = kwargs[self.__class__.REQUIRES_FIELDS[2]].copy(deep=True)
        all_signal_ranks_weekly = kwargs[self.__class__.REQUIRES_FIELDS[3]].copy(deep=True)
        self.data = pd.merge(all_data, all_signal_ranks.drop(self.drop_column, axis=1), how="left",
                             on=["date", "ticker"])
        self.weekly_data = pd.merge(all_data_weekly, all_signal_ranks_weekly.drop(self.drop_column, axis=1),
                                    how="left", on=["date", "ticker"])

        return {'data_full_population_signal': self.data, 'data_full_population_signal_weekly': self.weekly_data}

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0]: self.data,
                self.__class__.PROVIDES_FIELDS[1]: self.weekly_data}




params_dict = {
        'drop_column': "future_ret_21B"
    }


QuantamentalMergeSignalsWeekly_params = {'params': params_dict,
                                         'class': QuantamentalMergeSignalsWeekly,
                                         'start_date': RUN_DATE,
                                         'provided_data': {
                                             'data_full_population_signal': construct_destination_path(
                                                 'merge_signal'),
                                             'data_full_population_signal_weekly': construct_destination_path(
                                                 'merge_signal')
                                             },
                                         'required_data': {
                                             'normalized_data_full_population':
                                                 construct_required_path('standarization',
                                                                         'normalized_data_full_population'),
                                             'normalized_data_full_population_weekly':
                                                 construct_required_path('standarization',
                                                                         'normalized_data_full_population_weekly'),

                                            'intermediate_signals': construct_required_path('intermediate_model_training',
                                                                                        'intermediate_signals'),
                                             'intermediate_signals_weekly': construct_required_path('intermediate_model_training',
                                                                                               'intermediate_signals_weekly'),

                                             }}




