import pandas as pd
from core_classes import GCPReader,download_yahoo_data,DataReaderClass
from market_timeline import marketTimeline
import pandas as pd
import numpy as np
from datetime import datetime
import os
from core_classes import StatusType
from google.cloud import storage
import statsmodels.api as sm

current_date = datetime.now().date()
RUN_DATE = current_date.strftime('%Y-%m-%d')
from core_classes import construct_required_path,construct_destination_path

FILTER_MODES = ["growth", "value", "largecap_growth", "largecap_value"]


class AddFoldIdToNormalizedDataPortfolio(DataReaderClass):

    PROVIDES_FIELDS = ["r1k_neutral_normal_models_with_foldId"]
    REQUIRES_FIELDS = ["r1k_neutral_normal_models"]

    def __init__(self, cut_dates):
        self.cut_dates = sorted([pd.Timestamp(date) for date in cut_dates] + [pd.Timestamp("1900-12-31"),pd.Timestamp("2100-12-31")])
        self.r1k_neutral_normal_models_with_foldId = None

    def _get_data_lineage(self):
        pass

    def _prepare_to_pull_data(self):
        pass

    def _add_foldId(self, df):
        df["fold_id"] = pd.cut(df["date"], self.cut_dates, labels = np.arange(len(self.cut_dates)-1), right =False)
        df["fold_id"] = pd.Series(df["fold_id"].cat.codes).astype(int)
        df = df.set_index(["date", "ticker"]).reset_index()
        return df

    def do_step_action(self, **kwargs):
        r1k_neutral_normal_models = kwargs["r1k_neutral_normal_models"].copy(deep=True).to_dict(orient='dict')[0]

        assert set(r1k_neutral_normal_models)==set(FILTER_MODES), "AddFoldIdToNormalizedData - r1k_neutral_normal_models \
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_neutral_normal_models))

        r1k_foldId_dict = {}
        for mode in r1k_neutral_normal_models:
            print("Adding foldId to r1k data for {0} model".format(mode))
            r1k_foldId_dict[mode] = self._add_foldId(r1k_neutral_normal_models[mode])

        self.r1k_neutral_normal_models_with_foldId = pd.DataFrame.from_dict(r1k_foldId_dict, orient='index')
        return StatusType.Success

    def _get_additional_step_results(self):
        return {"r1k_neutral_normal_models_with_foldId" : self.r1k_neutral_normal_models_with_foldId}

    @classmethod
    def get_default_config(cls):
        return {"cut_dates" : ["2010-12-31", "2012-09-28", "2014-06-30", "2016-03-31", "2017-12-29"]}


class AddFoldIdToNormalizedDataPortfolioWeekly(AddFoldIdToNormalizedDataPortfolio):

    PROVIDES_FIELDS = ["r1k_neutral_normal_models_with_foldId", "r1k_sc_with_foldId_weekly", "r1k_lc_with_foldId_weekly"]
    REQUIRES_FIELDS = ["r1k_neutral_normal_models", "r1k_neutral_normal_sc_weekly", "r1k_neutral_normal_lc_weekly"]

    def __init__(self, cut_dates):
        self.r1k_sc_with_foldId_weekly = None
        self.r1k_lc_with_foldId_weekly = None
        AddFoldIdToNormalizedDataPortfolio.__init__(self, cut_dates)

    @staticmethod
    def _dictionary_format(**kwargs):
        return {k: v for k, v in kwargs.items()}

    def do_step_action(self, **kwargs):



        r1k_neutral_models_monthly = self._dictionary_format(growth=kwargs["r1k_neutral_normal_models_growth"],
                                value=kwargs["r1k_neutral_normal_models_value"],
                                largecap_growth=kwargs["r1k_neutral_normal_models_largecap_growth"],
                                largecap_value=kwargs["r1k_neutral_normal_models_largecap_value"]
                                )

        r1k_neutral_sc_weekly = self._dictionary_format(growth=kwargs["r1k_neutral_normal_sc_weekly_growth"],
                                                       value=kwargs["r1k_neutral_normal_sc_weekly_value"],
                                                       )
        r1k_neutral_lc_weekly = self._dictionary_format(largecap_growth=kwargs["r1k_neutral_normal_lc_weekly_largecap_growth"],
                                                       largecap_value=kwargs["r1k_neutral_normal_lc_weekly_largecap_value"],
                                                       )

        assert set(r1k_neutral_models_monthly)==set(FILTER_MODES), "AddFoldIdToNormalizedDataWeekly - r1k_neutral_models_monthly \
        doesn't seem to contain all expected modes. It contains- {0}".format(set(r1k_neutral_models_monthly))
        assert set(r1k_neutral_sc_weekly).union(set(r1k_neutral_lc_weekly))==set(FILTER_MODES), "AddFoldIdToNormalizedDataWeekly - \
        r1k_neutral_models_weekly doesn't seem to contain all expected modes. \
        It contains- {0}".format(set(r1k_neutral_sc_weekly).union(set(r1k_neutral_lc_weekly)))

        r1k_foldId_dict_monthly = {}
        r1k_foldId_sc_dict_weekly = {}
        r1k_foldId_lc_dict_weekly = {}
        for mode in r1k_neutral_models_monthly:
            print("Adding foldId to r1k data for {0} model".format(mode))
            r1k_foldId_dict_monthly[mode] = self._add_foldId(r1k_neutral_models_monthly[mode])
            if "largecap" in mode:
                r1k_foldId_lc_dict_weekly[mode] = self._add_foldId(r1k_neutral_lc_weekly[mode])
            else:
                r1k_foldId_sc_dict_weekly[mode] = self._add_foldId(r1k_neutral_sc_weekly[mode])



        self.r1k_neutral_normal_models_with_foldId = pd.DataFrame(list(r1k_foldId_dict_monthly.items()),
                         columns=['Key', 0]).set_index('Key')
        self.r1k_sc_with_foldId_weekly = pd.DataFrame(list(r1k_foldId_sc_dict_weekly.items()),
                         columns=['Key', 0]).set_index('Key')
        self.r1k_lc_with_foldId_weekly = pd.DataFrame(list(r1k_foldId_lc_dict_weekly.items()),
                         columns=['Key', 0]).set_index('Key')

        return self._get_additional_step_results()

    def _get_additional_step_results(self):

        return {"r1k_neutral_normal_models_with_foldId_growth" : self.r1k_neutral_normal_models_with_foldId.loc['growth'][0],
                'r1k_neutral_normal_models_with_foldId_value': self.r1k_neutral_normal_models_with_foldId.loc['value'][0],
                'r1k_neutral_normal_models_with_foldId_largecap_value':self.r1k_neutral_normal_models_with_foldId.loc['largecap_value'][0],
                'r1k_neutral_normal_models_with_foldId_largecap_growth':self.r1k_neutral_normal_models_with_foldId.loc['largecap_growth'][0],
                "r1k_sc_with_foldId_weekly_growth" : self.r1k_sc_with_foldId_weekly.loc['growth'][0],
                "r1k_sc_with_foldId_weekly_value": self.r1k_sc_with_foldId_weekly.loc['value'][0],
                "r1k_lc_with_foldId_weekly_largecap_growth" : self.r1k_lc_with_foldId_weekly.loc['largecap_growth'][0],
                "r1k_lc_with_foldId_weekly_largecap_value":self.r1k_lc_with_foldId_weekly.loc['largecap_value'][0]}





params = {'cut_dates' :["2010-12-31", "2012-09-28", "2014-06-30", "2016-03-31", "2017-12-29"]}

AddFoldIdToNormalizedDataPortfolioWeekly_params = {'params':params,
                                                   "start_date": RUN_DATE,
                                                'class': AddFoldIdToNormalizedDataPortfolioWeekly,
                                                   'provided_data': {'r1k_neutral_normal_models_with_foldId':
                                                                      construct_destination_path('add_final_fold_id'),
                                                                  'r1k_sc_with_foldId_weekly':
                                                                      construct_destination_path('add_final_fold_id'),
                                                                  'r1k_lc_with_foldId_weekly':
                                                                      construct_destination_path('add_final_fold_id'),
                                                                  },
                                                   'required_data': {'r1k_neutral_normal_models':
                                                                         construct_required_path('residualized_standardized',
                                                                                                 'r1k_neutral_normal_models'),
                                                                     'r1k_neutral_normal_sc_weekly':
                                                                         construct_required_path('residualized_standardized',
                                                                                                 'r1k_neutral_normal_sc_weekly'),
                                                                     'r1k_neutral_normal_lc_weekly':
                                                                         construct_required_path('residualized_standardized',
                                                                                                 'r1k_neutral_normal_lc_weekly'),
                                                                     }}








