import os
from abc import abstractmethod

import numpy as np
import os

from copy import deepcopy
from abc import abstractmethod

import numpy as np
import pandas as pd
import pyhocon
import json
import jmespath
from statsmodels.stats import proportion
from scipy.stats import bayes_mvs

import commonlib.jsonlogic as jl
from commonlib.util_classes import WeightRedistributor

from etl_workflow_steps import StatusType
from calibrations.core.calibration_taskflow_task import (CalibrationTaskflowTask, CalibrationFileTaskflowTask,
                                                         CalibrationTaskflowTaskWithFileTargets)
from calibrations.common.calibrations_utils import store_pandas_object
from calibrations.common.calibration_logging import calibration_logger

class RulesReader(CalibrationTaskflowTask):
    '''

    Loads rule json files from specified location

    '''
    PROVIDES_FIELDS = ["rule_definitions"]

    def __load_rules(self, rules_location):
        rules_location = os.path.realpath(os.path.join(os.path.dirname(__file__),*rules_location))
        all_rules = jl.load_all_rule_sets(rules_location)
        return all_rules

    def __init__(self, rule_set=None, ruleset_location=None):
        if not rule_set and not ruleset_location:
            raise ValueError("One of rule_set or ruleset_location need to be provided")
        if rule_set and ruleset_location:
            raise ValueError("Only one of rule_set and ruleset_location shall be provided at a time")
        self.rule_set = rule_set or self.__load_rules(ruleset_location)
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.rule_set}

    @classmethod
    def get_default_config(cls):
        return {"rule_set": None, "ruleset_location": [".", "features_rules", "*.conf"]}

class RuleApplications(CalibrationTaskflowTask):
    '''

    Filters securities based on rules.

    '''
    REQUIRES_FIELDS = ["final_data", "rule_definitions", "intervals_data"]
    PROVIDES_FIELDS = ["rule_applications", "calibration_weightings_dict"]

    def __init__(self, rule_organization_level=2, include_runtime_filters=False, block_rules=None):
        self.rule_organization_level = rule_organization_level
        self.include_runtime_filters = include_runtime_filters
        self.subrule_data = None
        self.calibration_weightings_dict = None
        self.block_rules = block_rules or []
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        full_data = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        rule_set = kwargs[self.__class__.REQUIRES_FIELDS[1]]
        intervals_data = kwargs[self.__class__.REQUIRES_FIELDS[2]]
        rules_to_run = list(set(map(lambda x:".".join(x.split(".")[:self.rule_organization_level]),
                                    pyhocon.HOCONConverter.to_properties(rule_set).split("\n"))))
        rules_to_run.sort()

        all_applications = []
        self.step_status["DataLineage"] = {}
        for rule in rules_to_run:
            query = jl.jsonLogic(rule_set["{0}.offline_filters".format(rule)],
                                 operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
            selection = full_data.query(query)
            if rule in self.block_rules and len(selection):
                selection = selection[0:0]
            if self.include_runtime_filters:
                runtime_query = jl.jsonLogic(rule_set["{0}.runtime_filters".format(rule)],
                                             operation_mode=jl.JsonLogicOperationMode.ExpressionBuilder)
                selection = selection.query(runtime_query)
            calibration_logger.info("** Got {2} records from rule {0} ({1})".format(rule, query, len(selection)))
            not_included_dates = pd.DatetimeIndex(intervals_data["entry_date"][~intervals_data["entry_date"]
                                                              .isin(selection["entry_date"])])
            self.step_status["DataLineage"][rule] = {"query": query, "records":len(selection)}
            if len(not_included_dates):
                calibration_logger.info("Adding {0} records for not-matched dates to rule {1}".format(len(not_included_dates), rule))
                self.step_status["DataLineage"][rule]["missing_dates"] = len(not_included_dates)
                integer_columns = full_data.dtypes[full_data.dtypes\
                                                   .apply(lambda x:pd.np.issubdtype(x, pd.np.integer))].index.tolist()
                for k in integer_columns:
                    selection[k] = selection[k].astype(float)
                not_included_dates.name = "entry_date"
                not_matched_dates = pd.DataFrame(data=pd.np.NaN, index=not_included_dates, columns=selection.columns)
                for k in not_matched_dates.columns:
                    not_matched_dates[k] = not_matched_dates[k].astype(selection[k].dtype)
                not_matched_dates = not_matched_dates.drop(["entry_date"], axis=1).reset_index()[selection.columns]
                not_matched_dates.index = -(not_matched_dates.index+1)
                selection = pd.concat([selection, not_matched_dates])
            selection["_rule"] = rule
            all_applications.append(selection)
        self.subrule_data = pd.concat(all_applications)
        self.subrule_data["_rule"] = self.subrule_data["_rule"].astype("category")
        self.calibration_weightings_dict = {}
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.subrule_data,
                self.__class__.PROVIDES_FIELDS[1]: self.calibration_weightings_dict}

    @classmethod
    def get_default_config(cls):
        return {"rule_organization_level": 2, "include_runtime_filters": False, "block_rules": None}

class BaseAssetSelector(CalibrationTaskflowTask):
    '''

    Base class

    '''
    REQUIRES_FIELDS = ["final_data", "rule_applications", "intervals_data"]
    SEPARATOR_VIEWNAME_REGIME_CODE = ":"

    def __init__(self, asset_group_name, regime_code, report_every_n_iterations=25):
        self.asset_group_name = asset_group_name
        self.regime_code = regime_code
        self.report_every_n_iterations = report_every_n_iterations
        name_sep = BaseAssetSelector.SEPARATOR_VIEWNAME_REGIME_CODE
        self.weights_name = "{0}{1}{2}".format(self.asset_group_name, name_sep, self.regime_code)
        CalibrationTaskflowTask.__init__(self)

    def _get_data_for_entry_date(self, rule_applications, entry_date):
        rules_in_dates = rule_applications[(rule_applications["entry_date"] == entry_date) &
                                           (rule_applications["_rule"].isin(self.RULE_DEPENDENCIES))]
        fully_matched_names = rules_in_dates.dropna(how="all", subset=rules_in_dates.columns.difference(["_rule",
                                                                                                         "entry_date"]))
        matched_rules = set(fully_matched_names["_rule"].unique())
        non_matched_rules = set(rule_applications["_rule"].unique()).difference(matched_rules)
        fully_matched_names.index.name = "index_full_data"
        fully_matched_names = fully_matched_names.reset_index().set_index(["_rule", "ticker"])
        return (fully_matched_names, matched_rules, non_matched_rules)

    @abstractmethod
    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        raise NotImplementedError("AssetSelectorAndWeighting.calculate_weights must be overriden in derived class")



class AssetSelectorAndWeighting(BaseAssetSelector):
    '''

    Assigns weights to the filtered securities

    '''
    REQUIRES_FIELDS = ["final_data", "rule_applications", "intervals_data", "calibration_weightings_dict"]

    def __init__(self, asset_group_name, regime_code, report_every_n_iterations=25):
        BaseAssetSelector.__init__(self, asset_group_name, regime_code, report_every_n_iterations)

    def do_step_action(self, **kwargs):
        intervals_data = kwargs["intervals_data"]
        rule_applications = kwargs["rule_applications"]
        final_data = kwargs["final_data"]
        calibration_weightings_dict = kwargs["calibration_weightings_dict"]
        if self.weights_name in calibration_weightings_dict:
            raise KeyError("The key {0} is already present on the calibration results dictionary. Please revise the "
                           "definition of the pipeline for existing steps populating it".format(self.weights_name))
        individual_results = []
        dates_to_process = intervals_data["entry_date"].sort_values().tolist()
        for n, dt in enumerate(dates_to_process):
            if not (n%self.report_every_n_iterations):
                calibration_logger.info("Processing {0} on date {1} ({2} out of {3})".format(self.__class__.__name__, dt, n+1,
                                                                           len(dates_to_process)))
            single_date_data = self._get_data_for_entry_date(rule_applications, dt)
            weights = {self.regime_code: self.calculate_weights(*single_date_data)}
            individual_results.append(weights)
        if (n%self.report_every_n_iterations):
            calibration_logger.info("Processing {0} on date {1} ({2} out of {3})".format(self.__class__.__name__, dt, n+1,
                                                                                   len(dates_to_process)))
        weights = pd.Series(data=individual_results, index=dates_to_process, name=self.weights_name)
        calibration_weightings_dict[self.weights_name] = weights
        return StatusType.Success

class RiskOnRegimeWeighting(AssetSelectorAndWeighting):
    '''

    Assigns weights to the filtered securities for the risk on regime

    '''
    RULE_DEPENDENCIES = ["prod_rules_risk_on_singlename.stock_rule_high_correl_low_indicator_with_negative_overnight",
                         "prod_rules_risk_on_singlename.overnight_negative_return_stock_high_correl",
                         "prod_rules_risk_on_singlename.overnight_small_return_stock_with_positive_sentiment_and_negative_indicator",
                         "prod_rules_risk_on_singlename.overnight_less_than_zero_great_q4_and_negative_momentum",
                         "prod_rules_risk_on_singlename.momentum_trend_follow_high_historicalgrowth_roe",
                         "prod_rules_risk_on_etf.etf_rules_low_indicator",
                         "prod_rules_risk_on_etf.etf_rules_high_indicator",
                         "prod_rules_risk_on_specialnames.specialname_rules"
                         ]

    TOTAL_WEIGHT = 1.0
    SINGLENAME_MAX_BASKET_SIZE = 75
    ETF_MAX_BASKET_SIZE = 30
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    SINGLENAME_SORTING_COLUMNS = ['indicator_rank']
    ETF_SORTING_COLUMNS = ['SPY_correl']
    SINGLENAME_ASCENDING_SORT = True
    ETF_ASCENDING_SORT = False
    SINGLENAME_RULE_LABEL = 'prod_rules_risk_on_singlename'
    ETF_RULE_LABEL = 'prod_rules_risk_on_etf'
    SPECIALNAME_RULE_LABEL = 'prod_rules_risk_on_specialnames'
    VOLUME_WEIGHTING = None

    def __init__(self, regime=1):
        AssetSelectorAndWeighting.__init__(self, "market_assets", regime)

    def redistribute_stock_weights_based_on_dollar_volume(self, weights, matched_name_info):
        stock_names_to_redist = list(matched_name_info[np.logical_not(matched_name_info["exclude"])]["ticker"])
        total_weight_to_redistribute = sum([np.abs(weights[k]) for k in stock_names_to_redist])
        avg_dollar_volumes = matched_name_info.set_index("ticker", drop=True)["avg_dollar_volume"]
        total_dollar_volume = avg_dollar_volumes[avg_dollar_volumes.index.isin(stock_names_to_redist)].sum()
        avg_dollar_volumes = avg_dollar_volumes.to_dict()
        volume_weighted_weights = \
            {k:max(min(np.sign(weights[k])*avg_dollar_volumes[k]/total_dollar_volume*total_weight_to_redistribute,
                               self.__class__.MAX_CONCENTRATION_LIMIT), -self.__class__.MAX_CONCENTRATION_LIMIT)
                   for k in stock_names_to_redist}
        result = deepcopy(weights)
        result.update(volume_weighted_weights)
        assert abs(sum(weights.values()) - sum(weights.values()))<1E-8
        return result

    def redistribute_weights(self, singlename_weights, etf_weights, special_weights):
        # TODO: We may do something more complex here (e.g. preserve ratios)
        regular_assets = list(singlename_weights.keys())
        regular_assets.extend(etf_weights.keys())
        special_assets = special_weights.keys()
        regular_weights = {k:min((1.0 - self.__class__.SPECIAL_WEIGHT)/len(regular_assets),
                                 self.__class__.MAX_CONCENTRATION_LIMIT) for k in regular_assets} \
            if regular_assets else {}
        special_weights = {k:self.__class__.SPECIAL_WEIGHT/len(special_assets) for k in special_assets} \
            if special_assets else {}
        regular_weights.update(special_weights)
        scaled_weights = {k:self.__class__.TOTAL_WEIGHT*regular_weights[k] for k in regular_weights.keys()}
        return scaled_weights

    def calculate_weights_for_subrule(self, rule_matches, sort_columns, ascending, max_basket_size):
        # TODO: we may want to have an option to increase weighting if confirmed by several rules
        filtered_tickers = list(rule_matches.reset_index().sort_values(sort_columns, ascending=ascending)['ticker']\
                                .drop_duplicates()[:max_basket_size])
        assets_dict = {k:1.0/len(filtered_tickers) for k in filtered_tickers}
        return assets_dict

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        final_tickers = list(set(matched_names.index.get_level_values(1)))
        matched_groups = dict([k for k in matched_names.groupby(lambda x:x[0].split(".")[0])])
        singlename_rule_matches = matched_groups[self.__class__.SINGLENAME_RULE_LABEL] \
            if self.__class__.SINGLENAME_RULE_LABEL in matched_groups.keys() else None
        etf_rule_matches = matched_groups[self.__class__.ETF_RULE_LABEL] \
            if self.__class__.ETF_RULE_LABEL in matched_groups.keys() else None
        specialname_rule_matches = matched_groups[self.__class__.SPECIALNAME_RULE_LABEL] \
            if self.__class__.SPECIALNAME_RULE_LABEL in matched_groups.keys() else None
        singlename_weights = \
            self.calculate_weights_for_subrule(singlename_rule_matches, self.__class__.SINGLENAME_SORTING_COLUMNS,
                                               self.__class__.SINGLENAME_ASCENDING_SORT,
                                               self.__class__.SINGLENAME_MAX_BASKET_SIZE) \
            if (singlename_rule_matches is not None) else {}
        etf_weights = \
            self.calculate_weights_for_subrule(etf_rule_matches, self.__class__.ETF_SORTING_COLUMNS,
                                               self.__class__.ETF_ASCENDING_SORT,
                                               self.__class__.ETF_MAX_BASKET_SIZE) \
            if (etf_rule_matches is not None) else {}
        special_names = list(specialname_rule_matches.reset_index()['ticker']) \
            if (specialname_rule_matches is not None) else []
        special_weights = {k:self.__class__.TOTAL_WEIGHT/len(special_names) for k in special_names} \
            if special_names else {}
        assets_dict = self.redistribute_weights(singlename_weights, etf_weights, special_weights)
        if self.__class__.VOLUME_WEIGHTING:
            matched_name_info = matched_names.reset_index()
            matched_name_info = matched_name_info[(matched_name_info['ticker'].isin(assets_dict.keys()))]\
                [['ticker', 'is_etf', 'avg_dollar_volume']]
            matched_name_info['exclude'] = matched_name_info['is_etf']
            if special_names:
                matched_name_info[matched_name_info['ticker'].isin(special_names)]['exclude'] = 1
            assets_dict = self.redistribute_stock_weights_based_on_dollar_volume(assets_dict, matched_name_info)
        return assets_dict

    @classmethod
    def get_default_config(cls):
        return {"regime": 1}

class SpyViewWeakRiskOnRegimeWeighting(RiskOnRegimeWeighting):
    '''

    Assigns weights to the filtered securities for the weak risk on regime

    '''
    RULE_DEPENDENCIES = ["prod_rules_risk_on_singlename.stock_rule_high_correl_low_indicator_with_negative_overnight",
                         "prod_rules_risk_on_singlename.overnight_negative_return_stock_high_correl",
                         "prod_rules_risk_on_singlename.overnight_small_return_stock_with_positive_sentiment_and_negative_indicator",
                         "prod_rules_risk_on_singlename.overnight_less_than_zero_great_q4_and_negative_momentum",
                         "prod_rules_risk_on_singlename.momentum_trend_follow_high_historicalgrowth_roe",
                         "prod_rules_risk_on_etf.etf_rules_low_indicator",
                         "prod_rules_risk_on_etf.etf_rules_high_indicator",
                         "prod_rules_risk_on_specialnames.specialname_rules"
                         ]

    TOTAL_WEIGHT = None

    def __init__(self, total_weight, volume_weighting):
        RiskOnRegimeWeighting.__init__(self, 1)
        self.__class__.TOTAL_WEIGHT = total_weight
        self.__class__.VOLUME_WEIGHTING = volume_weighting

    # No rules for now
    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        assets_dict = {}
        return assets_dict

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 0.0, "volume_weighting": None}

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 0.5, "volume_weighting":1.0}


class SpyViewStrongRiskOnRegimeWeighting(RiskOnRegimeWeighting):
    '''

    Assigns weights to the filtered securities for the strong risk on regime

    '''
    RULE_DEPENDENCIES = ["prod_rules_risk_on_singlename.stock_rule_high_correl_low_indicator_with_negative_overnight",
                         "prod_rules_risk_on_singlename.overnight_negative_return_stock_high_correl",
                         "prod_rules_risk_on_singlename.overnight_small_return_stock_with_positive_sentiment_and_negative_indicator",
                         "prod_rules_risk_on_singlename.overnight_less_than_zero_great_q4_and_negative_momentum",
                         "prod_rules_risk_on_singlename.momentum_trend_follow_high_historicalgrowth_roe",
                         "prod_rules_risk_on_etf.etf_rules_low_indicator",
                         "prod_rules_risk_on_etf.etf_rules_high_indicator",
                         "prod_rules_risk_on_specialnames.specialname_rules"
                         ]

    TOTAL_WEIGHT = None

    def __init__(self, total_weight, volume_weighting):
        RiskOnRegimeWeighting.__init__(self, 2)
        self.__class__.TOTAL_WEIGHT = total_weight
        self.__class__.VOLUME_WEIGHTING = volume_weighting

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 1.0, "volume_weighting": None}

class RiskOffRegimeWeighting(AssetSelectorAndWeighting):
    '''

    Assigns weights to the filtered securities for the risk off regime

    '''
    RULE_DEPENDENCIES = ["prod_rules_strong_risk_off_etf_short.etf_rules_low_indicator",
                         "prod_rules_strong_risk_off_etf_short.etf_rules_high_indicator",
                         #"prod_rules_strong_risk_off_etf_short.vxx_rules",
                         "prod_rules_strong_risk_off_singlename_short.momentum_trendfollow",
                         "prod_rules_strong_risk_off_singlename_short.momentum_reversal",
                         "prod_rules_strong_risk_off_singlename_short.sector_rules",
                         "prod_rules_strong_risk_off_sector_short.eq_intl_short",
                         "prod_rules_strong_risk_off_sector_short.eq_us_short",
                         "prod_rules_strong_risk_off_sector_short.sector_momentum",
                         "prod_rules_strong_risk_off_sector_long.precious_metals",
                         "prod_rules_strong_risk_off_specialnames_long.tlt_rules"
                         ]

    TOTAL_WEIGHT = 1.0
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    ADJUST_FOR_SHORT_LEVERAGE = False
    VOLUME_WEIGHTING = None
    SHORT_LEVERAGE = 2.0
    ETF_SHORT_RULE_LABEL = 'prod_rules_strong_risk_off_etf_short'
    STOCK_SHORT_LABEL = 'prod_rules_strong_risk_off_singlename_short'
    SECTOR_SHORT_RULE_LABEL = 'prod_rules_strong_risk_off_sector_short'
    SECTOR_LONG_RULE_LABEL = 'prod_rules_strong_risk_off_sector_long'
    SPECIALNAME_LONG_RULE_LABEL = 'prod_rules_strong_risk_off_specialnames_long'
    ETF_SHORT_SORTING_COLUMNS = ['SPY_correl']
    STOCK_SHORT_SORTING_COLUMNS = ['indicator_rank']
    SECTOR_SHORT_SORTING_COLUMNS = ['indicator_rank']
    SECTOR_LONG_SORTING_COLUMNS = ['indicator_rank']
    SPECIALNAME_LONG_SORTING_COLUMNS = ['indicator_rank']
    ETF_SHORT_ASCENDING_SORT = False
    STOCK_SHORT_ASCENDING_SORT = True
    SECTOR_SHORT_ASCENDING_SORT = True
    SECTOR_LONG_ASCENDING_SORT = False
    STOCK_SHORT_MAX_BASKET_SIZE = 40
    ETF_SHORT_MAX_BASKET_SIZE = 20
    SECTOR_SHORT_MAX_BASKET_SIZE = 30
    SECTOR_LONG_MAX_BASKET_SIZE = 10


    def __init__(self, regime=-1):
        AssetSelectorAndWeighting.__init__(self, "market_assets", regime)

    def redistribute_stock_weights_based_on_dollar_volume(self, weights, matched_name_info):
        stock_names_to_redist = list(matched_name_info[np.logical_not(matched_name_info["exclude"])]["ticker"])
        total_weight_to_redistribute = sum([np.abs(weights[k]) for k in stock_names_to_redist])
        avg_dollar_volumes = matched_name_info.set_index("ticker", drop=True)["avg_dollar_volume"]
        total_dollar_volume = avg_dollar_volumes[avg_dollar_volumes.index.isin(stock_names_to_redist)].sum()
        avg_dollar_volumes = avg_dollar_volumes.to_dict()
        volume_weighted_weights = \
            {k:max(min(np.sign(weights[k])*avg_dollar_volumes[k]/total_dollar_volume*total_weight_to_redistribute,
                               self.__class__.MAX_CONCENTRATION_LIMIT), -self.__class__.MAX_CONCENTRATION_LIMIT)
                   for k in stock_names_to_redist}
        result = deepcopy(weights)
        result.update(volume_weighted_weights)
        assert abs(sum(weights.values()) - sum(weights.values()))<1E-8
        return result

    def calculate_weights_for_subrule(self, rule_matches, sort_columns, ascending, max_basket_size, is_long=1):
        # TODO: we may want to have an option to increase weighting if confirmed by several rules
        filtered_tickers = list(rule_matches.reset_index().sort_values(sort_columns, ascending=ascending)['ticker']\
                                .drop_duplicates()[:max_basket_size])
        if is_long:
            assets_dict = {k:1.0/len(filtered_tickers) for k in filtered_tickers}
        else:
            assets_dict = {k:-1.0/len(filtered_tickers) for k in filtered_tickers}
        return assets_dict

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        final_tickers = list(set(matched_names.index.get_level_values(1)))
        matched_groups = dict([k for k in matched_names.groupby(lambda x:x[0].split(".")[0])])
        stock_short_rule_matches = matched_groups[self.__class__.STOCK_SHORT_LABEL] \
            if self.__class__.STOCK_SHORT_LABEL in matched_groups.keys() else None
        etf_short_rule_matches = matched_groups[self.__class__.ETF_SHORT_RULE_LABEL] \
            if self.__class__.ETF_SHORT_RULE_LABEL in matched_groups.keys() else None
        sector_short_rule_matches = matched_groups[self.__class__.SECTOR_SHORT_RULE_LABEL] \
            if self.__class__.SECTOR_SHORT_RULE_LABEL in matched_groups.keys() else None
        sector_long_rule_matches = matched_groups[self.__class__.SECTOR_LONG_RULE_LABEL] \
            if self.__class__.SECTOR_LONG_RULE_LABEL in matched_groups.keys() else None
        specialname_long_rule_matches = matched_groups[self.__class__.SPECIALNAME_LONG_RULE_LABEL] \
            if self.__class__.SPECIALNAME_LONG_RULE_LABEL in matched_groups.keys() else None
        stock_short_weights = \
            self.calculate_weights_for_subrule(stock_short_rule_matches, self.__class__.STOCK_SHORT_SORTING_COLUMNS,
                                               self.__class__.STOCK_SHORT_ASCENDING_SORT,
                                               self.__class__.STOCK_SHORT_MAX_BASKET_SIZE, 0) \
            if (stock_short_rule_matches is not None) else {}
        etf_short_weights = \
            self.calculate_weights_for_subrule(etf_short_rule_matches, self.__class__.ETF_SHORT_SORTING_COLUMNS,
                                               self.__class__.ETF_SHORT_ASCENDING_SORT,
                                               self.__class__.ETF_SHORT_MAX_BASKET_SIZE, 0) \
            if (etf_short_rule_matches is not None) else {}
        sector_short_weights = \
            self.calculate_weights_for_subrule(sector_short_rule_matches, self.__class__.SECTOR_SHORT_SORTING_COLUMNS,
                                               self.__class__.SECTOR_SHORT_ASCENDING_SORT,
                                               self.__class__.SECTOR_SHORT_MAX_BASKET_SIZE, 0) \
            if (sector_short_rule_matches is not None) else {}
        sector_long_weights = \
            self.calculate_weights_for_subrule(sector_long_rule_matches, self.__class__.SECTOR_LONG_SORTING_COLUMNS,
                                               self.__class__.SECTOR_LONG_ASCENDING_SORT,
                                               self.__class__.SECTOR_LONG_MAX_BASKET_SIZE) \
            if (sector_long_rule_matches is not None) else {}
        special_names = list(specialname_long_rule_matches.reset_index()['ticker']) \
            if (specialname_long_rule_matches is not None) else []
        special_weights = {k:self.__class__.TOTAL_WEIGHT*self.__class__.SPECIAL_WEIGHT/len(special_names) \
                           for k in special_names} if special_names else {}

        # Special case for VXX
        #if "VXX" in list(etf_short_rule_matches.reset_index()["ticker"]):
            #num_etf = len(etf_short_weights)
            #adjustment_factor = 1.*num_etf/(num_etf+1.)
            #etf_short_weights = {k: etf_short_weights[k]*adjustment_factor for k in etf_short_weights}
            #etf_short_weights.update({"VXX": -etf_short_weights.values()[0]})

        all_regular_weights = [stock_short_weights, etf_short_weights, sector_short_weights, sector_long_weights]
        all_regular_weights = [w for w in all_regular_weights if w]

        if (not all_regular_weights and not special_weights):
            assets_dict = {}
        elif all_regular_weights:
            weight_distributor = WeightRedistributor(absolute_target_weight=self.__class__.TOTAL_WEIGHT,
                                                     special_names_weights=special_weights,
                                                     adjust_negative_weights_for_leverage=self.ADJUST_FOR_SHORT_LEVERAGE,
                                                     max_abs_weight_per_name=self.__class__.MAX_CONCENTRATION_LIMIT,
                                                     calculate_pro_rata=False,
                                                     short_leverage=self.__class__.SHORT_LEVERAGE)
            assets_dict = weight_distributor(all_regular_weights)
            if self.__class__.VOLUME_WEIGHTING:
                matched_name_info = matched_names.reset_index()
                matched_name_info = matched_name_info[(matched_name_info['ticker'].isin(assets_dict.keys()))]\
                    [['ticker', 'is_etf', 'avg_dollar_volume']]
                matched_name_info['exclude'] = matched_name_info['is_etf']
                if special_names:
                    matched_name_info[matched_name_info['ticker'].isin(special_names)]['exclude'] = 1
                assets_dict = self.redistribute_stock_weights_based_on_dollar_volume(assets_dict, matched_name_info)
        else:
            assets_dict = special_weights
        return assets_dict

    @classmethod
    def get_default_config(cls):
        return {"regime": -1}

class SpyViewStrongRiskOffRegimeWeighting(RiskOffRegimeWeighting):
    '''

    Assigns weights to the filtered securities for the strong risk off regime

    '''
    RULE_DEPENDENCIES = ["prod_rules_strong_risk_off_etf_short.etf_rules_low_indicator",
                         "prod_rules_strong_risk_off_etf_short.etf_rules_high_indicator",
                         #"prod_rules_strong_risk_off_etf_short.vxx_rules",
                         "prod_rules_strong_risk_off_singlename_short.momentum_trendfollow",
                         "prod_rules_strong_risk_off_singlename_short.momentum_reversal",
                         "prod_rules_strong_risk_off_singlename_short.sector_rules",
                         "prod_rules_strong_risk_off_sector_short.eq_intl_short",
                         "prod_rules_strong_risk_off_sector_short.eq_us_short",
                         "prod_rules_strong_risk_off_sector_short.sector_momentum",
                         "prod_rules_strong_risk_off_sector_long.precious_metals",
                         "prod_rules_strong_risk_off_specialnames_long.tlt_rules"
                         ]

    TOTAL_WEIGHT = None
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    ETF_SHORT_RULE_LABEL = 'prod_rules_strong_risk_off_etf_short'
    STOCK_SHORT_LABEL = 'prod_rules_strong_risk_off_singlename_short'
    SECTOR_SHORT_RULE_LABEL = 'prod_rules_strong_risk_off_sector_short'
    SECTOR_LONG_RULE_LABEL = 'prod_rules_strong_risk_off_sector_long'
    SPECIALNAME_LONG_RULE_LABEL = 'prod_rules_strong_risk_off_specialnames_long'
    ETF_SHORT_SORTING_COLUMNS = ['SPY_correl']
    STOCK_SHORT_SORTING_COLUMNS = ['indicator_date']
    SECTOR_SHORT_SORTING_COLUMNS = ['indicator_date']
    SECTOR_LONG_SORTING_COLUMNS = ['indicator_date']
    SPECIALNAME_LONG_SORTING_COLUMNS = ['indicator_date']
    ETF_SHORT_ASCENDING_SORT = False
    STOCK_SHORT_ASCENDING_SORT = True
    SECTOR_SHORT_ASCENDING_SORT = True
    SECTOR_LONG_ASCENDING_SORT = False
    STOCK_SHORT_MAX_BASKET_SIZE = 40
    ETF_SHORT_MAX_BASKET_SIZE = 20
    SECTOR_SHORT_MAX_BASKET_SIZE = 30
    SECTOR_LONG_MAX_BASKET_SIZE = 10

    def __init__(self, total_weight, volume_weighting):
        RiskOffRegimeWeighting.__init__(self, -2)
        self.__class__.TOTAL_WEIGHT = total_weight
        self.__class__.VOLUME_WEIGHTING = volume_weighting

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 0.65, "volume_weighting": None}

class SpyViewWeakRiskOffRegimeWeighting(RiskOffRegimeWeighting):
    '''

    Assigns weights to the filtered securities for the weak risk off regime

    '''
    RULE_DEPENDENCIES = ["prod_rules_weak_risk_off_specialnames_long.xiv_rules",
                         "prod_rules_weak_risk_off_specialnames_long.tlt_rules"
                         ]

    TOTAL_WEIGHT = None
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 1.0
    ETF_SHORT_RULE_LABEL = 'prod_rules_weak_risk_off_etf_short'
    STOCK_SHORT_LABEL = 'prod_rules_weak_risk_off_singlename_short'
    SECTOR_SHORT_RULE_LABEL = 'prod_rules_weak_risk_off_sector_short'
    SECTOR_LONG_RULE_LABEL = 'prod_rules_weak_risk_off_sector_long'
    SPECIALNAME_LONG_RULE_LABEL = 'prod_rules_weak_risk_off_specialnames_long'
    ETF_SHORT_SORTING_COLUMNS = ['SPY_correl']
    STOCK_SHORT_SORTING_COLUMNS = ['indicator_date']
    SECTOR_SHORT_SORTING_COLUMNS = ['indicator_date']
    SECTOR_LONG_SORTING_COLUMNS = ['indicator_date']
    SPECIALNAME_LONG_SORTING_COLUMNS = ['indicator_date']
    ETF_SHORT_ASCENDING_SORT = False
    STOCK_SHORT_ASCENDING_SORT = True
    SECTOR_SHORT_ASCENDING_SORT = True
    SECTOR_LONG_ASCENDING_SORT = True
    STOCK_SHORT_MAX_BASKET_SIZE = 40
    ETF_SHORT_MAX_BASKET_SIZE = 20
    SECTOR_SHORT_MAX_BASKET_SIZE = 30
    SECTOR_LONG_MAX_BASKET_SIZE = 10

    TLT_RATIO = 0.5


    def __init__(self, total_weight, volume_weighting):
        RiskOffRegimeWeighting.__init__(self, -1)
        self.__class__.TOTAL_WEIGHT = total_weight
        self.__class__.VOLUME_WEIGHTING = volume_weighting

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        #TODO: This is a specific rule for TLT and XIV, should be generalized using the parent class once new
        #rules are added
        final_tickers = list(set(matched_names.index.get_level_values(1)))
        matched_groups = dict([k for k in matched_names.groupby(lambda x:x[0].split(".")[0])])
        specialname_long_rule_matches = matched_groups[self.__class__.SPECIALNAME_LONG_RULE_LABEL] \
            if self.__class__.SPECIALNAME_LONG_RULE_LABEL in matched_groups.keys() else None
        special_names = list(specialname_long_rule_matches.reset_index()['ticker']) \
            if (specialname_long_rule_matches is not None) else []
        if len(special_names)<=1:
            assets_dict = {"TLT": self.__class__.TOTAL_WEIGHT*self.TLT_RATIO}
        else:
            assets_dict = {"TLT": self.__class__.TOTAL_WEIGHT*self.TLT_RATIO,
                           "VXXB": -self.__class__.TOTAL_WEIGHT*(1.0-self.TLT_RATIO)}
        return assets_dict

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 0.1, "volume_weighting": None}

class NeutralMarketRegimeWeighting(AssetSelectorAndWeighting):
    '''

    Assigns weights to the filtered securities for the neutral regime

    '''
    RULE_DEPENDENCIES = []

    TOTAL_WEIGHT = 1.0
    SINGLENAME_MAX_BASKET_SIZE = 40
    ETF_MAX_BASKET_SIZE = 15
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    ACCOUNT_FOR_SHORT_LEVERAGE = True
    SHORT_LEVERAGE = 2.0

    def __init__(self):
        AssetSelectorAndWeighting.__init__(self, "market_assets", 0)

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        assets_dict = {}
        return assets_dict

class RisingVolRegimeWeighting(AssetSelectorAndWeighting):
    '''

    Assigns weights to the filtered securities for the vol regime +1

    '''
    RULE_DEPENDENCIES = []
    TOTAL_WEIGHT = None
    SINGLENAME_MAX_BASKET_SIZE = 40
    ETF_MAX_BASKET_SIZE = 15
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    ACCOUNT_FOR_SHORT_LEVERAGE = True
    SHORT_LEVERAGE = 2.0

    def __init__(self, total_weight):
        AssetSelectorAndWeighting.__init__(self, "vol_assets", 1)
        self.__class__.TOTAL_WEIGHT = total_weight

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        assets_dict = {"VXXB": self.__class__.TOTAL_WEIGHT}
        return assets_dict

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 1.0}

class FallingVolRegimeWeighting(AssetSelectorAndWeighting):
    '''

    Assigns weights to the filtered securities for the vol regime -1

    '''
    RULE_DEPENDENCIES = []
    TOTAL_WEIGHT = None
    SINGLENAME_MAX_BASKET_SIZE = 40
    ETF_MAX_BASKET_SIZE = 15
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    ACCOUNT_FOR_SHORT_LEVERAGE = True
    SHORT_LEVERAGE = 2.0

    def __init__(self, total_weight):
        AssetSelectorAndWeighting.__init__(self, "vol_assets", -1)
        self.__class__.TOTAL_WEIGHT = total_weight

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        #assets_dict = {"VXX": -self.__class__.TOTAL_WEIGHT}
        assets_dict = {"SVXY": self.__class__.TOTAL_WEIGHT}
        return assets_dict

    @classmethod
    def get_default_config(cls):
        return {"total_weight": 1.0}

class NeutralVolRegimeWeighting(AssetSelectorAndWeighting):
    '''

    Assigns weights to the filtered securities for the vol regime 0

    '''
    RULE_DEPENDENCIES = []

    TOTAL_WEIGHT = 1.0
    SINGLENAME_MAX_BASKET_SIZE = 40
    ETF_MAX_BASKET_SIZE = 15
    MAX_CONCENTRATION_LIMIT = 0.1
    SPECIAL_WEIGHT = 0.15
    ACCOUNT_FOR_SHORT_LEVERAGE = True
    SHORT_LEVERAGE = 2.0

    def __init__(self):
        AssetSelectorAndWeighting.__init__(self, "vol_assets", 0)

    def calculate_weights(self, matched_names, matched_rules_set, non_matched_rules_set):
        assets_dict = {}
        return assets_dict


class IndividualWeightsConsolidation(CalibrationTaskflowTask):
    '''

    Creates consolidated dataframe of asset weights

    '''
    PROVIDES_FIELDS = ["consolidated_weights"]
    REQUIRES_FIELDS = ["calibration_weightings_dict", "rule_applications"]

    def __init__(self):
        self.data = None
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        individual_weights = kwargs["calibration_weightings_dict"]
        rule_applications = kwargs["rule_applications"]
        rule_applications["_rule"] = rule_applications["_rule"].astype(str)
        self.data = pd.DataFrame(individual_weights)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

class ViewClassesFormation(CalibrationTaskflowTask):
    '''

    Creates the individual views with combined assets and weightings

    '''
    PROVIDES_FIELDS = ["view_classes"]
    REQUIRES_FIELDS = ["consolidated_weights"]
    def __init__(self, traders=[]):
        self.data = None
        self.traders = traders if traders else self.__class__.__name__
        CalibrationTaskflowTask.__init__(self)

    def merge_assets_in_views(self, view_template, asset_values):
        res = deepcopy(view_template)
        res["view_assets"] = asset_values
        return res

    def do_step_action(self, **kwargs):
        all_weights = kwargs["consolidated_weights"]
        unique_asset_groups = all_weights.columns.str.split(BaseAssetSelector.SEPARATOR_VIEWNAME_REGIME_CODE)\
            .str[0].unique()
        sum_dicts = lambda x:reduce(lambda a,b:a.update(b) or a, x, {})
        weight_groups = pd.DataFrame({k:all_weights.loc[:,all_weights.columns.str.startswith(k)].apply(sum_dicts,axis=1)
                                      for k in unique_asset_groups})
        class_config_as_dict = {}
        for trader in self.traders:
            class_config_as_dict.update(self.traders[trader])
        required_asset_groups = list(set(jmespath.search("*.view_assets", class_config_as_dict)))
        missing_asset_groups = filter(lambda x:x not in weight_groups.columns, required_asset_groups)
        if missing_asset_groups:
            raise RuntimeError("Some asset groups are missing are required and not provided. These are: {0}"
                               .format(missing_asset_groups))
        all_views = {}
        for k in class_config_as_dict:
            view_template = class_config_as_dict[k]
            jump_flt_key = "jump_parameters_for_filtering_per_view_value"
            #TODO: ugly hack; remove later. It comes from transforming the hocon dict to json
            if jump_flt_key in view_template:
                view_template[jump_flt_key] = {int(k):view_template[jump_flt_key][k] for k in view_template[jump_flt_key]}
            assets_to_merge = weight_groups[class_config_as_dict[k]["view_assets"]]
            new_view = assets_to_merge.apply(lambda x:self.merge_assets_in_views(view_template, x))
            all_views[k] = new_view
        self.data = pd.DataFrame(all_views)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"traders": ["ViewClassesFormation"]}

class ViewsInMarketTraderCalibration(CalibrationTaskflowTask):
    '''

    Creates final trader calibration combining views and auxiliary properties (e.g. rebalancing_time, trader_capital)

    '''
    PROVIDES_FIELDS = ["trader_calibration"]
    REQUIRES_FIELDS = ["view_classes"]
    TRADER_FIELDS = ["date", "actorID", "role", "ranking", "views", "rebalancing_time", "rebalancing_frequency",
                     "stop_limits_pct", "basket_selloff_date", "funding_buffer", "fundingRequestsAmount", "trader_capital",
                     "total_funding", "override_filter", "instructions_overrides"]

    def __init__(self, traders):
        self.data = None
        self.traders = traders
        CalibrationTaskflowTask.__init__(self)

    def _populate_all_fields(self, row, trader, trader_fields):
        full_dict = {key:None for key in ViewsInMarketTraderCalibration.TRADER_FIELDS}
        full_dict.update(trader_fields)
        full_dict['actorID'] = trader
        full_dict['views'] = row.to_dict()
        full_dict['role'] = 2
        return pd.Series(full_dict)

    def do_step_action(self, **kwargs):
        view_class_df = kwargs["view_classes"]
        trader_config = self.traders
        all_traders = list(trader_config.keys())
        all_traders.sort()

        trader_calibration = \
            [view_class_df[[trader_config[trader]["views"]] if isinstance(trader_config[trader]["views"], str)\
                           else trader_config[trader]["views"]]\
             .apply(lambda x:self._populate_all_fields(x, trader, dict(trader_config[trader]["trader_fields"])),
                                                       axis=1) for trader in all_traders]
        trader_calibration = pd.concat(trader_calibration)
        self.data = trader_calibration
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"traders": {"MarketViewNewTrader_20171226": { \
            "trader_fields": {"ranking": 1, "rebalancing_time": "09:31:00", "rebalancing_frequency": "1B",
                              "stop_limits_pct": "(-0.4, 100.0)", "basket_selloff_date": "2018-12-15",
                              "trader_capital": 9000000},
            "views": ["spy_view"]}}}

class ViewsInMarketSupervisorCalibration(CalibrationTaskflowTask):
    '''

    Creates supervisor calibration capital control related quantities

    '''
    PROVIDES_FIELDS = ["supervisor_calibration"]
    REQUIRES_FIELDS = ["view_classes"]

    def __init__(self, supervisor_config):
        self.data = None
        self.supervisor_config = supervisor_config
        CalibrationTaskflowTask.__init__(self)

    def _populate_all_fields(self, supervisor, supervisor_fields, index):
        columns = ['actorID', 'role']
        columns.extend(supervisor_fields.keys())
        supervisor_df = pd.DataFrame(index=index, columns=columns)
        supervisor_df['actorID'] = supervisor
        supervisor_df['role'] = 1
        supervisor_df[supervisor_fields.keys()] = supervisor_fields.values()
        return supervisor_df

    def do_step_action(self, **kwargs):
        view_class_df = kwargs["view_classes"]
        supervisor_config = self.supervisor_config
        all_supervisors = list(supervisor_config.keys())
        all_supervisors.sort()
        supervisor_calibration = map(lambda x:self._populate_all_fields(x, supervisor_config[x]["supervisor_fields"],
                                                                        view_class_df.index),
                                     all_supervisors)
        supervisor_calibration = pd.concat(supervisor_calibration)
        self.data = supervisor_calibration
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}


    @classmethod
    def get_default_config(cls):
        return {"supervisor_config": {"SUP": {
            "supervisor_fields": {"funding_buffer": 100000, "fundingRequestsAmount": 7000000,
                                  "total_funding": 20000000}}}}

class ViewsInMarketFullCalibration(CalibrationFileTaskflowTask):
    '''

    Combines the trader and supervisor calibrations to create final csv file and deposits to s3 or local location

    '''
    PROVIDES_FIELDS = ["views_in_market_calibration"]
    REQUIRES_FIELDS = ["trader_calibration", "supervisor_calibration"]

    def __init__(self, calibration_location, file_suffix, ignore_calibration_timeline=False,
                 calibration_date_offset=None, calibration_anchor_date=None):
        self.data = None
        self.calibration_date_offset = calibration_date_offset
        self.calibration_anchor_date = calibration_anchor_date
        CalibrationFileTaskflowTask.__init__(self, calibration_location, file_suffix, ignore_calibration_timeline)

    def do_step_action(self, **kwargs):
        trader_calibration = kwargs["trader_calibration"]
        supervisor_calibration = kwargs["supervisor_calibration"]
        final_calibration = pd.concat([trader_calibration, supervisor_calibration])
        final_calibration['date'] = final_calibration.index
        final_calibration = final_calibration.sort_values(['date', 'actorID']).reset_index(drop=True)
        self.data = final_calibration[ViewsInMarketTraderCalibration.TRADER_FIELDS]
        target_path = self.get_file_targets()[0]
        store_pandas_object(self.data, target_path, "Market Views Calibration")
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    def get_calibration_dates(self, task_params):
        return self.get_calibration_dates_with_offset_anchor(task_params, self.calibration_date_offset,
                                                             self.calibration_anchor_date)

    @classmethod
    def get_default_config(cls):
        return {"calibration_location": "C:/DCM/temp/", "file_suffix": "vol_views.csv",
                "ignore_calibration_timeline": True, "calibration_date_offset": None, "calibration_anchor_date": None}


class CalibrationPerformanceReport(CalibrationTaskflowTaskWithFileTargets):
    '''

    Generates performance report for historical analysis

    '''
    PROVIDES_FIELDS = ["summary_by_date", "time_summary_stats", "full_period_stats"]
    REQUIRES_FIELDS = ["views_in_market_calibration", "final_data", "regime_data"]
    def __init__(self, views_regimes_to_report, report_location, summary_by_date_suffix, time_summary_stats_suffix,
                 full_period_stats_suffix):
        self.data = None
        self.time_summary_stats = None
        self.full_period_stats = None
        self.views_regimes_to_report = views_regimes_to_report
        self.report_location = report_location
        self.summary_by_date_suffix = summary_by_date_suffix
        self.time_summary_stats_suffix = time_summary_stats_suffix
        self.full_period_stats_suffix = full_period_stats_suffix
        CalibrationTaskflowTask.__init__(self)

    def _decompose_instruction(self, dt, ins):
        results = []
        for viewn_name in ins:
            for regime in ins[viewn_name]["view_assets"]:
                assets = ins[viewn_name]["view_assets"][regime].keys()
                for asset in assets:
                    results.append((dt, viewn_name, regime, asset, ins[viewn_name]["view_assets"][regime][asset]))
        return results

    def _generate_selections(self, instructions, actor_id):
        instructions = instructions[instructions["actorID"]==actor_id]
        instructions.set_index("date", inplace=True)
        try:
            views_as_json = instructions["views"].apply(lambda x:eval(x))
        except:
            views_as_json = instructions["views"]
        views_as_dict = [k for k in views_as_json.to_dict().iteritems()]
        selections = map(lambda x:self._decompose_instruction(*x), views_as_dict)
        full_selections = reduce(lambda x,y:x.extend(y) or x, selections, [])
        res = pd.DataFrame(full_selections, columns=["date", "view", "regime", "ticker", "weight"])
        res = res.sort_values(['date', 'regime', 'ticker']).reset_index(drop=True)
        return res

    def _get_stat(self, series, thresh=0.0):
        mean_val = series.mean()
        std_val = series.std(ddof=1)
        median_val = series.median()
        max_val = series.max()
        min_val = series.min()
        num_val = len(series)
        hit_val = 100*pd.np.float(len(series[series>thresh]))/len(series)
        deciles = series.quantile([.1, .2, .3, .4, .5, .6, .7, .8, .9])
        hit_val_ci = proportion.proportion_confint((series>thresh).sum(), len(series), method="jeffrey")
        mean_val_ci = bayes_mvs(series)[0].minmax
        stat_dict = {"mean" : mean_val, "median" :median_val, "max" : max_val, "min" :min_val, "count" :num_val,
                     "hit_ratio": hit_val, "hit_ratio_interval": hit_val_ci, "mean_interval": mean_val_ci,
                     "deciles": list(deciles)}
        return json.dumps(stat_dict)

    def _get_report_path(self):
        report_paths = {}
        for report_type in ["summary_by_date", "time_summary_stats", "full_period_stats"]:
            filename = "{0}_{1}".format(str(pd.Timestamp.now().date()).replace("-", ""),
                                        getattr(self, report_type+"_suffix"))
            target_location = self.report_location
            if target_location.startswith("s3"):
                report_path = join_s3_path(target_location, filename)
            else:
                report_path = os.path.join(target_location, filename)
            report_paths[report_type] = report_path
        return report_paths

    def do_step_action(self, **kwargs):
        final_data = kwargs["final_data"]
        regime_data = kwargs["regime_data"].rename(columns={"signal_value": "regime"})
        instructions = kwargs["views_in_market_calibration"]
        views_regimes_to_report = self.views_regimes_to_report
        filter_func = lambda x:((x['view_name'] in views_regimes_to_report.keys()) and \
                                (x['regime'] in views_regimes_to_report[x['view_name']]))
        all_interval_dates = regime_data[regime_data.apply(filter_func, axis=1)]
        all_views = list(views_regimes_to_report.keys())
        all_actor_ids = instructions["actorID"].unique()
        summary_by_date = []
        time_summary_stats = []
        full_period_stats = []

        for actor_id in all_actor_ids:
            for view in all_views:
                selections = self._generate_selections(instructions, actor_id)
                current_view = selections[selections['view']==view].reset_index(drop=True)
                interval_dates = all_interval_dates[all_interval_dates['view_name']==view]
                traded_names = pd.merge(interval_dates, current_view, left_on=["entry_date", "regime"],
                                        right_on=["date", "regime"], how="left")
                cut_data = final_data[['entry_date', 'ticker', 'signal_ticker', 'return_long', 'return_short']]
                combined = pd.merge(traded_names, cut_data, how='left', left_on=['entry_date', 'ticker', 'signal_ticker'],
                                    right_on=['entry_date', 'ticker', 'signal_ticker']).dropna()
                combined['return'] = combined\
                    .apply(lambda x:x['return_long'] if x['weight']>=0 else x['return_short'], axis=1)
                combined['weighted_return'] = combined\
                    .apply(lambda x:x['weight']*x['return_long'] if x['weight']>=0 else -x['weight']*x['return_short'],
                           axis=1)
                agg_method = {'regime': np.mean, 'weight': lambda x:np.sum(abs(x)), 'weighted_return': np.sum}
                current_summary_by_date = combined[['date', 'regime', 'weight', 'weighted_return']].groupby('date').agg(agg_method)
                current_summary_by_date['return'] = current_summary_by_date['weighted_return']/current_summary_by_date['weight']
                current_summary_by_date['actor_id'] = actor_id
                current_summary_by_date['view'] = view
                summary_by_date.append(current_summary_by_date[['actor_id', 'view', 'regime', 'return']])

                agg_method = {'return': lambda x:self._get_stat(x)}
                current_time_summary = current_summary_by_date[['regime', 'return']].groupby('regime').agg(agg_method)
                current_time_summary = current_time_summary['return'].apply(json.loads).apply(pd.Series)
                current_time_summary['actor_id'] = actor_id
                current_time_summary['view'] = view
                time_summary_stats.append(current_time_summary)

                overall_stats = combined[['regime', 'return']].groupby('regime').agg(agg_method)
                overall_stats = overall_stats['return'].apply(json.loads).apply(pd.Series)
                overall_stats['actor_id'] = actor_id
                overall_stats['view'] = view
                full_period_stats.append(overall_stats)
                calibration_logger.info("done")

        summary_by_date = pd.concat(summary_by_date)
        time_summary_stats = pd.concat(time_summary_stats)
        full_period_stats = pd.concat(full_period_stats)
        self.data = summary_by_date
        self.time_summary_stats = time_summary_stats
        self.full_period_stats = full_period_stats

        reports = {"summary_by_date": summary_by_date, "time_summary_stats": time_summary_stats,
                   "full_period_stats": full_period_stats}
        target_paths = self._get_report_path()
        for report_type in ["summary_by_date", "time_summary_stats", "full_period_stats"]:
            target_path = target_paths[report_type]
            data = reports[report_type]
            store_pandas_object(data, target_path, "Report {0}".format(report_type))
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data,
                self.__class__.PROVIDES_FIELDS[1] : self.time_summary_stats,
                self.__class__.PROVIDES_FIELDS[2] : self.full_period_stats}

    @classmethod
    def get_default_config(cls):
        return {"views_regimes_to_report": {"spy_view": [-2, -1, 1, 2]},
                "report_location": "C:/DCM/temp/", "summary_by_date_suffix": "summary_by_date.csv",
                "time_summary_stats_suffix": "time_summary_stats.csv",
                "full_period_stats_suffix": "all_period_results.csv"}