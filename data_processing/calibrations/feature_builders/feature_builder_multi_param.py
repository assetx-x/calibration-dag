import pandas as pd

import talib

from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from calibrations.feature_builders.feature_builder import (CalculateTaLibSTOCHRSI, CalculateVolatility, 
                                                           CalculateTaLibWILLR, CalculateTaLibADX, 
                                                           CalculateTaLibPPO)
from commonlib.commonStatTools.technical_indicators.offline import (talib_PPO, talib_TRIX, talib_STOCHRSI,
                                                                    talib_STOCH, talib_STOCHF, talib_ULTOSC)
from etl_workflow_steps import StatusType

class CalculateTaLibSTOCHRSIMultiParam(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_stochrsi_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or [{"technical_indicator_params": {"timeperiod": 14, "fastk_period": 5,
                                                                   "fastd_period": 3, "fastd_matype": 0},
                                    "price_column": "close"}]
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibSTOCHRSI(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"configs": [{"technical_indicator_params": {"timeperiod": 14, "fastk_period": 5, "fastd_period": 3,
                                                            "fastd_matype": 0}, "price_column": "close"}]}


class CalculateVolatilityMultiParam(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["volatility_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or [{"volatility_lookback": 63, "price_column": "close"}]
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateVolatility(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"configs": [{"volatility_lookback": 63, "price_column": "close"}]}


class CalculateTaLibWILLRMultiParam(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_willr_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or  [{"technical_indicator_params": {"timeperiod": 14}, "smoothing_period": 3}]
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibWILLR(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"configs":[{"technical_indicator_params": {"timeperiod": 14}, "smoothing_period": 3}]}


class CalculateTaLibADXMultiParam(CalibrationTaskflowTask):
    '''

    Calculates the talib version of ADX with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_adx_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.configs = configs or  [{"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 14},
                                     "smoothing_period": 3}]
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibADX(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"configs": [{"technical_indicator": talib.ADX, "technical_indicator_params": {"timeperiod": 14},
                             "smoothing_period": 3}]}

class CalculateTaLibPPOMultiParam(CalibrationTaskflowTask):
    '''

    Calculates the talib version of PPO indiciator with multiple parameter configurations

    '''

    PROVIDES_FIELDS = ["talib_ppo_indicator_data"]
    REQUIRES_FIELDS = ["daily_price_data"]

    def __init__(self, configs):
        self.data = None
        self.technical_indicator = talib_PPO
        self.configs = configs or [{"technical_indicator_params": [{"fastperiod": 12, "slowperiod": 26, "matype": 0}],
                                    "price_column": "close", "invert_sign": True}]
        CalibrationTaskflowTask.__init__(self)

    def do_step_action(self, **kwargs):
        all_results = []
        for config in self.configs:
            indicator = CalculateTaLibPPO(**config)
            indicator.do_step_action(**kwargs)
            result = list(indicator._get_additional_step_results().values())[0].set_index(["date", "ticker"])
            all_results.append(result)
        all_results = pd.concat(all_results, axis=1)
        self.data = all_results.sort_index().reset_index()
        return StatusType.Success


    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"configs": [{"technical_indicator_params": [{"fastperiod": 12, "slowperiod": 26, "matype": 0}],
                             "price_column": "close", "invert_sign": True}]}

