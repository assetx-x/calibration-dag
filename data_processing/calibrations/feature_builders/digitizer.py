import gc

import numpy as np
import pandas as pd
from scipy.stats import norm as normal
#from statsmodels.robust.scale import mad

from calibrations.core.calibration_taskflow_task import CalibrationTaskflowTask
from etl_workflow_steps import StatusType
from calibrations.common.calibration_logging import calibration_logger


NORMALIZATION_CONSTANT = 0.67448975019608171
_SMALL_EPSILON = np.finfo(np.float64).eps

def mad(arr):
    med = np.lib.nanfunctions._nanmedian1d(arr)
    return np.lib.nanfunctions._nanmedian1d(np.abs(arr - med))/NORMALIZATION_CONSTANT

class Digitization(CalibrationTaskflowTask):
    '''

    Adds digitization of feature columns provided a digitization description (quantiles, groupby, and column names)

    '''

    REQUIRES_FIELDS = ["merged_data"]
    PROVIDES_FIELDS = ["final_data"]

    def __init__(self, digitization_description):
        self.data = None
        self.digitization_description = digitization_description
        CalibrationTaskflowTask.__init__(self)

    def cut_modified(self, x, q, use_mad_for_std=True, category_value_shift=0.0, qs_are_quantiles=True):
        quantiles_in_sigmas = np.asarray(list(map(normal.ppf, q))) if qs_are_quantiles else q
        x = pd.to_numeric(x)
        mean = np.nanmean(x.values)
        std = np.nanstd(x.values) if not use_mad_for_std else mad(x.values)
        bins = mean + quantiles_in_sigmas * std
        extreme_v = pd.np.asarray([pd.np.nanmin(x.values) - 1E-6, pd.np.nanmax(x.values) + 1E-6])
        bins = np.insert(bins, np.searchsorted(bins, extreme_v), extreme_v)
        digital_vals = pd.np.NaN if not pd.np.isfinite(mean) or not pd.np.isfinite(std) or pd.np.any(pd.np.diff(bins)==0)\
            else np.digitize(x.values, bins, right=True)
        digital_vals -= 1
        result = pd.Series(pd.np.sign(pd.np.abs(x.values)+_SMALL_EPSILON) * digital_vals, index=x.index)
        return result

    def add_digitized_columns_given_input_filter(self, ts, cut_point_list,suffix=None,
                                                 category_value_shift=0.0, qs_are_quantiles=True):
        new_col = self.cut_modified(ts, cut_point_list, category_value_shift=category_value_shift,
                                    qs_are_quantiles=qs_are_quantiles)
        new_col.name = ts.name + (suffix or "")
        return new_col

    def do_step_action(self, **kwargs):
        gc.collect()
        merged_data = kwargs[self.__class__.REQUIRES_FIELDS[0]]
        res = [merged_data]
        for digitization_type in self.digitization_description:
            calibration_logger.info("Running digitization schema: {0}".format(digitization_type))
            digitization_params = self.digitization_description[digitization_type]
            data_groups = merged_data.groupby(digitization_params["group_by"])[digitization_params["columns"]]
            shift = 1.0 if digitization_type=="tech_indicators_per_date" else 0.0
            cutpoints_in_sigmas = np.asarray(list(map(normal.ppf, digitization_params["quantiles"])))
            results = []
            for key, data in data_groups:
                digitized_cols = [self.add_digitized_columns_given_input_filter(data[x], cut_point_list = cutpoints_in_sigmas,
                                                                                suffix = digitization_params["suffix"], category_value_shift=shift,
                                                                                qs_are_quantiles=False) for x in digitization_params["columns"]]
                digitized_cols.sort(key=lambda x: x.name)
                grp_result = pd.concat(digitized_cols, axis=1)
                grp_result+=shift-1.0
                assert grp_result.index.equals(data.index)
                results.append(grp_result)
            digitized_res = pd.concat(results, ignore_index=False)
            assert len(merged_data) == len(digitized_res)
            res.append(digitized_res) # = pd.merge(merged_data, digitized_res, left_index=True, right_index=True)
        self.data = pd.concat(res, axis=1)
        return StatusType.Success

    def _get_additional_step_results(self):
        return {self.__class__.PROVIDES_FIELDS[0] : self.data}

    @classmethod
    def get_default_config(cls):
        return {"digitization_description": {"per_sector_date": \
                {"columns": ["ret_20B", "ret_60B", "ret_125B", "ret_252B", "indicator"],
                 "quantiles": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                 "group_by": ["sector", "entry_date"],
                 "suffix": "_sector_date"}}}
