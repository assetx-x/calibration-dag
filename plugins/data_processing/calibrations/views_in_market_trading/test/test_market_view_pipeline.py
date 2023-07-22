import os.path
from copy import deepcopy

import pytest
import numpy
from calibrations.views_in_market_trading.market_views_calibration_classes import AssetSelectorAndWeighting, RuleApplications

flow_file = os.path.join(os.path.dirname(__file__), "..", "market_view_calibration.conf")
results_dir = os.path.join(os.path.dirname(__file__.replace("dcm-intuition", "test_data")), "calibration_data")


def correct_mv_test_setups(step_instance, inputs, outputs):
    if isinstance(step_instance, AssetSelectorAndWeighting):
        inputs["calibration_weightings_dict"] = deepcopy(inputs["calibration_weightings_dict"])
        inputs["calibration_weightings_dict"].pop(step_instance.weights_name)
    if isinstance(step_instance, RuleApplications):
        outputs["calibration_weightings_dict"] = {}
    return inputs, outputs

def correct_mv_data(flow_data):
    flow_data["rule_applications"]["_rule"] = flow_data["rule_applications"]["_rule"].astype("category")
    return flow_data

#@pytest.mark.skip()
def test_market_views(step_class_name, test_flow_parameters, precalculated_pipeline_results, pipeline_to_test,
                      step_io_adjuster_function):
    return pytest.test_data_pipeline_steps(step_class_name, test_flow_parameters, precalculated_pipeline_results,
                                           pipeline_to_test, step_io_adjuster_function)

if __name__=="__main__":
    from calibrations.common.test_utils import create_test_data
    create_test_data(flow_file, results_dir)
elif getattr(pytest, "create_calibration_tests", None):
    test_market_views = pytest.create_calibration_tests(test_market_views, flow_file, results_dir, pytest.filter_data_reader_steps,
                                                        correct_mv_data, correct_mv_test_setups)

