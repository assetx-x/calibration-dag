import os.path
import pytest

flow_file = os.path.join(os.path.dirname(__file__), "..", "fundamental_rebalancing_calibration.conf")
results_dir = os.path.join(os.path.dirname("test_data".join(__file__.rsplit("dcm-intuition", 1))), "calibration_data")

def correct_fund_data(flow_data):
    flow_data["rule_applications"]["_rule"] = flow_data["rule_applications"]["_rule"].astype("category")
    return flow_data

#@pytest.mark.skip()
def test_fundamental(step_class_name, test_flow_parameters, precalculated_pipeline_results, pipeline_to_test,
                     step_io_adjuster_function):
    return pytest.test_data_pipeline_steps(step_class_name, test_flow_parameters, precalculated_pipeline_results,
                                           pipeline_to_test, step_io_adjuster_function)

if __name__=="__main__":
    from calibrations.common.test_utils import create_test_data
    create_test_data(flow_file, results_dir)
elif getattr(pytest, "create_calibration_tests", None):
    test_fundamental = pytest.create_calibration_tests(test_fundamental, flow_file, results_dir, pytest.filter_data_reader_steps,
                                                       correct_fund_data, None)

