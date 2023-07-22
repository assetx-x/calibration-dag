import os.path

import pytest

flow_file = os.path.join(os.path.dirname(__file__), "..", "basket_pairs_calibration_prod.conf")
results_dir = os.path.join(os.path.dirname(__file__.replace("dcm-intuition", "test_data")), "calibration_data")

def fix_integer_column_names(pipeline_results):
    pipeline_results["alpha_data"].columns = pipeline_results["alpha_data"].columns.astype(object)
    pipeline_results["cluster_size_data"].columns = pipeline_results["cluster_size_data"].columns.astype(object)
    return pipeline_results

#@pytest.mark.skip()
def test_basket_pairs(step_class_name, test_flow_parameters, precalculated_pipeline_results, pipeline_to_test,
                      step_io_adjuster_function):
    return pytest.test_data_pipeline_steps(step_class_name, test_flow_parameters, precalculated_pipeline_results,
                                           pipeline_to_test, step_io_adjuster_function)

if __name__=="__main__":
    from calibrations.common.test_utils import create_test_data
    create_test_data(flow_file, results_dir)
elif getattr(pytest, "create_calibration_tests", None):
    test_basket_pairs = pytest.create_calibration_tests(test_basket_pairs, flow_file, results_dir, pytest.filter_data_reader_steps,
                                                        fix_integer_column_names, None)

