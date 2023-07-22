import os.path
import pytest

flow_file = os.path.join(os.path.dirname(__file__), "..", "prod_quantamental_ensemble_aws.conf")
results_dir = os.path.join(os.path.dirname(__file__.replace("dcm-intuition", "test_data")), "calibration_data_ensemble")

def test_quantamental_ml_ensemble(step_class_name, test_flow_parameters, precalculated_pipeline_results, pipeline_to_test,
                                  step_io_adjuster_function):
    return pytest.test_data_pipeline_steps(step_class_name, test_flow_parameters, precalculated_pipeline_results,
                                           pipeline_to_test, step_io_adjuster_function)

if __name__=="__main__":
    from calibrations.common.test_utils import create_test_data
    create_test_data(flow_file, results_dir, 2)
elif getattr(pytest, "create_calibration_tests", None):
    test_quantamental_ml_ensemble = pytest.create_calibration_tests(test_quantamental_ml_ensemble, flow_file, results_dir,
                                                                    pytest.filter_for_velocity, None, None)
