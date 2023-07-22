import os.path
import pytest

flow_file = os.path.join(os.path.dirname(__file__), "..", "jump_recovery_calibration.conf")
results_dir = os.path.join(os.path.dirname(__file__.replace("dcm-intuition", "test_data")), "calibration_data")

@pytest.mark.skip()
def test_jump_recovery(step_class_name, test_flow_parameters, precalculated_pipeline_results, pipeline_to_test,
                       step_io_adjuster_function):
    return pytest.test_data_pipeline_steps(step_class_name, test_flow_parameters, precalculated_pipeline_results,
                                           pipeline_to_test, step_io_adjuster_function)

if __name__=="__main__":
    from calibrations.common.test_utils import create_test_data
    create_test_data(flow_file, results_dir)
elif getattr(pytest, "create_calibration_tests", None):
    test_jump_recovery = pytest.create_calibration_tests(test_jump_recovery, flow_file, results_dir, pytest.filter_data_reader_steps,
                                                         None, None)
