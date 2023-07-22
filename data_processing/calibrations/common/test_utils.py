import pandas as pd

from calibrations.calibration_pipelines import TaskflowPipelineDescription, TaskflowPipelineRunner
from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode, UniverseDates
from commonlib.util_classes import ResultsDictIOAsCSV


def test_flow_parameters():
    flow_params = BasicTaskParameters(pd.Timestamp("2018-12-03"), pd.Timestamp("2016-11-30"), pd.Timestamp("2018-11-30"),
                                      UniverseDates.UnitTest.value, TaskflowPipelineRunMode.Test)
    return flow_params

def test_flow_parameters_velocity():
    target_dt = pd.Timestamp("2018-11-30")
    start_dt = pd.Timestamp("2016-06-01")
    flow_parameters = BasicTaskParameters(target_dt, start_dt, target_dt, UniverseDates.UnitTestVelocity.value,
                                          TaskflowPipelineRunMode.Test)
    return flow_parameters

def create_test_data(pipeline_file, results_directory, flow_type=1, ignore_auxiliary_results=True):
    pipeline =  TaskflowPipelineDescription(pipeline_file)
    runner = TaskflowPipelineRunner(pipeline)
    if flow_type==1:
        flow_params = test_flow_parameters()
    else:
        flow_params = test_flow_parameters_velocity() # Unit tests for the velocity platform
    step_results = runner.run(flow_params)
    flow_data = runner.retrieve_full_flow_results()
    if ignore_auxiliary_results:
        for k in ["failure_information", "step_results", "flow_parameters"]:
            temp = flow_data.pop(k)
    ResultsDictIOAsCSV.create_csv_directory_tree_from_dict(flow_data, results_directory, max_recursion_depth=0)

    stored_test_results = ResultsDictIOAsCSV.read_directory_tree_to_dict(results_directory)
    from commonlib.util_functions_testing import assert_deep_almost_equal
    assert_deep_almost_equal(flow_data, stored_test_results)
    print("Done")

def explore_test_data(pipeline_file, results_directory):
    pipeline =  TaskflowPipelineDescription(pipeline_file)
    runner = TaskflowPipelineRunner(pipeline)
    flow_params = test_flow_parameters()
    step_results = runner.run(flow_params)
    flow_data = runner.retrieve_full_flow_results()
    stored_test_results = ResultsDictIOAsCSV.read_directory_tree_to_dict(results_directory)
    from commonlib.util_functions_testing import assert_deep_almost_equal
    assert_deep_almost_equal(flow_data, stored_test_results)

if __name__=="__main__":
    #results_dir = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\basket_pair_trading\test\flow_data"
    #pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\basket_pair_trading\basket_pairs_calibration.conf"

    #results_dir = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\fundamental_rebalancing_trader\test\flow_data"
    #pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\fundamental_rebalancing_trader\fundamental_rebalancing_calibration.conf"

    #results_dir = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\jump_recovery_trading\test\flow_data"
    #pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\jump_recovery_trading\jump_recovery_calibration.conf"

    #pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\views_in_market_trading\market_view_calibration.conf"
    #results_dir = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\views_in_market_trading\test\flow_data"

    #pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\quantamental_ml\quantamental_with_econ_full_gan_training_aws_first.conf"
    #results_dir = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\quantamental_ml\test\flow_data_gan"
    
    #pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\quantamental_ml\quantamental_with_econ_full_aws_first_day.conf"
    #results_dir = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\quantamental_ml\test\flow_data_ensemble"
    
    pipeline_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\basket_pair_trading\basket_pairs_calibration_prod.conf"
    results_dir = r"C:\DCM\temp\pipeline_tests\bp_testing"         

    create_test_data(pipeline_file, results_dir, 1)
    explore_test_data(pipeline_file, results_dir, 1)
    print("done")
