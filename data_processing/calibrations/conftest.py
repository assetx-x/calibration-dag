import sys, os
import os.path
from itertools import repeat
from functools import wraps

import pandas as pd
import pytest

from commonlib.util_classes import ResultsDictIOAsCSV
from calibrations.common.test_utils import test_flow_parameters
from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode
from commonlib.util_functions import flatten_list_of_lists
from commonlib.util_functions_testing import assert_deep_almost_equal
from calibrations.calibration_pipelines import TaskflowPipelineDescription, TaskflowPipelineRunner
from calibrations.core.calibration_tasks_registry import CalibrationTaskRegistry
from calibrations.data_pullers.data_pull import DataReader
from calibrations.quantamental_ml.quantamental_ml import DepositInstructionsS3
from etl_workflow_steps import BaseETLStep, compose_main_flow_and_engine_for_task

def filter_data_reader_steps(step_list):
    #exclude data pull steps as they would require a detailed mock of all database tables
    steps_to_test = [k for k in step_list if not issubclass(CalibrationTaskRegistry.get_calibration_class(k),
                                                            DataReader)]
    return steps_to_test

def filter_for_velocity(step_list):
    steps_to_test = filter_data_reader_steps(step_list)
    steps_to_test = [k for k in steps_to_test if not issubclass(CalibrationTaskRegistry.get_calibration_class(k),
                                                                DepositInstructionsS3)]
    return steps_to_test

@pytest.fixture(scope="module")
def alter_environment():
    new_values = dict(zip(["REDSHIFT_USER", "REDSHIFT_PWD", "REDSHIFT_ENDPOINT"],
                          ["test_user", "test_pwd", "test_endpoint"]))
    current_values = {k:os.environ.get(k, None) for k in new_values}
    for k in new_values:
        os.environ[k] = new_values[k]
    yield
    for k in new_values:
        if current_values[k]:
            os.environ[k] = current_values[k]
        else:
            os.environ.pop(k)

@pytest.fixture
def all_redshift_data():
    all_data = {os.path.basename(k).split(".")[0]: pd.read_csv(k, parse_dates=True, index_col=0) for k in
                glob.glob(os.path.realpath(os.path.join(os.path.dirname(__file__), "./inputs/sql_tables/*.csv")))}
    return all_data

@pytest.fixture
def redshift_mocker_with_data(mocker, all_redshift_data):
    new_engine = sa.engine.strategies.strategies["plain"].create("sqlite:///")
    mocker.patch.object(new_engine, "dispose")
    for tablename in all_redshift_data:
        data = all_redshift_data[tablename]
        assert isinstance(data, pd.DataFrame)
        data.to_sql(tablename, new_engine, index=False)
    mocker.patch("market_view_calibration.create_engine", side_effect=lambda *args, **kwargs:new_engine)

@pytest.fixture
def mock_etf_file_s3(mock_credentials, mocker):
    # using an internal of the pytest mocker to initialize and keep track of the S3 mock
    mocker.patch._start_patch(mock_s3)

test_flow_parameters = pytest.fixture()(test_flow_parameters)

def test_data_pipeline_steps(step_class_name, test_flow_parameters, precalculated_pipeline_results, pipeline_to_test,
                             step_io_adjuster_function):
    step_class = CalibrationTaskRegistry.get_calibration_class(step_class_name)
    requires = step_class.get_full_class_requires(True, False)
    provides = step_class.get_full_class_provides(True, False)

    required_data = {k:precalculated_pipeline_results[k] for k in requires}
    expected_data = {k:precalculated_pipeline_results[k] for k in provides}
    instance_params = pipeline_to_test.get_configuration_of_step(step_class_name)
    step_instance = step_class(**instance_params)

    required_data, expected_data = step_io_adjuster_function(step_instance, required_data, expected_data)

    engine, main_flow = compose_main_flow_and_engine_for_task("test", test_flow_parameters, step_instance,
                                                              store=required_data)
    engine.run(main_flow)
    results = engine.storage.fetch_all()
    provided_data = {k:results[k] for k in provides}
    if step_class.__name__ == 'CalculateDerivedFundamentalsQuandl':
        expected_data['derived_fundamental_data'].columns.rename('concept', inplace=True)
    assert_deep_almost_equal(expected_data, provided_data)

def create_calibration_tests(test_func, pipeline_file, results_directory, step_filter=None, results_postprocessing=None,
                             step_io_adjuster_function=None):
    def get_all_steps(pipeline, step_filter):
        all_steps = [pipeline.steps_iterator_for_stage(k) for k in pipeline]
        all_steps = flatten_list_of_lists(all_steps)
        step_filter = step_filter or (lambda x:x)
        steps_to_test = step_filter(all_steps)
        return steps_to_test

    def dynamic_parameterized_test(base_name, steps_to_test, pipeline_to_test, precalculated_pipeline_results,
                                   step_io_adjuster_function):
        test_cases = zip(iter(steps_to_test), repeat(pipeline_to_test), repeat(precalculated_pipeline_results),
                          repeat(step_io_adjuster_function))
        #test_ids = ["{0}.{1}".format(base_name, step) for step in steps_to_test]
        decorator = pytest.mark.parametrize("step_class_name, pipeline_to_test,precalculated_pipeline_results,step_io_adjuster_function",
                                            list(test_cases))#, ids=test_ids)
        return_func = decorator(test_func)
        return return_func

    def precalculated_pipeline_results(results_dir, results_postprocessing):
        INPUT_DIR = results_dir
        precalculated_pipeline_results = ResultsDictIOAsCSV.read_directory_tree_to_dict(INPUT_DIR)
        results_postprocessing = results_postprocessing or (lambda x:x)
        precalculated_pipeline_results = results_postprocessing(precalculated_pipeline_results)
        return precalculated_pipeline_results

    step_io_adjuster_function = step_io_adjuster_function or (lambda step_instance, inputs, outputs: (inputs, outputs))
    pipeline =  TaskflowPipelineDescription(pipeline_file)
    pipeline.enable_test_overrides()
    base_name = os.path.splitext(os.path.basename(pipeline_file))[0]
    results = precalculated_pipeline_results(results_directory, results_postprocessing)
    steps_to_test = get_all_steps(pipeline, step_filter)

    return wraps(test_func)(dynamic_parameterized_test)(base_name, steps_to_test, pipeline, results,
                                                        step_io_adjuster_function)

def pytest_namespace():
    return {"assert_deep_almost_equal": assert_deep_almost_equal,
            "create_calibration_tests": create_calibration_tests,
            "test_data_pipeline_steps": test_data_pipeline_steps,
            "filter_data_reader_steps": filter_data_reader_steps,
            "filter_for_velocity": filter_for_velocity}
