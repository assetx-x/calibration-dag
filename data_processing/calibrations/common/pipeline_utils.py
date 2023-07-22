from calibrations.calibration_pipelines import TaskflowPipelineDescription
from calibrations.core.calibration_tasks_registry import CalibrationTaskRegistry
from calibrations.data_pullers.data_pull import DataReader
from calibrations.core.calibration_taskflow_task import CalibrationFileTaskflowTask
from commonlib.util_functions import flatten_list_of_lists
from commonlib.data_requirements import DRDataTypes

def __get_all_classes_of_flow(pipeline_description, class_type_filter=None):
    if not isinstance(pipeline_description, TaskflowPipelineDescription):
        raise ValueError("parameter pipeline_description must be of type {0}".format(TaskflowPipelineDescription))
    all_steps_names = [l for k in pipeline_description for l in pipeline_description[k]]
    flow_classes = [CalibrationTaskRegistry.get_calibration_class(k) for k in all_steps_names]
    if class_type_filter:
        selected_classes = [k for k in flow_classes if issubclass(k, class_type_filter)]
    return selected_classes

def get_data_requirements_types_of_pipeline(pipeline_description):
    data_classes = __get_all_classes_of_flow(pipeline_description, DataReader)
    data_type_reqs = [klass.get_data_requirement_type() for klass in data_classes]
    data_type_reqs = flatten_list_of_lists(data_type_reqs)
    data_type_reqs = [DRDataTypes._value2member_map_[k] for k in
                      sorted(set([dr_type.value for dr_type in data_type_reqs]))]
    return data_type_reqs

def get_calibration_targets(pipeline_description, task_params):
    calib_classes = __get_all_classes_of_flow(pipeline_description, CalibrationFileTaskflowTask)
    calib_instances = [klass(**pipeline_description.get_configuration_of_step(klass.__name__))
                       for klass in calib_classes]
    calib_targets = [instance.get_file_targets(task_params) for instance in calib_instances]
    calib_targets = list(set(flatten_list_of_lists(calib_targets)))
    return calib_targets

if __name__=="__main__":
    from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode
    import pandas as pd
    flow_parameters = BasicTaskParameters(pd.Timestamp.now(), pd.Timestamp("2015-09-01"),
                                          pd.Timestamp("2018-12-31"), pd.Timestamp("2018-12-28"), TaskflowPipelineRunMode.Historical)

    pipeline_dec = TaskflowPipelineDescription(r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\fundamental_rebalancing_trader\fundamental_rebalancing_calibration.conf")
    pipeline_dec.setup_flow_according_to_params(flow_parameters)
    print(get_data_requirements_types_of_pipeline(pipeline_dec))
    print(get_calibration_targets(pipeline_dec, flow_parameters))
