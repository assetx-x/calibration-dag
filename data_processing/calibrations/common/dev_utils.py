import os.path
from glob import glob
from abc import abstractmethod
import warnings

import pandas as pd

from calibrations.common.snapshot import read_taskflow_engine_snapshot, snapshot_keys
from calibrations.core.calibration_tasks_registry import CalibrationTaskRegistry
from commonlib.util_classes import ResultsDictIOAsCSV
from etl_workflow_steps import BaseETLStep, compose_main_flow_and_engine_for_task

from taskflow_params import UniverseDates

from functools import reduce


class CalibrationDatasetIO(object):
    @abstractmethod
    def read_dataset(self, dataset_keys):
        raise NotImplementedError("CalibrationDataSetIO.read_dataset must be overriden in derived class")

    @abstractmethod
    def missing_datasets(self, dataset_keys):
        raise NotImplementedError("CalibrationDataSetIO.missing_datasets must be overriden in derived class")

class CalibrationDatasetFromSnapshot(CalibrationDatasetIO):
    def __init__(self, snapshot_location, run_params=None, verify_snapshot_integrity=False):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            if verify_snapshot_integrity:
                raise NotImplementedError("CalibrationDatasetFromSnapshot.__init__ -> verify_snapshot_integrity is not "
                                          "implemented")
            if not os.path.exists(snapshot_location):
                raise RuntimeError("CalibrationDatasetFromSnapshot.__init__ -> The snapshot location {0} does not exists"
                                   .format(snapshot_location))
            files_to_read = [snapshot_location] if os.path.isfile(os.path.realpath(snapshot_location)) \
                else glob(os.path.join(snapshot_location, "*.h5"))
            files_to_read = sorted(files_to_read, key=lambda x:os.path.getmtime(x), reverse=False)
            if not files_to_read:
                raise RuntimeError("CalibrationDatasetFromSnapshot.__init__ -> could not find any file to read on location"
                                   " {0}".format(snapshot_location))
            dataset_dfs = []
            for f in files_to_read:
                available_ds = pd.DataFrame([[ds] for ds in snapshot_keys(f, include_non_pandas_objects=True)],
                                            columns=["dataset"])
                if len(available_ds)>0:
                    available_ds["filename"] = f
                    available_ds.set_index("dataset", inplace=True)
                    dataset_dfs.append(available_ds)
            if not dataset_dfs:
                raise RuntimeError("CalibrationDatasetFromSnapshot.__init__ -> could not read any calibration dataset in "
                                   "files {0}".format(files_to_read))
            all_datasets_index = reduce(lambda x,y:x.union(y), map(lambda x:x.index, dataset_dfs))
            full_availability_df = dataset_dfs[0].copy(deep=True).reindex(all_datasets_index)
            for secondary_df in dataset_dfs[1:]:
                full_availability_df = full_availability_df.combine_first(secondary_df)
            self.dataset_inventory = full_availability_df

    def missing_datasets(self, dataset_keys):
        selected_sets = self.dataset_inventory.reindex(dataset_keys)
        missing_sets = selected_sets["filename"][pd.isnull(selected_sets)["filename"]].tolist()
        return missing_sets

    def read_dataset(self, dataset_keys):
        if self.missing_datasets(dataset_keys):
            return None
        result = {}
        selected_sets = self.dataset_inventory.reindex(dataset_keys)
        for f, data_to_read in selected_sets.groupby("filename"):
            f_dataset = read_taskflow_engine_snapshot(f, result_names_to_load=data_to_read.index.tolist())
            result.update(f_dataset)
        return result

class CalibrationDatasetFromUnitTestFiles(CalibrationDatasetIO):
    def __init__(self, snapshot_location):
        self.snapshot_location = snapshot_location
        if not os.path.exists(snapshot_location):
            raise RuntimeError("CalibrationDatasetFromUnitTestFiles.__init__ -> The snapshot location {0} does not exists"
                               .format(snapshot_location))
        if not os.path.isdir(snapshot_location):
            raise RuntimeError("CalibrationDatasetFromUnitTestFiles.__init__ -> The snapshot location {0} must be a "
                               "directory".format(snapshot_location))
        files_to_read = glob(os.path.join(snapshot_location, "*"))
        if not files_to_read:
            raise RuntimeError("CalibrationDatasetFromUnitTestFiles.__init__ -> could not find any file to read on location"
                               " {0}".format(snapshot_location))
        self.available_datasets = set([os.path.splitext(os.path.basename(k))[0] for k in files_to_read])

    def read_dataset(self, dataset_keys):
        if self.missing_datasets(dataset_keys):
            return None
        results = ResultsDictIOAsCSV.read_directory_tree_to_dict(self.snapshot_location, filter_dict_keys=dataset_keys)
        return results

    def missing_datasets(self, dataset_keys):
        return list(set(dataset_keys).difference(self.available_datasets))

def run_calibration_step_independently(step_name, run_params, dataset_retriever=None, step_configuration=None,
                                       step_input_modifier=None):
    if step_name not in CalibrationTaskRegistry.registered_classes:
        raise ValueError("run_calibration_step_independently -> the step {0} is nto available on calibration registry"
                         .format(step_name))

    if step_input_modifier and not hasattr(step_input_modifier, "__call__"):
        raise RuntimeError("run_calibration_step_independently -> step_input_modifier must be callable")

    klass = CalibrationTaskRegistry.get_calibration_class(step_name)
    klass_config = step_configuration or klass.get_default_config()
    if not CalibrationTaskRegistry._config_is_correct_for_class(klass, klass_config):
        raise RuntimeError("run_calibration_step_independently -> the provided configuration does not match the "
                           "__init__ of step {0}. Please verify".format(step_name))

    required_data = CalibrationTaskRegistry.get_required_data_of(step_name)
    if required_data:
        if not dataset_retriever or not isinstance(dataset_retriever, CalibrationDatasetIO):
            raise RuntimeError("run_calibration_step_independently -> step {0} requires datasets {1} that must be "
                               "retrieved using an instance of CalibrationDatasetIO; received {2}"
                               .format(step_name, required_data, type(dataset_retriever)))
        missin_datasets = dataset_retriever.missing_datasets(required_data)
        if missin_datasets:
            raise RuntimeError("run_calibration_step_independently -> the instance of CalibrationDatasetIO cannot find "
                               "all the datasets required by step {0}. The missing datasets are {1}"
                               .format(step_name, missin_datasets))
        print("Acquiring required datasets {0}".format(required_data))
        required_datasets = dataset_retriever.read_dataset(required_data)
    else:
        required_datasets = {}

    if step_input_modifier:
        required_datasets = step_input_modifier(required_datasets)

    step_instance = klass(**klass_config)
    engine, main_flow = compose_main_flow_and_engine_for_task("test_{0}".format(step_name), run_params, step_instance,
                                                              store=required_datasets)
    engine.run(main_flow)
    result_data_names = CalibrationTaskRegistry.get_produced_data_of(step_name)
    results = {k:engine.storage.fetch(k) for k in result_data_names}
    return results

if __name__=="__main__":
    step_name = "FilterRussell1000Augmented"
    dataset_cache = CalibrationDatasetFromSnapshot(r"C:\DCM\temp\pipeline_tests\russell_update\debug")
    unit_test_cache = CalibrationDatasetFromUnitTestFiles(r"C:\DCM\test_data\processes\data_processing\calibrations\views_in_market_trading\test\calibration_data")

    from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode
    #run_params = BasicTaskParameters(pd.Timestamp.now(), pd.Timestamp("2000-01-01"), pd.Timestamp("2019-12-01"),
    #                                 pd.Timestamp("2019-12-01"), TaskflowPipelineRunMode.Historical)

    target_dt = pd.Timestamp.now().normalize()
    start_dt = pd.Timestamp("2000-01-03")
    run_params = BasicTaskParameters(target_dt, start_dt, target_dt, target_dt, TaskflowPipelineRunMode.Historical) 

    run_calibration_step_independently(step_name, run_params, dataset_cache)
