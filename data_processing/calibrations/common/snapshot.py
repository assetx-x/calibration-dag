
import _pickle as pickle
import h5py
import pandas as pd

from calibrations.common.calibration_logging import calibration_logger
from commonlib.util_functions import flatten_list_of_lists
from etl_workflow_steps import _BASE_FLOW_STEP_RESULT_LABEL

def snapshot_taskflow_engine_results(taskflow_engine, location, include_non_pandas_objects=True, complevel=9,
                                     complib="blosc", result_names_to_store=None, metadata_to_store=None):
    if metadata_to_store and not isinstance(metadata_to_store, dict):
        raise TypeError("snapshot_taskflow_engine_results - 'metadata_to_store' is expected to be a dict; received {0}"
                        .format(type(metadata_to_store)))
    results = taskflow_engine.storage.fetch_all()
    step_results = results[_BASE_FLOW_STEP_RESULT_LABEL]
    if step_results and isinstance(step_results[0], list):
        step_results = flatten_list_of_lists(step_results)
    step_results = pd.DataFrame(step_results)
    pd.set_option('expand_frame_repr', False)
    pd.set_option("max_rows", len(step_results.index))
    pd.set_option("max_columns", len(step_results.columns))
    calibration_logger.info("DEBUG:")
    calibration_logger.info(step_results)
    failure = step_results[step_results["status_type"] == "Fail"]
    if len(failure):
        calibration_logger.info("FAILURE:")
        calibration_logger.info(failure.iloc[0, :].T.to_dict())
    result_names_to_store = result_names_to_store or results.keys()
    with pd.HDFStore(location, "w", complevel=complevel, complib=complib) as store:
        concepts_to_store = [k for k in results if isinstance(results[k], pd.core.base.PandasObject)
                             and k in result_names_to_store]
        for k in concepts_to_store:
            calibration_logger.info("Storing pandas object {0} with key {1}".format(k, "/pandas/{0}".format(k)))
            if isinstance(results[k], pd.DataFrame):
                categorical_check = [pd.api.types.is_categorical_dtype(col) for col in results[k].dtypes.values]
                if any(categorical_check):
                    categorical_cols = results[k].columns.values[pd.np.asarray(categorical_check)]
                    for col in categorical_cols:
                        results[k][col] = results[k][col].astype(object)
            else:
                categorical_check = pd.api.types.is_categorical_dtype(results[k].dtype)
                if categorical_check:
                    results[k] = results[k].astype(object)
            store["/pandas/{0}".format(k)] = results[k]
    if include_non_pandas_objects:
        with h5py.File(location, "a") as store_as_plain_h5:
            concepts_to_store = [k for k in results if not isinstance(results[k], pd.core.base.PandasObject)
                                 and k in result_names_to_store]
            for k in concepts_to_store:
                calibration_logger.info("Storing non pandas object {0} with key {1}".format(k, "/nonpandas/{0}".format(k)))
                store_as_plain_h5.create_dataset("/nonpandas/{0}".format(k), data=pickle.dumps(results[k], protocol=0))

    if metadata_to_store is not None:
        with h5py.File(location, "a") as store_as_plain_h5:
            for k in metadata_to_store:
                calibration_logger.info("Storing snapshot metadata with key {1}".format(k, "/metadata/{0}".format(k)))
                store_as_plain_h5.create_dataset("/metadata/{0}".format(k), data=pickle.dumps(metadata_to_store[k], protocol=0))

def snapshot_keys(snapshot_location, include_non_pandas_objects=True):
    keys = []
    with h5py.File(snapshot_location, "r") as store_as_plain_h5:
        if include_non_pandas_objects and "nonpandas" in store_as_plain_h5.keys():
            keys.extend(store_as_plain_h5["nonpandas"].keys())
    with pd.HDFStore(snapshot_location, "r") as store:
        pandas_object_keys = filter(lambda x:x.startswith("/pandas/"), store.keys())
        pandas_object_keys = map(lambda x:x.split("/")[2], pandas_object_keys)
        keys.extend(pandas_object_keys)
    return keys

def read_taskflow_engine_snapshot(snapshot_location, include_non_pandas_objects=True, result_names_to_load=None):
    result = {}
    with h5py.File(snapshot_location, "r") as store_as_plain_h5:
        if include_non_pandas_objects and "nonpandas" in list(store_as_plain_h5.keys()):
            name_filters = result_names_to_load or list(store_as_plain_h5["nonpandas"].keys())
            for k in list(store_as_plain_h5["nonpandas"].keys()):
                if k in name_filters:
                    calibration_logger.info("Reading non-pandas object {0} from key {1}".format(k, "/nonpandas/{0}".format(k)))
                    result[k] = pickle.loads(store_as_plain_h5["nonpandas"][k].value)
    with pd.HDFStore(snapshot_location, "r") as store:
        pandas_object_keys = list(filter(lambda x:x.startswith("/pandas/"), list(store.keys())))
        pandas_object_keys = list(map(lambda x:x.split("/")[2], pandas_object_keys))
        name_filters = result_names_to_load or pandas_object_keys
        for k in pandas_object_keys:
            if k in name_filters:
                calibration_logger.info("Reading pandas object {0} from key {1}".format(k, "/pandas/{0}".format(k)))
                result[k] = store["/pandas/{0}".format(k)]
    return result

def get_snapshot_metadata(snapshot_location):
    metadata = {}
    with h5py.File(snapshot_location, "r") as store_as_plain_h5:
        if "metadata" in store_as_plain_h5.keys():
            for k in store_as_plain_h5["metadata"].keys():
                calibration_logger.info("Readings metadata {0} from key {1}".format(k, "/metadata/{0}".format(k)))
                metadata[k] = pickle.loads(store_as_plain_h5["metadata"][k].value)
    return metadata
