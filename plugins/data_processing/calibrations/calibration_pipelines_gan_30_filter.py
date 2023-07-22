import os.path
import sys
import traceback
NoneType = type(None)
import tempfile
from glob import glob
from copy import deepcopy
from itertools import groupby as iter_groupby

import pyhocon
import json
from collections import OrderedDict, defaultdict
import pandas as pd
from toposort import toposort

from calibrations.core.calibration_tasks_registry import CalibrationTaskRegistry
from commonlib.util_functions import (get_parameters_from_bound_method, create_directory_if_does_not_exists,
                                      flatten_list_of_lists)
from commonlib.util_functions_testing import assert_deep_almost_equal
from calibrations.common.taskflow_params import TaskflowPipelineRunMode
from commonlib.config import _merge_configs
from calibrations.common.calibration_logging import calibration_logger
from calibrations.common.config_functions import get_config, set_config
from calibrations.common.snapshot import (snapshot_taskflow_engine_results, read_taskflow_engine_snapshot,
                                          snapshot_keys, get_snapshot_metadata)
from etl_workflow_steps import (BaseETLStep, BaseETLStage, compose_main_flow_and_engine_for_task,
                                _BASE_FLOW_STEP_RESULT_LABEL)

from calibrations.common.taskflow_params import BasicTaskParameters, TaskflowPipelineRunMode, UniverseDates
from functools import reduce

_FLOW_KEY = "Flow"
_STEP_CONFIG_KEY = "StepsConfiguration"
_FLOW_CONFIG_KEY = "FlowConfiguration"
_CALIB_OVERRIDES_KEY = "calibration_mode_overrides"
_TEST_OVERRIDES_KEY = "test_mode_overrides"

class TaskflowPipelineDescriptionBuilder(object):
    @classmethod
    def build_valid_pipeline_desc_file_from_flow(cls, flow_file, output_file):
        pipeline_data = TaskflowPipelineDescription._read_pipeline_file(flow_file)
        pipeline_data[_STEP_CONFIG_KEY] = OrderedDict()
        TaskflowPipelineDescriptionValidation._verify_basic_format(pipeline_data, flow_file)
        TaskflowPipelineDescriptionValidation._verify_flow_configuration(pipeline_data, flow_file)
        TaskflowPipelineDescriptionValidation._verify_flow_section_format(pipeline_data, flow_file)
        flow = pipeline_data[_FLOW_KEY]
        flow_classes = [l for k in flow for l in flow[k]]
        for klass_name in flow_classes:
            klass = CalibrationTaskRegistry.registered_classes[klass_name]
            klass_config = klass.get_default_config()
            pipeline_data[_STEP_CONFIG_KEY][klass_name] = klass_config
        TaskflowPipelineDescriptionValidation._verify_step_configuration_section_format(pipeline_data, flow_file, True)
        TaskflowPipelineDescriptionValidation._verify_flow_integrity(pipeline_data, flow_file)
        cls.__output_pipeline_to_file(pipeline_data, output_file)

    @classmethod
    def __output_pipeline_to_file(cls, pipeline_data, output_file):
        valid_hocon_dict = pyhocon.ConfigFactory.from_dict(pipeline_data)
        hocon_str = pyhocon.tool.HOCONConverter.to_hocon(valid_hocon_dict, indent=4)
        with open(output_file, "w") as fd:
            fd.write(hocon_str)

    @classmethod
    def __default_flow_config_section(cls):
        return dict(snapshot_location = None, base_configuration_filepath = None, base_configuration_overrides = {},
                    use_existing_cache_for_stages=[], calibration_mode_overrides={}, test_mode_overrides={})

    @classmethod
    def verify_all_get_default_methods(cls):
        pipeline_data = pyhocon.ConfigTree()
        pipeline_data[_FLOW_KEY] = pyhocon.ConfigTree()
        pipeline_data[_FLOW_KEY]["all_step_tests"] = [k for k in CalibrationTaskRegistry.registered_classes]
        pipeline_data[_FLOW_CONFIG_KEY] = cls.__default_flow_config_section()
        temp_filepath = tempfile.mktemp()
        output_temp_filepath = tempfile.mktemp()
        cls.__output_pipeline_to_file(pipeline_data, temp_filepath)
        cls.build_valid_pipeline_desc_file_from_flow(temp_filepath, output_temp_filepath)
        return output_temp_filepath

    @classmethod
    def _order_steps_topologically(cls, steps_to_order):
        dependency_sets = defaultdict(set)
        for step in steps_to_order:
            dependency_sets[step]
            possible_providers = CalibrationTaskRegistry.get_possible_providers_of(step)
            provider_steps_for_stage = set(possible_providers).intersection(set(steps_to_order))
            for provider_step in provider_steps_for_stage:
                dependency_sets[step].add(provider_step)
        return toposort(dependency_sets)

    @classmethod
    def _order_all_flow_steps_topologically(cls, flow):
        for stage in flow:
            stage_steps = flow[stage]
            topological_sorted_steps = cls._order_steps_topologically(stage_steps)
            optimal_ordering = reduce(lambda x,y:x.extend(y) or x, list(map(list, topological_sorted_steps)), [])
            if len(optimal_ordering)!=len(stage_steps):
                raise RuntimeError("TaskflowPipelineDescriptionBuilder._order_all_flow_steps_topologically - there was "
                                   "a problem while sorting calibration tasks of stage {0}. Ordered steps do not have "
                                   "the same length as the original provided list".format(stage))
            flow[stage] = optimal_ordering

    @classmethod
    def __extract_minimal_set_of_steps_for_targets(cls, existing_steps, target_steps):
        selected_steps = set()
        steps_to_process = set(target_steps)
        existing_steps = set(existing_steps)
        while len(steps_to_process):
            next_step = steps_to_process.pop()
            selected_steps.add(next_step)
            step_providers = CalibrationTaskRegistry.get_possible_providers_of(next_step)
            steps_to_add = set(step_providers).intersection(existing_steps).difference(selected_steps)
            steps_to_process = steps_to_process.union(steps_to_add)
        return selected_steps

    @classmethod
    def __build_and_test_flow(cls, flow, flow_config, steps_config, output_file):
        cls._order_all_flow_steps_topologically(flow)
        new_pipeline = pyhocon.ConfigTree()
        new_pipeline[_FLOW_KEY] = flow
        new_pipeline[_FLOW_CONFIG_KEY] = flow_config
        new_pipeline[_STEP_CONFIG_KEY] = pyhocon.ConfigTree()
        for stage in flow:
            for step in flow[stage]:
                new_pipeline[_STEP_CONFIG_KEY][step] = steps_config[step]
        cls.__output_pipeline_to_file(new_pipeline, output_file)
        try:
            TaskflowPipelineDescription(output_file)
        except BaseException as e:
            raise RuntimeError("TaskflowPipelineDescriptionBuilder.__build_and_test_flow - there was an error "
                               "while validating simplified flow. Error is {0}".format(e))

    @classmethod
    def build_simplified_flow_file_for_targets(cls, original_flow_file, target_steps, output_file):
        calibration_logger.info("TaskflowPipelineDescriptionBuilder is loading {0}".format(original_flow_file))
        original_flow_file = TaskflowPipelineDescription(original_flow_file)
        existing_steps = [original_flow_file.steps_iterator_for_stage(k) for k in original_flow_file]
        existing_steps = flatten_list_of_lists(existing_steps)
        misspecified_steps = list(set(target_steps).difference(set(existing_steps)))
        if misspecified_steps:
            raise RuntimeError("TaskflowPipelineDescriptionBuilder.build_simplified_flow_file - steps {0} cannot be "
                               "found on reference file {1}".format(misspecified_steps, original_flow_file))
        selected_steps = cls.__extract_minimal_set_of_steps_for_targets(existing_steps, target_steps)
        calibration_logger.info("TaskflowPipelineDescriptionBuilder has selected the following steps to copy into "
                                "new flow: {0}".format(selected_steps))

        possible_simplified_flow = [(k, list(set(original_flow_file.pipeline_flow[k]).intersection(selected_steps)))
                                    for k in original_flow_file]
        possible_simplified_flow = OrderedDict([k for k in possible_simplified_flow if k[1]])
        cls._order_all_flow_steps_topologically(possible_simplified_flow)
        calibration_logger.info("TaskflowPipelineDescriptionBuilder is copying {0} options and correcting for "
                                "selected steps".format(_FLOW_CONFIG_KEY))
        flow_config = deepcopy(original_flow_file.flow_configuration)
        flow_config["use_existing_cache_for_stages"] = []
        flow_config["snapshot_location"] = None
        flow_config["base_configuration_filepath"] = flow_config["base_configuration_filepath"].replace(os.path.sep,
                                                                                                        os.path.altsep)
        flow_config[_TEST_OVERRIDES_KEY] =  original_flow_file.get_calib_overrides(_TEST_OVERRIDES_KEY)
        for override_sections in [_CALIB_OVERRIDES_KEY, _TEST_OVERRIDES_KEY]:
            flow_config[override_sections] = original_flow_file.get_calib_overrides(override_sections)
            override_step_section = flow_config[override_sections].get(_STEP_CONFIG_KEY, pyhocon.ConfigTree())
            for step_name in override_step_section:
                if step_name not in selected_steps:
                    del override_step_section[step_name]
            if not override_step_section and _STEP_CONFIG_KEY in flow_config[override_sections]:
                del flow_config[override_sections][_STEP_CONFIG_KEY]

        calibration_logger.info("TaskflowPipelineDescriptionBuilder is creating and validating target file {0}"
                                .format(output_file))
        cls.__build_and_test_flow(possible_simplified_flow, flow_config,
                                  original_flow_file.step_configurations, output_file)

    @classmethod
    def __collect_steps_for_target(cls, target_steps):
        print("\n\n******   Collecting minimalsitic flow to run {0}   ******".format(target_steps))
        selected_steps = set()
        pending_steps = [k for k in target_steps]
        while pending_steps:
            step = pending_steps.pop()
            print("  --> Adding to flow step {0}".format(step))
            selected_steps.add(step)
            required_data = CalibrationTaskRegistry.get_required_data_of(step)
            for dataset in required_data:
                possible_providers = set(CalibrationTaskRegistry.get_provider_of(dataset))
                if not possible_providers.intersection(selected_steps) and possible_providers:
                    possible_providers = sorted(list(possible_providers))
                    choice = 0
                    if len(possible_providers)>1:
                        print('\n === Please pick a provider for dataset "{1}" required by step *{0}*\n'
                              .format(step, dataset))
                        msg = "\n".join(["{0}) {1} - {2}".format(n+1, provider,
                                                                 CalibrationTaskRegistry.get_description_of(provider))
                                         for n, provider in enumerate(possible_providers)])
                        print(msg)
                        choice = None
                        while choice is None:
                            raw_choice = input("\nWhich provider shall be used? : ")
                            if not raw_choice.isdigit() or not 1<=int(raw_choice)<=len(possible_providers):
                                print("Incorrect choice. Try again")
                            else:
                                choice = int(raw_choice)-1
                    dataset_provider = possible_providers[choice]
                    pending_steps.append(dataset_provider)
        return selected_steps


    @classmethod
    def build_flow_from_scratch(cls, target_steps, output_file):
        if isinstance(target_steps, str):
            target_steps = [target_steps]
        problem_targets = [k for k in target_steps if k not in CalibrationTaskRegistry.registered_classes]
        if problem_targets:
            skipped_classes = [k for k in problem_targets if k in CalibrationTaskRegistry.skipped_classes]
            unknown_classes = [k for k in problem_targets if k not in CalibrationTaskRegistry.skipped_classes]
            err_msg = ""
            if skipped_classes:
                err_msg += ("Some of the targets are classes that could not be registered on CalibrationTaskRegistry; "
                            "verify if they contains abstract methods: {0}".format(skipped_classes))
            if unknown_classes:
                err_msg += ("Some of the targets are classes that are unknown on the Task Registry: {0} "
                            .format(unknown_classes))
            raise ValueError("TaskflowPipelineDescriptionBuilder.build_flow_from_scratch - {0}".format(err_msg))
        flow_steps = cls.__collect_steps_for_target(target_steps)
        dependent_steps = [k for k in flow_steps if k not in target_steps]
        dependent_steps_groups = [list(k) for k in cls._order_steps_topologically(dependent_steps)]
        proposed_flow = pyhocon.ConfigTree(("Stage_{0}".format(n+1),k) for n,k in enumerate(dependent_steps_groups))
        proposed_flow["Targets"] =  target_steps
        steps_config = OrderedDict()
        for step in flow_steps:
            steps_config[step] = CalibrationTaskRegistry.get_default_configuration_of(step)
        cls.__build_and_test_flow(proposed_flow, cls.__default_flow_config_section(), steps_config, output_file)


class TaskflowPipelineDescriptionValidation(object):
    @classmethod
    def _verify_basic_format(cls, pipeline_data, pipeline_file_path):
        expected_section_labels = [_FLOW_KEY, _STEP_CONFIG_KEY, _FLOW_CONFIG_KEY]
        for k in expected_section_labels:
            if k not in pipeline_data:
                raise KeyError("TaskflowPipelineDescriptionValidation.__verify_basic_format - the file {0} does not contain a "
                               "section called {1}. Please correct the file".format(pipeline_file_path, k))
        additional_keys = set(pipeline_data.keys()).difference(expected_section_labels)
        if additional_keys:
            raise KeyError("TaskflowPipelineDescriptionValidation.__verify_basic_format - the file {0} has unrecognized keys: {1}"
                           .format(pipeline_file_path, additional_keys))

    @classmethod
    def _verify_flow_section_format(cls, pipeline_data, pipeline_file_path):
        flow = pipeline_data[_FLOW_KEY]
        if not isinstance(flow, OrderedDict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_section_format - The '{0}' section of file {1} "
                             "must be a dictionary. Please verify.".format(_FLOW_KEY, pipeline_file_path))
        wrong_sections = [k for k in flow if not isinstance(flow[k], list)]
        if wrong_sections:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_section_format - the '{0}' section of file {1} "
                             "has stages that are not lists. The stages to correct are {2}"
                             .format(_FLOW_KEY, pipeline_file_path, wrong_sections))
        wrong_step_structure = ["{0}.{1}".format(k, l) for k in flow for l in flow[k]
                                if not isinstance(l, str)]
        if wrong_step_structure:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_section_format - the '{0}' section of file {1} "
                             "must be a dictionary of lists of steps; found a more complex structure at the following "
                             "labels: {2}".format(_FLOW_KEY, pipeline_file_path, ", ".join(wrong_step_structure)))
        wrong_steps = ["{0}.{1}".format(k, l) for k in flow for l in flow[k]
                       if l not in CalibrationTaskRegistry.registered_classes]
        if wrong_steps:
            wrong_steps = ", ".join(wrong_steps)
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_section_format - the '{0}' section of file {1} "
                             "must be a dictionary of lists of registered steps; found unrecognized steps on the "
                             "following labels: {2}".format(_FLOW_KEY, pipeline_file_path, wrong_steps))

        flow_steps = set([l for k in flow for l in flow[k]])
        locations = {k:[l for l in flow if k in flow[l]] for k in flow_steps}
        repeated_steps = {k:locations[k] for k in locations if len(locations[k])>1}
        if repeated_steps:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_section_format - the '{0}' section of file {1} "
                             "contains repeated steps. Please remove duplicates. Duplicates are {2}"
                             .format(_FLOW_KEY, pipeline_file_path, repeated_steps))


    @classmethod
    def _verify_step_configuration_section_format(cls, pipeline_data, pipeline_file_path, verify_full_steps_configuration):
        configuration = pipeline_data[_STEP_CONFIG_KEY]
        flow = pipeline_data[_FLOW_KEY]
        flow_steps = [l for k in flow for l in flow[k]]
        if not isinstance(configuration, OrderedDict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_configuration_section_format - The '{0}' section of "
                             "file {1} must be a dictionary. Please verify."
                            .format(_STEP_CONFIG_KEY, pipeline_file_path))
        unrecognized_steps = [k for k in configuration if k not in CalibrationTaskRegistry.registered_classes]
        if unrecognized_steps:
            skipped_classes = [k for k in unrecognized_steps if k in CalibrationTaskRegistry.skipped_classes]
            unknown_classes = [k for k in unrecognized_steps if k not in skipped_classes]
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_configuration_section_format - The '{0}' section of "
                             "file {1} contains steps that were not accepted in the registry: {2} and unknown steps {3}"
                             .format(_STEP_CONFIG_KEY, pipeline_file_path, skipped_classes, unknown_classes))
        steps_not_in_flow = [k for k in configuration if k not in flow_steps]
        if steps_not_in_flow:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_configuration_section_format - The '{0}' section of "
                             "file {1} contains reference to steps not conatined on the flow section: {2}"
                             .format(_STEP_CONFIG_KEY, pipeline_file_path, steps_not_in_flow))
        missing_configs = [k for k in flow_steps if k not in configuration]
        if missing_configs:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_configuration_section_format - The '{0}' section of "
                             "file {1} is missing configuration description for some steps in the flow: {2}"
                             .format(_STEP_CONFIG_KEY, pipeline_file_path, missing_configs))

        wrong_config_sections = [k for k in configuration if not isinstance(configuration[k], dict)]
        if wrong_config_sections:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_configuration_section_format - The '{0}' section of "
                             "file {1} has configuration blocks that are nto dictionaries for the following "
                             "classes: {2}".format(_STEP_CONFIG_KEY, pipeline_file_path, wrong_config_sections))
        missing_config_options = []
        unrecognized_config_options = []
        for k in configuration:
            klass = CalibrationTaskRegistry.registered_classes[k]
            init_params = get_parameters_from_bound_method(klass.__init__)
            required_params = set(init_params[1])
            if verify_full_steps_configuration:
                required_params = required_params.union(init_params[0])
            provided_params = set(configuration[k].keys())
            missing_config_options.extend(["{0}.{1}".format(k, l) for l in required_params.difference(provided_params)])
            unrecognized_config_options.extend(["{0}.{1}".format(k, l)
                                                for l in provided_params.difference(required_params)])
        if missing_config_options or unrecognized_config_options:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_configuration_section_format - there are problems "
                             "with the options on the '{0}' section of file {1}. There are missing config options {2}"
                             " and there are unrecognized config options {3}"
                             .format(_STEP_CONFIG_KEY, pipeline_file_path, missing_config_options,
                                     unrecognized_config_options))


    @classmethod
    def _verify_flow_integrity(cls, pipeline_data, pipeline_file_path):
        flow = pipeline_data[_FLOW_KEY]
        flow_classes = [CalibrationTaskRegistry.registered_classes[l] for k in flow for l in flow[k]]
        flow_inputs = [k.get_full_class_provides(True, False) for k in flow_classes]
        flow_inputs = set(reduce(lambda x,y:x.extend(y) or x, flow_inputs, []))
        flow_outputs = [k.get_full_class_provides(True, False) for k in flow_classes]
        flow_outputs = set(reduce(lambda x,y:x.extend(y) or x, flow_outputs, []))
        missing_inputs = flow_inputs.difference(flow_outputs)
        if missing_inputs:
            missing_input_dict = dict()
            possible_providers_dict = dict()
            for k in missing_inputs:
                tasks_with_missing_input = [l.__name__ for l in flow_classes if k in l.get_full_class_requires()]
                for l in tasks_with_missing_input:
                    missing_input_dict.setdefault(l, list()).extend([k])
                possible_providers_dict[k] = CalibrationTaskRegistry.get_provider_of(k)
            raise RuntimeError("TaskflowPipelineDescriptionValidation.__verify_flow_integrity - the flow described on file {0}"
                               " is incomplete. The following tasks are missing required inputs: {1}. The classes that "
                               "can provide the missing inputs are {2}"
                               .format(pipeline_file_path, missing_input_dict, possible_providers_dict))
        TaskflowPipelineDescriptionBuilder._order_all_flow_steps_topologically(flow)

    @classmethod
    def __verifiy_snapshot_and_caching_config(cls, pipeline_data, pipeline_file_path):
        flow_config = pipeline_data[_FLOW_CONFIG_KEY]
        snapshot_location = flow_config["snapshot_location"]
        if not isinstance(snapshot_location, (str, NoneType)):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - The key {0} of '{1}' section "
                             "within file {2} must be a string, or None. Please verify."
                             .format("snapshot_location", _FLOW_CONFIG_KEY, pipeline_file_path))
        if snapshot_location and os.path.exists(snapshot_location) and not os.path.isdir(snapshot_location):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration  - the snapshot location {0} "
                             " on section {1} of file {2} exists and is not a directory. Location must be a directory"
                             .format(snapshot_location, _FLOW_CONFIG_KEY, pipeline_file_path))
        existing_cache = flow_config["use_existing_cache_for_stages"]
        flow = pipeline_data[_FLOW_KEY]
        if not isinstance(existing_cache, list):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - use_existing_cache_for_stages"
                             "key of  '{0}' section of file {1} must be a list. Please verify"
                             .format(_FLOW_CONFIG_KEY, pipeline_file_path))
        if "*" in existing_cache:
            known_caches = list(map(lambda x:os.path.basename(x).replace(".h5", ""),
                                  glob(os.path.join(snapshot_location, "*.h5"))))
            known_caches = [k for k in known_caches if k in list(flow.keys())]
            existing_cache.remove("*")
            existing_cache.extend(known_caches)
            existing_cache = list(set(existing_cache))
        unknown_cache_stages = set(existing_cache).difference(set(flow.keys()))
        if unknown_cache_stages:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - use_existing_cache_for_stages"
                             "key of  '{0}' section of file {1} contains unknown stages {2}. Please verify"
                             .format(_FLOW_CONFIG_KEY, pipeline_file_path, unknown_cache_stages))

    @classmethod
    def __verifiy_configuration_for_flow(cls, pipeline_data, pipeline_file_path):
        flow_config = pipeline_data[_FLOW_CONFIG_KEY]
        config_filepath = flow_config["base_configuration_filepath"]
        if config_filepath and (not os.path.exists(config_filepath) or not os.path.isfile(config_filepath)):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - The '{0}' section of file {1} "
                             "refers to a configuration file that does not exists {2}. Please verify"
                             .format(_FLOW_CONFIG_KEY, pipeline_file_path, config_filepath))
        if flow_config["base_configuration_filepath"] is not None:
            flow_config["base_configuration_filepath"] = os.path.realpath(flow_config["base_configuration_filepath"])
        config_overrides = flow_config["base_configuration_overrides"]
        if not isinstance(config_overrides, dict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - The key {0} of '{1}' section "
                             "within file {2} must be a dictionary. Please verify."
                             .format("config_overrides", _FLOW_CONFIG_KEY, pipeline_file_path))
        if flow_config["snapshot_location"] is not None:
            flow_config["snapshot_location"] = os.path.realpath(flow_config["snapshot_location"])

    @classmethod
    def __verify_override_section(cls, section_key, pipeline_data, pipeline_file_path):
        flow_config = pipeline_data[_FLOW_CONFIG_KEY]
        overrides = flow_config[section_key]
        if not isinstance(overrides, dict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_calibration_overrides - The section "
                             "'{0}.{1}' of file {2} must be a dictionary. Please verify."
                             .format(_FLOW_CONFIG_KEY, section_key, pipeline_file_path))
        allowed_keys = ["FlowConfiguration", "StepsConfiguration"]
        unrecognized_keys = list(set(overrides.keys()).difference(set(allowed_keys)))
        if unrecognized_keys:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_calibration_overrides - The section "
                             "'{0}.{1}' of file {2} must be a dictionary where only allowed keys are {3}"
                             .format(_FLOW_CONFIG_KEY, section_key, pipeline_file_path, allowed_keys))
        flow_overrides = overrides.get(_FLOW_CONFIG_KEY, {})
        if not isinstance(flow_overrides, dict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_calibration_overrides - The section "
                             "'{0}.{1}.{2}' of file {3} must be a dictionary. Please verify."
                             .format(_FLOW_CONFIG_KEY, section_key, _FLOW_CONFIG_KEY,
                                     pipeline_file_path))
        valid_flow_config_overrides = list(flow_config.keys())
        valid_flow_config_overrides.remove(section_key)
        bad_flow_config_overrides = list(set(flow_overrides.keys()).difference(set(valid_flow_config_overrides)))
        if bad_flow_config_overrides:
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_calibration_overrides - The section "
                             "'{0}.{1}.{2}' of file {2} must be a dictionary where only allowed keys are {3}; received "
                             "{4}".format(_FLOW_CONFIG_KEY, section_key, _FLOW_CONFIG_KEY,
                                     pipeline_file_path, valid_flow_config_overrides, bad_flow_config_overrides))
        step_overrides = overrides.get(_STEP_CONFIG_KEY, {})
        if not isinstance(step_overrides, dict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_calibration_overrides - The section "
                             "'{0}.{1}.{2}' of file {3} must be a dictionary. Please verify."
                             .format(_FLOW_CONFIG_KEY, section_key, _STEP_CONFIG_KEY,
                                     pipeline_file_path))

    @classmethod
    def __verify_calibration_overrides(cls, pipeline_data, pipeline_file_path):
        for section_key in [_CALIB_OVERRIDES_KEY, _TEST_OVERRIDES_KEY]:
            calibration_logger.info("Verifying format of {0} overrides".format(section_key))
            cls.__verify_override_section(section_key, pipeline_data, pipeline_file_path)

    @classmethod
    def _verify_flow_configuration(cls, pipeline_data, pipeline_file_path):
        flow_config = pipeline_data[_FLOW_CONFIG_KEY]
        if not isinstance(flow_config, OrderedDict):
            raise ValueError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - The '{0}' section of file {1} "
                             "must be a dictionary. Please verify.".format(_FLOW_CONFIG_KEY, pipeline_file_path))
        expected_keys = ["snapshot_location", "base_configuration_filepath", "base_configuration_overrides",
                         "use_existing_cache_for_stages", _CALIB_OVERRIDES_KEY, _TEST_OVERRIDES_KEY]
        missing_keys = set(expected_keys).difference(set(flow_config.keys()))
        unknown_keys = set(flow_config.keys()).difference(set(expected_keys))
        if missing_keys or unknown_keys:
            raise KeyError("TaskflowPipelineDescriptionValidation.__verify_flow_configuration - The '{0}' section of file {1} "
                           "must strictly contain only expected keys {2}. Please verify"
                           .format(_FLOW_CONFIG_KEY, pipeline_file_path, expected_keys))
        cls.__verifiy_configuration_for_flow(pipeline_data, pipeline_file_path)
        cls.__verifiy_snapshot_and_caching_config(pipeline_data, pipeline_file_path)
        cls.__verify_calibration_overrides(pipeline_data, pipeline_file_path)

class TaskflowPipelineDescription(object):
    def __init__(self, pipeline_file_path, verify_full_steps_configuration=True):
        calibration_logger.disabled = False

        self.pipeline_file_path = pipeline_file_path
        self.verify_full_steps_configuration = verify_full_steps_configuration

        self.original_raw_pipeline_data = None
        self.original_pipeline_data = None
        self.pipeline_data_with_overrides = None

        self.pipeline_flow = None
        self.step_configurations = None
        self.flow_configuration = None
        self.effective_pipeline_configuration = None
        self.calib_overrides = None
        self.__read_and_validate_pipeline_file()

    def __read_effective_pipeline_configuration(self):
        base_config = pyhocon.ConfigFactory.parse_file(self.flow_configuration["base_configuration_filepath"]) \
            if self.flow_configuration["base_configuration_filepath"] else pyhocon.ConfigTree()
        final_config = _merge_configs(base_config, self.flow_configuration["base_configuration_overrides"])
        return final_config

    @classmethod
    def _read_pipeline_file(self, pipeline_file_path):
        if not os.path.exists(pipeline_file_path):
            raise RuntimeError("TaskflowPipelineDescription.__read_pipeline_file - The file {0} does not exists, "
                               "please verify".format(pipeline_file_path))
        try:
            calibration_logger.info("Reading pipeline description file %s", pipeline_file_path)
            pipeline_data = pyhocon.ConfigFactory.parse_file(pipeline_file_path)
        except BaseException as e:
            raise RuntimeError("TaskflowPipelineDescription.__read_pipeline_file - There was an error parsing the file"
                               "{0}; the original error is {1}".format(pipeline_file_path, e))
        return pipeline_data

    def __set_data_from_pipeline_data(self, pipeline_data):
        self.pipeline_flow = pipeline_data[_FLOW_KEY]
        self.step_configurations = pipeline_data[_STEP_CONFIG_KEY]
        self.flow_configuration = pipeline_data[_FLOW_CONFIG_KEY]
        self.effective_pipeline_configuration = self.__read_effective_pipeline_configuration()

    def __validate_pipeline_w_overrides(self, pipeline_data, override_section):
        calibration_logger.info("Verifying {0}".format(override_section))
        calib_overrides = pipeline_data[_FLOW_CONFIG_KEY].pop(override_section)
        try:
            pipeline_data_w_overrides = _merge_configs(deepcopy(pipeline_data), calib_overrides)
            if not pipeline_data_w_overrides[_FLOW_CONFIG_KEY]["use_existing_cache_for_stages"] and \
               self.original_raw_pipeline_data[_FLOW_CONFIG_KEY]["use_existing_cache_for_stages"]:
                pipeline_data_w_overrides[_FLOW_CONFIG_KEY]["use_existing_cache_for_stages"] = \
                    deepcopy(self.original_raw_pipeline_data[_FLOW_CONFIG_KEY]["use_existing_cache_for_stages"])
            for k in [_CALIB_OVERRIDES_KEY, _TEST_OVERRIDES_KEY]:
                pipeline_data_w_overrides[_FLOW_CONFIG_KEY][k] = pyhocon.ConfigTree()
            TaskflowPipelineDescriptionValidation._verify_basic_format(pipeline_data_w_overrides, self.pipeline_file_path)
            TaskflowPipelineDescriptionValidation._verify_flow_configuration(pipeline_data_w_overrides, self.pipeline_file_path)
            TaskflowPipelineDescriptionValidation._verify_flow_section_format(pipeline_data_w_overrides, self.pipeline_file_path)
            TaskflowPipelineDescriptionValidation._verify_step_configuration_section_format(pipeline_data_w_overrides, self.pipeline_file_path,
                                                           self.verify_full_steps_configuration)
            TaskflowPipelineDescriptionValidation._verify_flow_integrity(pipeline_data_w_overrides, self.pipeline_file_path)
        except BaseException as e:
            calibration_logger.error(" ***** There was an error while verifying flow integrity after applying "
                                     "section {0} overrides. *****".format(override_section))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            calibration_logger.error("".join(traceback.format_exception(exc_type, exc_value,exc_traceback)))
            e.message = "--> ERROR WHEN INCORPORATING CALIBRATION OVERRIDES: <-- {0}".format(e.message)
            e = e.__class__(e.message)
            raise e
        return calib_overrides, pipeline_data_w_overrides

    def __read_and_validate_pipeline_file(self):
        # change directory to interpet relative paths properly
        current_dir = os.getcwd()
        target_dir = os.path.dirname(self.pipeline_file_path)
        os.chdir(target_dir)
        try:
            pipeline_data = self._read_pipeline_file(self.pipeline_file_path)
            self.original_raw_pipeline_data = deepcopy(pipeline_data)
            calibration_logger.info("Verifying basic format fo file")
            TaskflowPipelineDescriptionValidation._verify_basic_format(pipeline_data, self.pipeline_file_path)
            calibration_logger.info("Verifying format of section %s of file", _FLOW_CONFIG_KEY)
            TaskflowPipelineDescriptionValidation._verify_flow_configuration(pipeline_data, self.pipeline_file_path)
            calibration_logger.info("Verifying format of section %s of file", _FLOW_KEY)
            TaskflowPipelineDescriptionValidation._verify_flow_section_format(pipeline_data, self.pipeline_file_path)
            calibration_logger.info("Verifying format of section %s of file", _STEP_CONFIG_KEY)
            TaskflowPipelineDescriptionValidation._verify_step_configuration_section_format(pipeline_data, self.pipeline_file_path,
                                                           self.verify_full_steps_configuration)
            calibration_logger.info("Verifying flow integrity")
            TaskflowPipelineDescriptionValidation._verify_flow_integrity(pipeline_data, self.pipeline_file_path)

            self.pipeline_data_with_overrides = {}
            self.calib_overrides = {}
            for override_section in [_CALIB_OVERRIDES_KEY, _TEST_OVERRIDES_KEY]:
                calib_overrides, pipeline_data_w_overrides = self.__validate_pipeline_w_overrides(pipeline_data,
                                                                                                  override_section)
                self.calib_overrides[override_section] = calib_overrides
                self.pipeline_data_with_overrides[override_section] = pipeline_data_w_overrides

            self.original_pipeline_data = pipeline_data
            self.__set_data_from_pipeline_data(self.original_pipeline_data)
        finally:
            os.chdir(current_dir)

    def enable_calibration_overrides(self):
        self.__set_data_from_pipeline_data(self.pipeline_data_with_overrides[_CALIB_OVERRIDES_KEY])

    def enable_test_overrides(self):
        self.__set_data_from_pipeline_data(self.pipeline_data_with_overrides[_TEST_OVERRIDES_KEY])

    def disable_overrides(self):
        self.__set_data_from_pipeline_data(self.original_pipeline_data)

    def __iter__(self):
        return iter(self.pipeline_flow.keys())

    def __getitem__(self, k):
        return self.pipeline_flow[k]

    def __contains__(self, val):
        return val in self.pipeline_flow

    def get_configuration_of_step(self, step_name):
        return json.loads(pyhocon.tool.HOCONConverter.to_json(self.step_configurations[step_name]))

    def steps_iterator_for_stage(self, stage_name):
        return iter(self.pipeline_flow[stage_name])

    def get_effective_pipeline_configuration(self):
        return self.effective_pipeline_configuration

    def get_snapshot_location(self):
        return self.flow_configuration["snapshot_location"]

    def can_reuse_cache(self, stage_name):
        return stage_name in self.flow_configuration["use_existing_cache_for_stages"]

    def get_calib_overrides(self, key):
        return self.calib_overrides[key]

    def setup_flow_according_to_params(self, flow_parameters):
        if flow_parameters.run_mode is TaskflowPipelineRunMode.Calibration:
            self.enable_calibration_overrides()
        elif flow_parameters.run_mode is TaskflowPipelineRunMode.Historical:
            self.disable_overrides()
        elif flow_parameters.run_mode is TaskflowPipelineRunMode.Test:
            self.enable_test_overrides()
        else:
            raise RuntimeError("TaskflowPipelineDescription.setup_flow_according_to_taskparams - unknown runmode {0}"
                               .format(flow_parameters.run_mode))

    def get_expected_datasets_from_stage(self, stage):
        if stage not in self:
            raise ValueError("TaskflowPipelineDescription.get_expected_datasets_from_stage - stage {0} is not in flow "
                             "description. Avilable flow stages are: {1}".format(stage, list(self.pipeline_flow.keys())))
        datasets = [CalibrationTaskRegistry.get_produced_data_of(k) for k in self.steps_iterator_for_stage(stage)]
        datasets = set(reduce(lambda x,y:x.extend(y) or x, datasets, []))
        return datasets

    def get_required_datasets_from_stage(self, stage):
        if stage not in self:
            raise ValueError("TaskflowPipelineDescription.get_required_datasets_from_stage - stage {0} is not in flow "
                             "description. Avilable flow stages are: {1}".format(stage, list(self.pipeline_flow.keys())))
        datasets = [CalibrationTaskRegistry.get_required_data_of(k) for k in self.steps_iterator_for_stage(stage)]
        datasets = set(reduce(lambda x,y:x.extend(y) or x, datasets, []))

        produced_data_by_stage = self.get_expected_datasets_from_stage(stage)
        datasets = datasets.difference(produced_data_by_stage)
        return datasets

    def get_full_stage_configuration(self, stage):
        if stage not in self:
            raise ValueError("TaskflowPipelineDescription.get_expected_datasets_from_stage - stage {0} is not in flow "
                             "description. Avilable flow stages are: {1}".format(stage, list(self.pipeline_flow.keys())))
        steps_config = {k:self.get_configuration_of_step(k) for k in self.steps_iterator_for_stage(stage)}
        return steps_config


class TaskflowPipelineRunner(object):
    def __init__(self, pipeline_description):
        if not isinstance(pipeline_description, TaskflowPipelineDescription):
            raise TypeError("TaskflowPipelineRunner.__init__ - the parameter pipeline_description must of of type "
                            "TaskflowPipelineDescription")
        self.pipeline_description = pipeline_description
        self._old_config = None
        self.snapshot_directory = None

    def __create_taskflow_stage(self, stage_name):
        step_list = []
        for step_name in self.pipeline_description.steps_iterator_for_stage(stage_name):
            step_config = self.pipeline_description.get_configuration_of_step(step_name)
            step_class = CalibrationTaskRegistry.registered_classes[step_name]
            step = step_class(**step_config)
            step_list.append(step)
        stage = BaseETLStage(stage_name, "Stage", *step_list)
        return stage

    def __get_location_of_snapshot(self, stage_name):
        snapshot_location =  os.path.join(self.snapshot_directory, "{0}.h5".format(stage_name))
        return snapshot_location

    def __setup_snapshot_directory(self):
        snapshot_directory = self.pipeline_description.get_snapshot_location()
        if snapshot_directory and not os.path.exists(snapshot_directory):
            create_directory_if_does_not_exists(snapshot_directory)
        self.snapshot_directory = snapshot_directory

    def __available_results_from_previous_snapshots(self, current_stage):
        all_stages = [k for k in self.pipeline_description]
        loc_current_stage = all_stages.index(current_stage)
        previous_stages = all_stages[0:loc_current_stage]
        result_keys = []
        for stage in previous_stages:
            snapshot_loc = self.__get_location_of_snapshot(stage)
            available_snapshot_keys = snapshot_keys(snapshot_loc, True)
            result_keys.extend(available_snapshot_keys)
        return result_keys

    def __determine_results_to_store(self, stage_name, engine):
        existing_results = set(self.__available_results_from_previous_snapshots(stage_name))
        results = set(engine.storage.fetch_all().keys())
        results_to_store = results.difference(existing_results)
        info_fields_to_store = BaseETLStep.get_full_class_requires(True, True)
        for k in info_fields_to_store:
            if k in results:
                results_to_store.add(k)
        return list(results_to_store)

    def __determine_completness_of_snapshot(self, stage_name):
        existing_results = set(self.__available_results_from_previous_snapshots(stage_name))
        required_results = self.pipeline_description.get_required_datasets_from_stage(stage_name)
        missing_datasets = list(set(required_results).difference(set(existing_results)))
        if missing_datasets:
            raise RuntimeError("TaskflowPipelineRunner.__determine_data_to_read - the snapshots available for the runs "
                               "do not contain all results required by stage {0}. The missing datasets are {1}"
                               .format(stage_name, missing_datasets))

    def __determine_location_of_data_to_read(self, stage_list, stage_name=None):
        stages_results_list = []
        for stage in stage_list:
            snapshot_loc = self.__get_location_of_snapshot(stage)
            available_results = snapshot_keys(snapshot_loc, True)
            new_dict = {k:stage for k in available_results}
            stages_results_list.append(new_dict)
        effective_locations = reduce(lambda a,b:a.update(b) or a, stages_results_list, {})
        if stage_name:
            required_datasets = self.pipeline_description.get_required_datasets_from_stage(stage_name)
            data_to_read = [(k, effective_locations[k]) for k in effective_locations if k in required_datasets]
        else:
            data_to_read = [(k, effective_locations[k]) for k in effective_locations]
        data_to_read = sorted(data_to_read, key=lambda x:x[1])
        return iter_groupby(data_to_read, lambda x:x[1])

    def __read_data_from_snapshots(self, data_location_iter):
        store = {}
        for stage, grp in data_location_iter:
            stage_datasets = [k[0] for k in grp]
            snapshot_loc = self.__get_location_of_snapshot(stage)
            results = read_taskflow_engine_snapshot(snapshot_loc, result_names_to_load=stage_datasets)
            store.update(results)
        return store

    def __read_snapshot_data_for_stage(self, stage_name):
        self.__determine_completness_of_snapshot(stage_name)
        all_stages = [k for k in self.pipeline_description]
        loc_current_stage = all_stages.index(stage_name)
        previous_stages = all_stages[0:loc_current_stage]
        data_to_read = self.__determine_location_of_data_to_read(previous_stages, stage_name)
        results = self.__read_data_from_snapshots(data_to_read)
        return results

    def retrieve_full_flow_results(self):
        stage_list = [k for k in self.pipeline_description]
        data_to_read = self.__determine_location_of_data_to_read(stage_list)
        results = self.__read_data_from_snapshots(data_to_read)
        return results

    def __run_pipeline_stage(self, stage_name, flow_parameters, dependency_data, create_snapshot_for_stage=True):
        stage = self.__create_taskflow_stage(stage_name)
        engine, main_flow = compose_main_flow_and_engine_for_task("{0}_flow".format(stage_name), flow_parameters,
                                                                  stage, store = dependency_data)
        engine.run(main_flow)
        if create_snapshot_for_stage:
            metadata = {}
            metadata[_STEP_CONFIG_KEY] = self.pipeline_description.get_full_stage_configuration(stage_name)
            metadata["FlowParams"] = flow_parameters
            snapshot_location = self.__get_location_of_snapshot(stage_name)
            calibration_logger.info("Storing results for stage %s into file %s",
                                    stage_name, snapshot_location)
            results_to_store = self.__determine_results_to_store(stage_name, engine)
            snapshot_taskflow_engine_results(engine, snapshot_location, metadata_to_store=metadata,
                                             result_names_to_store=results_to_store)
        return engine.storage.fetch_all()

    def __setup_config_before_running(self):
        self._old_config = get_config()
        new_config = self.pipeline_description.get_effective_pipeline_configuration()
        set_config(new_config)

    def __restore_config(self):
        set_config(self._old_config)
        self._old_config = None

    def __verify_snapshot_integrity(self, stage, flow_parameters):
        snapshot_loc = self.__get_location_of_snapshot(stage)
        valid = (self.pipeline_description.can_reuse_cache(stage) and os.path.exists(snapshot_loc))
        if valid:
            calibration_logger.info("Verifying dataset integrity of cache for stage %s, read from file %s",
                                    stage, snapshot_loc)
            available_snapshot_keys = snapshot_keys(snapshot_loc, True)
            flow_keys = self.pipeline_description.get_expected_datasets_from_stage(stage)
            missing_datasets = bool(set(flow_keys).difference(set(available_snapshot_keys)))
            valid = valid and not missing_datasets
        if valid:
            calibration_logger.info("Verifying metadata integrity of cache for stage %s, read from file %s",
                                    stage, snapshot_loc)
            snapshot_metadata = get_snapshot_metadata(snapshot_loc)
            snapshot_params = snapshot_metadata.get("FlowParams",
                                                    BasicTaskParameters(pd.NaT, pd.NaT, pd.NaT, pd.NaT,
                                                                        TaskflowPipelineRunMode.Undefined))
            try:
                assert_deep_almost_equal(snapshot_metadata.get(_STEP_CONFIG_KEY, {}),
                                         self.pipeline_description.get_full_stage_configuration(stage))
                assert snapshot_params==flow_parameters
            except AssertionError:
                calibration_logger.info("Metadata for stage %s is different for flow; the cached results are not valid",
                                        stage)
                valid = False
        else:
            calibration_logger.info("Datasets for stage %s are different for flow; the cached results are not valid",
                                    stage)
        return valid

    def run(self, flow_parameters):
        self.pipeline_description.setup_flow_according_to_params(flow_parameters)
        self.__setup_snapshot_directory()
        self.__setup_config_before_running()
        #flow_results = {}
        all_step_results = []
        try:
            for stage in self.pipeline_description:
                if self.__verify_snapshot_integrity(stage, flow_parameters):
                    snapshot_loc = self.__get_location_of_snapshot(stage)
                    calibration_logger.info("Reading cache results for stage %s, read from file %s",
                                            stage, snapshot_loc)
                    results_to_read = list(self.pipeline_description.get_expected_datasets_from_stage(stage))
                    results_to_read = BaseETLStep.get_full_class_requires(True, True)
                    stage_results = read_taskflow_engine_snapshot(snapshot_loc, result_names_to_load=results_to_read)
                else:
                    calibration_logger.info("Running stage %s", stage)
                    stage_required_data = self.__read_snapshot_data_for_stage(stage)
                    stage_results = self.__run_pipeline_stage(stage, flow_parameters, stage_required_data,
                                                              self.snapshot_directory is not None)

                step_results = stage_results.pop(_BASE_FLOW_STEP_RESULT_LABEL)
                step_results = pd.DataFrame(step_results)
                # Remove all the elements that are set by the Retry policy, which are the fields that keep track of flow
                # progress
                #flow_results.update(stage_results)
                #for k in BaseETLStep.get_full_class_requires(True, True):
                #    flow_results.pop(k, None)
                pd.set_option('expand_frame_repr', False)
                pd.set_option("max_rows", len(step_results.index))
                pd.set_option("max_columns", len(step_results.columns))
                calibration_logger.info("\n{0}".format(step_results))
                failure = step_results[step_results["status_type"] == "Fail"]
                if len(failure):
                    calibration_logger.error(failure.iloc[0, :].T.to_dict())
                all_step_results.append(step_results)
        except NotImplementedError as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            calibration_logger.error("".join(traceback.format_exception(exc_type, exc_value,exc_traceback)))
            calibration_logger.error("There was an exception while running the pipeline: %s", e)
        finally:
            self.__restore_config()
        if all_step_results:
            all_step_results = pd.concat(all_step_results, ignore_index=True)
        else:
            all_step_results = None
        pd.set_option("max_rows", len(all_step_results.index))
        pd.set_option("max_columns", len(all_step_results.columns))
        calibration_logger.info("\n{0}".format(all_step_results))
        return all_step_results

if __name__=="__main__":
    import os
    inventory = CalibrationTaskRegistry.get_inventory()
    TaskflowPipelineDescriptionBuilder.verify_all_get_default_methods()

    os_name = os.name
    print("Running on *********")
    if os.name=="nt":
        print("WINDOWS PLATFORM ")
        base_file = "C:/DCM/dcm-intuition/processes/data_processing/calibrations/quantamental_ml/prod_quantamental_gan_aws_30_filter_windows.conf"
    else:
        print("LINUX PLATFORM ")
        base_file = "/opt/dcm-intuition/processes/data_processing/calibrations/quantamental_ml/prod_quantamental_gan_aws_30_filter.conf"

    target_dt = pd.Timestamp.now().normalize()
    start_dt = pd.Timestamp("2000-01-03")
    print(target_dt)

    if len(sys.argv)<2:
        mode = "h"
    else:
        mode = sys.argv[1]
    if sys.argv[1]=="c":
        print("** Performing LIVE Calibration for:")
        print("** {0} ".format(target_dt))        
        flow_parameters = BasicTaskParameters(target_dt, start_dt, target_dt, target_dt, TaskflowPipelineRunMode.Calibration)
    else:
        print("** Performing HISTORICAL Calibration from:")
        print("** {0} to".format(start_dt))
        print("** {0} ".format(target_dt))
        flow_parameters = BasicTaskParameters(target_dt, start_dt, target_dt, target_dt, TaskflowPipelineRunMode.Historical)


    #############################################################
    ## For unit testing debugging
    #target_dt = pd.Timestamp("2018-11-30")
    #start_dt = pd.Timestamp("2016-06-01")
    #flow_parameters = BasicTaskParameters(target_dt, start_dt, target_dt, UniverseDates.UnitTestVelocity,
                                          #TaskflowPipelineRunMode.Historical)
    #base_file = r"C:\DCM\dcm-intuition\processes\data_processing\calibrations\quantamental_ml\no_gan_testing.conf"


    #############################################################



    print("***********************************************")
    print("***********************************************")
    print(base_file)
    print(base_file)
    print(base_file)
    print("***********************************************")
    print("***********************************************")

    pipeline_desc = TaskflowPipelineDescription(base_file)
    pipeline_runner = TaskflowPipelineRunner(pipeline_desc)
    pipeline_runner.run(flow_parameters)
