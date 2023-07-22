import sys
from inspect import ismethod
from abc import ABCMeta, abstractmethod
import six
import os.path
from collections import OrderedDict
import traceback

import pandas as pd

from etl_workflow_steps import BaseETLStep, StatusType
from commonlib.util_functions import get_parameters_from_bound_method, find_le
from commonlib.market_timeline import marketTimeline
from calibrations.common.calibration_logging import calibration_logger
from calibrations.common.taskflow_params import BasicTaskParameters
from functools import reduce

class CalibrationTaskRegistry(type):
    registered_classes = {}
    skipped_classes = {}

    @classmethod
    def _config_is_correct_for_class(cls, klass, cfg):
        klass_params = get_parameters_from_bound_method(klass.__init__)
        full_params = set(klass_params[0]).union(set(klass_params[1]))
        return not bool(full_params.symmetric_difference(set(cfg.keys())))

    @classmethod
    def register_calibration_task_class(cls, klass):
        if klass.__name__ == "CalibrationTaskflowTask" and cls.__module__==klass.__module__:
            return
        if not issubclass(klass, CalibrationTaskflowTask):
            raise RuntimeError("Attempted to register a Calibration class that is not of type CalibrationTaskflowTask")
        can_be_registered = True
        klass_methods = [k for k in klass.__dict__.keys() if ismethod(getattr(klass, k))]
        abstract_methods = [k for k in klass_methods if getattr(getattr(klass, k), "__isabstractmethod__", False)]
        abstract_methods.extend(list(getattr(klass, "__abstractmethods__", [])))
        if abstract_methods:
            cls.skipped_classes[klass.__name__] = klass
            print("**** ERROR *** The class {0} has not been registered because it has abstract methods: {1}"
                  .format(klass, abstract_methods))
        if not abstract_methods:
            if klass.__name__ in cls.registered_classes and klass.__name__!="CalibrationTaskflowTask":
                raise RuntimeError("There exists two CalibrationTaskflowTask classess with the same name: {0}. "
                                   "One is located on module {1} and the other on module {2}"
                                   .format(klass.__name__, klass.__module__,
                                           cls.registered_classes[klass.__name__].__module__))
            default_config = klass.get_default_config()
            if not cls._config_is_correct_for_class(klass, default_config):
                klass_params = get_parameters_from_bound_method(klass.__init__)
                full_params = set(klass_params[0]).union(set(klass_params[1]))
                raise RuntimeError("The get_default_config method of class {0} does not match the signature of it "
                                   "__init__ method. Please verify. The __init__ has parameters {1} and default config"
                                   " has parameters {2}. Differences are {3}"
                                   .format(klass, sorted(list(full_params)), sorted(default_config.keys()),
                                           list(full_params.symmetric_difference(set(default_config.keys())))))
            try:
                klass(**default_config)
            except BaseException as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                exc_string = "".join(traceback.format_exception(exc_type, exc_value,exc_traceback))
                calibration_logger.info(exc_string)
                print(exc_string)
                raise RuntimeError("The class {0} cannot be instantiated with its default configuration. Exception is "
                                   "{1}".format(klass, e))
            cls.registered_classes[klass.__name__] = klass

    @classmethod
    def get_calibration_class(cls, calib_task_name):
        return cls.registered_classes.get(calib_task_name, None)

    @classmethod
    def get_provider_of(cls, named_result):
        possible_providers = [k for k in cls.registered_classes
                              if named_result in cls.registered_classes[k].get_full_class_provides(True, False)]
        return possible_providers

    @classmethod
    def get_recipient_of(cls, named_result):
        possible_recipients = [k for k in cls.registered_classes
                               if named_result in cls.registered_classes[k].get_full_class_requires(True, False)]
        return possible_recipients

    @classmethod
    def get_possible_dependent_tasks_of(cls, class_name):
        klass = cls.registered_classes[class_name]
        provides = klass.get_full_class_provides(True, False)
        possible_dependents = reduce(lambda x,y: x.extend(y) or x, map(lambda x:cls.get_recipient_of(x), provides),[])
        return possible_dependents

    @classmethod
    def get_possible_providers_of(cls, class_name):
        klass = cls.registered_classes[class_name]
        requires = klass.get_full_class_requires(True, False)
        possible_providers = reduce(lambda x,y: x.extend(y) or x, map(lambda x:cls.get_provider_of(x), requires),[])
        return possible_providers

    @classmethod
    def get_required_data_of(cls, class_name):
        klass = cls.registered_classes[class_name]
        requires = klass.get_full_class_requires(True, False)
        return requires

    @classmethod
    def get_produced_data_of(cls, class_name):
        klass = cls.registered_classes[class_name]
        provides = klass.get_full_class_provides(True, False)
        return provides

    @classmethod
    def get_description_of(cls, class_name):
        klass = cls.registered_classes[class_name]
        return (getattr(klass, "__doc__", []) or "").replace("\n"," ").strip()

    @classmethod
    def get_default_configuration_of(cls, class_name):
        klass = cls.registered_classes[class_name]
        return klass.get_default_config()

    @classmethod
    def get_inventory(cls):
        step_names = list(cls.registered_classes.keys())
        modules = [cls.registered_classes[k].__module__ for k in step_names]
        descriptions = [cls.get_description_of(k) for k in step_names]
        provides = [cls.get_produced_data_of(k) for k in step_names]
        requires = [cls.get_required_data_of(k) for k in step_names]
        sample_configs = [cls.get_default_configuration_of(k) for k in step_names]
        possible_providers = list(map(lambda x:list(set(reduce(lambda a,b:a.extend(b) or a,
                                                          [cls.get_provider_of(k) for k in x], []))), requires))
        possible_recipients = list(map(lambda x:list(set(reduce(lambda a,b:a.extend(b) or a,
                                                          [cls.get_recipient_of(k) for k in x], []))), requires))
        classes_data = {"step_name": step_names, "module": modules, "provides": provides, "requires": requires,
                        "sample_config": sample_configs, "possible_providers": possible_providers,
                        "possible_recipients": possible_recipients, "description": descriptions}
        inventory_information = pd.DataFrame(classes_data)
        inventory_information = inventory_information[["step_name", "module", "description", "provides", "requires",
                                                       "sample_config", "possible_providers", "possible_recipients"]]
        return inventory_information

class CalibrationTaskRegistryMetaclass(ABCMeta):
    def __init__(cls, name, bases, dct):
        CalibrationTaskRegistry.register_calibration_task_class(cls)
        super(CalibrationTaskRegistryMetaclass, cls).__init__(name, bases, dct)

class CalibrationTaskflowTask(six.with_metaclass(CalibrationTaskRegistryMetaclass,BaseETLStep)):
    #__metaclass__ = CalibrationTaskRegistryMetaclass
    def __init__(self, **kwargs):
        BaseETLStep.__init__(self, disable_test=False, report_failure_as_warning=False, **kwargs)

    @classmethod
    def get_default_config(cls):
        return {}

class CalibrationTaskflowTaskWithFileTargets(CalibrationTaskflowTask):
    @abstractmethod
    def get_file_targets(self):
        raise NotImplementedError("The method CalibrationTaskflowTaskWithTargets.get_target must be overriden in "
                                  "derived class")

class CalibrationFileTaskflowTask(CalibrationTaskflowTaskWithFileTargets):
    def __init__(self, calibration_file_path, calibration_file_suffix, ignore_calibration_timeline, **kwargs):
        self.calibration_file_path = calibration_file_path
        self.calibration_file_suffix = calibration_file_suffix
        self.target_file_scheme = pd.io.common.parse_url(calibration_file_path).scheme
        if self.target_file_scheme=="s3" and self.calibration_file_path.endswith("/"):
            self.calibration_file_path = self.calibration_file_path[0:-1]
        self.ignore_calibration_timeline = ignore_calibration_timeline
        CalibrationTaskflowTaskWithFileTargets.__init__(self, **kwargs)

    def get_file_targets(self, task_params=None):
        task_params = task_params or self.task_params
        if self.ignore_calibration_timeline:
            calib_dt = task_params.run_dt.tz_localize(None).normalize()
        else:
            calibration_timeline = self.get_calibration_dates(task_params)
            try:
                _, calib_dt = find_le(calibration_timeline, task_params.run_dt)
            except BaseException as e:
                raise RuntimeError("CalibrationFileTaskflowTask.get_target --> Could not form a proper name for the "
                                   "calibration file; problem is {0}".format(e))
        filename = "{0}_{1}".format(calib_dt.strftime("%Y%m%d"), self.calibration_file_suffix)
        if self.target_file_scheme == "s3":
            filename = "/".join([self.calibration_file_path, filename])
        else:
            filename = os.path.join(self.calibration_file_path, filename)
        return [filename]

    def full_market_timeline_of_task_params(self, task_params):
        return marketTimeline.get_trading_days(task_params.start_dt.tz_localize(None).normalize(),
                                               max(task_params.end_dt.tz_localize(None).normalize(),
                                                   task_params.run_dt.tz_localize(None).normalize()))

    def get_calibration_dates_with_offset_anchor(self, task_params, calibration_date_offset, calibration_anchor_date):
        trading_days = self.full_market_timeline_of_task_params(task_params)
        calibration_anchor_date = pd.Timestamp(calibration_anchor_date) if calibration_anchor_date \
            else task_params.start_dt.tz_localize(None).normalize()
        if (not calibration_date_offset) or (calibration_date_offset=="1B"):
            calibration_dates = trading_days
        elif calibration_date_offset=="BMS":
            calibration_dates = trading_days[pd.Series(trading_days.month).diff()!=0]
        else:
            timeline = pd.bdate_range(calibration_anchor_date.tz_localize(None).normalize(), periods=240,
                                      freq=calibration_date_offset)
            timeline = timeline[timeline<=pd.Timestamp("2024-12-31")]
            calibration_dates = pd.DatetimeIndex(map(marketTimeline.get_next_trading_day_if_holiday, timeline))
        return calibration_dates

    @abstractmethod
    def get_calibration_dates(self, task_params):
        raise NotImplementedError("The method CalibrationFileTaskflowTask.get_calibration_dates must be overriden in "
                                  "derived class")
