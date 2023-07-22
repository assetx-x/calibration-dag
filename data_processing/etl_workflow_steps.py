import sys
from copy import deepcopy
from taskflow.task import Task, FunctorTask
from taskflow.engines import load as taskflow_load_engine
from taskflow.retry import Retry,RETRY
import taskflow.types.failure as tff
from taskflow.flow import Flow
from taskflow.patterns import linear_flow
from taskflow.exceptions import TaskFlowException
from wrapt import function_wrapper
from pandas import Timestamp, read_csv
import os

from collections import namedtuple
from status_objects import StatusType

from process_info import ProcessInfo

EquitiesETLTaskParameters = namedtuple("EquitiesETLTaskParameters", ["ticker", "as_of_dt", "run_dt", "start_dt", "end_dt"])
EarningsCalendarTaskParams = namedtuple("EarningsCalendarTaskParams", ["as_of_dt", "run_dt", "backward_horizon",
                                                                       "forward_horizon", "start_dt", "end_dt"])

_BASE_FLOW_STEP_RESULT_LABEL = "step_results"
_BASE_FLOW_FAILURE_LABEL = "failure_information"
_BASE_FLOW_GLOBAL_PARAMETERS_LABEL = "flow_parameters"
_STATUS_TYPE_LABEL = "status_type"
_STATUS_STEP_NAME_LABEL = "step_name"

_TASKFLOW_RAISE_EXCEPTION_FLAG = "TASKFLOW_RAISE_EXCEPTION"

class StatusNew(dict):
    def __init__(self, step_name, **kwargs):
        self.update(kwargs)
        self[_STATUS_STEP_NAME_LABEL] = step_name

    def add_step_parameters(self, step_parameters):
        self.update(step_parameters._asdict())

    def __setitem__(self, key, value):
        if _STATUS_TYPE_LABEL in self:
            raise TaskFlowException("StatusNew.__setitem__ was called over an object that already has a set status"
                                    "type. Offending object content is {0}".format(self))
        dict.__setitem__(self, key, value)

    def set_status_type(self, status_type):
        self[_STATUS_TYPE_LABEL] = status_type.name

    def standard_set_step_status(self, status, step_parameters, **kwargs):
        self.update(**kwargs)
        self.add_step_parameters(step_parameters)
        self.set_status_type(status)


class BaseETLStepRetry(Retry):
    default_provides = set([_BASE_FLOW_FAILURE_LABEL, _BASE_FLOW_GLOBAL_PARAMETERS_LABEL, _BASE_FLOW_STEP_RESULT_LABEL])

    def __init__(self, name, task_parameters, max_retries = 3, **kwargs):
        self.max_retries = max_retries + 1
        self.task_parameters = task_parameters
        super(BaseETLStepRetry, self).__init__(name=name, **kwargs)

    def on_failure(self, history):
        return RETRY

    def execute(self, history):
        if len(history)>self.max_retries:
            raise RuntimeError("BaseETLStepRetry.execute - the maximum number of retries has been reached")
        if history.caused_by(TaskFlowException):
            history[-1][1].values()[0].reraise()
        if len(history):
            failure_information = {"step_name": list(history[0][1].keys())[0], "details": list(history[0][1].values())[0].to_dict()}
            return {_BASE_FLOW_FAILURE_LABEL: failure_information,
                    _BASE_FLOW_GLOBAL_PARAMETERS_LABEL: self.task_parameters,
                    _BASE_FLOW_STEP_RESULT_LABEL: []}
        else:
            return {_BASE_FLOW_FAILURE_LABEL: None, _BASE_FLOW_GLOBAL_PARAMETERS_LABEL: self.task_parameters,
                    _BASE_FLOW_STEP_RESULT_LABEL: []}

class BaseETLStep(FunctorTask):
    REQUIRES_FIELDS = [_BASE_FLOW_STEP_RESULT_LABEL, _BASE_FLOW_FAILURE_LABEL, _BASE_FLOW_GLOBAL_PARAMETERS_LABEL]

    STATUS_TO_DISCONTINUE_FLOW = StatusType.Fail

    @staticmethod
    def set_status_level_to_discontinue_flow(status):
        BaseETLStep.STATUS_TO_DISCONTINUE_FLOW = status

    @staticmethod
    def __determine_overall_flow_status(results):
        if not results:
            return StatusType.Success
        return StatusType(max(map(lambda x:StatusType[x[_STATUS_TYPE_LABEL]].value, filter(None, results))))

    @classmethod
    def get_full_class_requires(cls, include_parent_classes=True, include_base_etl_requires=True):
        requires = [k for k in getattr(cls, "REQUIRES_FIELDS", [])]
        if include_parent_classes:
            parent_classes = list(filter(lambda x:issubclass(x,BaseETLStep), cls.mro()))
            additional_requires = [klass.get_full_class_requires(False, True) for klass in parent_classes]
            for reqs in additional_requires:
                requires.extend(reqs)
        if not include_base_etl_requires:
            requires = set(requires).difference(set(getattr(BaseETLStep, "REQUIRES_FIELDS", [])))
        return sorted(list(set(requires)))

    @classmethod
    def get_full_class_provides(cls, include_parent_classes=True, include_base_etl_provides=True):
        provides = [k for k in getattr(cls, "PROVIDES_FIELDS", [])]
        if include_parent_classes:
            parent_classes = list(filter(lambda x:issubclass(x,BaseETLStep), cls.mro()))
            additional_provides = [klass.get_full_class_provides(False, True) for klass in parent_classes]
            for reqs in additional_provides:
                provides.extend(reqs)
        if not include_base_etl_provides:
            provides = set(provides).difference(set(getattr(BaseETLStep, "PROVIDES_FIELDS", [])))
        # for the subclasses to return dictionaries the provides type must be a set
        return set(provides)

    def __init__(self, disable_test=False, report_failure_as_warning=False, **kwargs):
        name = self.__class__.__name__ if "name" not in kwargs else kwargs.pop("name")
        self.step_status = StatusNew(name)
        self.start_step_time = None
        self.end_step_time = None
        self.process_info = None
        self.__existing_step_results = None
        self.task_params = None
        all_requires = self.get_full_class_requires()

        all_provides = self.get_full_class_provides()
        FunctorTask.__init__(self, self.run_task, name="{0}.task".format(name), requires=all_requires,
                             provides=all_provides, **kwargs)
        self.disable_test = disable_test
        self.report_failure_as_warning = report_failure_as_warning
        self.do_step_action = self.modify_step_behavior_based_on_flags(self.do_step_action)

    @function_wrapper
    def modify_step_behavior_based_on_flags(self, wrapped, instance, args, kwargs):
        if self.disable_test:
            result = StatusType.Not_Tested
        else:
            result = wrapped(*args, **kwargs)

            if result is StatusType.Fail and self.report_failure_as_warning:
                result = StatusType.Warn

        return result

    def _all_results_as_dict(self, **kwargs):
        self.__existing_step_results.append(self.step_status)
        base_result = {}
        additional_results = self._get_additional_step_results()
        base_result.update(additional_results)
        base_result = {k:base_result[k] for k in self.provides if k in base_result}
        result = {k:kwargs[k] for k in self.provides if k in kwargs}
        result.update(base_result)
        if set(result.keys()).symmetric_difference(set(self.provides)):
            raise TaskFlowException("The result set for task {0} of type {1} is incomplete. Provides has fields {2}"
                                    "but the result set has {3}. Verify".format(self.name, self.__class__.__name__,
                                                                                self.provides, result.keys()))
        return result

    def _set_step_status(self, status, step_parameters, **kwargs):
        self.process_info = ProcessInfo.retrieve_all_process_info()
        self.end_step_time = str(Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
        kwargs.update({"task_start": self.start_step_time, "task_end": self.end_step_time,
                       "process_info": self.process_info})
        self.step_status.standard_set_step_status(status, step_parameters, **kwargs)

    def set_task_params(self, **kwargs):
        self.task_params = kwargs.pop(_BASE_FLOW_GLOBAL_PARAMETERS_LABEL)

    def run_task(self, **kwargs):
        self.set_task_params(**kwargs)
        self.__existing_step_results = kwargs.get(_BASE_FLOW_STEP_RESULT_LABEL)
        print("Running now {0}({1})".format(self.name, self.task_params))
        sys.stdout.flush()
        if _STATUS_TYPE_LABEL in self.step_status:
            return self._all_results_as_dict()
        flow_failure = kwargs.pop(_BASE_FLOW_FAILURE_LABEL, None)
        flow_overall_status = self.__determine_overall_flow_status(self.__existing_step_results)
        do_not_test = flow_failure and flow_failure["step_name"] != self.name
        if flow_overall_status.value >= BaseETLStep.STATUS_TO_DISCONTINUE_FLOW.value or do_not_test:
            self.start_step_time = str(Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
            self._set_step_status(StatusType.Not_Tested,self.task_params)
            return self._all_results_as_dict()
        if flow_failure:
            failure_details = flow_failure["details"]
            self._set_step_status(StatusType.Fail, self.task_params, error=failure_details)
            return self._all_results_as_dict()
        self.start_step_time = str(Timestamp.now("US/Eastern").tz_convert("UTC").tz_localize(None))
        step_status = self.do_step_action(**kwargs)
        self._set_step_status(step_status, self.task_params)
        return self._all_results_as_dict()

    def do_step_action(self, **kwargs):
        raise NotImplementedError("BaseETLStep.do_step_action must be overriden in derived class")

    def _get_additional_step_results(self):
        return {}

    def record_exception_info(self):
        self.step_status["error"] = tff.Failure().to_dict()


class BaseETLStage(linear_flow.Flow):
    STATUS_TO_DISCONTINUE_FLOW = StatusType.Fail

    def wrap_results_call_for_stage(self, step, wrapper=None):
        wrapper = self.add_task_data if not wrapper else wrapper
        if isinstance(step, BaseETLStep):
            step._set_step_status = wrapper(step._set_step_status)
        elif isinstance(step, BaseETLStage):
            for k,d in step.iter_nodes():
                step.wrap_results_call_for_stage(k, wrapper)
        else:
            raise TaskFlowException("BaseETLStage.wrap_results_call_for_stage - found a step that is not of type"
                                    "BaseETLStep or BaseETLStage. Please verify")

    def __init__(self, name, stage_category, *etl_tasks):
        linear_flow.Flow.__init__(self, "{0}".format(name))
        self.stage_category = stage_category
        self.add(*etl_tasks)

    def add(self, *etl_tasks):
        for step in etl_tasks:
            self.wrap_results_call_for_stage(step)
        linear_flow.Flow.add(self, *etl_tasks)

    @function_wrapper
    def add_task_data(self, wrapped, instance, args, kwargs):
        kwargs[self.stage_category] = self.name
        result = wrapped(*args, **kwargs)
        return result


def compose_main_flow_and_engine_for_task(name, task_parameters, *steps, **kwargs):
    taskflow_debug_flag = bool(int(os.environ.get(_TASKFLOW_RAISE_EXCEPTION_FLAG, 0)))
    retry_policy = BaseETLStepRetry(name="{0}_retry".format(name), task_parameters=task_parameters) \
        if not taskflow_debug_flag else None
    main_flow = linear_flow.Flow(name, retry=retry_policy).add(*steps)
    store = kwargs.get("store", {})
    if taskflow_debug_flag:
        store.update({_BASE_FLOW_FAILURE_LABEL: None, _BASE_FLOW_GLOBAL_PARAMETERS_LABEL: task_parameters,
                      _BASE_FLOW_STEP_RESULT_LABEL: []})
    engine = taskflow_load_engine(main_flow, store=store)
    return engine, main_flow
