import six
from flask_restful import Resource
from flask.views import MethodViewType, http_method_funcs
from flask import request
import werkzeug.exceptions

from taskflow.types.failure import Failure
from wrapt import decorator as wrapt_decorator
from process_info import ProcessInfo

@wrapt_decorator
def wrap_resource_method_with_error_handling(wrapped, instance, args, kwargs):
    try:
        result = wrapped(*args, **kwargs)
        return result
    except BaseException as e:
        error_code = e.code if isinstance(e, werkzeug.exceptions.HTTPException) else 400
        error_info = Failure().to_dict()
        error_info["traceback"] = error_info.pop("traceback_str").split("\n")
        proc_info = {k:v for k,v in ProcessInfo.retrieve_all_process_info().items() if v}
        instance = getattr(wrapped, "__self__", None)
        state_provider = getattr(wrapped.__self__, "__resource_state__", None) if instance else None
        state = state_provider() if state_provider else {}
        error_message = error_info.pop("exception_str")
        http_request = {"method": request.method, "url":request.url, "body":request.json or {},
                        "headers": dict(request.headers.items())}
        error_json = {
            "error": error_message,
            "error_description": error_info,
            "machine_state": proc_info,
            "backend_state": state,
            "http_request": http_request
        }
        return error_json, error_code

class ResourceWithErrorReportingType(MethodViewType):
    def __new__(mcl, name, bases, nmspc):
        methods_to_wrap = [k for k in nmspc if k in http_method_funcs]
        for method in methods_to_wrap:
            nmspc[method] = wrap_resource_method_with_error_handling(nmspc[method])
        return super(ResourceWithErrorReportingType, mcl).__new__(mcl, name, bases, nmspc)

class ResourceWithErrorReporting(six.with_metaclass(ResourceWithErrorReportingType, Resource)):
    pass
