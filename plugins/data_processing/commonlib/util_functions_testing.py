import pandas as pd
from numpy.testing import assert_almost_equal, assert_equal
from numpy import ndarray, int64
from wrapt import decorator as wrapt_decorator
from collections import namedtuple
import mock

from commonlib.intuition_objects_representation import ObjectRepresenter

def pandas_assert_equal(expected_results, test_results, *args, **kwargs):
    if not isinstance(test_results, pd.core.base.PandasObject) \
       or not isinstance(expected_results, pd.core.base.PandasObject):
        raise AssertionError("pandas_assert_equal can only work with pandas objects; received a {0} for "
                             "expected_results and a {1} for test_results"
                             .format(type(expected_results), type(test_results)))
    if type(test_results) is not type(expected_results):
        raise AssertionError("pandas_assert_equal received two different types of obejcts to compare: {0} for "
                             "expected_results and a {1} for test_results"
                             .format(type(expected_results), type(test_results)))

    assert expected_results.index.equals(test_results.index)
    if isinstance(test_results, pd.DataFrame):
        assert expected_results.columns.equals(test_results.columns)
        expected_results_numeric = expected_results._get_numeric_data()

        test_results_numeric = test_results._get_numeric_data()
        expected_results_non_numeric = expected_results.drop(expected_results_numeric.columns, axis=1)
        test_results_numeric = test_results._get_numeric_data()
        test_results_non_numeric = test_results.drop(test_results_numeric.columns, axis=1)

        pd.util.testing.assert_frame_equal(expected_results_non_numeric, test_results_non_numeric, check_exact=True,
                                           check_dtype=False)
        pd.util.testing.assert_frame_equal(expected_results_numeric, test_results_numeric, check_less_precise=True,
                                           check_column_type=False)
    elif isinstance(test_results, pd.Series):
        if test_results._get_numeric_data().equals(test_results):
            pd.util.testing.assert_series_equal(expected_results, test_results, check_less_precise=True)
        else:
            pd.util.testing.assert_series_equal(expected_results, test_results, check_exact=True, check_dtype=False)
    else:
        raise AssertionError("An unknown type of pandas object is being compared: {0}".format(type(test_results)))

def assert_deep_almost_equal(expected, actual, *args, **kwargs):
    is_root = '__trace' not in kwargs
    trace = kwargs.pop('__trace', 'ROOT')
    try:
        if isinstance(expected, (int, float, int64, complex)):
            assert_almost_equal(actual, expected, *args, **kwargs)
        if isinstance(expected, pd.core.base.PandasObject):
            pandas_assert_equal(expected, actual, *args, **kwargs)
        elif isinstance(expected, (list, tuple, ndarray)):
            assert_equal(len(actual), len(expected))
            for index in range(len(expected)):
                assert_deep_almost_equal(expected[index], actual[index],
                                         __trace=repr(index), *args, **kwargs)
        elif isinstance(expected, dict):
            assert_equal(set(actual), set(expected))
            for key in expected:
                assert_deep_almost_equal(expected[key], actual[key],
                                         __trace=repr(key), *args, **kwargs)
        elif isinstance(expected, float):
            assert_almost_equal(actual, expected, decimal=5)
        else:
            assert_equal(actual, expected)
    except (AssertionError, TypeError) as exc:
        exc.__dict__.setdefault('traces', []).append(trace)
        if is_root:
            trace = ' -> '.join(reversed(exc.traces))
            error_message = "\n++++ TRACE ++++\n {0} \n \n ++++ ERROR ++++\n {1} \n \n ++++ ACTUAL ++++\n {2} \n \n"\
                            "++++ EXPECTED ++++\n {3} \n"\
                            .format(trace, exc, str(actual), str(expected))
            print(error_message)
            exc = AssertionError("\n++++ TRACE ++++\n {0} \n \n ++++ ERROR ++++\n {1} \n".format(trace, exc))
        raise exc

CallRecorderItem = namedtuple("CallRecorderItem", ["function", "args", "kwargs", "result"])

class CallRecorder(object):
    def __init__(self):
        self.recordings_list = []

    def wrap_with_call_recording(self):
        @wrapt_decorator
        def record_function_call(wrapped, instance, args, kwargs):
            real_func = wrapped
            result = wrapped(*args, **kwargs)
            func_name = "{0}.{1}.{2}".format(real_func.im_class.__module__, real_func.im_class.__name__, real_func.__func__.__name__)
            function_call_item = CallRecorderItem(func_name, args, kwargs, result)
            self.recordings_list.append(function_call_item)
            return result
        return record_function_call

    def spy(self, method_to_spy):
        getter, attr = mock.mock._get_target(method_to_spy)
        method = getattr(getter(), attr)
        wrapped_func = self.wrap_with_call_recording()(method)
        setattr(getter(), attr, wrapped_func)

    def get_recorded_function(self, as_dataframe=True):
        result = self.recordings_list
        if as_dataframe:
            result = pd.DataFrame(result)
        return result


