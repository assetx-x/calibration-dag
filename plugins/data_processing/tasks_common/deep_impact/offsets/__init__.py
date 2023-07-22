from importlib import import_module

from pandas.tseries.frequencies import to_offset
from pandas.tseries.offsets import prefix_mapping

from tasks_common.deep_impact.offsets.business_minute import BusinessMinute

CUSTOM_OFFSETS = [
    BusinessMinute
]


def get_offset_object(offset_specifier):
    for offset in CUSTOM_OFFSETS:
        prefix_mapping[offset._prefix] = offset

    return to_offset(offset_specifier)
