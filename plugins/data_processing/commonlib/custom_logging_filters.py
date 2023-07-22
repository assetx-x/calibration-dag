from commonlib.intuition_objects_representation import ObjectRepresenter
from commonlib.intuition_base_objects import DCMException
from commonlib import actor_clock

from pickle import dumps as cPickle_dumps
import logging

class AddAsOfDateFilter(logging.Filter):
    """Filters and logs dates

    """
    def filter(self, record_data):
        """ Filters dates
        Args:
            record_data:
        Returns:
             A boolean indicating the time was set.
        """
        record_data.asofdt = actor_clock.get_clock().get_time()
        return True

class AddPickleAndPrettyPrintDataFilter(logging.Filter):
    def filter(self, record_data):
        if record_data.msg != "%s":
            raise DCMException("AddPickleDataFilter.filter - This filter is supposed to receive a single message, to "
                               "be serialized in pickle format")
        record_data.pickled_message = cPickle_dumps(record_data.args)
        record_data.pretty_message = ObjectRepresenter.pretty_dict_repr(record_data.args)
        return True

