from collections import namedtuple, OrderedDict
from enum import Enum
from operator import itemgetter as _itemgetter

from pandas import Timestamp

_tuple = tuple
_property = property

class TaskflowPipelineRunMode(Enum):
    Undefined = -1
    Historical = 1
    Calibration = 2
    Test = 3

class UniverseDates(Enum):
    UnitTest = Timestamp("1789-07-14")
    UnitTestVelocity = Timestamp("1987-10-19")

class BasicTaskParameters(tuple):
    'BasicTaskParameters(run_dt, start_dt, end_dt, universe_dt, run_mode)'
    """
    Automatically generated with namedtuple(...verbose=True)
    """
    __slots__ = ()

    _fields = ('run_dt', 'start_dt', 'end_dt', 'universe_dt', 'run_mode')

    def __new__(_cls, run_dt, start_dt, end_dt, universe_dt, run_mode):
        if isinstance(universe_dt, UniverseDates):
            universe_dt = universe_dt.value
        'Create new instance of BasicTaskParameters(run_dt, start_dt, end_dt, universe_dt, run_mode)'
        return _tuple.__new__(_cls, (run_dt, start_dt, end_dt, universe_dt, run_mode))

    @classmethod
    def _make(cls, iterable, new=tuple.__new__, len=len):
        'Make a new BasicTaskParameters object from a sequence or iterable'
        result = new(cls, iterable)
        if len(result) != 5:
            raise TypeError('Expected 5 arguments, got %d' % len(result))
        return result

    def __repr__(self):
        'Return a nicely formatted representation string'
        return 'BasicTaskParameters(run_dt=%r, start_dt=%r, end_dt=%r, universe_dt=%r, run_mode=%r)' % self

    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values'
        return OrderedDict(zip(self._fields, self))

    def _replace(_self, **kwds):
        'Return a new BasicTaskParameters object replacing specified fields with new values'
        result = _self._make(map(kwds.pop, ('run_dt', 'start_dt', 'end_dt', 'universe_dt', 'run_mode'), _self))
        if kwds:
            raise ValueError('Got unexpected field names: %r' % kwds.keys())
        return result

    def __getnewargs__(self):
        'Return self as a plain tuple.  Used by copy and pickle.'
        return tuple(self)

    __dict__ = _property(_asdict)

    def __getstate__(self):
        'Exclude the OrderedDict from pickling'
        pass

    run_dt = _property(_itemgetter(0), doc='Alias for field number 0')

    start_dt = _property(_itemgetter(1), doc='Alias for field number 1')

    end_dt = _property(_itemgetter(2), doc='Alias for field number 2')

    universe_dt = _property(_itemgetter(3), doc='Alias for field number 3')

    run_mode = _property(_itemgetter(4), doc='Alias for field number 4')



def create_pipeline_parameters_for_calibration(target_dt, universe_dt=None):
    start_dt = target_dt - pd.tseries.frequencies.to_offset(get_config()["calibration_mode"]["start_dt_offset"])
    return BasicTaskParameters(pd.Timestamp.now(), start_dt, target_dt, universe_dt or target_dt)

if __name__=="__main__":
    print(BasicTaskParameters(Timestamp.now(), Timestamp.now(), Timestamp.now(), UniverseDates.UnitTest, TaskflowPipelineRunMode.Historical))