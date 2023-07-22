from pylru import lrudecorator
from inspect import getargspec
from pandas import Timestamp
from pandas.tseries.frequencies import to_offset
from commonlib.intuition_base_objects  import DCMException
from commonlib.intuition_objects_representation import ObjectRepresenterMetaclass
from commonlib.util_functions import get_third_friday_of_month
NoneType = type(None)

def _cmp(x, y):
    if((type(x)==NoneType) and (type(y)!=NoneType)):
        return(-1)
    elif((type(x)!=NoneType) and (type(y)==NoneType)):
        return(1)
    elif((type(x)==NoneType) and (type(y)==NoneType)):
        return(0)
    else:
        return(-(x<y) + (x>y))


class Entity(object, metaclass=ObjectRepresenterMetaclass):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = -1
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = ["subject"]

    def __init__(self, subject):
        if not isinstance(subject, str):
            raise RuntimeError()
        self.subject = subject
        self.hash_code = self._build_hash_value()

    def __reduce__(self):
        return (self.__class__, self.__getinitargs__(), self.__getstate__(),)

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        pass

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.subject))

    def simple_string_symbol(self):
        return self.subject

    def __str__(self):
        return self.__repr__()

    def __entity_comparison__(self, other):
        if not isinstance(other, (Entity, str)):
            raise RuntimeError("Entity object can only be compared with another Entity object")
        elif isinstance(other, str):
            return -1
        elif type(other) is not self.__class__:
            return _cmp(self.__class__.SUBJECT_CLASS_PRIORITY_FOR_SORT, other.__class__.SUBJECT_CLASS_PRIORITY_FOR_SORT)
        return self.__cmp_aux__(other)

    def __cmp_aux__(self, other):
        return _cmp(self.subject, other.subject)

    def __lt__(self, other):
        return self.__entity_comparison__(other)<0

    def __le__(self, other):
        return self.__entity_comparison__(other)<=0

    def __gt__(self, other):
        return self.__entity_comparison__(other)>0

    def __ge__(self, other):
        return self.__entity_comparison__(other)>=0

    def __eq__(self, other):
        return self.__entity_comparison__(other)==0

    def __ne__(self, other):
        return self.__entity_comparison__(other)!=0

    def _build_hash_value(self):
        return hash(self.subject)

    def __hash__(self):
        return self.hash_code

    def matches(self, other):
        return False

    def __reduce__(self):
        return (self.__class__, self.__getinitargs__(), {},)

    def __getinitargs__(self):
        raise DCMException("Entity.__getinitargs__ needs to be overriden in derived class {0}"
                           .format(self.__class__))

    @classmethod
    @lrudecorator(100)
    def __getinitargs_names__(cls):
        return getargspec(cls.__init__).args[1:]

class TradeableInstrument(Entity):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 1
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    def __init__(self):
        Entity.__init__(self, str(self))

    def _build_hash_value(self):
        return hash(self.__repr__())

class Ticker(TradeableInstrument):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 2
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    def __init__(self, symbol):
        self.symbol = str(symbol.replace('_', ' ').replace('.', ' '))
        TradeableInstrument.__init__(self)

    def simple_string_symbol(self):
        return self.symbol

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.symbol))

    def __cmp_aux__(self, other):
        return _cmp(self.symbol, other.symbol)

    def __getinitargs__(self):
        return (self.symbol, )


#TODO: need to remove at some point Index as a tradeable isntrument; is a shortcut for now with IB
class Index(TradeableInstrument):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 3
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    def __init__(self, name):
        self.index_name = str(name)
        TradeableInstrument.__init__(self)

    def simple_string_symbol(self):
        return self.index_name

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.index_name))

    def __cmp_aux__(self, other):
        return _cmp(self.index_name, other.index_name)

    def __getinitargs__(self):
        return (self.index_name, )

class Future(TradeableInstrument):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 4
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    def __init__(self, underlying, expiry=None):
        self.symbol = str(underlying)
        self.expiry = expiry
        TradeableInstrument.__init__(self)

    def simple_string_symbol(self):
        if not self.expiry:
            return self.symbol
        expiry = repr(self.expiry.strftime("%Y%m%d")) if isinstance(self.expiry, Timestamp) else repr(self.expiry)
        return "{1} {2}".format(self.symbol, expiry)

    def __repr__(self):
        expiry = repr(self.expiry.strftime("%Y%m%d")) if isinstance(self.expiry, Timestamp) else repr(self.expiry)
        return "{0}({1},{2})".format(self.__class__.__name__, repr(self.symbol), expiry)

    def __cmp_aux__(self, other):
        return _cmp(self.symbol, other.symbol) or _cmp(self.expiry, other.expiry)

    def __getinitargs__(self):
        return (self.symbol, self.expiry)


class Option(TradeableInstrument):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 5
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    STRIKE_MULTIPLE = 1000.0
    def __init__(self, underlying, expiry=None, option_right= None, strike= None):
        self.underlying = underlying
        try:
            self.expiry = Timestamp(expiry).normalize()
            third_friday_of_month = get_third_friday_of_month(self.expiry, 0)
            if self.expiry == third_friday_of_month:
                self.expiry -= to_offset("1B")
        except ValueError:
            try:
                to_offset(expiry)
                self.expiry = str(expiry)
            except ValueError:
                raise DCMException("The option expiry shpould be a string for a timestamp or for a tenor. {0} is "
                                   "received".format(expiry))
        if option_right and not isinstance(option_right, str):
            raise RuntimeError("option_right should be a string")
        if option_right  and not option_right.upper() in ["P","C"]:
            raise RuntimeError("option_right should be eith P, or C (for put/call options). The {0} is an unkonwn type".
                               format(option_right.upper()))
        self.option_right = str(option_right.upper()) if option_right else option_right
        self.strike = int(float(strike)*Option.STRIKE_MULTIPLE) if strike else strike
        TradeableInstrument.__init__(self)

    def matches(self, other):
        if not isinstance(other, self.__class__):
            return False
        comparisons = []
        for k in ["underlying", "expiry", "option_right", "strike"]:
            this_attr = getattr(self, k)
            other_attr = getattr(other,k)
            comparisons.append((this_attr or other_attr) == (other_attr or this_attr))
        return all(comparisons)

    def __repr__(self):
        expiry = repr(self.expiry.strftime("%Y%m%d")) if isinstance(self.expiry, Timestamp) else repr(self.expiry)
        strike = repr("{0:.3f}".format(self.strike / Option.STRIKE_MULTIPLE)) if self.strike else repr(self.strike)
        return "{0}({1},{2},{3},{4})".format(self.__class__.__name__, repr(self.underlying), expiry,
                                             repr(self.option_right), strike)

    def __cmp_aux__(self, other):
        return _cmp(self.underlying, other.underlying) or _cmp(self.option_right, other.option_right) or \
               _cmp(self.strike, other.strike) or _cmp(self.expiry, other.expiry)

    def __getinitargs__(self):
        #TODO (Test)
        return (self.underlying, str(self.expiry), self.option_right,
                self.strike/Option.STRIKE_MULTIPLE if self.strike else self.strike)

class Cash(TradeableInstrument):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 6
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    def __init__(self, symbol):
        self.symbol = str(symbol)
        TradeableInstrument.__init__(self)

    def simple_string_symbol(self):
        return self.symbol

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.symbol))

    def __cmp_aux__(self, other):
        return __cmp(self.symbol, other.symbol)

    def __getinitargs__(self):
        return (self.symbol, )


class Algorithm(Entity):
    SUBJECT_CLASS_PRIORITY_FOR_SORT = 7
    INCLUDED_ATTRS_FOR_OBJECTREPRESENTER = []

    def __init__(self, algo_name):
        self.algo_name = str(algo_name)
        Entity.__init__(self, str(algo_name))

    def __repr__(self):
        return "{0}({1})".format(self.__class__.__name__, repr(self.algo_name))

    def __cmp_aux__(self, other):
        return __cmp(self.algo_name, other.algo_name)

    def __getinitargs__(self):
        return (self.algo_name, )
