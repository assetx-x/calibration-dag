#!/usr/bin/env python
# coding:utf-8
"""
  Author:  Chris Ingrassia --<c.ingrassia@datacapitalmanagement.com>
  Purpose: Data Requirement Composition
  Created: 12/7/2015
"""
NoneType = type(None)

import pandas as pd

__author__ = "Chris Ingrassia, Napoleon Hernandez, Ali Nazari, Jack Kim, Karina Uribe"
__copyright__ = "Copyright (C) 2015, The Data Capital Management Intuition project - All Rights Reserved"
__credits__ = ["Napoleon Hernandez", "Ali Nazari", "Jack Kim",
               "Karina Uribe", "Chris Ingrassia"]
__license__ = "Proprietary and confidential. Unauthorized copying of this file, via any medium is strictly prohibited"
__version__ = "1.0.0b"
__maintainer__ = "Chris Ingrassia"
__email__ = "it@datacapitalmanagement.com"
__status__ = "Development"

from commonlib.intuition_base_objects  import DCMException
from commonlib.intuition_messages  import MessageObject
from commonlib.subjects import *
from collections import Mapping
import re
import six

from enum import Enum
from copy import deepcopy as deep_copy_of_object
from functools import reduce

class DRDataTypes(Enum):
    Equities = 1
    Sentiment = 2
    Fundamentals = 3
    MacroEconomics = 4
    DailyEquities = 5
    DailyFutures = 7
    DailyIndices = 8
    Indices = 9
    Futures = 10
    Options = 11
    EquityAdjustmentFactors = 12
    USDFXRates = 13
    SecurityReference = 14
    Earnings = 15

class DRMandatoryFields(Enum):
    _data_type = 1
    subject = 2
    fields = 3
    _granularity = 4


AsOfField = 'as_of'
COBField = 'cob'
LookbackField = '_lookback'

DataQualityField = 'data_quality'


class DataRequirements(Mapping):
    """
        Represents the set of requirements that are required to fulfill a request for data from an
        arbitrary source.  Primarily each key/value pair is intended to represent a filter, although
        the exact interpretation is the responsibility of the DataProvider or other implementation using
        a DataRequirements instance.

        DataRequirements is a collections.Mapping, and can be used in much the same way
        >>> data_requirements = DataRequirements(subject='a subject', key1='a value', _data_type=DRDataTypes.Equities, fields=['key1'], _granularity='1T')
        >>> data_requirements
        {'fields': ['key1'], 'key1': 'a value', 'subject': 'a subject'}
        >>> data_requirements['key1']
        'a value'

        ###########
        Composition
        ###########

        DataRequirements are composable and are intended to be immutable (though this is not enforced in the
        implementation right now, this is subject to change in the future)
        >>> added = data_requirements + DataRequirements(subject='another subject', key2='another value', _data_type=DRDataTypes.Equities, fields=['key2'], _granularity='1T')
        >>> added == {'key2': 'another value', 'key1': 'a value', 'subject': ['a subject', 'another subject'], 'fields': ['key2', 'key1']}
        True

        Values will be combined when objects are added/composed together, such that values associated with keys common
        to both sides of the equation will first be wrapped in lists if they are strings or do not respond to __len__,
        then both values will be added via the + operator
        >>> data_requirements + DataRequirements(subject='subj', key1='more of key1', _data_type=DRDataTypes.Equities, fields=['key2'], _granularity='1T')
        {'fields': ['key2', 'key1'], 'key1': ['more of key1', 'a value'], 'subject': ['a subject', 'subj']}
        >>> DataRequirements(some_list=[1,2,3], subject='subj', _data_type=DRDataTypes.Equities, fields=['some_list'], _granularity='1T') + DataRequirements(some_list=[4,5,6], subject='subj', _data_type=DRDataTypes.Equities, fields=['some_list'], _granularity='1T')
        {'fields': ['some_list'], 'some_list': [1, 2, 3, 4, 5, 6], 'subject': ['subj']}

        Any collections.Mapping object can be added to a DataRequirements:
        >>> added = data_requirements + {'key2': 'another value'}
        >>> added == {'key2': 'another value', 'key1': 'a value', 'subject': 'a subject', 'fields': ['key1']}
        True

        The DataRequirements class itself does not enforce any restrictions on the types or composition of
        the mappings it contains, if such restrictions are required they are assumed to be the responsibility
        of a class making use of a DataRequirements object or a subclass.

    """
    #__slots__ = ["requirements"]
    PROTECTED_KEY_PATTERN = re.compile(r'_(?:((?P<protected_id>[^_]+)_)?)(?P<key>.*)$')
    MANDATORY_FIELDS = set([k.name for k in DRMandatoryFields])
    def __mandatory_fields(mandatory_fields, pattern):
        return [k for k in mandatory_fields if pattern.match(k)]
    MANDATORY_PROTECTED_FIELDS = set(__mandatory_fields(MANDATORY_FIELDS, PROTECTED_KEY_PATTERN))
    ENHANCED_MANDATORY_PROTECTED_FIELDS = MANDATORY_PROTECTED_FIELDS|set([LookbackField])
    COUNT = 0

    @staticmethod
    def is_timey_wimey(thing):
        return pd.Timestamp(thing)

    @staticmethod
    def is_timey_wimey_or_null(thing):
        return pd.NaT if pd.isnull(thing) else DataRequirements.is_timey_wimey(thing)

    @staticmethod
    def is_timey_wimey_or_range(thing, require_max=False):
        """
        Validates whether or not the passed thing is a single timestamp or a tuple
        containing a (min, max) range.

        :param thing: The thing to validate
        :param require_max: If True, require the upper bound of the tuple to be set, otherwise
            allow values that are pd.isnull
        """
        if isinstance(thing, tuple) and len(thing) == 2:
            min, max = (DataRequirements.is_timey_wimey(thing[0]),
            DataRequirements.is_timey_wimey(thing[1]) if require_max else DataRequirements.is_timey_wimey_or_null(
                thing[1]))
            if thing[1] != pd.NaT and thing[0] >= thing[1]:
                raise ValueError("Lower bound of time range ({}) is > upper bound ({}), "
                                 "is this what you intended?".format(thing[0],thing[1]))
            return (min,max)
        return DataRequirements.is_timey_wimey(thing)

    # Mapping of standard field names and validation/conversion method that will be executed and can throw an exception
    # or return a (potentially different) value if a field passes validation, or a new value if it is valid and should be replaced
    # with the new value
    StandardFieldValidations = {
        AsOfField: lambda x: DataRequirements.is_timey_wimey(x),
        # Tuples of (min,max) of Timestamps or single discrete timestamp, with max optionally being pd.isnull to indicate
        # no upper bound should be valid and return a normalized tuple of (min,max) where any isnull max values are
        # normalized to NaT
        COBField: lambda x: DataRequirements.is_timey_wimey_or_range(x, require_max=True)
    }

    def __init__(self, **requirements):
        if DataRequirements.MANDATORY_FIELDS.difference(set(requirements)):
            raise KeyError("All requirements must have the following fields always declared at initialization: {0}."
                           " Received {1}".format(DataRequirements.MANDATORY_FIELDS, requirements))

        if requirements[DRMandatoryFields._data_type.name] not in DRDataTypes:
            try:
                requirements[DRMandatoryFields._data_type.name] = DRDataTypes[
                    requirements[DRMandatoryFields._data_type.name]]
            except KeyError:
                raise TypeError("The Data Requirements field {0} must be an instance of DRDataTypes Enum or must be "
                                "able to cast into one".format(DRMandatoryFields._data_type.name))

        self.validate_standard_fields(requirements)

        self.requirements = requirements
        #controlling the ordering of fields that are lists, representing sets of elements to filter for
        self.requirements = {k:(sorted(self.requirements[k]) if isinstance(self.requirements[k], list)
                                else self.requirements[k]) for k in self.requirements}

    #def __reduce__(self):
    #    return (self.__class__, self.__getinitargs__(), self.__getstate__(),)

    #def __getstate__(self):
        #return (self.requirements, )

    #def __setstate__(self, state):
        #self.requirements = state["requirements"]

    #def __getinitargs__(self):
    #    return ()

    @staticmethod
    def validate_standard_fields(requirements):
        """
            Applies the map function found in StandardFieldValidations for each standard field whose key is also
            present in requirement, returning the (possibly changed/normalized) value if valid, or throwing
            an exception otherwise

            :param requirements: A dictionary containing field/value mappings in which to find standard fields and
                validate the associated values
            :returns: A dictionary containing mappings of any standard fields from requirements and a potentially normalized
                or otherwise updated value for them if the value provided in requirements was valid.  An exception will be
                thrown from the map function in StandardFieldValidations if any field value is invalid
        """
        return {k: DataRequirements.StandardFieldValidations[k](v) for k, v in requirements.items()
                if k in DataRequirements.StandardFieldValidations}

    @staticmethod
    def _value_is_scalar(v):
        return not (hasattr(v, '__len__') and (not isinstance(v, str)))

    def __add__(self, other):
        """ Combines this DataRequirements with another DataRequirements, or another object
            that isinstance of collections.Mapping

            Will return None if the other object being added is not something that looks like
            another Mapping-like object
        """
        valid = [isinstance(other, k) for k in (NoneType, DataRequirements, Mapping)]
        if not any(valid):
            raise ValueError(
                "Cannot add non-collections.Mapping object of {0} to DataRequirements".format(str(type(other))))
        if valid[0]:
            return self
        other_dict = other.requirements if valid[1] else other
        myValue = self.requirements.copy()

        other_keys = set(other_dict.keys())
        self_keys = set(myValue.keys())
        constant_keys_to_check = self.ENHANCED_MANDATORY_PROTECTED_FIELDS.intersection(other_keys).intersection(self_keys)
        non_constant_keys = other_keys.difference(constant_keys_to_check)
        non_constant_keys_to_compare = non_constant_keys.intersection(self_keys)
        non_constant_keys_to_add = non_constant_keys.difference(non_constant_keys_to_compare)

        invalid_constant_keys = [k for k in constant_keys_to_check if self[k]!=other_dict[k]]
        if invalid_constant_keys:
            k = invalid_constant_keys[0]
            raise TypeError("Addition of DataRequirements need equal values for the mandatory protected "
                            "field {0}. This is set to {1} on self, and {2} on other".format(k, self[k], other_dict[k]))
        for k in non_constant_keys_to_compare:
            v = other_dict[k]
            left = [myValue[k]] if self._value_is_scalar(myValue[k]) else myValue[k]
            right = [v] if self._value_is_scalar(v) else v
            if isinstance(left, tuple) and isinstance(right, tuple):
                myValue[k] = reduce(lambda a, b: (min(a[0], b[0]), max(a[1], b[1])), [left, right])
            else:
                myValue[k] = list(set(left + right))
        for k in non_constant_keys_to_add:
            myValue[k] = other_dict[k]
        DataRequirements.COUNT += 1
        if not (DataRequirements.COUNT % 500):
            print(pd.Timestamp.now(), self.COUNT)
        return DataRequirements(**myValue)

    @classmethod
    def get_max_lookback(cls, lb1, lb2):
        """
        Returns the largest lookback. Substract the lookback from today's date and return
        lookback corresponds to the date more in the past
        """
        if lb1 and lb2:
            today = pd.Timestamp.today(tz='UTC')
            return lb2 if (today - pd.tseries.frequencies.to_offset(lb1)) > (today - pd.tseries.frequencies.to_offset(lb2)) else lb1
        return (lb1 or lb2)

    @classmethod
    def add_list(cls, list_reqs):
        """ Combines a list of DataRequirements (or Mapping) into a new DataRequirements
        """
        valid = [[isinstance(m, k) for k in (NoneType, DataRequirements, Mapping)] for m in list_reqs]
        not_valid_objs = [list_reqs[n] for n,k in enumerate(valid) if not any(k)]
        if not_valid_objs:
            raise ValueError(
                "Cannot add non-collections. The following objects are not valid: {0}".format(not_valid_objs))
        dicts_to_merge = [(list_reqs[n].requirements if valid[n][1] else list_reqs[n]) for n in range(len(valid))]
        all_keys = set(reduce(lambda a,b:a.extend(b) or a, map(lambda x:x.keys(), dicts_to_merge), []))
        result = {}

        constant_keys_to_check = cls.MANDATORY_PROTECTED_FIELDS.intersection(all_keys)
        non_constant_keys = all_keys.difference(constant_keys_to_check)

        for k in constant_keys_to_check:
            values = [d[k] for d in dicts_to_merge if k in d]
            valid = all([v == values[0] for v in values])
            if not valid:
                raise TypeError("Addition of DataRequirements need equal values for protected fields; found different "
                                "values for field {0}".format(k))
            result[k] = values[0]

        max_lookback = None
        for k in non_constant_keys:
            values_as_list = [d[k] for d in dicts_to_merge if k in d]
            all_tuples = all([isinstance(v, tuple) for v in values_as_list])
            if all_tuples:
                values_to_compare = list(zip(*values_as_list))
                result[k] = (min(values_to_compare[0]), max(values_to_compare[1]))
            elif k == LookbackField:
                for d in dicts_to_merge:
                    max_lookback = cls.get_max_lookback(max_lookback, d[LookbackField])
                result[k] = max_lookback
            elif len(values_as_list)==1:
                result[k] = values_as_list[0]
            else:
                values_as_list = [([v] if cls._value_is_scalar(v) else v) for v in values_as_list]
                consolidated_list = reduce(lambda a,b:a.extend(b) or a, values_as_list, [])
                result[k] = list(set(consolidated_list))
        return DataRequirements(**result)

    def __radd__(self, other):
        return self.__add__(other)

    def __getitem__(self, key):
        return (self.requirements[key.name] if key in DRMandatoryFields else self.requirements[key])

    #def __getattr__(self, key):
        #if key in self.requirements:
            #return self.requirements[key]
        #else:
            #raise AttributeError(
                #"'{0}' does not exist in requirements".format(key))

    def __iter__(self):
        for k, v in self._public_requirements().items():
            yield k

    def __len__(self):
        return len(self._public_requirements())

    def __repr__(self):
        return repr(self._public_requirements())

    def _public_requirements(self):
        return {k: v for k, v in self.requirements.items() if not (k.startswith('_'))}

    def protected_keys(self, for_id=None):
        return [k for k, v in self.protected_items(for_id).items()]

    def protected_items(self, for_id=None):
        """
        Returns (as another DataRequirements instance) the key/value items in this DataRequirements
        which are protected and invisible to a normal iteration over the object, optionally including
        protected keys marked by for_id
        """
        items = {m.group('key'): v for k, v in self.requirements.items()
                 for m in [self.PROTECTED_KEY_PATTERN.match(k)]
                 if m and m.group('protected_id') in (for_id, None)}
        return items

    def apply(self, data_source=None):
        """ Applies these data requirements to a data source, returning a pandas DataFrame
        of the results.  What data_source is expected to be and how the requirements are applied
        is left up to the implementation.  This method throws a NotImplementedError until
        overridden in a child class
        """
        raise NotImplementedError(
            "apply has not been implemented in this base class")

    def set_requirements_topic_identifier(self, name):
        pass

    def as_raw_dict(self):
        return deep_copy_of_object(self.requirements)

    def copy(self, updates=None):
        if updates is None:
            updates = {}
        raw_dict = self.as_raw_dict()
        raw_dict.update(updates)
        return self.__class__(**raw_dict)

    def __deepcopy__(self, memo):
        return self.__class__(**self.as_raw_dict())


class DataRequirementsCollection(MessageObject):
    def __init__(self):
        MessageObject.__init__(self)
        self.__data_reqs = {}

    def append(self, a_requirement):
        if not isinstance(a_requirement, DataRequirements):
            try:
                req = DataRequirements(**a_requirement)
            except BaseException as e:
                raise TypeError("DataRequirementsCollection.append - The received object is not a DataRequirements "
                                "nor can it be casted into one. Casting exception was : {0}. Received object was {1}"
                                .format(e, a_requirement))
        req_type = a_requirement[DRMandatoryFields._data_type]
        if req_type not in self.__data_reqs:
            self.__data_reqs[req_type] = []
        self.__data_reqs[req_type].append(a_requirement)

    def get_requirements_types(self):
        return self.__data_reqs.keys()

    def get_requirements_for_type(self, key):
        if key not in self.__data_reqs:
            raise TypeError("key {} not present in collection [{}]".format(key, self.__data_reqs))
        return self.__data_reqs[key]

    def __contains__(self, key):
        return key in self.__data_reqs

    def __verify_requirements_type_list(self, req_types):
        if not hasattr(req_types, "__iter__") and req_types is not None:
            raise TypeError("DataRequirementsCollection.__verify_requirements_type_list - expected an iterable, "
                            "received {0}".format(req_types))
        if req_types is None:
            req_types = self.__data_reqs.keys()
        if not all([k in self.__data_reqs for k in req_types]):
            raise TypeError("DataRequirementsCollection.__verify_requirements_type_list - req_types {0} does not exists"
                            " on this collection".format(req_types))
        return req_types

    def extend(self, other_req_collection):
        if not isinstance(other_req_collection, DataRequirementsCollection):
            raise TypeError("DataRequirementsCollection.extend must receive another instance of "
                            "DataRequirementsCollection")
        for k in other_req_collection.get_requirements_types():
            if k not in self.__data_reqs:
                self.__data_reqs[k] = []
            self.__data_reqs[k].extend(other_req_collection.get_requirements_for_type(k))

    def __iadd__(self, other_req_collection):
        self.extend(other_req_collection)
        return self

    def __add__(self, other_req_collection):
        new_reqs = DataRequirementsCollection()
        new_reqs += self
        new_reqs += other_req_collection
        return new_reqs

    def update_all_requirements_with_dict(self, update_dict=None, req_types=None):
        if update_dict is None:
            update_dict = {}
        req_types = self.__verify_requirements_type_list(req_types)
        for k in req_types:
            new_reqs = []
            for r in self.__data_reqs[k]:
                new_reqs.append(r.copy(update_dict))
            del self.__data_reqs[k]
            self.__data_reqs[k] = new_reqs

    def reduce_requirements(self, req_types=None):
        req_types = self.__verify_requirements_type_list(req_types)
        for k in req_types:
            #self.__data_reqs[k] = [reduce(lambda a, b: a + b, self.__data_reqs[k])]
            self.__data_reqs[k] = [DataRequirements.add_list(self.__data_reqs[k])]

    def __len__(self):
        return len(self.__data_reqs)

    def delete_requirements(self, req_types=None):
        req_types = self.__verify_requirements_type_list(req_types)
        for k in req_types:
            del self.__data_reqs[k]

    def only_keep_requirements(self, req_types=None):
        req_types = self.__verify_requirements_type_list(req_types)
        for k in list(self.__data_reqs.keys()):
            if k not in req_types:
                del self.__data_reqs[k]

    def total_len(self):
        total = 0
        for k in self.__data_reqs:
            total += len(self.__data_reqs[k])
        return total

    # TODO: make this an interator in the future
    def get_all_requirements(self):
        all_reqs = []
        for k in self.__data_reqs:
            all_reqs.extend(self.__data_reqs[k])
        return all_reqs

    def __iter__(self):
        return iter(self.__data_reqs)

    def __getitem__(self, key):
        return self.get_requirements_for_type(key)

def _consolidate_all_tradeable_data_requirements(all_requirements):
    #(TODO): improve the logic below to create the request for ExecutionUnit
    all_subjects = []
    for req in all_requirements:
        for dr_type in req:
            subjects = req[dr_type][0]["subject"]
            if not hasattr(subjects, "__iter__") or isinstance(subjects, str):
                subjects = [subjects]
            all_subjects.extend(subjects)

    all_equity_subjects = list(set(filter(lambda x:isinstance(x, Ticker), all_subjects)))
    all_index_subjects = list(set(filter(lambda x:isinstance(x, Index), all_subjects)))
    all_option_subjects = list(set(filter(lambda x:isinstance(x, Option), all_subjects)))

    execution_data_reqs = DataRequirementsCollection()
    if all_equity_subjects:
        execution_data_reqs.append(DataRequirements(_data_type = DRDataTypes.Equities,
                                                    subject = all_equity_subjects,
                                                    #fields = ["count", "high", "price", "volume", "low",
                                                    #          "close", "open"],
                                                    fields = ["price", "volume"],
                                                    _granularity = "1T",
                                                    _lookback = None))
        # TODO: this is necessary because of the trading restrictions, to analyze volume of security. Also, to get
        # close price of previous day in case of missing spot price
        execution_data_reqs.append(DataRequirements(_data_type = DRDataTypes.DailyEquities,
                                                    subject = all_equity_subjects,
                                                    fields = ["volume", "close"],
                                                    _granularity = "1T",
                                                    _lookback = "30B"))

    if all_option_subjects:
        execution_data_reqs.append(DataRequirements(_data_type=DRDataTypes.Options,
                                                    subject=all_option_subjects,
                                                    fields=["bid_open", "bid_high", "bid_low", "bid_close",
                                                            "ask_open", "ask_high", "ask_low", "ask_close", "last_open",
                                                            "last_high", "last_low", "last_close", "volume", "price"],
                                                    _granularity="1T",
                                                    _lookback=None))
    if all_index_subjects:
        execution_data_reqs.append(DataRequirements(_data_type=DRDataTypes.Indices,
                                                    subject=all_index_subjects,
                                                    #fields = ["count", "high", "price", "volume", "low",
                                                    #          "close", "open"],
                                                    fields = ["price"],
                                                    _granularity="1T",
                                                    _lookback=None))

    if not(all_equity_subjects or all_option_subjects):
        raise DCMException("_consolidate_all_tradeable_data_requirements - the system is not "
                           "configured to trade any securities, the consolidated Tradeable Data Requirements have "
                           "no subjects. Please verify the configuration udner the key IntuitionSetup")
    return execution_data_reqs
