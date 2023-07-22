import pandas.tseries.offsets
from datetime import timedelta, datetime, time
import types
from numpy import ndarray, asarray, int64    #pylint: disable=E0611
from pandas import Timestamp
from datetime import datetime
from commonlib.util_functions import get_all_subclasses_of_class
import logging
from functools import cmp_to_key

def sort_cmp(a,b):
    try:
        return (a > b) - (a < b)
    except TypeError:
        s1, s2 = type(a).__name__, type(b).__name__
        return (s1 > s2) - (s1 < s2)

class ObjectRepresenter(type):
    REGISTERED_FIELDS = 1
    UNREGISTERED_FIELDS = 2
    explicitly_registered_fields = {}
    full_representation_of_class = {}
    classes_with_repr = []

    @classmethod
    def add_external_classes_with_repr(cls):
        cls.declare_class_with_repr(time)
        cls.declare_class_with_repr(datetime)
        cls.declare_class_with_repr(pandas.tseries.offsets.DateOffset, True)
        cls.declare_class_with_repr(timedelta)


    @staticmethod
    def repr_with_pretty_print(obj):
        return ObjectRepresenter.pretty_dict_repr(ObjectRepresenter.get_representation_of_object_instance(obj))

    @staticmethod
    def pretty_dict_repr(x, sort = True, indent = 0):
        if isinstance(x, dict):
            r = '{\n'
            for (key, value) in (sorted(x.items()) if sort else x.iteritems()):
                r += (' ' * (indent + 4)) + repr(key) + ': '
                r += ObjectRepresenter.pretty_dict_repr(value, sort, indent + 4) + ',\n'
            r = r.rstrip(',\n') + '\n'
            r += (' ' * indent) + '}'
        elif isinstance(x, str):
            r = repr(x)
        elif hasattr(x, '__iter__'):
            r = '[\n'
            for value in (sorted(x,key=cmp_to_key(sort_cmp)) if sort else x):
                r += (' ' * (indent + 4)) + ObjectRepresenter.pretty_dict_repr(value, sort, indent + 4) + ',\n'
            r = r.rstrip(',\n') + '\n'
            r += (' ' * indent) + ']'
        else:
            r = repr(x)
        return r

    @classmethod
    def register_attributes_for_representation(cls, registered_class, fields_to_include, fields_to_exclude):
        if not isinstance(registered_class,type):
            raise DCMException("ObjectRepresenter.register_attributes_for_representation - provided parameter "
                               "registered_class that is not a class type (ineherited from object); received {0}"
                               .format(registered_class))
        if not isinstance(fields_to_include, list):
            raise DCMException("ObjectRepresenter.register_attributes_for_representation - fields_to_include parameter "
                               "must be of type list; received {0}".format(fields_to_include))
        if not isinstance(fields_to_exclude, list):
            raise DCMException("ObjectRepresenter.register_attributes_for_representation - fields_to_exclude parameter "
                               "must be of type list; received {0}".format(fields_to_exclude))
        fields_to_include_check = [isinstance(x, str) for x in fields_to_include]
        fields_to_exclude_check = [isinstance(x, str) for x in fields_to_exclude]
        if not all(fields_to_include_check):
            raise DCMException("ObjectRepresenter.register_attributes_for_representation - all elements of "
                               "fields_to_include must be of string type. Received {0}".format(fields_to_include))
        if not all(fields_to_exclude_check):
            raise DCMException("ObjectRepresenter.register_attributes_for_representation - all elements of "
                               "fields_to_exclude must be of string type. Received {0}".format(fields_to_exclude))

        # register the attributes from list
        if registered_class not in cls.explicitly_registered_fields:
            cls.explicitly_registered_fields[registered_class] = {}
            cls.full_representation_of_class[registered_class] = {}
        cls.explicitly_registered_fields[registered_class][cls.REGISTERED_FIELDS] = fields_to_include
        cls.explicitly_registered_fields[registered_class][cls.UNREGISTERED_FIELDS] = fields_to_exclude
        # update full representation of this class
        cls.update_full_representation_of_class(registered_class)
        #update full representation of all registered descendants
        cls.update_full_representation_for_descendants_of_class(registered_class)
        logging.debug("{0:<40} :: ObjectRepresenter provided. Included fields are {1}".format(str(registered_class),
                                                                                              fields_to_include))

    @classmethod
    def update_full_representation_for_descendants_of_class(cls, registered_class):
        # look for all registered classes that inherit from this type, and update the full representation
        all_subclasses = get_all_subclasses_of_class(registered_class)
        subclasses_to_update = [sc for sc in all_subclasses if sc in cls.explicitly_registered_fields]
        for sc in subclasses_to_update:
            cls.update_full_representation_of_class(sc)

    @classmethod
    def update_full_representation_of_class(cls, class_type):
        class_fields_registration = cls.explicitly_registered_fields[class_type]
        full_representation_included_attributes = list(class_fields_registration[cls.REGISTERED_FIELDS])
        full_representation_excluded_attributes = list(class_fields_registration[cls.UNREGISTERED_FIELDS])
        # got through all ancestors
        ancestor_classes = set(class_type.__mro__)
        for ac in ancestor_classes:
            if ac is not class_type and ac in cls.explicitly_registered_fields:
                class_fields_registration = cls.explicitly_registered_fields[ac]
                full_representation_included_attributes.extend(class_fields_registration[cls.REGISTERED_FIELDS])
                full_representation_excluded_attributes.extend(class_fields_registration[cls.UNREGISTERED_FIELDS])
        cls.full_representation_of_class[class_type][cls.REGISTERED_FIELDS] = full_representation_included_attributes
        cls.full_representation_of_class[class_type][cls.UNREGISTERED_FIELDS] = full_representation_excluded_attributes

    @classmethod
    def __filter_object_attributes(cls, obj):
        object_vars = vars(obj)
        if type(obj) not in cls.full_representation_of_class:
            return object_vars
        class_represenation = cls.full_representation_of_class[type(obj)]
        full_representation_attributes = set(vars(obj).keys())
        attributes_to_display = full_representation_attributes.intersection(set(class_represenation[cls.REGISTERED_FIELDS]))
        representation = {k:object_vars[k] for k in attributes_to_display}
        return representation

    @classmethod
    def declare_class_with_repr(cls, class_with_repr, include_subclasses=False):
        cls.classes_with_repr.append(class_with_repr)
        if include_subclasses:
            subclasses = get_all_subclasses_of_class(class_with_repr)
            cls.classes_with_repr.extend(subclasses)
        message = "{0:<40} :: ObjectRepresenter will use existing __repr__ method for class"
        message = message + " and all its subclasses" if include_subclasses else message
        logging.debug(message.format(str(class_with_repr)))

    @classmethod
    def _get_representation_of_object_instance_basic(cls, obj):
        if type(obj) in cls.classes_with_repr:
            return repr(obj)
        if isinstance(obj, (list)):
            new_list = []
            for index in range(len(obj)):
                new_list.append(cls.get_representation_of_object_instance(obj[index]))
            return new_list
        elif isinstance(obj, (tuple)):
            new_list = []
            for index in range(len(obj)):
                new_list.append(cls.get_representation_of_object_instance(obj[index]))
            return tuple(new_list)
        elif isinstance(obj, (ndarray)):
            new_list = []
            for index in range(len(obj)):
                new_list.append(cls.get_representation_of_object_instance(obj[index]))
            return asarray(new_list)
        elif isinstance(obj, set):
            new_list = []
            for element in obj:
                new_list.append(cls.get_representation_of_object_instance(element))
            try:
                new_list = set(new_list)
            except TypeError as e:
                pass
            return new_list
        elif isinstance(obj, dict):
            new_dict = {}
            for key in obj:
                new_dict[key] = cls.get_representation_of_object_instance(obj[key])
            return new_dict
        elif isinstance(obj, (int, float, int64, complex, str, bytes, type(None))):
            return obj
        elif isinstance(obj, (Timestamp, datetime)):
            return str(obj)
        elif isinstance(obj, object) and hasattr(obj,"__dict__"):
            filtered_representation = cls.__filter_object_attributes(obj)
            return cls.get_representation_of_object_instance(filtered_representation)
        elif isinstance(obj, object) and not hasattr(obj,"__dict__"):
            return obj
        else:
            raise DCMException("ObjectRepresenter.get_representation_of_object_instance - do not know how to "
                               "represent object {0} of type {1}".format(obj, type(obj)))

    @classmethod
    def get_representation_of_object_instance(cls, obj):
        if hasattr(obj, "ObjectRepresenter_custom_method"):
            return obj.ObjectRepresenter_custom_method()
        return cls._get_representation_of_object_instance_basic(obj)

class ObjectRepresenterMetaclass(type):
    @staticmethod
    def set_complete_list_of_excluded_attributes_for_recovery(cls):
        ancestor_classes = set(cls.__mro__)
        excluded_attributes = []
        for ac in ancestor_classes:
            excluded_attributes.extend(getattr(ac, "EXCLUDED_ATTRS_FOR_STATE",[]))
        setattr(cls, "EXCLUDED_ATTRS_FOR_STATE", excluded_attributes)

    def __new__(mcl, name, bases, nmspc):
        if "INCLUDED_ATTRS_FOR_OBJECTREPRESENTER" in nmspc and "EXCLUDED_ATTRS_FOR_OBJECTREPRESENTER" in nmspc:
            included_fields = nmspc["INCLUDED_ATTRS_FOR_OBJECTREPRESENTER"]
            nmspc["__str__"] = ObjectRepresenter.repr_with_pretty_print
        return super(ObjectRepresenterMetaclass, mcl).__new__(mcl, name, bases, nmspc)

    def __init__(cls, name, bases, dct):
        if not ObjectRepresenter.classes_with_repr:
            ObjectRepresenter.add_external_classes_with_repr()
        if "__repr__" in dct:
            ObjectRepresenter.declare_class_with_repr(cls)
        elif "INCLUDED_ATTRS_FOR_OBJECTREPRESENTER" in dct and "EXCLUDED_ATTRS_FOR_OBJECTREPRESENTER" in dct:
            included_fields = dct["INCLUDED_ATTRS_FOR_OBJECTREPRESENTER"]
            excluded_fields = dct["EXCLUDED_ATTRS_FOR_OBJECTREPRESENTER"]
            ObjectRepresenter.register_attributes_for_representation(cls, included_fields, excluded_fields)

        ObjectRepresenterMetaclass.set_complete_list_of_excluded_attributes_for_recovery(cls)

        super(ObjectRepresenterMetaclass, cls).__init__(name, bases, dct)
