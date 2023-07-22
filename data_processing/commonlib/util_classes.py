from datetime import timedelta, datetime, time
from os import name as os_name
from io import BytesIO
from collections import Counter, defaultdict
from functools import reduce
from itertools import combinations
import inspect
import sys
import os
from copy import deepcopy
import _pickle as pickle
from glob import glob
import copyreg as copy_reg
import inspect
import gzip
import pyarrow

# TODO: commenting out because all the time this breaks in Windows. Need to update protobuf to a newer version
import google.protobuf.json_format as json_format
from google.protobuf.message import Message as proto_message
from pandas.tseries.offsets import apply_wraps, BusinessHour, BusinessDay, prefix_mapping
from pandas.core.dtypes.generic import ABCSeries, ABCDatetimeIndex
from boto3 import resource as aws_resource, client as aws_client, setup_default_session as boto3_setup_default_session
from botocore.compat import urlsplit
from botocore.exceptions import ClientError as botocore_ClientError, EndpointConnectionError
from enum import Enum
from google.cloud import storage
from google.cloud.exceptions import NotFound
from pandas import Timestamp
from numpy import NaN, sign as np_sign, int64
import pandas as pd

from commonlib.config import get_config
from commonlib.intuition_base_objects  import DCMIntuitionObject, DCMException
from commonlib.intuition_objects_representation import ObjectRepresenterMetaclass
from commonlib.intuition_loggers import log
from commonlib.actor_clock import get_clock
from commonlib.util_functions import create_directory_if_does_not_exists, load_module_by_name, get_all_subclasses_of_class

class OMSSystems(Enum):
    ZIPLINE = 0
    IB = 1
    FLEXTRADE = 2
    LOOPBACK = -1

class _GrpcAuxSerializerMeta(type):
    def __new__(mcl, name, bases, nmspc):
        klass = super(_GrpcAuxSerializerMeta, mcl).__new__(mcl, name, bases, nmspc)
        copy_reg.pickle(proto_message, klass.pickle_serializer, klass.deserialize_tuple)
        return klass

class GrpcAuxSerializer(object, metaclass=_GrpcAuxSerializerMeta):
    """Collection of method to supports the serialization of gRPC calls"""
    #TODO: maybe requires caching of objects as they are loaded

    @classmethod
    def __register_sub_classes_for_proto_message(cls):
        msg_classes = set(get_all_subclasses_of_class(proto_message))
        msg_classes = list(msg_classes.difference(list(copy_reg.dispatch_table.keys())))
        for msg_klass in msg_classes:
            copy_reg.pickle(msg_klass, GrpcAuxSerializer.pickle_serializer, GrpcAuxSerializer.deserialize_tuple)

    @classmethod
    def pickle_serializer(cls, message):
        data = cls.serialize_message_as_tuple(message)
        return GrpcAuxSerializer.deserialize_tuple, (data,)


    @classmethod
    def correct_module_locations_of_message_classes(cls, module_name):
        module = load_module_by_name(module_name)
        klasses =[val for key, val in module.__dict__.items() if inspect.isclass(val)
                  and issubclass(val, proto_message)]
        for klass in klasses:
            cls.__correct_module_location_of_class(klass)
        cls.__register_sub_classes_for_proto_message()

    @classmethod
    def __correct_module_location_of_class(cls, obj):
        klass = obj if inspect.isclass(obj) else obj.__class__
        mod_name = klass.__module__
        if mod_name not in sys.modules:
            full_mod_name = [k for k in sys.modules.keys() if k.endswith(mod_name)][0]
            setattr(klass, "__module__", full_mod_name)
            mod_name = full_mod_name
        return klass

    @classmethod
    def get_class_module_and_name(cls, obj):
        """Method to recover the module and name of a class. Protobuf messages require special handling for
        deserialization since the module name by default is missing the full path of the module"""
        klass = cls.__correct_module_location_of_class(obj)
        return (klass.__module__, klass.__name__)

    @classmethod
    def load_class(cls, module, name):
        """Auxiliary method to load a class"""
        return getattr(load_module_by_name(module), name)

    @classmethod
    def to_json(cls, message):
        """Method to convert a protobuf message into JSON format"""
        return json_format.MessageToJson(message, including_default_value_fields=True, preserving_proto_field_name=True,
                                         sort_keys=True)

    @classmethod
    def to_dict(cls, message):
        """Method to convert into a regular dictionary any protobuf message"""
        return json_format.MessageToDict(message, including_default_value_fields=True, preserving_proto_field_name=True)

    @classmethod
    def from_dict(cls, js_dict, msg_module, msg_name):
        new_instance = cls.load_class(msg_module, msg_name)()
        """Method to convert into a regular dictionary any protobuf message"""
        return json_format.ParseDict(js_dict, new_instance, ignore_unknown_fields=False)

    @classmethod
    def serialize_message_as_tuple(cls, message):
        """Serialize a protobuf message as module, class and binary serialization of data"""
        return cls.get_class_module_and_name(message) + (message.SerializeToString(),)

    @classmethod
    def deserialize_tuple(cls, serialized_message):
        """Method to deserialize a tuple, pulling the class and filling the data of the instance"""
        req_class = cls.load_class(serialized_message[0], serialized_message[1])
        return req_class.FromString(serialized_message[2])


class PyArrowTableCustomSerialization(object):
    prefix_len=16
    codec="lz4"

    @classmethod
    def compress_pa_table(cls, pa_table):
        serialized_table = pyarrow.serialize(pa_table).to_buffer()
        serialized_len = len(serialized_table)
        serialied_prefix = serialized_len.to_bytes(cls.prefix_len, "big")
        comp_table = pyarrow.compress(serialized_table, codec=cls.codec, asbytes=True)
        final_comp = serialied_prefix + comp_table
        return (cls.decompress_pa_table, (final_comp,))

    @classmethod
    def decompress_pa_table(cls, serialized_table):
        prefix = serialized_table[0:cls.prefix_len]
        serialized_table = serialized_table[cls.prefix_len:]
        serialized_length = int.from_bytes(prefix, "big")
        decomp_string = pyarrow.decompress(serialized_table, serialized_length, codec=cls.codec)
        table = pyarrow.deserialize(decomp_string)
        return table

copy_reg.pickle(pyarrow.Table, PyArrowTableCustomSerialization.compress_pa_table, PyArrowTableCustomSerialization.decompress_pa_table)


class BusinessMinute(BusinessHour):
    """
    DateOffset subclass representing possibly n busines minutes

    """
    _prefix = 'BT'

    @apply_wraps
    def apply(self, other):
        # calcurate here because offset is not immutable
        daytime = self._get_daytime_flag()
        businesshours = self._get_business_hours_by_sec()
        bhdelta = timedelta(seconds=businesshours)

        if isinstance(other, datetime):
            # used for detecting edge condition
            nanosecond = getattr(other, 'nanosecond', 0)
            # reset timezone and nanosecond
            # other may be a Timestamp, thus not use replace
            other = datetime(other.year, other.month, other.day,
                             other.hour, other.minute,
                             other.second, other.microsecond)
            n = self.n
            if n >= 0:
                if (other.time() == self.end or
                    not self._onOffset(other, businesshours)):
                    other = self._next_opening_time(other)
            else:
                if other.time() == self.start:
                    # adjustment to move to previous business day
                    other = other - timedelta(seconds=1)
                if not self._onOffset(other, businesshours):
                    other = self._next_opening_time(other)
                    other = other + bhdelta

            # this is the only change compared with BusinessHour class
            bd, r = divmod(abs(n), businesshours // 60)
            if n < 0:
                bd, r = -bd, -r

            if bd != 0:
                skip_bd = BusinessDay(n=bd)
                # midnight busienss hour may not on BusinessDay
                if not self.next_bday.onOffset(other):
                    remain = other - self._prev_opening_time(other)
                    other = self._next_opening_time(other + skip_bd) + remain
                else:
                    other = other + skip_bd

            hours, minutes = divmod(r, 60)
            result = other + timedelta(hours=hours, minutes=minutes)

            # because of previous adjustment, time will be larger than start
            if ((daytime and (result.time() < self.start or self.end < result.time())) or
                not daytime and (self.end < result.time() < self.start)):
                if n >= 0:
                    bday_edge = self._prev_opening_time(other)
                    bday_edge = bday_edge + bhdelta
                    # calcurate remainder
                    bday_remain = result - bday_edge
                    result = self._next_opening_time(other)
                    result += bday_remain
                else:
                    bday_edge = self._next_opening_time(other)
                    bday_remain = result - bday_edge
                    result = self._next_opening_time(result) + bhdelta
                    result += bday_remain
            # edge handling
            if n >= 0:
                if result.time() == self.end:
                    result = self._next_opening_time(result)
            else:
                if result.time() == self.start and nanosecond == 0:
                    # adjustment to move to previous business day
                    result = self._next_opening_time(result- timedelta(seconds=1)) +bhdelta

            return result
        else:
            raise TypeError('Only know how to combine business hour with ')

#adding BusinessMinute as a standalone offset inside pandas offsets library
prefix_mapping[BusinessMinute._prefix] = BusinessMinute


class TradingBusinessHour(BusinessHour, metaclass=ObjectRepresenterMetaclass):
    """ A class representing a time period expressed in business hours.

    Attributes
    ----------
        None
    """
    def __init__(self, tdelta, start=None, end=None):
        """
        Args
        ----
            tdelta : datetime.timedelta
               timedelta object used to construct the TradingBusinessHour offset (only hours and days are accepted)

            start : datetime.time
                an optional time to define the beginning of a trading day; if None then defaults to 9:30

            end : datetime.time
                an optional time to define the end of a trading day; if None then defaults to 16:00

        Returns: None

        """
        start = get_config()["TradingBusinessHour"]["default_workday_start_time"] if start is None else start
        end = get_config()["TradingBusinessHour"]["default_workday_end_time"] if end is None else end
        ndays = tdelta.days
        nhours = int(round(tdelta.seconds / 3600.0, 0))
        hours_per_trading_day = ((datetime.combine(datetime.min, end) -
                                  datetime.combine(datetime.min, start)).seconds)/3600.0
        total_hours = ndays * hours_per_trading_day + nhours
        BusinessHour.__init__(self, n=int(total_hours), start=start, end=end)

    def __getstate__(self):
        """ Serializes an instance of TradingBusinessHour class through the start and end of the trading business day
            and the number of hours

            Args
            ----
                None
            Returns:
               A list with the following structure
                n: integer
                    total business hours in the instance
                start.hour : datetime.time
                    The hour on which a trading day begins
                start.minute : datetime.time
                    The minute of an hour that a trading day begins
                end.hour : datetime.time
                    The hour on which a Trading day ends
                end.minute : datetime.time
                    The minute of an hour that a Trading dey ends
            """
        return [self.n, self.start.hour, self.start.minute, self.end.hour, self.end.minute]

    def __setstate__(self, state):
        """ Receives and interprets a serialized version of a TradingBusinessHour instance and reconstructs it

            Args
            ----
               state : A list with the following structure
               n: integer
                    total business hours in the instance
                start.hour : datetime.time
                    The hour on which a trading day begins
                start.minute : datetime.time
                    The minute of an hour that a trading day begins
                end.hour : datetime.time
                    The hour on which a Trading day ends
                end.minute : datetime.time
                    The minute of an hour that a Trading dey ends

            Returns:
                None

            """
        BusinessHour.__init__(self, n=state[0], start=time(state[1], state[2]), end=time(state[3], state[4]))

    def __repr__(self):
        return "{0} TradingBusinessHour(start={1}, end={2})".format(self.n, str(self.start), str(self.end))


class TradingBusinessMinute(BusinessMinute, metaclass=ObjectRepresenterMetaclass):
    """ A class representing a time period expressed in business minutes.

    Attributes
    ----------
        None
    """
    def __init__(self, tdelta, start=None, end=None):
        """
        Args
        ----
            tdelta : datetime.timedelta
               timedelta object used to construct the TradingBusinessHour offset (only hours and days are accepted)

            start : datetime.time
                an optional time to define the beginning of a trading day; if None then defaults to 9:30

            end : datetime.time
                an optional time to define the end of a trading day; if None then defaults to 16:00

        Returns: None

        """
        start = get_config()["TradingBusinessHour"]["default_workday_start_time"] if start is None else start
        end = get_config()["TradingBusinessHour"]["default_workday_end_time"] if end is None else end
        ndays = tdelta.days
        nminutes = int(round(tdelta.seconds / 60.0, 0))
        minutes_per_trading_day = ((datetime.combine(datetime.min, end) -
                                  datetime.combine(datetime.min, start)).seconds)/60.0
        total_minutes = ndays * minutes_per_trading_day + nminutes
        BusinessMinute.__init__(self, n=total_minutes, start=start, end=end)

    #need to override because this one calls the init method again
    def __rsub__(self, other):
        if isinstance(other, (ABCDatetimeIndex, ABCSeries)):
            return other - self
        return BusinessMinute(-self.n, start=self.start, end=self.end) + other

    def __getstate__(self):
        """ Serializes an instance of TradingBusinessHour class through the start and end of the trading business day
            and the number of hours

            Args
            ----
                None
            Returns:
               A list with the following structure
                n: integer
                    total business hours in the instance
                start.hour : datetime.time
                    The hour on which a trading day begins
                start.minute : datetime.time
                    The minute of an hour that a trading day begins
                end.hour : datetime.time
                    The hour on which a Trading day ends
                end.minute : datetime.time
                    The minute of an hour that a Trading dey ends
            """
        return [self.n, self.start.hour, self.start.minute, self.end.hour, self.end.minute]

    def __setstate__(self, state):
        """ Receives and interprets a serialized version of a TradingBusinessHour instance and reconstructs it

            Args
            ----
               state : A list with the following structure
               n: integer
                    total business hours in the instance
                start.hour : datetime.time
                    The hour on which a trading day begins
                start.minute : datetime.time
                    The minute of an hour that a trading day begins
                end.hour : datetime.time
                    The hour on which a Trading day ends
                end.minute : datetime.time
                    The minute of an hour that a Trading dey ends

            Returns:
                None

            """
        BusinessMinute.__init__(self, n=state[0], start=time(state[1], state[2]), end=time(state[3], state[4]))

    def __repr__(self):
        return "{0} TradingBusinessMinute(start={1}, end={2})".format(self.n, str(self.start), str(self.end))


class InstructionsFileURILocator(DCMIntuitionObject):
    @classmethod
    def __parse_s3_bucket_name(cls, parsed_instructions_path):
        bucket_id = (parsed_instructions_path.netloc + parsed_instructions_path.path).split('@')
        key_id = None
        access_id = None
        access_secret = None
        if len(bucket_id) == 1:
            # URI without credentials: s3://bucket/object
            log.debug("InstructionsFileURILocator.__parse_s3_bucket_name does not have access to the credentials, boto3 "
                      "is looking for the credentials on a default location")
            bucket_id, key_id = bucket_id[0].split('/', 1)
            # "None" credentials are interpreted as "look for credentials in other locations" by boto
        elif len(bucket_id) == 2 and len(bucket_id[0].split(':')) == 2:
            # URI in full format: s3://key:secret@bucket/object
            acc, bucket_id = bucket_id
            access_id, access_secret = acc.split(':')
            bucket_id, key_id = bucket_id.split('/', 1)
            log.debug("InstructionsFileURILocator.__parse_s3_bucket_name is using the credentials receieved in the url")
        else:
            # more than 1 '@' means invalid uri
            # Bucket names must be at least 3 and no more than 63 characters long.
            # Bucket names must be a series of one or more labels.
            # Adjacent labels are separated by a single period (.).
            # Bucket names can contain lowercase letters, numbers, and hyphens.
            # Each label must start and end with a lowercase letter or a number.
            raise DCMException("GlobalManagementUnit.__get_object_to_read_configuration_from - invalid S3 URI: "
                               "{0}".format(parsed_instructions_path))
        if access_id and access_secret:
            boto3_setup_default_session(aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        return (access_id, access_secret, bucket_id, key_id)

    @classmethod
    def __verify_s3_object_exists(cls, bucket_id, key_id):
        storage_client = storage.Client()
        try:
            bucket = storage_client.get_bucket(bucket_id)
        except NotFound as exc:
            raise DCMException("GlobalManagementUnit.__verify_s3_object_exists - the bucket {0} is not available on gcs."
                               "Please verify. Original Exception is {1}".format(bucket_id, exc))

        blob = bucket.get_blob(key_id)
        if blob is None:
            raise DCMException("GlobalManagementUnit.__verify_s3_object_exists - the key {0} cannot be accesed within "
                               "bucket {1} in S3. Please verify."
                               .format(key_id, bucket_id))

    @classmethod
    def get_path_or_stream_for_file(cls, instructions_path):
        fully_qualified_uri = 'file://' + instructions_path if os_name == "nt" and "://" not in instructions_path\
            else instructions_path
        parsed_instructions_path = urlsplit(fully_qualified_uri)
        scheme = parsed_instructions_path.scheme if parsed_instructions_path.scheme else "file"
        if scheme == "s3":
            access_id, access_secret, bucket_id, key_id = cls.__parse_s3_bucket_name(parsed_instructions_path)
            cls.__verify_s3_object_exists(bucket_id, key_id)
            s3 = aws_resource('s3')
            return BytesIO(s3.Object(bucket_id, key_id).get()["Body"].read())
        elif scheme == 'gs':
            _, _, bucket_id, key_id = cls.__parse_s3_bucket_name(parsed_instructions_path)
            storage_client = storage.Client()
            return BytesIO(storage_client.bucket(bucket_id).blob(key_id).download_as_string())
        elif scheme == "file":
            return instructions_path




class ExitTradingReasons(Enum):
    NoExit = 1
    Signal = 2
    StopLoss = 3
    StopGain = 4
    TimeLimit = 5

class StopGLMethodology(Enum):
    Trailing = 1
    FromOrigination = 2

class ExitTradingEvaluator(DCMIntuitionObject):
    EXCLUDED_ATTRS_FOR_STATE = ["compute_return"]

    def __available_methodologies(self):
        return {StopGLMethodology.FromOrigination: self._from_origination_return,
                StopGLMethodology.Trailing: self._trailing_return}

    def __set_return_calculation(self, stop_gl_methodology):
        self.stop_gl_methodology = stop_gl_methodology or StopGLMethodology.FromOrigination
        return_calc = self.__available_methodologies()
        self.compute_return = return_calc[self.stop_gl_methodology]

    def __init__(self, stop_gl_methodology=None, stop_loss_pct=None, stop_gain_pct=None, max_exit_date=None):
        self.stop_loss_pct = stop_loss_pct
        self.stop_gain_pct = stop_gain_pct
        #self.exit_date = Timestamp(max_exit_date or "2099-12-31").asm8.astype(long)
        self.exit_date = Timestamp(max_exit_date or "2099-12-31").asm8.astype(int64)
        self.initial_value = None
        self.running_max = None
        self.running_min = None
        self.last_return = None
        self.initial_value_date = None
        self.running_max_date = None
        self.running_min_date = None
        self.last_value = None
        self.last_date = None
        self.stop_gl_methodology = None
        self.__set_return_calculation(stop_gl_methodology)

    def __getinitargs__(self):
        return (getattr(self, "stop_gl_methodology", None), self.stop_loss_pct, self.stop_gain_pct,
                self.exit_date.astype("M8[ns]"))

    def set_exit_date(self, exit_dt):
        self.exit_date = Timestamp(exit_dt).asm8.astype(int64)

    def get_last_return(self):
        return self.last_return

    def reset(self, initial_value, stop_gl_methodology=None, stop_loss_pct=None, stop_gain_pct=None, max_exit_date=None):
        self.stop_loss_pct = stop_loss_pct or self.stop_loss_pct
        self.stop_gain_pct = stop_gain_pct or self.stop_gain_pct
        self.exit_date = self.exit_date if self.exit_date \
            else Timestamp(max_exit_date or "2099-12-31").asm8.astype(long)
        if stop_gl_methodology:
            self.__set_return_calculation(stop_gl_methodology)

        self.initial_value = initial_value
        self.running_max = initial_value
        self.running_min = initial_value
        current_time = get_clock().get_time()
        self.initial_value_date = current_time
        self.running_max_date = current_time
        self.running_min_date = current_time

    def get_return_speed(self, methodology_override=None):
        methodology = methodology_override or self.stop_gl_methodology
        values_to_use = {StopGLMethodology.FromOrigination: self.initial_value_date,
                        StopGLMethodology.Trailing: self.running_max_date}
        ret_value = self.get_return(methodology_override)
        time_delta = (self.last_date or pd.NaT) - (values_to_use[methodology] or pd.NaT)
        time_delta = time_delta / pd.Timedelta("1D")
        speed = ret_value / time_delta if time_delta>=1.0 else 0.0
        return speed

    def get_return(self, methodology_override=None):
        methodology = methodology_override or self.stop_gl_methodology
        return_calc = self.__available_methodologies()[methodology]
        ret_value = return_calc(self.last_value)
        return ret_value

    def _from_origination_return(self, value):
        return (value or NaN) / (self.initial_value or NaN) - 1.0

    def _trailing_return(self, value):
        return (value or NaN) / (self.running_max or NaN) - 1.0

    def __call__(self, value):
        if self.initial_value is None:
            return None
        current_time = get_clock().get_time()
        max_condition = value>self.running_max
        min_condition = value<self.running_min
        self.running_max = (max_condition and value) or self.running_max
        self.running_min = (min_condition and value) or self.running_min
        self.running_max_date = (max_condition and current_time) or self.running_max_date
        self.running_min_date = (min_condition and current_time) or self.running_min_date
        self.last_return = self.compute_return(value)
        self.last_value = value
        self.last_date = current_time
        self.stop_loss_pct = self.stop_loss_pct or pd.np.nan
        self.stop_gain_pct = self.stop_gain_pct or pd.np.nan
        return (get_clock().get_unix_timestamp()>=self.exit_date and ExitTradingReasons.TimeLimit)\
               or (self.last_return<self.stop_loss_pct and ExitTradingReasons.StopLoss) \
               or (self.last_return>self.stop_gain_pct and ExitTradingReasons.StopGain) \
               or None

class WeightRedistributor(object):
    def __init__(self, signed_target_weights=None, absolute_target_weight=None, special_names_weights=None,
                 adjust_negative_weights_for_leverage=True, max_abs_weight_per_name = 1.0, calculate_pro_rata=False,
                 short_leverage=2.0):
        if signed_target_weights and absolute_target_weight:
            raise ValueError("Only one of signed_target_weights or absolute_target_weight can be provided at a time")
        if signed_target_weights and (not isinstance(signed_target_weights, dict) or len(signed_target_weights)!=2 or
                                      set(signed_target_weights.keys()).symmetric_difference(set([-1,1]))):
            raise ValueError("The parameter signed_target_weights must be a dict of length 2, containing the target"
                             "weight for positive or negative sides in the form {-1: X, 1:Y}")
        self.signed_target_weights = signed_target_weights
        self.absolute_target_weight = absolute_target_weight
        self.special_names_weights = special_names_weights or {}
        self.adjust_negative_weights_for_leverage = adjust_negative_weights_for_leverage
        self.max_abs_weight_per_name = max_abs_weight_per_name
        self.calculate_pro_rata = calculate_pro_rata
        self.short_leverage = short_leverage

    def calculate_new_weights_based_on_abs_target(self, subsets):
        special_names_weight_original = sum(map(abs, self.special_names_weights.values()))
        special_names_weight = min([special_names_weight_original, self.absolute_target_weight])
        adjustment_factor = special_names_weight / (special_names_weight_original or 1.0)
        total_number_of_names = sum(map(len, [set(subsets[k]).difference(set(self.special_names_weights.keys()))
                                              for k in subsets]))
        weight_per_name = (self.absolute_target_weight - special_names_weight) / (total_number_of_names or 1.0)
        allocation = reduce(lambda x,y:x.update(y) or x, [{k:l * weight_per_name for k in subsets[l]} for l in [-1,1]])
        allocation.update({k:self.special_names_weights[k] * adjustment_factor for k in self.special_names_weights})
        if not pd.np.allclose(sum(map(abs, allocation.values())), self.absolute_target_weight):
            raise ValueError("The total weights at the end do not up add to the target weight")

        return allocation

    def calculate_new_weights_based_on_signed_target_weight(self, subsets):
        original_special_weights_amounts = {l:sum(self.special_names_weights[k] for k in self.special_names_weights
                                                  if pd.np.sign(self.special_names_weights[k])==l) for l in [-1, 1]}
        special_weights_amounts = {k:min([original_special_weights_amounts[k], self.signed_target_weights[k]])
                                   if k==1 else max([original_special_weights_amounts[k], self.signed_target_weights[k]])
                                   for k in self.signed_target_weights}
        adjustment_factors = {k:(special_weights_amounts[k] / (original_special_weights_amounts[k] or 1.0) or 1.0) for k in
                              original_special_weights_amounts}
        num_names_per_side = {k:len(set(subsets[k]).difference(set(self.special_names_weights.keys())))
                              for k in subsets}
        weight_per_side = {k:float(self.signed_target_weights[k]-special_weights_amounts[k]) / (num_names_per_side[k] or 1.0) for k in special_weights_amounts}
        allocations = {k:weight_per_side[l] for l in subsets for k in subsets[l] }
        final_special_weights = {k:self.special_names_weights[k] *
                                 adjustment_factors[pd.np.sign(self.special_names_weights[k])] for k in
                                 self.special_names_weights}
        allocations.update(final_special_weights)
        return allocations

    def calculate_pro_rata_based_on_original_weights_signed_target_weights(self, subsets):
        original_special_weights_amounts = {l:sum(self.special_names_weights[k] for k in self.special_names_weights
                                                      if pd.np.sign(self.special_names_weights[k])==l) for l in [-1, 1]}
        special_weights_amounts = {k:min([original_special_weights_amounts[k], self.signed_target_weights[k]])
                                       if k==1 else max([original_special_weights_amounts[k], self.signed_target_weights[k]])
                                       for k in self.signed_target_weights}
        remaining_weights = {k:min([self.signed_target_weights[k]-special_weights_amounts[k],0.0])
                             if k==-1 else max([self.signed_target_weights[k]-special_weights_amounts[k],0.0])
                             for k in original_special_weights_amounts}
        adj_factors = {k:(remaining_weights[k]/sum(subsets[k].values())
                          if sum(subsets[k].values()) else 1.0) for k in original_special_weights_amounts}
        adj_subsets = {l:{k:subsets[l][k]*adj_factors[l] for k in subsets[l]} for l in subsets}
        consolidated_adj_weights = reduce(lambda x,y:x.update(y) or x, adj_subsets.values(), {})
        consolidated_adj_weights.update(self.special_names_weights)
        return consolidated_adj_weights

    def calculate_pro_rata_based_on_original_weights_abs_target(self, subsets):
        #TODO: this is code for completeness, but has not been tested at all. If used needs to be verified
        special_names_weight_original = sum(map(abs, self.special_names_weights.values()))
        special_names_weight = min([special_names_weight_original, self.absolute_target_weight])
        remaining_weight = max([self.absolute_target_weight - special_names_weight, 0.0])

        consolidated_weights = reduce(lambda x,y:x.update(y) or x, subsets, {})
        remaining_names = {k:consolidated_weights for k in consolidated_weights if k not in
                           self.special_names_weights.keys()}
        total_weight_remaining_names = sum(remaining_names.values())

        adj_factor = remaining_weight / total_weight_remaining_names if total_weight_remaining_names else 1.0
        updated_subsets = {k:remaining_names[k]*adj_factor for k in remaining_names}
        updated_subsets.update(self.special_names_weights)
        return updated_subsets

    def __call__(self, individual_assets):
        if not isinstance(individual_assets, (list, tuple)):
            individual_assets = [individual_assets]
        all_assets = reduce(lambda x,y:x+y, [list(x.keys()) for x in individual_assets],[])
        signed_subsets = [{k:list(filter(lambda x:np_sign(assets[x])==k, assets.keys())) for k in [-1,1]}
                          for assets in individual_assets]
        final_subsets = dict(reduce(lambda x,y:x+y, list(map(Counter, signed_subsets))))
        final_subsets = {k:list(set(final_subsets[k])) for k in final_subsets}
        conflicting_names = [set(x_y[0]).intersection(set(x_y[1])) \
                             for x_y in [k for k in combinations(list(final_subsets.values()),2)]]
        all_conflicting_names = reduce(lambda x,y:x.union(y), conflicting_names, set())
        final_subsets = {k:list(set(final_subsets[k]).difference(all_conflicting_names)) for k in final_subsets}

        if self.calculate_pro_rata:
            original_allocations_of_final_subsets = {n:{k:sum([asset_group.get(k,0.0)
                                                               for asset_group in individual_assets])
                                                            for k in final_subsets[n]} for n in final_subsets}
            func = self.calculate_pro_rata_based_on_original_weights_abs_target if self.absolute_target_weight else\
                self.calculate_pro_rata_based_on_original_weights_signed_target_weights
            result = func(original_allocations_of_final_subsets)
        else:
            result = self.calculate_new_weights_based_on_abs_target(final_subsets) if self.absolute_target_weight \
                else self.calculate_new_weights_based_on_signed_target_weight(final_subsets)
        if self.adjust_negative_weights_for_leverage:
            result = {k:(result[k] / (self.short_leverage if np_sign(result[k])<0 else 1.0)) \
                      for k in result if abs(result[k])>1E-5}
        result = {k:(result[k] if k in self.special_names_weights else
                     min(abs(result[k]), self.max_abs_weight_per_name)*int(np_sign(result[k]))) for k in result}
        if self.special_names_weights:
            all_assets.extend(self.special_names_weights.keys())
        result = {k:(result[k] if k in result else 0.0) for k in all_assets}
        return result

class ResultsDictIOAsCSV(object):
    @staticmethod
    def dump_pandas_object(pandas_obj, path, to_csv_args):
        if isinstance(pandas_obj, pd.Series):
            pandas_obj = pandas_obj.to_frame()
        dtypes = pandas_obj.dtypes.to_dict()
        dtypes["__index__"] = pandas_obj.index.dtype
        if isinstance(pandas_obj.index, pd.MultiIndex):
            for k in pandas_obj.index.names:
                dtypes[k] = pandas_obj.index.get_level_values(k).dtype
        for k in dtypes:
            dtypes[k] = repr(dtypes[k]).split("(")[1].split(")")[0][1:-1]
        object_cols = [k for k in dtypes if dtypes[k]=="O"]
        if object_cols:
            specific_types = {k:pandas_obj[k].dropna().map(type).unique().tolist()
                              for k in object_cols if k!="__index__" and k in pandas_obj}
            if dtypes["__index__"]=="O":
                if not isinstance(pandas_obj.index, pd.MultiIndex):
                    specific_types["__index__"] = pandas_obj.index.dropna().map(type).unique().tolist()
                else:
                    specific_types["__index__"] = list(pandas_obj.index.names)
            for k in object_cols:
                if len(specific_types[k])==1:
                    specific_type_str = "{0}.{1}".format(specific_types[k][0].__module__,
                                                         specific_types[k][0].__name__).replace("__builtin__.", "")
                    dtypes[k] = "{0}^{1}".format(dtypes[k], specific_type_str)
        with open(path, "w") as f:
            f.write(str(dtypes).replace("'",'"')+"\n")
            args = {"path_or_buf": f}
            if to_csv_args:
                args.update(to_csv_args)
            pandas_obj.to_csv(**args)

    @staticmethod
    def read_csv_as_pandas_df(path, read_csv_kwargs):
        data_types_as_dict = eval(pd.read_csv(path, nrows=1, sep="\n", header=None).iloc[0,0])
        data_types_as_dict = {k:pd.np.dtype(data_types_as_dict[k]) for k in data_types_as_dict}
        index_dtype = data_types_as_dict.pop("__index__", None)
        datetime_cols = [k for k in data_types_as_dict if data_types_as_dict[k].name.find("time")>=0]
        data_types_as_dict = {k:data_types_as_dict[k] for k in data_types_as_dict if k not in datetime_cols}
        kwargs = dict(header=1, dtype=data_types_as_dict)
        if datetime_cols:
            kwargs["parse_dates"] = datetime_cols
        kwargs.update(read_csv_kwargs)
        data = pd.read_csv(path, **kwargs)
        data.index = data.index.astype(index_dtype)
        return data

    @staticmethod
    def create_csv_directory_tree_from_dict(input_dict, base_dir, max_recursion_depth=1, store_non_pandas_objects=False,
                                            to_csv_args={"quotechar": "'"}, dict_path=None):
        dict_path = dict_path or []
        effective_dir = os.path.join(base_dir, *dict_path)
        #if (type(input_dict)!=dict) or (max_recursion_depth==0):
        #    return
        for key, val in input_dict.iteritems():
            saved = False
            if (type(val)!=dict) or (max_recursion_depth==0):
                if not os.path.exists(effective_dir):
                    create_directory_if_does_not_exists(effective_dir)
                if isinstance(val, pd.core.base.PandasObject):
                    #try:
                        #filename = os.path.join(effective_dir, key+".msg")
                        #val.to_msgpack(filename, compress="zlib")
                    #except BaseException as e:
                    filename = os.path.join(effective_dir, key+".h5")
                    with pd.HDFStore(filename, "w", 9, "bzip2") as f:
                        f .put("__data__", val)

                    #with gzip.GzipFile(filename, "wb") as f:
                    #    pickle.dump(val, f, protocol=2)
                    saved = True
                elif store_non_pandas_objects:
                    filename = os.path.join(effective_dir, key+".gz")
                    with gzip.GzipFile(filename, "wb") as f:
                        pickle.dump(val, f, protocol=2)
                    saved = True
            else:
                depth = max_recursion_depth-1
                dict_path.append(key)
                ResultsDictIOAsCSV.create_csv_directory_tree_from_dict(val, base_dir, depth, store_non_pandas_objects,
                                                                       to_csv_args, dict_path)
            if saved:
                print("{0} ===> {1}".format(".".join(dict_path + [key]), os.path.relpath(filename, base_dir)))

    @staticmethod
    def read_directory_tree_to_dict(base_dir, quotechar="'", index_col=[0], filter_dict_keys=None):
        results = {}
        all_files = []
        for root, dirs, files in os.walk(base_dir):
            for basename in files:
                if filter_dict_keys is None or os.path.splitext(basename)[0] in filter_dict_keys:
                    all_files.append(os.path.join(root, basename))

        for f in all_files:
            dict_path = os.path.relpath(f, base_dir).split(os.path.sep)[0:-1]
            file_name = os.path.basename(f)
            extension = os.path.splitext(f)[-1]
            dict_key = file_name.replace(extension,"")
            loaded = False
            if extension == '.csv':
                res = ResultsDictIOAsCSV.read_csv_as_pandas_df(f, dict(quotechar=quotechar, index_col=index_col))
                loaded = True
            elif extension == ".pickle":
                with open(f, "r") as fd:
                    res = pickle.load(fd)
                loaded = True
            elif extension == ".gz":
                with gzip.GzipFile(f, "rb") as fd:
                    res = pickle.load(fd)
                loaded = True
            elif extension == ".h5":
                with pd.HDFStore(f, "r") as fd:
                    res = fd["__data__"]
                loaded = True
            elif extension == ".msg":
                res = pd.read_msgpack(f)
                loaded = True
            if loaded:
                subdict = results
                for p in dict_path:
                    subdict = results.setdefault(p,{})
                subdict[dict_key] = res
                print("{0} <=== {1}".format(".".join(dict_path + [dict_key]), os.path.relpath(f, base_dir)))
            else:
                print("Skipping not recognized file type {0}".format(f))
        return results

class ReturnTypeForPermutation(Enum):
    LogReturn=1
    AbsReturn=2
    Level=3
    #TODO: this is a horrible hack
    LogOU_VIX = 4
    LogOU_VXST = 5
    LogOU_VX1 = 6
    LogOU_VX2 = 7

if __name__=="__main__":
    pass
    #data_to_upload = ResultsDictIOAsCSV.read_csv_as_pandas_df(r"C:\Users\Napoleon\Downloads\historical_transactions\with_last_runname_with_types.csv",{"index_col":0})

    ##aggregation = data_to_upload.groupby(["requester_id","executor_order_no", "created_dt_order"])["execution_transaction_capital"].agg(pd.np.sum)
    ##agg2 = data_to_upload.set_index(["requester_id","executor_order_no", "created_dt_order"]).loc[aggregation.index,["execution_order_capital"]]
    ##pd.merge(aggregation.to_frame(), agg2, left_index=True, right_index=True).reset_index().to_clipboard()
    #aggregation = data_to_upload.groupby(["requester_id","executor_order_no", "created_dt_order"])["target_transaction_capital"].agg(pd.np.sum)
    #target_order_capital = aggregation.loc[data_to_upload.set_index(["requester_id","executor_order_no", "created_dt_order"]).index]
    #data_to_upload["target_order_capital"] = target_order_capital.values
    #data_to_upload["last_running_cost_dt"] = pd.DatetimeIndex(data_to_upload["last_running_cost_dt"].replace({"nan":pd.np.NaN, "None":pd.np.NaN}))
    #data_to_upload["leg_id"] = data_to_upload["leg_id"].fillna(-1).astype(int)


    #from sqlalchemy import create_engine
    #engine = create_engine("redshift+psycopg2://dcm_portfolio_manager:9ZDTPn!s6g*mu@marketdata-test.cq0v4ljf3cuw.us-east-1.redshift.amazonaws.com:5439/dev")
    #data_to_upload.to_sql("trading_system_transactions", engine, if_exists="append", index=False)

    #TEST_DIR = "C:/DCM/dcm-intuition/processes/data_processing/pipelines/market_view_calibration/test/inputs/input_pipeline_results/"
    #aa = ResultsDictIOAsCSV.read_directory_tree_to_dict(TEST_DIR)
    #print("done")
