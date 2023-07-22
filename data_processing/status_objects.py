import collections
import json

"""
The following are the expected possible keys for status objects
Status objects are only meant for useage within QA tasks,
for now they are only used in conjunction with data that has an associated ticker
(at that later point I would abstract the idea of a ticker to be similar to the way referances are handled)

The goal of the attributes is to provide clarity as to
-the context of the validation being performed
-which validation is bieng performed
-a liniage to retrieve the data which it was bieng performed on
- liniage for all referances sources bieng used
- A human readable explination that backed the decision of outputted status

"""
#STATUS_CLASS_KEY =  "class_name"
#STATUS_AS_OF_DATE_KEY = "as_of_date"
#STATUS_END_DATE_KEY = "end_date"
#STATUS_START_DATE_KEY = "start_date"
#STATUS_RUN_DATE_KEY = "run_date"

STATUS_PROCESS_KEY = "system_process"
"""Currently the only process that uses Status objects is the equity whitelist process"""
STATUS_STAGE_KEY = "process_stage"
STATUS_QA_NAME_KEY = "qa"
STATUS_TEST_NAME_KEY = "test_name"
STATUS_TEST_USAGE_KEY = "test_used_for"
STATUS_MESSAGE_KEY = "message"
STATUS_TICKER_KEY = "ticker"
STATUS_DATA_SOURCE_KEY = "data_source"
STATUS_DATA_SOURCE_TYPE_KEY = "type_for_data_source"
STATUS_REF_SOURCE_PREFIX_KEY = "ref_source_"
STATUS_REF_SOURCE_TYPE_PREFIX_KEY = "type_for_{}".format(STATUS_REF_SOURCE_PREFIX_KEY)
#STATUS_REF_SOURCE_TYPE_PROSTFIX_KEY = "_type"
STAUS_STATUS_KEY = "status"
STATUS_PROGRAM_RUN_DATE_KEY = "run_date"

from enum import Enum

class StatusType(Enum):
    Not_Tested = 0
    Success = 1
    Warn = 2
    Fail = 3
    Not_Tested_Warn = 10

class StatusJsonEncoder(json.JSONEncoder):
    def default(self, o):
        # Here you can serialize your object depending of its type
        # or you can define a method in your class which serializes the object
        if isinstance(o, Status):
            return o.dic  # Or another method to serialize it
        else:
            return json.JSONEncoder.encode(self, o)

class Status(collections.Mapping):

    STATUS_LABEL = STAUS_STATUS_KEY
    PROGRAM_RUN_DATE_LABEL = STATUS_PROGRAM_RUN_DATE_KEY
    PROCESS_LABEL = STATUS_PROCESS_KEY
    STAGE_LABEL = STATUS_STAGE_KEY
    QA_NAME_LABEL = STATUS_QA_NAME_KEY
    TEST_NAME_LABEL = STATUS_TEST_NAME_KEY
    TEST_USAGE_LABEL = STATUS_TEST_USAGE_KEY
    MESSAGE_LABEL = STATUS_MESSAGE_KEY
    TICKER_LABEL = STATUS_TICKER_KEY
    DATA_SOURCE_LABEL = STATUS_DATA_SOURCE_KEY
    DATA_SOURCE_TYPE_LABEL = STATUS_DATA_SOURCE_TYPE_KEY
    REF_SOURCE_PREFIX = STATUS_REF_SOURCE_PREFIX_KEY
    REF_SOURCE_TYPE_PREFIX = STATUS_REF_SOURCE_TYPE_PREFIX_KEY
    preset_keywords = [STATUS_LABEL, PROGRAM_RUN_DATE_LABEL, PROCESS_LABEL, STAGE_LABEL, QA_NAME_LABEL, TEST_NAME_LABEL,\
                       MESSAGE_LABEL, TICKER_LABEL, DATA_SOURCE_LABEL, DATA_SOURCE_TYPE_LABEL]
    preset_prefixes = [REF_SOURCE_PREFIX, REF_SOURCE_TYPE_PREFIX]
    EXTRA_DETAILS_DICTONARY_LABEL = "extra_details_dict"
    json_encoder = StatusJsonEncoder

    def __init__(self, status_type, message=None, testname= None, **kwargs):
        self.status_type = status_type
        self.dic = {self.STATUS_LABEL:self.status_type, self.EXTRA_DETAILS_DICTONARY_LABEL:{}}
        self.number_of_referances = 0
        if message != None:
            self.add_message(message)
        if testname != None:
            self.add_testname(testname)
        # for python 3.5 #  self.dic = {**extra_info, **self.dic}
        temp_dic = kwargs.copy()
        temp_dic.update(self.dic)
        self.dic = temp_dic

    def set_status_type(self, status_type):
        self.status_type = status_type

    def get_status_type(self):
        return self.status_type

    def get_special_keywords():
        #TODO (ALI): This FUnction is throwing an error since a list does not have a copy method
        return preset_keywords.copy()

    def get_special_prefixes():
        return preset_prefixes.copy()

    def get_status_type(self):
        return self.status_type

    def __getitem__(self, key):
        if key in self.preset_keywords or key is self.EXTRA_DETAILS_DICTONARY_LABEL:
            return self.dic[key]
        elif any((key.startswith(self.REF_SOURCE_PREFIX), key.startswith(self.REF_SOURCE_TYPE_PREFIX))):
            return self.dic[key]
        else:
            return self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL][key]
        return self.dic[key]

    def __setitem__(self, key, value):
        if key == self.EXTRA_DETAILS_DICTONARY_LABEL:
            raise ValueError("Status objects cannot take an key that equals the key of their extra_details_dictionary")
        if key == self.MESSAGE_LABEL:
            raise ValueError("To set the value of message you must use ammend_message method")
        if key == self.STATUS_LABEL:
            raise ValueError("Status objects status properties should be set only in the init method")
        if key in self.preset_keywords:
            self.dic[key] = value
        elif any((key.startswith(self.REF_SOURCE_PREFIX), key.startswith(self.REF_SOURCE_TYPE_PREFIX))):
            self.dic[key] = value
        else:
            self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL][key] = value

    def  __iter__(self):
        for i in self.dic.__iter__():
            yield i

    def __len__(self,):
        return self.dic.__len__()

    def add_testname(self, testname):
        if not self.has_testname():
            self.dic[self.TEST_NAME_LABEL] = testname
        else:
            raise RuntimeError("Class Status {} already has a testname {} therefore cannot add testname {}"\
                               .format(self, self.dic[self.TEST_NAME_LABEL], testname))

    def add_message(self, message):
        if not self.has_message():
            self.dic[self.MESSAGE_LABEL] = message
        else:
            raise RuntimeError("Class Status {} already has a message {} therefore cannot add message {}"\
                   .format(self, self.dic[self.MESSAGE_LABEL], message))
    def has_message(self):
        return self.MESSAGE_LABEL in self.dic

    def has_testname(self):
        return self.TEST_NAME_LABEL in self.dic

    def override_testname(self, new_testname):
        self.dic[self.TEST_NAME_LABEL] = new_testname

    def append_message(self, message):
        self.dic[self.MESSAGE_LABEL] = self.dic.get(self.MESSAGE_LABEL, default="") + message

    def ammend_message(self, message):
        self.dic[self.MESSAGE_LABEL] = message

    def add_info(self, infoName, content):
        if infoName in self.dic:
            raise ValueError("Stauts object already contains value for {}".format(infoName))
        self.__setitem__(infoName, content)

    def add_data_source(self, data_source_unique_identifier, data_source_type):
        try:
            self.add_info(self.DATA_SOURCE_LABEL, data_source_unique_identifier)
            self.add_info(self.DATA_SOURCE_TYPE_LABEL, data_source_type)
        except Exception as e:
            raise e

    def add_referance(self, ref_identifier, ref_type):
        self.number_of_referances += 1
        self.add_info("{}{}".format(self.REF_SOURCE_PREFIX, self.number_of_referances), ref_identifier)
        self.add_info("{}{}".format(self.REF_SOURCE_TYPE_PREFIX, self.number_of_referances), ref_type)

    def add_referance_sources(self, ref_sources_identifiers_and_types=[], extra_info={}):
        for ref in ref_sources_identifiers_and_types:
            if len(ref) < 2:
                raise ValueError("""referances in ref_sources_identifiers_and_types argument must contain 2 elements,
                a unique identifier and a description of the source""")
            self.add_referance(ref[0], ref[1])
        for key, value in extra_info.iteritems():
            self.add_info(key, value)
        return self

    def items(self):
        r = []
        r = [f for f in self.iteritems()]
        return r

    def keys(self):
        r = []
        r = [f for f in self.iterkeys()]
        return r

    def values(self):
        r = []
        r = [f for f in self.itervalues()]
        return r

    #def keys(self):
        ##main_keys = [k for k in self.dic.keys() if k != self.EXTRA_DETAILS_DICTONARY_LABEL]
        #extra_info_dic = self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL]
        #extra_keys = [k for k in extra_info_dic.keys() if k != self.EXTRA_DETAILS_DICTONARY_LABEL]
        #return self.main_keys() + extra_keys

    def main_keys(self):
        main_keys = [k for k in self.dic.keys() if k != self.EXTRA_DETAILS_DICTONARY_LABEL]
        return main_keys

    def iterkeys(self):
        for i in self.dic.iterkeys():
            if i != self.EXTRA_DETAILS_DICTONARY_LABEL:
                yield i
        for i in self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL].iterkeys():
            yield i
    def iteritems(self):
        for k,v in self.dic.iteritems():
            if all((k != self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL], k != self.EXTRA_DETAILS_DICTONARY_LABEL,\
                      k !=  (self.EXTRA_DETAILS_DICTONARY_LABEL, self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL]))):
                yield (k,v)
        for i in self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL].iteritems():
            yield i
    def itervalues(self):
        for v in self.dic.itervalues():
            if all((v != self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL], v != self.EXTRA_DETAILS_DICTONARY_LABEL,\
                      v !=  (self.EXTRA_DETAILS_DICTONARY_LABEL, self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL]))):
                yield v
        for i in self.dic[self.EXTRA_DETAILS_DICTONARY_LABEL].itervalues():
            yield i
    def get_flat_safe_dic(self):
        r= {}
        r = {k: self._safe_interpret(v) for k, v in self.dic.items()}
        return r
    def get_safe_dic(self):
        r= {}
        r = {k: self._safe_interpret(v) for k, v in self.dic.items()}
        return r

    def _safe_interpret(self, i):
        if isinstance(i, dict):
            return str({k: self._safe_interpret(v) for k, v in i.items()})
        else:
            return str(i)





