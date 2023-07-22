
from commonlib.intuition_base_objects  import DCMException, DCMIntuitionObject
from commonlib.intuition_messages  import MessageObject
from commonlib.util_classes import TradingBusinessHour
from copy import deepcopy as deep_copy_of_object
from commonlib.intuition_loggers import *
import pandas as pd
from re import match as re_match
from datetime import timedelta, datetime, time
from commonlib.subjects import *
from functools import reduce

class ActorConfigurationInstructions(MessageObject):
    """ Represents the set of instructions that may be sent to each individual
    actor to configure it.

    Attributes:
        role: Type of role for the actor
        actor_id: Identification number of the actor
        instructions: A json object with specific instructions for the actor
    """
    EXCLUDED_ATTRS_FOR_STATE = ["role","actor_id","instructions"]

    def __init__(self, role, actor_id, instructions):
        self.role = role
        self.actor_id = actor_id
        self.instructions = instructions

    def __getinitargs__(self):
        return (self.role, self.actor_id, self.instructions)

class ActorConfigurationInstructionsContainer(MessageObject):

    EXCLUDED_ATTRS_FOR_STATE = ["actor_group_id", "ignore_these_instructions_if_empty"]

    def __init__(self, actor_group_id, ignore_these_instructions_if_empty=False):
        self.actor_group_id = actor_group_id
        self.ignore_these_instructions_if_empty = ignore_these_instructions_if_empty
        self.instructions_set = []

    def __getinitargs__(self):
        return (self.actor_group_id, self.ignore_these_instructions_if_empty)

    def __len__(self):
        return len(self.instructions_set)

    def ignore_instructions(self):
        return not len(self) and self.ignore_these_instructions_if_empty

    def add_instructions(self, instructions):
        if not isinstance(instructions, ActorConfigurationInstructions):
            raise DCMException("ActorConfigurationInstructionsContainer.add_instructions - trying to add an object "
                               "that is not an instructions object; received object of type {0}"
                               .format(type(instructions)))
        self.instructions_set.append(instructions)

    def get_instructions(self):
        return self.instructions_set

    def set_instructions_list(self, list_of_instructions):
        self.instructions_set = list_of_instructions

class RawConfigurationInstructionsContainer(MessageObject):
    EXCLUDED_ATTRS_FOR_STATE = ["actor_group_id", "instructions_date", "ignore_these_instructions_if_empty"]

    def __init__(self, actor_group_id, instructions_date, ignore_these_instructions_if_empty=False):
        self.actor_group_id = actor_group_id
        self.instructions_date = instructions_date
        self.ignore_these_instructions_if_empty = ignore_these_instructions_if_empty
        self.raw_instructions_df = pd.DataFrame()

    def __len__(self):
        return len(self.raw_instructions_df)

    def ignore_instructions(self):
        return not len(self) and self.ignore_these_instructions_if_empty

    def get_raw_instructions(self):
        return self.raw_instructions_df

    def get_instructions_date(self):
        return self.instructions_date

    def set_raw_instructions(self, raw_instructions_df):
        self.raw_instructions_df = raw_instructions_df

    def __getinitargs__(self):
        return (self.actor_group_id, self.instructions_date, self.ignore_these_instructions_if_empty)


class InstructionParser(DCMIntuitionObject):
    """ Reads a csv file and forms the JSON objects that are used to configure actors. The
    assumptions are that the csv will have three special columns: date, actor_id, and
    actorRole.

    The parsing result is a dataframe that for each date on the original
    frame contains a list of ActorConfigurationInstructions. For now all
    columns that are not date, actor_id or actorRole are rolled up into a dictionary.

    Attributes:
        column_parsing_instructions: decoding instructions
        expected_fields: A list of the expected fields in the instructions
        attach_actor_id_prefix: A string used as prefix to the identity number of the actor

    """
    ACTOR_ROLE_FIELD = "role"
    ACTOR_ID_FIELD = "actorID"
    INSTRUCTIONS_OVERRIDES_FIELDS = "instructions_overrides"
    OVERRIDE_FILTER_FIELD = "override_filter"
    DATE_FIELD = "date"
    QUOTE_CHAR = "'"
    MANDATORY_FIELDS = [DATE_FIELD, ACTOR_ID_FIELD, ACTOR_ROLE_FIELD, INSTRUCTIONS_OVERRIDES_FIELDS,
                        OVERRIDE_FILTER_FIELD]
    EXCLUDED_ATTRS_FOR_STATE = ["column_parsing_instructions", "expected_fields", "override_filter", "override_dict",
                                "attach_actor_id_prefix", "original_columns_parsing_instructions"]

    def __init__(self, column_parsing_instructions, expected_fields, override_filter=None, override_dict=None,
                 attach_actor_id_prefix=""):
        parsing_function_dict = {"dict":self._parse_dict,
                                 "tuple":self._parse_tuple,
                                 "list": self._parse_list,
                                 "timedelta":self._parse_time_delta,
                                 "timestamp": self._parse_timestamp,
                                 "eval":self._parse_using_eval}

        self.original_columns_parsing_instructions = column_parsing_instructions
        self.column_parsing_instructions = deep_copy_of_object(column_parsing_instructions)
        self.column_parsing_instructions[InstructionParser.INSTRUCTIONS_OVERRIDES_FIELDS] = "eval"
        self.expected_fields = expected_fields
        self.attach_actor_id_prefix = attach_actor_id_prefix
        self.override_filter = override_filter
        self.override_dict = override_dict if override_dict else {}
        self.__validate_correctness_of_overrides()
        for k in self.column_parsing_instructions:
            if isinstance(self.column_parsing_instructions[k], str):
                parsing_func = parsing_function_dict.get(self.column_parsing_instructions[k], None)
                if not parsing_func:
                    raise DCMException("instructionParser.__init__ the requested parsing type {0} is unrecognized"
                                       .format(self.column_parsing_instructions[k]))
                self.column_parsing_instructions[k] = parsing_func


    def __getinitargs__(self):
        return (self.original_columns_parsing_instructions, self.expected_fields, self.override_filter,
                self.override_dict, self.attach_actor_id_prefix)

    def check_expected_fields(self, fields):
        """ Verifies that the mandatory fields are included and if the are any
        additional ones.
        Args:
            fields:
        Returns:
            A boolean value that all mandatory fields were received.
        Raises:
            DCMException: Exception that shows if the fields received are not expected and
                which fields are missing, if any.
        """
        all_fields = set(self.expected_fields).union(set(InstructionParser.MANDATORY_FIELDS))
        received_fields = set(fields)
        # To only checks for unexpected fields switch to difference instead of symmetric_difference
        # TODO: put back symmetric difference here
        if len(received_fields.difference(all_fields)) != 0:
            extra_fields = ", ".join(list(received_fields.difference(all_fields)))
            missing_fields = ", ".join(list(all_fields.difference(received_fields)))
            raise DCMException("InstructionParser.check_expected_fields - the list of fields received has extra "
                               "columns [{0}] and is missing fields [{1}]".format(extra_fields, missing_fields))
        return True

    @classmethod
    def _parse_dict(cls, dict_str):
        """ Changes the string values of the dictionary to float numbers.
        Args:
            dict_str:

        Returns:
            A dictionary keyed by the number of values (float).
        """
        if pd.isnull(dict_str):
            return None
        data = dict_str.replace("'", "").split(',')
        data = [i.strip('{ }') for i in data]
        data = dict([i.split(":")for i in data])
        for key in data:
            try:
                data[key] = float(data[key])
            except ValueError:
                pass
        return data

    @classmethod
    def _parse_tuple(cls, tup_str):
        """
        Args:
            tup_str:
        """
        if pd.isnull(tup_str):
            return None
        data = tup_str.split(',')
        data = [s.strip('( )') for s in data]
        lst = [float(s) for s in data]
        return tuple(lst)

    @classmethod
    def _parse_list(cls, list_str):
        """
        Args:
            list_str:
        """
        if pd.isnull(list_str):
            return None
        data = list_str.strip("[ ]")
        data = data.strip("'")
        data = data.strip("'")
        test = data.split(",")
        data = [x.strip(" '") for x in test]
        return data

    @classmethod
    def _parse_time_delta(cls, sample):
        """
        Args:
            sample: A variable for date and time. Acceptable formats are: "X days, HH:MM:SS"
                or "HH:MM:SS".
        Returns:

        """
        if pd.isnull(sample):
            return None
        date = re_match(
            r"((?P<days>\d+) days, )?(?P<hours>\d+):"
            r"(?P<minutes>\d+):(?P<seconds>\d+)",
            str(sample)).groupdict(0)
        return TradingBusinessHour(timedelta(**dict(((key, int(value))
                                                     for key, value in date.items()))))

    @classmethod
    def _parse_timestamp(cls, sample):
        """
        Args:
            sample: A variable for date and time. Acceptable formats are: "X days, HH:MM:SS"
                or "HH:MM:SS".
        Returns:

        """
        if pd.isnull(sample):
            return None
        return pd.Timestamp(sample)

    @classmethod
    def _parse_using_eval(cls, sample):
        if pd.isnull(sample):
            return None
        return eval(sample)

    def __validate_correctness_of_overrides(self):
        if not isinstance(self.override_filter, str):
            raise DCMException("InstructionParser.__validate_correctness_of_overrides - Only type string is accepted "
                               "for filters")
        valid_string_values = ["all", "none"]
        if self.override_filter not in valid_string_values:
            raise DCMException("InstructionParser.__validate_correctness_of_overrides - Valid values for override are "
                               "only one of {0}".format(valid_string_values))
        for k in self.override_dict:
            if k not in self.expected_fields:
                raise DCMException("InstructionParser.incorrect override configuration; the field {0} is not within "
                                   "the list of expected fields {1}".format(k, self.expected_fields))

    def parse_df_columns(self, data_set):
        """
        Args:
            data_set:
        Returns:

        """
        for k in self.column_parsing_instructions:
            data_set[k] = data_set[k].dropna().apply(self.column_parsing_instructions[k])
        return data_set

    def _null_special(self, item):
        if isinstance(item, list):
            return pd.isnull(item).any()
        else:
            return pd.isnull(item)

    def form_instruction_per_line(self, my_data):
        """
        Args:
            my_data:
        Returns:

        """
        indexing_column_names = InstructionParser.MANDATORY_FIELDS
        indexing_columns = my_data[indexing_column_names]
        data_columns = my_data.drop(indexing_column_names, axis=1)
        data_columns_as_dict = pd.DataFrame([[{l:getattr(k,l) for l in k._asdict() if not self._null_special(getattr(k,l))}] for k in data_columns.itertuples(index=False)],
                                            columns=["instructionsData"], index=data_columns.index)
        new_data_set = pd.merge(indexing_columns, data_columns_as_dict, left_index=True, right_index=True)
        parse_func = lambda x: ActorConfigurationInstructions(x[InstructionParser.ACTOR_ROLE_FIELD],
                                                              x[InstructionParser.ACTOR_ID_FIELD],
                                                              x["instructionsData"])
        new_data_set["instructions"] = new_data_set.apply(parse_func, axis=1)
        new_data_set.drop([InstructionParser.ACTOR_ID_FIELD, "instructionsData", InstructionParser.ACTOR_ROLE_FIELD],
                          axis=1, inplace=True)
        return new_data_set

    def group_records_per_date(self, my_data_set):
        """ Groups recorded data by date
        Args:
            my_data_set: A data frame that will be group by date
        Returns:
            A data frame with instruction group by date field.
        """
        return my_data_set.groupby(InstructionParser.DATE_FIELD).apply(lambda x: reduce(lambda a, b: a + [b],
                                                                                        x["instructions"], []))

    def apply_overrides(self, my_data_set):
        #JACK properly populate apply
        if not my_data_set.empty:
            for k in self.override_dict:
                column_type = type(my_data_set[k].iloc[0])
                casted_override = column_type(self.override_dict[k])
                my_data_set[k] = casted_override
        return my_data_set

    def create_instruction_dataframe(self, my_data_set):
        """
        Args:
            my_data_set:

        Returns:
            A data frame with instructions
        """
        my_data_set[InstructionParser.DATE_FIELD] = pd.to_datetime(my_data_set[InstructionParser.DATE_FIELD])
        self.check_expected_fields(my_data_set.columns)
        my_data_set = self.apply_overrides(my_data_set)
        my_data_set = self.parse_df_columns(my_data_set)
        my_data_set[InstructionParser.ACTOR_ID_FIELD] = my_data_set[InstructionParser.ACTOR_ID_FIELD]\
            .apply(lambda x: self.attach_actor_id_prefix + str(x))
        my_data_set = self.form_instruction_per_line(my_data_set)
        my_data_set = self.group_records_per_date(my_data_set)
        return my_data_set

    def create_instructions_container(self, raw_df, date_to_filter, container_name):
        ins_dataframe = self.create_instruction_dataframe(raw_df)
        instructions = ins_dataframe[date_to_filter]
        ins_container = ActorConfigurationInstructionsContainer(container_name)
        ins_container.set_instructions_list(instructions)
        return ins_container
