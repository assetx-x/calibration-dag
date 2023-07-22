from commonlib.intuition_base_objects  import DCMIntuitionObject, DCMException
from commonlib.util_classes import GrpcAuxSerializer
import google.protobuf.message

import jsonpickle
import pandas as pd
import datetime as dt
import numpy as np
import pytz
import ib.opt.message
from commonlib.data_requirements  import DRDataTypes, DataRequirements
import bcolz
import pyarrow as pa
import commonlib.subjects

class PandasTimestampHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["__str_repr"] = obj.strftime("%Y-%m-%d %H:%M:%S")
        if obj.tzinfo:
            data["tz"] = obj.tzinfo.zone
        else:
            data["tz"] = None
        return data

    def restore(self, obj):
        date_object =  pd.Timestamp(obj["__str_repr"])
        if obj["tz"]:
            date_object = date_object.tz_localize(pytz.timezone(obj["tz"]))
        return date_object

class PandasTimedeltaHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["__str_repr"] = str(obj)
        return data

    def restore(self, obj):
        timedelta_object =  pd.Timedelta(str(obj["__str_repr"]))
        return timedelta_object


class DatetimeHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["__str_repr"] = obj.strftime("%Y-%m-%d %H:%M:%S")
        if obj.tzinfo:
            data["tz"] = obj.tzinfo.zone
        else:
            data["tz"] = None
        return data

    def restore(self, obj):
        date_object = dt.datetime.strptime(obj["__str_repr"], "%Y-%m-%d %H:%M:%S")
        if obj["tz"]:
            date_object.replace(tzinfo=pytz.timezone(obj["tz"]))
        return date_object


class DateHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["__str_repr"] = obj.strftime("%Y-%m-%d")
        return data

    def restore(self, obj):
        date_object = dt.datetime.strptime(obj["__str_repr"], "%Y-%m-%d").date()
        return date_object

class TimeHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["__str_repr"] = obj.strftime("%H:%M:%S")
        return data

    def restore(self, obj):
        date_object = dt.datetime.strptime(obj["__str_repr"], "%H:%M:%S").time()
        return date_object

class TimeDeltaHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["tot_secs"] = obj.total_seconds()
        return data

    def restore(self, obj):
        timedelta_object = dt.timedelta(seconds = obj["tot_secs"])
        return timedelta_object

class NumpyFloatHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data = float(obj)
        return data

    def restore(self, obj):
        float_object = np.float64(obj)
        return float_object

class NumpyInt64Handler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data = int(obj)
        return data

    def restore(self, obj):
        int_object = np.int64(obj)
        return int_object

class NumpyInt32Handler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data = int(obj)
        return data

    def restore(self, obj):
        int_object = np.int32(obj)
        return int_object

class PandasDataFrameHandler(jsonpickle.handlers.BaseHandler):
    def get_type(self, dtype):
        if dtype.isnative:
            return dtype.descr[0][1]
        elif dtype.kind=="M":
            return '|O'
            #metadata = ",".join(map(lambda x:str(getattr(dtype, x)), dtype._metadata))
            #return "{0}{1}[{2}]".format(dtype.kind, dtype.itemsize, metadata)
        else:
            raise RuntimeError("Unknown type in dataframe: {0}".format(dtype))

    def flatten(self, obj, data):
        p = self.context
        dtypes = obj.dtypes.apply(self.get_type).to_dict()
        data["dtypes"] = dtypes
        obj = obj.replace({pd.NaT:pd.np.NaN})
        data["index"] = p._flatten(obj.index.values.tolist())
        data["columns"] = p._flatten(obj.columns.values.tolist())
        data["values"] = p._flatten(obj.values.tolist())
        return data

    def restore(self, obj):
        #p = jsonpickle.unpickler.Unpickler()
        #df =  pd.DataFrame(data=p.restore(obj["values"]), index=p.restore(obj["index"]),
                           #columns=p.restore(obj["columns"]))
        p = self.context
        df =  pd.DataFrame(data=p._restore(obj["values"]), columns=p._restore(obj["columns"]), 
                           index=p._restore(obj["index"]))

        if "dtypes" in obj:
            dtypes = p._restore(obj["dtypes"])
            for k in dtypes:
                # dtype '|O' not understood now in pandas
                if dtypes[k]=='|O': dtypes[k]='O'
                df[k] = df[k].astype(dtypes[k])
        return df

class PandasSeriesHandler(jsonpickle.handlers.BaseHandler):
    def get_type(self, dtype):
        if dtype.isnative:
            return dtype.descr[0][1]
        elif dtype.kind=="M":
            return 'O'
        else:
            raise RuntimeError("Unknown type in Series: {0}".format(dtype))

    def flatten(self, obj, data):
        p = self.context

        data["name"] = obj.name
        data["dtypes"] = list(map(self.get_type, [obj.index.dtype, obj.dtypes]))

        obj = obj.replace({pd.NaT:pd.np.NaN})
        data["index"] = p._flatten(obj.index.values.tolist())
        data["values"] = p._flatten(obj.values.tolist())
        return data

    def restore(self, obj):
        p = self.context
        series = pd.Series(p._restore(obj['values']), index=p._restore(obj['index']), name=obj['name'])

        if "dtypes" in obj:
            dtypes = p._restore(obj["dtypes"])
            series.index = series.index.astype(dtypes[0])
            series = series.astype(dtypes[1])
        return series


class InteractiveBrokersMessageHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        # strangely, the message classes start with lowercase in python, but are reported capitalized on the
        #py/object field; for reconstuction need to override it. Also, for safety, the class slots are
        #properly serialized on a container
        module, name = data[jsonpickle.tags.OBJECT].rsplit(".",1)
        name = obj.__class__.typeName
        data[jsonpickle.tags.OBJECT] = ".".join([module,name])

        p = self.context
        if not hasattr(obj,"__slots__"):
            slots = p._flatten(obj.__dict__)
        else:
            slots = dict(
                    (slot, p._flatten(getattr(obj, slot)))
                    for slot in obj.__slots__
                    if hasattr(obj, slot)
                )
        data["slots"] = slots
        return data

    def restore(self, obj):
        p = self.context
        class_of_object = jsonpickle.unpickler.loadclass(obj[jsonpickle.tags.OBJECT])
        message = class_of_object()
        for k in obj["slots"]:
            setattr(message, k, p._restore(obj["slots"][k]))
        return message

class ProtobufMessageHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context
        serialization_data = p._flatten(GrpcAuxSerializer.to_dict(obj))
        data[jsonpickle.tags.OBJECT] = "{0}.{1}".format(*GrpcAuxSerializer.get_class_module_and_name(obj))
        data.update(serialization_data)
        return data

    def restore(self, obj):
        p = self.context
        class_name = obj.pop(jsonpickle.tags.OBJECT).split(".")
        msg_content = p._restore(obj)
        module, name = (".".join(class_name[:-1]), class_name[-1])
        message = GrpcAuxSerializer.from_dict(msg_content, module, name)
        return message


class DCMExceptionMessageHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context

        # Shamelessly taken from the internal functionality of jsonpickle library (jsonpickle\pickler.py) version 0.9.2
        reduce_val = obj.__reduce_ex__(2)
        if reduce_val and not isinstance(reduce_val, str):
            # at this point, reduce_val should be some kind of iterable
            # pad out to len 5
            rv_as_list = list(reduce_val)
            insufficiency = 5 - len(rv_as_list)
            if insufficiency:
                rv_as_list += [None] * insufficiency

            if rv_as_list[0].__name__ == '__newobj__':
                rv_as_list[0] = jsonpickle.tags.NEWOBJ

            data[jsonpickle.tags.REDUCE] = list(map(p._flatten, rv_as_list))

            # lift out iterators, so we don't have to iterator and uniterator their content
            # on unpickle
            if data[jsonpickle.tags.REDUCE][3]:
                data[jsonpickle.tags.REDUCE][3] = data[jsonpickle.tags.REDUCE][3][jsonpickle.tags.ITERATOR]

            if data[jsonpickle.tags.REDUCE][4]:
                data[jsonpickle.tags.REDUCE][4] = data[jsonpickle.tags.REDUCE][4][jsonpickle.tags.ITERATOR]

        return data

    def restore(self, obj):
        class_name = obj[jsonpickle.tags.OBJECT]
        cls = jsonpickle.unpickler.loadclass(class_name)
        p = self.context
        instance = cls.__new__(cls)
        return p._restore_object_instance_variables(obj, instance)

class DCMIntuitionObjectMessageHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context
        return p._flatten_dict_obj(obj.__dict__, data)

    def restore(self, obj):
        class_name = obj[jsonpickle.tags.OBJECT]
        cls = jsonpickle.unpickler.loadclass(class_name)
        p = self.context
        instance = cls.__new__(cls)
        return p._restore_object_instance_variables(obj, instance)

class DRDataTypeHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        data["type"] = obj.name
        return data

    def restore(self, obj):
        return DRDataTypes[obj["type"]]


class BcolzCTableHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context
        data["data_frame"] = p._flatten(obj.todataframe())
        return data

    def restore(self, obj):
        p = self.context
        return bcolz.ctable.fromdataframe(p._restore(obj["data_frame"]))


class BcolzCArrayHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context
        data["value_list"] = p._flatten(list(obj))
        return data

    def restore(self, obj):
        p = self.context
        return bcolz.carray(p._restore(obj["value_list"]))


class PyArrowTableHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context
        data["data_frame"] = p._flatten(obj.to_pandas())
        return data

    def restore(self, obj):
        p = self.context
        return pa.Table.from_pandas(p._restore(obj["data_frame"]))


class SubjectEntityHandler(jsonpickle.handlers.BaseHandler):
    def flatten(self, obj, data):
        p = self.context
        for n,v in zip(obj.__getinitargs_names__(), obj.__getinitargs__()):
            data[n] = p._flatten(v)
        return data

    def restore(self, obj):
        class_name = obj.pop(jsonpickle.tags.OBJECT)
        cls = jsonpickle.unpickler.loadclass(class_name)
        p = self.context
        instance = cls(**p._restore(obj))
        return instance


jsonpickle.handlers.registry.register(DCMIntuitionObject, DCMIntuitionObjectMessageHandler, base=True)
jsonpickle.handlers.registry.register(DataRequirements, DCMIntuitionObjectMessageHandler, base=True)
jsonpickle.handlers.registry.register(DCMException, DCMExceptionMessageHandler, base=True)
jsonpickle.handlers.registry.register(ib.opt.message.Message, InteractiveBrokersMessageHandler, base=True)
jsonpickle.handlers.registry.register(commonlib.subjects.Entity, SubjectEntityHandler, base=True)
jsonpickle.handlers.registry.register(google.protobuf.message.Message, ProtobufMessageHandler, base=True)

jsonpickle.handlers.registry.register(pd.Timestamp, PandasTimestampHandler)
jsonpickle.handlers.registry.register(pd.Timedelta, PandasTimedeltaHandler)
jsonpickle.handlers.registry.register(dt.datetime, DatetimeHandler)
jsonpickle.handlers.registry.register(dt.date, DateHandler)
jsonpickle.handlers.registry.register(dt.time, TimeHandler)
jsonpickle.handlers.registry.register(dt.timedelta, TimeDeltaHandler)
jsonpickle.handlers.registry.register(np.float64, NumpyFloatHandler)
jsonpickle.handlers.registry.register(np.int64, NumpyInt64Handler)
jsonpickle.handlers.registry.register(np.int32, NumpyInt32Handler)
jsonpickle.handlers.registry.register(pd.DataFrame, PandasDataFrameHandler)
jsonpickle.handlers.registry.register(pd.Series, PandasSeriesHandler)
jsonpickle.handlers.registry.register(DRDataTypes, DRDataTypeHandler)
jsonpickle.handlers.registry.register(bcolz.ctable, BcolzCTableHandler)
jsonpickle.handlers.registry.register(bcolz.carray, BcolzCArrayHandler)
jsonpickle.handlers.registry.register(pa.Table, PyArrowTableHandler)
