def get_udf_object(specifier):
    if '(' in specifier:
        try:
            name, rest = specifier.split("(")
            name = name.strip()
            args = [arg.strip() for arg in rest.split(")")[0].split(",")]
        except Exception:
            raise Exception("Incorrect udf specifier: %s" % specifier)
        return get_udf_by_name(name), args
    else:
        return get_udf_by_name(specifier), []


class Udf(object):
    @staticmethod
    def serialize_args(args, spark_context, sql_context, sources, prices_source, redshift_config):
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()


def as_function(*types):
    def wrapper(func):
        class UdfFunc(Udf):
            def __init__(self, *args):
                self.args = args

            @staticmethod
            def serialize_args(args, spark_context, sql_context, sources, prices_source, redshift_config):
                return [type(arg) for arg, type in zip(args, types)]

            def __call__(self, *args, **kwargs):
                return func(*(list(self.args) + list(args)))

        return UdfFunc
    return wrapper


from tasks_common.deep_impact.udf.excess_return import excess_return
from tasks_common.deep_impact.udf.cut_return import cut_return
from tasks_common.deep_impact.udf.get_return import get_return


UDFS = {
    "get_return": get_return,
    "excess_return": excess_return,
    "cut_return": cut_return
}

def get_udf_by_name(name):
    return UDFS[name]