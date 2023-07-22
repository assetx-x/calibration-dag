import inspect
import _pickle as pickle
from bisect import bisect_right
import sqlalchemy as sa
from functools import partial
import pandas as pd
import commonlib
from commonlib.market_timeline import marketTimeline
import os
import sys
import time
import importlib
from scipy.stats import norm as normal
from statsmodels.robust.scale import mad
import glob
import numpy as np
from copy import deepcopy
import errno
from pylru import lrudecorator
from trading_calendars import get_calendar
import git
from functools import reduce, wraps


def find_le(a, x):
    """Find rightmost value less than or equal to x"""
    i = bisect_right(a, x)
    if i:
        return (i-1, a[i-1])
    raise ValueError

def safe_log(x):
    if pd.isnull(x) or (abs(x-0.0)<0.0001):
        return 0.0
    else:
        return pd.np.log(x)

def __get_params_from_callable(clb, arg_offset):
    arg_description = inspect.getargspec(clb)
    required_init_arguments = arg_description.args
    params_defaults = arg_description.defaults
    if params_defaults:
        params_with_defaults = set(required_init_arguments[-len(params_defaults):])
        params_without_defaults = set(required_init_arguments[arg_offset:-len(params_defaults)])
    else:
        params_with_defaults = set()
        params_without_defaults = set(required_init_arguments[arg_offset:None])
    return params_with_defaults, params_without_defaults

def get_parameters_from_bound_method(method):
    return __get_params_from_callable(method, 1)

def get_parameters_from_standalone_function(func):
    return __get_params_from_callable(func, 0)

def retrieve_and_initialize_class_instance_with_parameters(namespace, class_name, class_parameters,
                                                           parent_class_check=None):
    if isinstance(namespace, dict):
        if not class_name in namespace:
            raise RuntimeError("class {0} not available in provided namespace; please verify".format(class_name))
        cls = namespace.get(class_name)
    else:
        if not hasattr(namespace, class_name):
            raise RuntimeError("class {0} not available in provided namespace; please verify".format(class_name))
        cls = getattr(namespace, class_name)
    if parent_class_check is not None and not issubclass(cls, parent_class_check):
        raise RuntimeError("Class {0} must be a subclass of {1} for initialization".format(cls, parent_class_check))
    params_with_defaults, params_without_defaults = get_parameters_from_bound_method(cls.__init__)
    all_parameters = params_with_defaults.union(params_without_defaults)
    provided_parameters = set(class_parameters.keys())
    if provided_parameters - all_parameters:
        raise RuntimeError("Not usable arguments for {0}.__init__ : wrong paremeters include {1}"
                           .format(class_name, provided_parameters - all_parameters))
    if params_without_defaults - provided_parameters:
        raise RuntimeError("There are missing parameters for {0}.__init__; missing parameters are {1}"
                           .format(class_name, params_without_defaults - provided_parameters))
    instance = cls(**class_parameters)
    return instance

def bind_parameters_in_standalone_function(namespace, function_name, function_bound_parameters,
                                           function_free_parameters=None):
    if isinstance(namespace, dict):
        if not function_name in namespace:
            raise RuntimeError("class {0} not available in provided namespace; please verify".format(function_name))
        func = namespace.get(function_name)
    else:
        if not hasattr(namespace, function_name):
            raise RuntimeError("class {0} not available in provided namespace; please verify".format(function_name))
        func = getattr(namespace, function_name)
    params_with_defaults, params_without_defaults = get_parameters_from_standalone_function(func)
    all_parameters = params_with_defaults.union(params_without_defaults)
    function_free_parameters = [] if function_free_parameters is None else function_free_parameters
    provided_parameters = set(function_bound_parameters.keys()).union(set(function_free_parameters))
    if provided_parameters - all_parameters:
        raise RuntimeError("Not usable arguments for function {0} : wrong paremeters include {1}"
                           .format(function_name, provided_parameters - all_parameters))
    if params_without_defaults - provided_parameters:
        raise RuntimeError("There are missing parameters for function {0}; missing parameters are {1}"
                           .format(function_name, params_without_defaults - provided_parameters))
    bound_func = partial(func, **function_bound_parameters)
    return bound_func

def get_all_subclasses_of_class(cls):
    return cls.__subclasses__() + [g for s in cls.__subclasses__()
                                   for g in get_all_subclasses_of_class(s)]

def get_max_of_pandas_frequency(list_of_frequencies, base_date, past_flag = True):
    lookbacks = set(filter(pd.notnull, list_of_frequencies))
    if not lookbacks:
        return None
    if past_flag:
        past_dates = [(k, base_date - pd.tseries.frequencies.to_offset(k)) for k in lookbacks]
        biggest_lb = min(past_dates, key=lambda x:x[1])[0]
    else:
        future_dates = [(k, base_date + pd.tseries.frequencies.to_offset(k)) for k in lookbacks]
        biggest_lb = max(future_dates, key=lambda x:x[1])[0]
    return biggest_lb


#TODO (Napoleon): transfer this to util function when merged with the OLMAR trader changes
def get_np_dtype_for_sqlalchemy_type(model, field_name):
    if not hasattr(model, field_name):
        raise ValueError("get_np_dtype_for_sqlalchemy_type - attributed {0} is not present on model {1}"
                           .format(field_name, model))
    sql_alchemy_type = getattr(model, field_name).property.columns[0].type
    python_type = sql_alchemy_type.python_type
    if isinstance(sql_alchemy_type, sa.types.String):
        dtype = "a{0}".format(sql_alchemy_type.length)
    elif isinstance(sql_alchemy_type, sa.types.Integer):
        dtype = "i8" if not getattr(model, field_name).property.columns[0].nullable else "f8"
    elif isinstance(sql_alchemy_type, sa.types.Numeric):
        dtype = "f8"
    elif isinstance(sql_alchemy_type, sa.types.DateTime):
        dtype = "<M8[ns]"
    elif isinstance(sql_alchemy_type, sa.types.Boolean):
        dtype = "b"
    elif isinstance(sql_alchemy_type, sa.types.Date):
        dtype = "O"
    else:
        raise ValueError("get_np_dtype_for_sqlalchemy_type - do not know how to cast sql type {0} for field {1} using "
                     "model {2}".format(sql_alchemy_type, field_name, model))
    return dtype

def flatten_dict(d):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '.' + k, v) for k, v in flatten_dict(value).items() ]
        else:
            return [ (key, value) ]

    items = [ item for k, v in d.items() for item in expand(k, v) ]

    return dict(items)

def flatten_list_of_lists(l):
    return reduce(lambda x,y:x.extend(y) or x, l, [])

def get_third_friday_of_month(curr_date, month_offset_int):
    curr_date = curr_date.normalize().tz_localize(None) - pd.tseries.frequencies.to_offset("1M")
    requested_third_friday = curr_date + pd.tseries.frequencies.to_offset("{0}M".format(month_offset_int)) \
        + pd.tseries.frequencies.to_offset("3W-FRI")
    if not marketTimeline.isTradingDay(requested_third_friday):
        requested_third_friday -= pd.tseries.frequencies.to_offset("1B")
    return requested_third_friday.normalize()

def create_directory_if_does_not_exists(dir_path):
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def load_module_by_name(module_name):
    mdl = importlib.import_module(module_name)
    return mdl

def load_module_by_full_path(p):
    current_sys_paths = deepcopy(sys.path)
    module_path = os.path.dirname(p)
    module_name = os.path.basename(p).split(".")[0]
    sys.path.insert(0, module_path)
    mdl = importlib.import_module(module_name)
    sys.path = current_sys_paths
    return mdl

def load_obj_from_module(python_file, name):
    module = load_module_by_full_path(python_file)
    obj = getattr(module, name, None)
    return obj

def digitlalize(x,thresh_list):
    return bisect_right(thresh_list, x)

def qcut_modified(x, q):
    try:
        return pd.qcut(x,q,labels=False)
    except ValueError as e:
        print("{0} group had an exception {1}".format(x.name, e))
        return [pd.np.NaN]*len(x)

def cut_modified(x,q, use_mad_for_std=True):
    try:
        quantiles_in_sigmas = np.asarray(map(normal.ppf, q))
        x_clean = x.dropna()
        mean = np.mean(x_clean)
        std = np.std(x_clean) if not use_mad_for_std else mad(x_clean)
        bins = mean + quantiles_in_sigmas*std
        bins = np.sort(np.append(bins, (x_clean.min()-1E-6, x_clean.max()+1E-6)))
        return pd.cut(x, bins, labels=range(len(bins)-1))
    except ValueError as e:
        #print len(x_clean)
        #print "{0} group had an exception {1}".format(x.name, e)
        return [pd.np.NaN]*len(x)


def add_digitized_columns_given_input_filter(df_orig, columns_list, cut_point_list, based_on_filter, quantile_cut_point_format= True,
                                             digitized_columns_names=[]):
    df = df_orig.copy()
    filter_name = '*'.join(['_Digital'] +based_on_filter) if based_on_filter else '*'.join(['_Digital'])
    if not digitized_columns_names:
        digitized_columns_names = map(lambda x: x + filter_name, columns_list)
    print(digitized_columns_names)
    if not based_on_filter:
        if quantile_cut_point_format:
            for k,col in enumerate(columns_list):
                df[digitized_columns_names[k]] = cut_modified(df[col], cut_point_list )
        else:
            for k,col in enumerate(columns_list):
                df[digitized_columns_names[k]] = pd.cut(df[col], cut_point_list, labels =range(len(cut_point_list)-1))

    else:
        df_groups = df.groupby(based_on_filter)
        if quantile_cut_point_format:
            for k,col in enumerate(columns_list):
                #df[digitized_columns_names[k]] = df_groups[col].transform(lambda x: pd.qcut(x,cut_point_list,
                #labels =digital_vals))
                df[digitized_columns_names[k]] = df_groups[col].transform(lambda x: cut_modified(x,cut_point_list))
        else:
            for k,col in enumerate(columns_list):
                df[digitized_columns_names[k]] = df_groups[col].transform(lambda x: pd.cut(x,cut_point_list,
                                                                                           labels =range(len(cut_point_list)-1)))
    return df, digitized_columns_names

def nonnull_lb(value):
    value = value if pd.notnull(value) else -999999999999999
    return value

def nonnull_ub(value):
    value = value if pd.notnull(value) else 999999999999999
    return value

def pick_trading_week_dates(start_date, end_date, mode='w-mon'):
    weekly_days = pd.date_range(start_date, end_date, freq=mode)
    trading_dates = pd.Series(weekly_days).apply(marketTimeline.get_next_trading_day_if_holiday)
    dates = trading_dates[(trading_dates>=start_date) & (trading_dates<=end_date)]
    return dates

def pick_trading_month_dates(start_date, end_date, mode="bme"):
    trading_days = marketTimeline.get_trading_days(start_date, end_date).tz_localize(None)
    if mode=="bme":
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).last()
    else:
        dates = pd.Series(trading_days).groupby(trading_days.to_period('M')).first()
    dates = dates[(dates>=start_date) & (dates<=end_date)]
    return dates

def pick_trading_year_dates(start_date, end_date, mode="end"):
    trading_days = marketTimeline.get_trading_days(start_date, end_date).tz_localize(None)
    if mode=="end":
        dates = pd.Series(trading_days).groupby(trading_days.to_period('A')).last()
    else:
        dates = pd.Series(trading_days).groupby(trading_days.to_period('A')).first()
    dates = dates[(dates>=start_date) & (dates<=end_date)]
    return dates

@lrudecorator(size=1)
def get_redshift_engine(verbose=False):
    gcp_conf = commonlib.config.get_config()["gcp"]
    project_id = gcp_conf["project_id"]
    default_dataset = gcp_conf["default_dataset"]
    return sa.create_engine('bigquery://{project_id}/{default_dataset}'.format(project_id=project_id, default_dataset=default_dataset))

@lrudecorator(size=1)
def get_mysql_engine(verbose=False):
    user = os.environ["MYSQL_USER"]
    pwd =  os.environ["MYSQL_PWD"]
    host = os.environ["MYSQL_HOST"]
    dbname = os.environ["MYSQL_DB"]
    return sa.create_engine('mysql+pymysql://{user}:{pwd}@{host}/{dbname}'.format(user=user, pwd=pwd, host=host, dbname=dbname))

@lrudecorator(size=1)
def get_snowflake_engine():
    user = os.environ["SNOWFLAKE_USER"]
    password = os.environ["SNOWFLAKE_PWD"]
    account = "sb29751.us-east-1"
    database = "dcm_main"
    schema = "public"
    warehouse = "DEV_WH"
    role = "researcher"
    db_url = "snowflake://{u}:{pa}@{act}/{db}/{sch}?warehouse={wh}?role={role}".format(u=user, pa=password, act=account,
                                                                                       db=database, sch=schema,
                                                                                       wh=warehouse, role=role)

    sa_engine = sa.create_engine(db_url)
    return sa_engine

def pickle_copy(obj):
    return pickle.loads(pickle.dumps(obj, protocol=2))

def get_git_revision_sha1():
    current_file = __file__
    base_dir = current_file[0:current_file.rfind("dcm-intuition") + len("dcm-intuition")]
    repo = git.Repo(base_dir)
    return repo.head.commit.name_rev

def performance(x, scale_factor=12, return_pnl=True):
    ann_ret = x.mean() * scale_factor
    ann_std = x.std() * pd.np.sqrt(scale_factor)
    sharpe = ann_ret / ann_std
    pnl = (1 + x).cumprod()
    mdd = (1 - pnl / pnl.cummax()).max()
    smry = pd.Series({
        'ann_ret': ann_ret,
        'ann_std': ann_std,
        'sharpe': sharpe,
        'mdd': mdd
    })
    if return_pnl:
        return smry, pnl
    else:
        return smry


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck:
                    msg = "%s, Retrying in %d seconds..." % (str(ExceptionToCheck), mdelay)
                    if logger:
                        logger.exception(msg) # would print stack trace
                        #logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry
