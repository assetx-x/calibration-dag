import os
import pytz
import inspect
import pandas as pd

from enum import Enum
from pylru import lrudecorator
from base import credentials_conf
from sqlalchemy import create_engine
from datetime import datetime, time, timedelta
from commonlib.market_timeline import marketTimeline
from commonlib.util_functions import retry

@lrudecorator(size=1)
def get_postgres_engine():
    username = credentials_conf["postgres"]["username"]
    password = credentials_conf["postgres"]["password"]
    host = credentials_conf["postgres"]["host"]
    port = credentials_conf["postgres"]["port"]
    database = credentials_conf["postgres"]["database"]
    sa_engine = create_engine('postgresql+psycopg2://{u}:{pa}@{h}:{po}/{db}'.format(u=username, pa=password,
                                                                                    h=host, po=port,
                                                                                    db=database))
    return sa_engine


def get_pipeline_name():
    s = inspect.stack()
    rel_path = os.path.relpath(s[1][1], s[0][1])
    return [d for d in rel_path.split(os.path.sep) if d != '..'][0]


class TimePart(Enum):
    NoTime = 0
    Preserve = 1
    Normalize = 2


def get_prev_bus_day(ref_date_utc=None, start_time_local=None, local_tz_name='US/Eastern', time_part=TimePart.NoTime):
    local_tz = pytz.timezone(local_tz_name)
    if not ref_date_utc:
        ref_date_utc = datetime.utcnow()
    if not start_time_local:
        start_time_local = time(0, 0, tzinfo=local_tz)
    else:
        start_time_local = start_time_local.replace(tzinfo=local_tz)
    local_ref_date = ref_date_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)
    if local_ref_date.time() < start_time_local:
        local_ref_date -= timedelta(days=1)

    while True:
        local_ref_date -= timedelta(days=1)
        if marketTimeline.isTradingDay(local_ref_date):
            trading_day = local_ref_date.astimezone(pytz.utc).replace(tzinfo=None)
            if time_part == TimePart.NoTime:
                return trading_day.date()
            elif time_part == TimePart.Preserve:
                return trading_day
            elif time_part == TimePart.Normalize:
                return datetime.combine(trading_day.date(), time(0, 0))

@retry(Exception, tries=6, delay=2, backoff=2, logger=None)
def read_sql_with_retry(sql_command, sql_engine):
    return pd.read_sql(sql_command, sql_engine)