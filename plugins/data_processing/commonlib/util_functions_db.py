import pandas as pd
import sqlalchemy as sa
import sqlalchemy.orm

try:
    from pandas.lib import checknull as pd_checknull
except ImportError:
    from pandas._libs.missing import checknull as pd_checknull

import json
import boto3
from io import StringIO
from commonlib.util_functions import get_redshift_engine, get_mysql_engine
from commonlib.util_classes import InstructionsFileURILocator
from sqlalchemy_redshift.commands import CopyCommand, UnloadFromSelect

__session = None
__engine = None
__mysql_engine = None
__mysql_session = None

def __get_session(engine):
    session = sa.orm.sessionmaker()
    session.configure(bind=engine)
    return session(autocommit=False)

def get_engine_and_session(verbose=False, engine=None):
    global __engine
    global __session
    global __mysql_engine
    global __mysql_session

    if engine=='MYSQL':
        if not __mysql_engine :
            __mysql_engine = get_mysql_engine(verbose)
        if not __mysql_session:
            __mysql_session = __get_session(__mysql_engine)
        return __mysql_engine, __mysql_session
    else:
        if not __engine:
            __engine = get_redshift_engine(verbose)
        if not __session:
            __session = __get_session(__engine)
        return __engine, __session

    return None

def drop_redshift_engine_and_session():
    global __engine
    global __session
    if __session:
        __session.flush()
        __session.close()
        __session = None
    if __engine:
        __engine.dispose()
        __engine = None

def drop_mysql_engine_and_session():
    global __mysql_engine
    global __mysql_session
    if __mysql_session:
        __mysql_session.flush()
        __mysql_session.close()
        __mysql_session = None
    if __mysql_engine:
        __mysql_engine.dispose()
        __mysql_engine = None

def drop_engine_and_session(engine=None):
    if engine == 'MYSQL':
        drop_mysql_engine_and_session()
    else:
        drop_redshift_engine_and_session()


# Adapted from code in library pytest-mock-resources.
# https://github.com/schireson/pytest-mock-resources/blob/master/src/pytest_mock_resources/patch/redshift/create_engine.py

class RedshiftToSQLiteAdapter(object):
    Base = None

    @classmethod
    def set_db_base(cls, base_class):
        cls.Base = base_class

    @classmethod
    def strip(cls, input_string):
        """Strip trailing whitespace, single/double quotes."""
        return input_string.strip().rstrip(";").strip('"').strip("'")

    @classmethod
    def table_and_s3location_from_copy(cls, statement, engine):
        full_query = str(statement.compile(engine, compile_kwargs={'literal_binds': True}))
        query_tokens = full_query.split()[1:]
        table_name = query_tokens.pop(0)
        for_keyword = query_tokens.pop(0).lower()
        if for_keyword!="from":
            raise ValueError("Possibly malformed COPY statement: {0}".format(full_query))
        s3_location = cls.strip(query_tokens.pop(0))
        return table_name, s3_location

    @classmethod
    def pull_csv_and_fix_dt_cols_for_db(cls, table_name, s3_location, **kwargs):
        file_to_upload = InstructionsFileURILocator.get_path_or_stream_for_file(s3_location)
        data = pd.read_csv(file_to_upload, na_values = ["None"], **kwargs)
        table_to_load = cls.Base.metadata.tables.get(table_name, None)
        if table_to_load is None:
            raise ValueError("Cannot find table {0} in sqlalchemy models".format(table_name))
        datetime_cols = [k.name for k in table_to_load.c if isinstance(k.type, sa.DateTime)]
        for col in datetime_cols:
            data[col] = pd.DatetimeIndex(data[col])
        return data

    @classmethod
    def prepare_record_dicts_to_upload(cls, table_name, s3_location, **kwargs):
        data = cls.pull_csv_and_fix_dt_cols_for_db(table_name, s3_location, **kwargs)
        table_to_load = cls.Base.metadata.tables.get(table_name, None)
        if table_to_load is None:
            raise ValueError("Cannot find table {0} in sqlalchemy models".format(table_name))
        datetime_cols = [k.name for k in table_to_load.c if isinstance(k.type, sa.DateTime)]
        integer_cols = [k.name for k in table_to_load.c if isinstance(k.type, sa.Integer)]
        float_cols = [k.name for k in table_to_load.c if isinstance(k.type, sa.Float)]
        boolean_cols = [k.name for k in table_to_load.c if isinstance(k.type, sa.Boolean)]
        string_cols = [k.name for k in table_to_load.c if isinstance(k.type, sa.String)]
        records = data.to_dict(orient='records')
        for rec in records:
            for col in datetime_cols:
                rec[col] = None if pd_checknull(rec[col]) else rec[col].to_pydatetime()
            for col in integer_cols:
                rec[col] = None if pd_checknull(rec[col]) else int(pd.np.float(rec[col]))
            for col in float_cols:
                rec[col] = None if pd_checknull(rec[col]) else float(pd.np.float(rec[col]))
            for col in boolean_cols:
                rec[col] = None if pd_checknull(rec[col]) else rec[col]
            for col in string_cols:
                rec[col] = None if pd_checknull(rec[col]) else rec[col]
        return records

    @classmethod
    def unload_records_to_s3(cls, statement, records):
        s3 = boto3.resource('s3')
        bucket, key = statement.unload_location[5:].split("/", 1)

        records_df = pd.DataFrame(records)
        buffer = StringIO()
        escapechar = '\\' if statement.escape else None
        sep = statement.delimiter if statement.delimiter else ","
        records_df.to_csv(buffer, sep=sep, escapechar=escapechar, header=False, index=False)
        s3.Object(bucket, key+"0000_part_00").put(Body=buffer.getvalue())

        if statement.manifest:
            manifest_content = json.dumps({"entries":[{"url":statement.unload_location+"0000_part_00"}]})
            s3.Object(bucket, key+"manifest").put(Body=manifest_content)


    def __call__(self, engine):
        default_execute = engine.contextual_connect().execute
        SQLALCHEMY_BASES = (sa.sql.expression.Select, sa.sql.expression.Insert, sa.sql.expression.Update, sa.sql.elements.TextClause)
        def custom_execute(statement, *args, **kwargs):
            if not isinstance(statement, SQLALCHEMY_BASES) and self.strip(str(statement)).lower().startswith("copy"):
                table_name, s3_location = self.table_and_s3location_from_copy(statement, engine)
                records = self.prepare_record_dicts_to_upload(table_name, s3_location, quotechar="'")
                table_to_load = self.Base.metadata.tables[table_name]
                return default_execute(table_to_load.insert(), records)
            if isinstance(statement, UnloadFromSelect):
                selected_records = default_execute(statement.select).fetchall()
                self.unload_records_to_s3(statement, selected_records)
                return
            return default_execute(statement, *args, **kwargs)
        engine.execute = custom_execute
        return engine