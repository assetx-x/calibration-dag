import logging
from uuid import uuid4

import boto3
import sqlalchemy


class RedshiftCredentials(object):
    _logger = logging.getLogger("redshift_helpers.credentials")

    def __init__(self, session_name):
        self.credentials = self._get_redshift_credentials(session_name)
        self._credentials_as_string = None

    def _get_redshift_credentials(self, session_name):
        self._logger.info("Acquiring temporary credentials for redshift")
        sts = boto3.client("sts")
        try:
            credentials = sts.assume_role(
                RoleArn="arn:aws:iam::294659765478:role/slave",
                RoleSessionName=session_name
            )
        except Exception:
            self._logger.warning("STS:AssumeRole failed, falling back to normal session token retrieval")
            credentials = sts.get_session_token()
        return credentials

    def as_string(self):
        if self._credentials_as_string is None:
            self._credentials_as_string = self._format_credentials_string()
        return self._credentials_as_string

    def _format_credentials_string(self):
        credentials_values = {
            "access_key": self.credentials["Credentials"]["AccessKeyId"],
            "secret_access_key": self.credentials["Credentials"]["SecretAccessKey"],
            "session_token": self.credentials["Credentials"]["SessionToken"]
        }
        return "aws_access_key_id={access_key};aws_secret_access_key={secret_access_key};token={session_token}" \
            .format(**credentials_values)


# noinspection SpellCheckingInspection
class BitemporalTable(object):
    def __init__(self, connection, table_name, cob_field="cob", key_fields=None):
        self._logger = logging.getLogger("bitemporal_table." + table_name)
        self.connection = connection
        self.table_name = table_name
        self.cob_field = cob_field
        self.key_fields = key_fields if key_fields else ["ticker"]

        self.temp_table_name = table_name + "_" + str(uuid4()).replace("-", "")

        md = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table(self.table_name, md, autoload=True, autoload_with=connection)
        self.credentials = RedshiftCredentials(self.temp_table_name).as_string()

    def merge(self, manifest_path):
        self._create_temp_table()
        self._populate_temp_table(manifest_path)
        self._update_existing_records()
        self._insert_new_records()

    def _create_temp_table(self):
        self._logger.info("Creating temporary table '%s'", self.temp_table_name)
        query = """
            create temp table {temp_table_name}(like {table_name})
            """.format(table_name=self.table_name,
                       temp_table_name=self.temp_table_name)
        self.connection.execute(query)

    def _populate_temp_table(self, manifest_path):
        self._logger.info("Copying data from S3 into temp table '%s'", self.temp_table_name)
        query = """
            COPY {temp_table_name}
            FROM '{manifest_path}'
            WITH CREDENTIALS AS '{credentials}'
            dateformat 'auto'
            timeformat 'auto'
            ignoreheader 1
            delimiter ','
            acceptinvchars
            blanksasnull
            emptyasnull
            manifest
            csv
            """.format(temp_table_name=self.temp_table_name,
                       manifest_path=manifest_path,
                       credentials=self.credentials)
        self.connection.execute(query)

    def _update_existing_records(self):
        self._logger.info("Setting 'as_of_end' of old records to a real value")
        key_fields = ", ".join(['"%s"' % key_field for key_field in self.key_fields])
        key_condition = " AND ".join(['%s."%s" = ut."%s"' % (self.table_name, key_field, key_field)
                                      for key_field in self.key_fields])
        join_condition = " AND ".join(['ut."%s" = as_of."%s"' % (key_field, key_field)
                                       for key_field in self.key_fields])
        query = """
            UPDATE {table_name}
            SET as_of_end = as_of.min_as_of_start
            FROM {temp_table_name} ut
            INNER JOIN (
                SELECT {key_fields}, date("{cob_field}") as cob_day, min(as_of_start) as min_as_of_start
                FROM {temp_table_name}
                GROUP BY {key_fields}, cob_day
            ) as_of
            ON
                {join_condition}
                AND date(ut."{cob_field}") = as_of.cob_day
            WHERE
                {table_name}.as_of_end IS NULL
                AND {key_condition}
                AND {table_name}."{cob_field}" = ut."{cob_field}"
                AND {table_name}.as_of_start < ut.as_of_start
            """.format(table_name=self.table_name,
                       temp_table_name=self.temp_table_name,
                       key_fields=key_fields,
                       cob_field=self.cob_field,
                       key_condition=key_condition,
                       join_condition=join_condition)
        self.connection.execute(query)

    def _insert_new_records(self):
        self._logger.info("Inserting new records")
        quoted_columns = ", ".join(['"%s"' % c.name for c in self.table.c])
        ut_quoted_columns = ", ".join(['ut."%s"' % c.name for c in self.table.c])
        ut_quoted_columns = ut_quoted_columns.replace('ut."as_of_end"', "cast(NULL as timestamp)")
        key_fields = ", ".join(['"%s"' % key_field for key_field in self.key_fields])
        join_condition = " AND ".join(['starts."%s" = ut."%s"' % (key_field, key_field)
                                       for key_field in self.key_fields])
        query = """
            INSERT INTO {table_name} (
                {quoted_columns}
            )
            SELECT DISTINCT
                {ut_quoted_columns}
            FROM
                {temp_table_name} ut
            LEFT JOIN (SELECT {key_fields}, max(as_of_start) as latest_start FROM {table_name} 
                 GROUP BY {key_fields}) starts
            ON {join_condition}
            WHERE
                starts.latest_start IS NULL OR
                starts.latest_start < ut.as_of_start
            """.format(table_name=self.table_name,
                       temp_table_name=self.temp_table_name,
                       quoted_columns=quoted_columns,
                       ut_quoted_columns=ut_quoted_columns,
                       key_fields=key_fields,
                       join_condition=join_condition)
        self.connection.execute(query)


def _main():
    import os
    username = os.environ.get("REDSHIFT_USER", "dcm_dev")
    password = os.environ.get("REDSHIFT_PWD", "password")
    redshift_engine = sqlalchemy.create_engine(
        "redshift+psycopg2://%s:%s@marketdata-test.cq0v4ljf3cuw.us-east-1.redshift.amazonaws.com:5439/dev" % (
            username, password))
    with redshift_engine.begin() as connection:
        table = BitemporalTable(connection, "daily_index")
        table.merge("s3://manifest_path_somewhere_on_s3")


if __name__ == '__main__':
    _main()
