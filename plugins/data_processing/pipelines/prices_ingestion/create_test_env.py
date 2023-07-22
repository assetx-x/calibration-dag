import os

from sqlalchemy import create_engine


access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

redshift_url = "cq0v4ljf3cuw.us-east-1.redshift.amazonaws.com"
prod_redshift_url = "redshift+psycopg2://adjustments:%s@marketdata-test.%s:5439/dev" % ( '%s', redshift_url)
test_redshift_url = "redshift+psycopg2://envtest:%s@envtest.%s:5439/envtest" % ( '%s', redshift_url)
prod_postgres_url = "postgresql+psycopg2://postgres:%s@postgres.aws:5432/dev"
test_postgres_url = "postgresql+psycopg2://postgres:%s@postgres.aws:5432/test"


class PriceIngestionTestEnv(object):
    def __init__(self, name, all_tickers, full_adj_tickers):
        self.name = name
        self.all_tickers = all_tickers
        self.full_adj_tickers = full_adj_tickers

        self._unload_location = "s3://dcm-data-test/test_env/%s" % name

        self._prod_redshift = create_engine(prod_redshift_url % self._password('prod_redshift'))
        self._test_redshift = create_engine(test_redshift_url % self._password('test_redshift'))
        self._prod_postgres = create_engine(prod_postgres_url % self._password('prod_postgres'))
        self._test_postgres = create_engine(test_postgres_url % self._password('test_postgres'))

    def create(self):
        pass

    def tear_down(self):
        pass

    @staticmethod
    def _password(password_id):
        return os.environ[password_id.upper() + '_PASSWORD']

    def _create_ticker_universe(self):
        pass

    def _create_data_tables(self):
        # with production redshift
        #     unload data from production
        with self._test_redshift.begin() as connection:
            self._create_raw_equity_prices_table(connection)

    def _drop_data_tables(self):
        pass

    def _reset_pipeline_stages(self):
        pass

    def _create_raw_equity_prices_table(self, connection):
        table = "raw_equity_prices_" + self.name
        connection.execute(
            """ DROP TABLE IF EXISTS {table} CASCADE;

                CREATE TABLE {table}
                (
                   "open"       float4,
                   high         float4,
                   low          float4,
                   close        float4,
                   volume       float4,
                   ticker       varchar(10)   NOT NULL,
                   cob          timestamp     NOT NULL,
                   as_of        timestamp     DEFAULT getdate() NOT NULL,
                   import_time  timestamp     DEFAULT getdate() NOT NULL
                );
                
                ALTER TABLE {table}
                   ADD CONSTRAINT {table}_pkey
                   PRIMARY KEY (ticker, cob, as_of);
                
                COPY {table}
                FROM '{unload_location}/{table}/manifest'
                ACCESS_KEY_ID '{access_key_id}'
                SECRET_ACCESS_KEY '{secret_access_key}'
                manifest;
            """.format(table=table,
                       unload_location=self._unload_location,
                       access_key_id=access_key_id,
                       secret_access_key=secret_access_key)
        )

    def _create_equity_price_table(self, connection):
        table = "equity_prices_" + self.name
        connection.execute(
            """ DROP TABLE IF EXISTS {table} CASCADE;
            
                CREATE TABLE {table}
                (
                   cob          timestamp     NOT NULL,
                   volume       float4,
                   "open"       float4,
                   close        float4,
                   high         float4,
                   low          float4,
                   as_of_start  timestamp     NOT NULL,
                   as_of_end    timestamp,
                   ticker       varchar(10)   NOT NULL
                );
                
                ALTER TABLE {table}
                   ADD CONSTRAINT {table}_pkey
                   PRIMARY KEY (ticker, cob, as_of_start);
                
                copy {table}
                from '{unload_location}/{table}/manifest'
                ACCESS_KEY_ID '{access_key_id}'
                SECRET_ACCESS_KEY '{secret_access_key}'
                manifest;
            """.format(table=table,
                       unload_location=self._unload_location,
                       access_key_id=access_key_id,
                       secret_access_key=secret_access_key)
        )

if __name__ == '__main__':
    tickers = ['EMB', 'HYG', 'HYS', 'IEF', 'IEI', 'JNK', 'SHY', 'TLH', 'TLT']
    env = PriceIngestionTestEnv("boris", tickers, ['TLT'])
    env.create()
