import pandas as pd
from pipelines.prices_ingestion.etl_workflow_aux_functions import get_redshift_engine, store_data_in_s3, build_s3_url
import boto3
import quandl
from io import BytesIO
from StringIO import StringIO
import json

from pyspark import SparkContext

def store_formatted_quandl_data(data, cob_date, dim):
    #TODO: put base path on config
    s3_file_bucket = "dcm-data-test"
    s3_prefix = "chris/quandl/fundamentals/sharadar"

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    location = store_data_in_s3(csv_buffer, s3_file_bucket, s3_prefix,
                                "SHARADAR|{0}".format(dim), cob_date)
    return location

def format_quandl_data(data_per_date_per_dimension, tickers, id_vars, concepts_df):
    data_per_date_per_dimension = data_per_date_per_dimension[data_per_date_per_dimension["ticker"].isin(tickers)]
    data_per_date_per_dimension = pd.melt(data_per_date_per_dimension, id_vars=id_vars, var_name="concept")

    fundamental_data = pd.merge(data_per_date_per_dimension, concepts_df.rename(columns={"quandl_column":"concept",
                                                                                         "concept":"new_concept"}),
                                how="left", on=["concept", "dimension"]).drop(["concept","dimension"], axis=1)\
        .rename(columns={"new_concept":"concept", "datekey":"as_of_start", "calendardate":"cob"})
    fundamental_data["as_of_end"] = pd.np.NaN
    fundamental_data["source"] = "quandl"
    fundamental_data["import_time"] = pd.Timestamp.now("UTC").tz_localize(None)
    fundamental_data = fundamental_data[["ticker", "cob", "concept", "value", "source", "as_of_start", "as_of_end",
                                         "import_time"]]
    return fundamental_data

def get_quandl_data_for_date_and_dimension(dim, date, run_dt, concepts_df, tickers, attempt=0):
    try:
        id_vars = ["ticker" , "dimension", "datekey", "calendardate", "lastupdated"]
        concepts = concepts_df[concepts_df["dimension"]==dim]["quandl_column"].tolist()
        concepts = id_vars + concepts
        print("Downloading {0} on {1}".format(dim, date))
        quandl.ApiConfig.api_key = "tzfgtC1umXNxmDLcUZ-5"
        data_per_date_per_dimension = quandl.get_table('SHARADAR/SF1', dimension=dim,
                                                   calendardate=date.strftime("%Y-%m-%d"),
                                                   qopts={"columns":concepts}, paginate=True)

        print(len(data_per_date_per_dimension))
        formatted_data = format_quandl_data(data_per_date_per_dimension, tickers, id_vars, concepts_df)
        data_location = store_formatted_quandl_data(formatted_data, date, dim)
        return data_location
    except BaseException as e:
        if attempt > 3:
            raise e
        else:
            return get_quandl_data_for_date_and_dimension(dim, date, run_dt, concepts_df, tickers, attempt + 1)

class QuandlPullerAndFormatter(object):
    def __init__(self, local_cache_file=None):
        self.tickers = self.get_ticker_list()
        self.fundamental_concepts = self.get_concept_list()

        #todo: get from config
        self.start_dt = pd.Timestamp("2000-01-01", tz="US/Eastern")
        self.end_dt = pd.Timestamp.now(tz="US/Eastern")
        #self.end_dt = pd.Timestamp("2000-07-01", tz="US/Eastern")
        self.run_dt = pd.Timestamp.now(tz="US/Eastern").normalize()

    def get_ticker_list(self):
        #tickers on redshift
        engine = get_redshift_engine()
        query = "select distinct ticker from equity_prices"
        tickers_redshift = map(str, pd.read_sql(query, engine).iloc[:,0].tolist())
        engine.dispose()

        #tickers on universe
        s3_file_bucket = "dcm-data-test"
        s3_file_key = "chris/universe.txt"
        s3_client = boto3.client("s3")
        tickers_file = BytesIO(s3_client.get_object(Bucket = s3_file_bucket, Key=s3_file_key)["Body"].read())
        tickers_s3 = map(str, pd.read_csv(tickers_file).iloc[:,0].tolist())

        #tickers == tickers in current universe + tickers previously in redshift
        return sorted(list(set(tickers_redshift).union(tickers_s3)))

    def get_concept_list(self):
        #TODO: transfer this to config
        s3_file_bucket = "dcm-data-test"
        s3_file_key = "chris/dcm_to_quandl_concept_map.csv"
        s3_client = boto3.client("s3")
        concepts_file = BytesIO(s3_client.get_object(Bucket = s3_file_bucket, Key=s3_file_key)["Body"].read())
        concepts_data = pd.read_csv(concepts_file)
        return concepts_data

    def acquire_and_store_quandl_data(self):
        dimensions = self.fundamental_concepts["dimension"].unique()
        calendar_dates = pd.bdate_range(self.start_dt, self.end_dt, freq="Q")
        all_locations = []
        for dim in dimensions:
            for date in calendar_dates:
                location = get_quandl_data_for_date_and_dimension(dim, date, self.run_dt, self.fundamental_concepts,
                                                                  self.tickers)
                all_locations.append(location)
        return all_locations

    def acquire_and_store_quandl_data_parallel(self):
        dimensions = self.fundamental_concepts["dimension"].unique()
        calendar_dates = pd.bdate_range(self.start_dt, self.end_dt, freq="Q")
        query_params = []
        for dim in dimensions:
            for date in calendar_dates:
                query_params.append((dim, date, self.run_dt))
        sc = SparkContext("local[10]", "test")
        concepts_bc = sc.broadcast(self.fundamental_concepts)
        tickers_bc = sc.broadcast(self.tickers)
        query_params_rdd = sc.parallelize(query_params)
        all_data_rdd = query_params_rdd.map(lambda x:get_quandl_data_for_date_and_dimension(*x, concepts_df=concepts_bc.value, tickers=tickers_bc.value))
        all_data_locations = all_data_rdd.collect()
        sc.stop()
        return all_data_locations

    def create_manifest_file(self, locations):
        manifest_content = {"entries": map(lambda x: {"url": build_s3_url(*x), "mandatory": True}, locations)}
        manifest_buffer = StringIO(json.dumps(manifest_content))
        #TODO: connect this to config
        s3_file_bucket = "dcm-data-test"
        s3_prefix = "chris/quandl/fundamentals/sharadar"
        s3_manifest_key = store_data_in_s3(manifest_buffer, s3_file_bucket, s3_prefix, ".manifest",
                                           pd.Timestamp.now().normalize(), extension="json")
        return s3_manifest_key

    def __get_connection_and_credentials_for_redshift(self):
        sts = boto3.client("sts")
        try:
            credentials = sts.assume_role(
                RoleArn="arn:aws:iam::294659765478:role/slave",
                RoleSessionName="AdjustmentImportDaily"
            )
        except Exception:
            credentials = sts.get_session_token()
        engine = get_redshift_engine()
        return engine, credentials

    def load_data_into_redshift(self, manifest_file):
        engine, credentials = self.__get_connection_and_credentials_for_redshift()
        with engine.begin() as connection:
            connection.execute("""TRUNCATE fundamental_data""")
            connection.execute(
                """
                COPY fundamental_data
                FROM '{manifest_file}'
                WITH CREDENTIALS AS 'aws_access_key_id={access_key};aws_secret_access_key={secret_access_key};token={session_token}'
                dateformat 'auto'
                timeformat 'auto'
                ignoreheader 1
                delimiter ','
                acceptinvchars
                blanksasnull
                emptyasnull
                manifest
                csv
                """.format(manifest_file= build_s3_url(*manifest_file),
                           access_key=credentials["Credentials"]["AccessKeyId"],
                           secret_access_key=credentials["Credentials"]["SecretAccessKey"],
                           session_token=credentials["Credentials"]["SessionToken"])
            )
        engine.dispose()

    def acquire_data(self, parallel=False):
        data_locations = self.acquire_and_store_quandl_data_parallel() if parallel \
            else self.acquire_and_store_quandl_data()
        manifest_file = self.create_manifest_file(data_locations)
        self.load_data_into_redshift(manifest_file)

if __name__=="__main__":
    temp = QuandlPullerAndFormatter()
    temp.acquire_data(True)

